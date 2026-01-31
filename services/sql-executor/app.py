import os
import time
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import OperationalError, IntegrityError, DatabaseError
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sqlglot
from sqlglot import exp


# =========================================================
# ENV
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "orionis-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

SQL_GUARD_URL = os.getenv("SQL_GUARD_URL", "http://sql-guard:8000")

DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "5000"))
MAX_RETURN_ROWS = int(os.getenv("MAX_RETURN_ROWS", "200"))

ENV = os.getenv("ENV", "dev")

AUDIT_URL = os.getenv("AUDIT_URL", "http://audit:8000")
AUDIT_TIMEOUT_S = int(os.getenv("AUDIT_TIMEOUT_S", "5"))

# Retry when DB restarts
DB_RETRY_ON_OPERATIONAL_ERROR = os.getenv("DB_RETRY_ON_OPERATIONAL_ERROR", "1") == "1"
DB_RETRY_SLEEP_MS = int(os.getenv("DB_RETRY_SLEEP_MS", "300"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
logger = logging.getLogger("sql_executor")


# =========================================================
# Models (same SQLPlan contract)
# =========================================================
class RiskInfo(BaseModel):
    level: str
    needs_approval: bool


class StaticChecks(BaseModel):
    ast_parsed: bool = False
    ddl_blocked: bool = True
    schema_ok: bool = True
    single_statement: bool = True
    tenant_scoped: bool = True


class SQLPlan(BaseModel):
    request_id: str
    user_input: str
    operation: str
    sql: str
    params: List[Any]
    tables: List[str]
    risk: RiskInfo
    static_checks: StaticChecks
    context: Dict[str, Any]
    notes: List[str] = Field(default_factory=list)


class ExecuteResponse(BaseModel):
    status: str  # executed|refused|error
    request_id: str
    operation: str
    normalized_sql: Optional[str] = None
    reasons: List[str] = Field(default_factory=list)
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: Optional[int] = None
    affected_rows: Optional[int] = None
    duration_ms: Optional[int] = None


# =========================================================
# DB
# =========================================================
def _pg_connect():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        connect_timeout=5,
    )


def set_app_context(cur, plan: SQLPlan) -> None:
    """
    Inject context for DB triggers (audit/tracing) via set_config.
    Your DB triggers can read:
      current_setting('app.request_id', true), etc.
    """
    ctx = plan.context or {}
    cur.execute("SELECT set_config('app.request_id', %s, true);", (str(plan.request_id or ""),))
    cur.execute("SELECT set_config('app.actor_id', %s, true);", (str(ctx.get("actor_id") or ""),))
    cur.execute("SELECT set_config('app.role', %s, true);", (str(ctx.get("role") or ""),))
    cur.execute("SELECT set_config('app.entreprise_id', %s, true);", (str(ctx.get("entreprise_id") or ""),))
    cur.execute("SELECT set_config('app.projet_id', %s, true);", (str(ctx.get("projet_id") or ""),))
    cur.execute("SELECT set_config('app.confirmed', %s, true);", (str(ctx.get("confirmed") or ""),))


# =========================================================
# Audit emitter (external service OR stdout JSON)
# =========================================================
def emit_audit_event(event: Dict[str, Any]) -> None:
    """
    Best-effort:
    - If AUDIT_URL configured => POST /event
    - else log JSON to stdout
    """
    if AUDIT_URL:
        try:
            requests.post(
                f"{AUDIT_URL.rstrip('/')}/event",
                json=event,
                timeout=AUDIT_TIMEOUT_S,
            )
            return
        except Exception:
            pass

    logger.info(json.dumps({"event": "audit", **event}, ensure_ascii=False))


# =========================================================
# Guard call
# =========================================================
def guard_check(plan: SQLPlan) -> Tuple[bool, List[str], str, List[Any]]:
    """
    Returns:
      (allowed, reasons, normalized_sql, normalized_params)
    """
    try:
        r = requests.post(
            f"{SQL_GUARD_URL.rstrip('/')}/check",
            json=plan.model_dump(),
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        allowed = bool(data.get("allowed"))
        reasons = data.get("reasons") or []

        normalized_sql = (data.get("normalized_sql") or plan.sql or "").strip()
        normalized_params = data.get("normalized_params")
        if normalized_params is None:
            normalized_params = plan.params or []

        return allowed, reasons, normalized_sql, list(normalized_params)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erreur sql-guard (HTTP): {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erreur appel sql-guard: {e}")


# =========================================================
# SQL parsing helpers
# =========================================================
def parse_single_statement(sql: str) -> exp.Expression:
    statements = sqlglot.parse(sql, read="postgres")
    if len(statements) != 1:
        raise ValueError("Multi-statements interdits")
    return statements[0]


def detect_operation(sql: str) -> str:
    node = parse_single_statement(sql)
    if isinstance(node, exp.Select):
        return "SELECT"
    if isinstance(node, exp.Insert):
        return "INSERT"
    if isinstance(node, exp.Update):
        return "UPDATE"
    if isinstance(node, exp.Delete):
        return "DELETE"
    return "UNKNOWN"

def map_db_error(e: Exception) -> Tuple[int, Dict[str, Any], str]:
    """
    Retourne:
      - http_status
      - detail dict (propre côté client)
      - audit_status (tag court pour audit)
    """
    msg = str(e) or ""

    # ---- Budget dépassé (trigger check_budget_projet)
    if re.search(r"Depassement du budget du projet", msg, re.IGNORECASE):
        m = re.search(r"projet\s+(\d+)", msg, re.IGNORECASE)
        projet_id = int(m.group(1)) if m else None

        return 409, {
            "status": "rejected",
            "error_type": "BUDGET_EXCEEDED",
            "user_message": (
                "❌ Dépassement de budget.\n"
                "Cette dépense ne peut pas être enregistrée"
                + (f" pour le projet #{projet_id}." if projet_id else ".")
                + "\n👉 Réduis le montant, change de projet, ou augmente le budget."
            ),
            # On garde le détail technique pour debug, mais l’interface WhatsApp NE doit pas l’afficher
            "technical_message": msg,
        }, "business_rule"

    # ---- fallback DB
    return 400, {
        "status": "db_error",
        "error_type": "DB_ERROR",
        "user_message": "❌ Erreur lors de l’enregistrement. Modifie la demande et réessaie.",
        "technical_message": msg,
    }, "db_error"

# =========================================================
# FastAPI
# =========================================================
app = FastAPI(title="SQL Executor Service", version="2.1.0")


@app.get("/health")
def health():
    ok = True
    err = None
    try:
        conn = _pg_connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
    except Exception as e:
        ok = False
        err = str(e)

    return {
        "status": "ok" if ok else "error",
        "env": ENV,
        "db_host": POSTGRES_HOST,
        "sql_guard_url": SQL_GUARD_URL,
        "db_statement_timeout_ms": DB_STATEMENT_TIMEOUT_MS,
        "max_return_rows": MAX_RETURN_ROWS,
        "audit_url": AUDIT_URL or None,
        "db_error": err,
    }


@app.post("/execute", response_model=ExecuteResponse)
def execute(plan: SQLPlan):
    start = time.time()
    normalized_sql = plan.sql
    normalized_params: List[Any] = list(plan.params or [])

    # 1) Guard decision (final gate)
    allowed, reasons, normalized_sql, normalized_params = guard_check(plan)

    # petit check de “SQL cassé”
    if re.search(r"\bLIMIT\s*$", normalized_sql, re.I):
        allowed = False
        reasons = reasons + ["Executor: SQL invalide (LIMIT sans valeur)."]

    # 2) Detect operation from normalized SQL (anti mismatch)
    try:
        op_real = detect_operation(normalized_sql)
    except Exception as e:
        allowed = False
        reasons = reasons + [f"Executor: SQL non parsable: {e}"]
        op_real = (plan.operation or "UNKNOWN").upper()

    op_plan = (plan.operation or "UNKNOWN").upper()
    if allowed and op_real != op_plan and op_real != "UNKNOWN":
        allowed = False
        reasons = reasons + [f"Executor: opération incohérente (plan={op_plan} sql={op_real})."]

    # 3) If refused, audit + return
    if not allowed:
        duration_ms = int((time.time() - start) * 1000)
        emit_audit_event({
            "request_id": plan.request_id,
            "status": "refused",
            "operation": op_real,
            "sql": normalized_sql,
            "params": normalized_params,
            "reasons": reasons[:20],
            "duration_ms": duration_ms,
            "context": plan.context or {},
        })
        return ExecuteResponse(
            status="refused",
            request_id=plan.request_id,
            operation=op_real,
            normalized_sql=normalized_sql,
            reasons=reasons,
            duration_ms=duration_ms,
        )

    # 4) Execute with timeout + transaction
    def _run_once() -> ExecuteResponse:
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                # statement_timeout needs a transaction scope
                cur.execute("SET LOCAL statement_timeout = %s;", (DB_STATEMENT_TIMEOUT_MS,))

                # Inject context for DB triggers/audit
                set_app_context(cur, plan)

                cur.execute(normalized_sql, tuple(normalized_params))

                if op_real == "SELECT":
                    rows = cur.fetchmany(MAX_RETURN_ROWS)
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    row_count = len(rows)
                    duration_ms = int((time.time() - start) * 1000)

                    emit_audit_event({
                        "request_id": plan.request_id,
                        "status": "executed",
                        "operation": op_real,
                        "sql": normalized_sql,
                        "params": normalized_params,
                        "row_count": row_count,
                        "affected_rows": None,
                        "duration_ms": duration_ms,
                        "context": plan.context or {},
                    })

                    return ExecuteResponse(
                        status="executed",
                        request_id=plan.request_id,
                        operation=op_real,
                        normalized_sql=normalized_sql,
                        columns=columns,
                        rows=[list(r) for r in rows],
                        row_count=row_count,
                        duration_ms=duration_ms,
                    )

                affected_rows = cur.rowcount if cur.rowcount is not None else 0
                conn.commit()
                duration_ms = int((time.time() - start) * 1000)

                emit_audit_event({
                    "request_id": plan.request_id,
                    "status": "executed",
                    "operation": op_real,
                    "sql": normalized_sql,
                    "params": normalized_params,
                    "row_count": None,
                    "affected_rows": int(affected_rows),
                    "duration_ms": duration_ms,
                    "context": plan.context or {},
                })

                return ExecuteResponse(
                    status="executed",
                    request_id=plan.request_id,
                    operation=op_real,
                    normalized_sql=normalized_sql,
                    affected_rows=int(affected_rows),
                    duration_ms=duration_ms,
                )
        finally:
            try:
                conn.close()
            except Exception:
                pass

    try:
        try:
            return _run_once()
        except OperationalError as e:
            # DB restarted / connection reset
            if DB_RETRY_ON_OPERATIONAL_ERROR:
                time.sleep(DB_RETRY_SLEEP_MS / 1000.0)
                return _run_once()
            raise

    except OperationalError as e:
        duration_ms = int((time.time() - start) * 1000)
        logger.exception("DB unavailable (OperationalError)")
        emit_audit_event({
            "request_id": plan.request_id,
            "status": "db_unavailable",
            "operation": op_real,
            "sql": normalized_sql,
            "params": normalized_params,
            "reasons": [str(e)],
            "duration_ms": duration_ms,
            "context": plan.context or {},
        })
        raise HTTPException(
    status_code=503,
    detail={
        "status": "db_unavailable",
        "error_type": "DB_UNAVAILABLE",
        "user_message": "⚠️ Base de données indisponible. Réessaie dans quelques minutes.",
        "technical_message": str(e),
    },
)


    except IntegrityError as e:
        # NOT NULL / FK / UNIQUE / CHECK
        duration_ms = int((time.time() - start) * 1000)
        logger.exception("Constraint error (IntegrityError)")
        emit_audit_event({
            "request_id": plan.request_id,
            "status": "constraint_error",
            "operation": op_real,
            "sql": normalized_sql,
            "params": normalized_params,
            "reasons": [str(e)],
            "duration_ms": duration_ms,
            "context": plan.context or {},
        })
        raise HTTPException(
            status_code=409,
            detail={
                "status": "constraint_error",
                "error_type": "DB_CONSTRAINT",
                "user_message": "❌ Données invalides (contrainte). Vérifie le projet/compte/date/montant et réessaie.",
                "technical_message": str(e),
            },
        )

    except DatabaseError as e:
        duration_ms = int((time.time() - start) * 1000)
        logger.exception("Database error (DatabaseError)")

        http_status, detail, audit_status = map_db_error(e)

        emit_audit_event({
            "request_id": plan.request_id,
            "status": audit_status,          # <-- au lieu de db_error brut
            "operation": op_real,
            "sql": normalized_sql,
            "params": normalized_params,
            "reasons": [detail.get("technical_message", str(e))],
            "duration_ms": duration_ms,
            "context": plan.context or {},
            "error_type": detail.get("error_type"),
        })

        raise HTTPException(status_code=http_status, detail=detail)


    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        logger.exception("Internal error")
        emit_audit_event({
            "request_id": plan.request_id,
            "status": "internal_error",
            "operation": op_real,
            "sql": normalized_sql,
            "params": normalized_params,
            "reasons": [str(e)],
            "duration_ms": duration_ms,
            "context": plan.context or {},
        })
        raise HTTPException(
            status_code=500,
            detail={
                "status": "internal_error",
                "error_type": "INTERNAL",
                "user_message": "❌ Erreur interne. Réessaie ou contacte un administrateur.",
                "technical_message": str(e),
            },
        )
