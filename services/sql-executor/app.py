import os
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# =========================================================
# ENV
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

SQL_GUARD_URL = os.getenv("SQL_GUARD_URL", "http://sql-guard:8000")
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "5000"))
MAX_RETURN_ROWS = int(os.getenv("MAX_RETURN_ROWS", "200"))

ENV = os.getenv("ENV", "dev")


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
    notes: List[str] = []


class ExecuteResponse(BaseModel):
    status: str  # executed|refused|error
    request_id: str
    operation: str
    normalized_sql: Optional[str] = None
    reasons: List[str] = []
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


def ensure_audit_table(conn):
    # MVP: create audit table if not exists
    # (en prod tu feras une migration SQL)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_event (
              id SERIAL PRIMARY KEY,
              request_id TEXT,
              created_at TIMESTAMPTZ DEFAULT NOW(),
              actor_id TEXT,
              role TEXT,
              entreprise_id INT,
              projet_id INT,
              operation TEXT,
              sql TEXT,
              params JSONB,
              status TEXT,
              reasons TEXT,
              duration_ms INT,
              row_count INT,
              affected_rows INT
            );
            """
        )
        conn.commit()


def audit_write(conn, plan: SQLPlan, status: str, reasons: List[str], duration_ms: int, row_count: int, affected_rows: int):
    ctx = plan.context or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_event
              (request_id, actor_id, role, entreprise_id, projet_id,
               operation, sql, params, status, reasons, duration_ms, row_count, affected_rows)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s);
            """,
            (
                plan.request_id,
                ctx.get("actor_id"),
                ctx.get("role"),
                ctx.get("entreprise_id"),
                ctx.get("projet_id"),
                plan.operation,
                plan.sql,
                psycopg2.extras.Json(plan.params),
                status,
                "\n".join(reasons),
                duration_ms,
                row_count if row_count >= 0 else None,
                affected_rows if affected_rows >= 0 else None,
            ),
        )
        conn.commit()


# =========================================================
# Guard call
# =========================================================
def guard_check(plan: SQLPlan) -> Tuple[bool, List[str], str]:
    try:
        r = requests.post(f"{SQL_GUARD_URL.rstrip('/')}/check", json=plan.model_dump(), timeout=10)
        data = r.json()
        allowed = bool(data.get("allowed"))
        reasons = data.get("reasons") or []
        normalized_sql = data.get("normalized_sql") or plan.sql
        return allowed, reasons, normalized_sql
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erreur appel sql-guard: {e}")


# =========================================================
# FastAPI
# =========================================================
app = FastAPI(title="SQL Executor Service", version="1.0.0")


@app.get("/health")
def health():
    ok = True
    err = None
    try:
        conn = _pg_connect()
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
        "db_error": err,
    }


@app.post("/execute", response_model=ExecuteResponse)
def execute(plan: SQLPlan):
    # 1) Guard decision (final technical gate)
    allowed, reasons, normalized_sql = guard_check(plan)
    if not allowed:
        # audit refused (best effort)
        try:
            conn = _pg_connect()
            ensure_audit_table(conn)
            audit_write(conn, plan, "refused", reasons, duration_ms=0, row_count=-1, affected_rows=-1)
            conn.close()
        except Exception:
            pass

        return ExecuteResponse(
            status="refused",
            request_id=plan.request_id,
            operation=plan.operation,
            normalized_sql=normalized_sql,
            reasons=reasons,
        )

    # 2) Execute with timeout + transaction
    start = time.time()
    row_count = 0
    affected_rows = 0

    try:
        conn = _pg_connect()
        ensure_audit_table(conn)

        with conn.cursor() as cur:
            # Statement timeout for safety
            cur.execute("SET LOCAL statement_timeout = %s;", (DB_STATEMENT_TIMEOUT_MS,))
            cur.execute(normalized_sql, tuple(plan.params))

            op = plan.operation.upper()

            if op == "SELECT":
                rows = cur.fetchmany(MAX_RETURN_ROWS)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                row_count = len(rows)
                duration_ms = int((time.time() - start) * 1000)

                audit_write(conn, plan, "executed", [], duration_ms, row_count=row_count, affected_rows=-1)
                conn.close()

                return ExecuteResponse(
                    status="executed",
                    request_id=plan.request_id,
                    operation=op,
                    normalized_sql=normalized_sql,
                    columns=columns,
                    rows=[list(r) for r in rows],
                    row_count=row_count,
                    duration_ms=duration_ms,
                )

            else:
                # INSERT/UPDATE/DELETE
                affected_rows = cur.rowcount if cur.rowcount is not None else 0
                conn.commit()
                duration_ms = int((time.time() - start) * 1000)

                audit_write(conn, plan, "executed", [], duration_ms, row_count=-1, affected_rows=affected_rows)
                conn.close()

                return ExecuteResponse(
                    status="executed",
                    request_id=plan.request_id,
                    operation=op,
                    normalized_sql=normalized_sql,
                    affected_rows=affected_rows,
                    duration_ms=duration_ms,
                )

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        # Try audit error
        try:
            conn = _pg_connect()
            ensure_audit_table(conn)
            audit_write(conn, plan, "error", [str(e)], duration_ms, row_count=-1, affected_rows=-1)
            conn.close()
        except Exception:
            pass

        raise HTTPException(status_code=500, detail=f"Erreur exécution SQL: {e}")
