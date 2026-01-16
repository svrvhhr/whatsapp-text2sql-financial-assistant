import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from fastapi import FastAPI
from contextlib import asynccontextmanager
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

ENV = os.getenv("ENV", "dev")

# If SELECT has no LIMIT:
# - if GUARD_AUTO_LIMIT=1 => add LIMIT GUARD_MAX_ROWS and allow
# - else => reject
GUARD_AUTO_LIMIT = os.getenv("GUARD_AUTO_LIMIT", "1") == "1"
GUARD_MAX_ROWS = int(os.getenv("GUARD_MAX_ROWS", "200"))

# Schema refresh
SCHEMA_REFRESH_SECONDS = int(os.getenv("SCHEMA_REFRESH_SECONDS", "60"))


# FinanceAdmin: all
# ProjectManager: SELECT + INSERT + UPDATE (no DELETE)
# ReadOnly: SELECT only
ROLE_POLICIES: Dict[str, Dict[str, Any]] = {
    "FinanceAdmin": {"ops": {"SELECT", "INSERT", "UPDATE", "DELETE"}},
    "ProjectManager": {"ops": {"SELECT", "INSERT", "UPDATE"}},
    "ReadOnly": {"ops": {"SELECT"}},
}


# =========================================================
# Models
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


class GuardResponse(BaseModel):
    allowed: bool
    reasons: List[str] = []
    normalized_sql: Optional[str] = None
    operation: str
    tables: List[str] = []
    risk_override: Optional[RiskInfo] = None


# =========================================================
# Schema cache
# =========================================================
_SCHEMA_CACHE: Dict[str, Any] = {}
_SCHEMA_LAST_LOAD_TS: Optional[float] = None


def _pg_connect():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        connect_timeout=5,
    )


def load_schema_mapping(retries: int = 20, sleep_s: float = 1.0) -> Dict[str, Any]:
    last_err = None
    for _ in range(retries):
        try:
            conn = _pg_connect()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
                """
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            schema: Dict[str, Dict[str, List[str]]] = {}
            for table_name, column_name in rows:
                schema.setdefault(table_name, {"columns": []})
                schema[table_name]["columns"].append(column_name)

            return schema
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)

    raise RuntimeError(f"Impossible de charger le schéma PostgreSQL: {last_err}")


def ensure_schema_loaded(force: bool = False) -> None:
    global _SCHEMA_CACHE, _SCHEMA_LAST_LOAD_TS
    now = time.time()

    if force or not _SCHEMA_CACHE:
        _SCHEMA_CACHE = load_schema_mapping()
        _SCHEMA_LAST_LOAD_TS = now
        return

    if SCHEMA_REFRESH_SECONDS > 0 and _SCHEMA_LAST_LOAD_TS is not None:
        if now - _SCHEMA_LAST_LOAD_TS >= SCHEMA_REFRESH_SECONDS:
            _SCHEMA_CACHE = load_schema_mapping()
            _SCHEMA_LAST_LOAD_TS = now


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema_loaded(force=True)
    yield


app = FastAPI(title="SQL Guard Service", version="1.0.0", lifespan=lifespan)


# =========================================================
# Guard rules
# =========================================================
_FORBIDDEN = [
    r"\bDROP\b",
    r"\bTRUNCATE\b",
    r"\bALTER\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCREATE\b",
]
_MULTI_STATEMENT = re.compile(r";\s*\S+", re.DOTALL)


def contains_forbidden(sql: str) -> bool:
    s = (sql or "")
    return any(re.search(pat, s, flags=re.IGNORECASE) for pat in _FORBIDDEN)


def seems_multi_statement(sql: str) -> bool:
    if not sql:
        return False
    stripped = sql.strip()
    if stripped.endswith(";"):
        stripped = stripped[:-1]
    return bool(_MULTI_STATEMENT.search(stripped))


def parse_sql_ast_strict(sql: str) -> exp.Expression:
    # strict parse (postgres dialect)
    return sqlglot.parse_one(sql, read="postgres")


def ast_operation(node: exp.Expression) -> str:
    if isinstance(node, exp.Select):
        return "SELECT"
    if isinstance(node, exp.Insert):
        return "INSERT"
    if isinstance(node, exp.Update):
        return "UPDATE"
    if isinstance(node, exp.Delete):
        return "DELETE"
    return "UNKNOWN"


def extract_tables_ast(node: exp.Expression) -> List[str]:
    tables = []
    for t in node.find_all(exp.Table):
        if t.name:
            tables.append(t.name)
    return sorted(set(tables))


def extract_columns_ast(node: exp.Expression) -> List[Tuple[Optional[str], str]]:
    cols = []
    for c in node.find_all(exp.Column):
        cols.append((c.table, c.name))
    return cols


def has_where_clause(node: exp.Expression) -> bool:
    # For Update/Delete: sqlglot uses exp.Where under node
    return node.args.get("where") is not None


def select_has_limit(node: exp.Expression) -> bool:
    if isinstance(node, exp.Select):
        return node.args.get("limit") is not None
    return False


def add_limit_to_select(node: exp.Expression, limit_n: int) -> exp.Expression:
    if not isinstance(node, exp.Select):
        return node
    if node.args.get("limit") is None:
        node.set("limit", exp.Limit(this=exp.Literal.number(limit_n)))
    return node


def rbac_allows(role: Optional[str], operation: str) -> bool:
    if not role:
        # no role => safest: only SELECT
        return operation == "SELECT"
    pol = ROLE_POLICIES.get(role)
    if not pol:
        # unknown role => safest: only SELECT
        return operation == "SELECT"
    return operation in pol["ops"]


def schema_allowlist_check(node: exp.Expression, schema: Dict[str, Any]) -> List[str]:
    """
    Strict allowlist:
    - All referenced tables must exist
    - Qualified columns must exist
    - Unqualified columns are not validated here (can be ambiguous)
    """
    reasons: List[str] = []
    tables = extract_tables_ast(node)

    for t in tables:
        if t not in schema:
            reasons.append(f"Table interdite/inconnue: {t}")

    schema_cols = {t: set(schema[t]["columns"]) for t in schema}

    for tbl, col in extract_columns_ast(node):
        if col == "*":
            continue
        if tbl:
            if tbl not in schema_cols:
                reasons.append(f"Table interdite/inconnue (col check): {tbl}")
            else:
                if col not in schema_cols[tbl]:
                    reasons.append(f"Colonne interdite/inconnue: {tbl}.{col}")

    return reasons


# =========================================================
# Endpoints
# =========================================================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "env": ENV,
        "db_host": POSTGRES_HOST,
        "schema_loaded": bool(_SCHEMA_CACHE),
        "schema_last_load_ts": _SCHEMA_LAST_LOAD_TS,
        "auto_limit": GUARD_AUTO_LIMIT,
        "max_rows": GUARD_MAX_ROWS,
        "roles": list(ROLE_POLICIES.keys()),
    }


@app.post("/check", response_model=GuardResponse)
def check(plan: SQLPlan):
    ensure_schema_loaded()

    reasons: List[str] = []
    normalized_sql: Optional[str] = None

    sql = (plan.sql or "").strip()
    if not sql:
        return GuardResponse(
            allowed=False,
            reasons=["SQL vide."],
            operation=plan.operation or "UNKNOWN",
            tables=[],
        )

    # Hard: forbid DDL/DCL
    if contains_forbidden(sql):
        reasons.append("Commande interdite détectée (DDL/DCL).")

    # Hard: no multi-statements
    if seems_multi_statement(sql):
        reasons.append("Multi-statements détectés (interdit).")

    # Parse strict AST
    node = None
    try:
        node = parse_sql_ast_strict(sql)
    except Exception as e:
        reasons.append(f"Parse AST strict échoué: {e}")

    # If parse ok: determine operation from AST
    op = plan.operation or "UNKNOWN"
    tables: List[str] = plan.tables or []
    if node is not None:
        op = ast_operation(node)
        tables = extract_tables_ast(node)

    # RBAC
    role = None
    try:
        role = (plan.context or {}).get("role")
    except Exception:
        role = None

    if not rbac_allows(role, op):
        reasons.append(f"RBAC: rôle '{role}' non autorisé pour opération '{op}'.")

    # Strict schema allowlist (tables/columns)
    if node is not None:
        reasons.extend(schema_allowlist_check(node, _SCHEMA_CACHE))

    # Rule: UPDATE/DELETE must have WHERE
    if node is not None and op in ("UPDATE", "DELETE"):
        if not has_where_clause(node):
            reasons.append(f"{op} sans WHERE (interdit).")

    # Rule: SELECT must have LIMIT (either add or reject)
    risk_override: Optional[RiskInfo] = None
    if node is not None and op == "SELECT":
        if not select_has_limit(node):
            if GUARD_AUTO_LIMIT:
                node = add_limit_to_select(node, GUARD_MAX_ROWS)
                normalized_sql = node.sql(dialect="postgres")
                # keep risk low, but add note
            else:
                reasons.append("SELECT sans LIMIT (interdit).")

    # Decide
    allowed = len(reasons) == 0

    # If not allowed -> enforce risk_override high + approval
    if not allowed:
        risk_override = RiskInfo(level="high", needs_approval=True)

    # If allowed but we normalized sql -> return normalized_sql
    if allowed and normalized_sql is None:
        normalized_sql = sql  # stable echo

    return GuardResponse(
        allowed=allowed,
        reasons=reasons,
        normalized_sql=normalized_sql,
        operation=op,
        tables=tables,
        risk_override=risk_override,
    )
