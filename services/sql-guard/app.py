import os
import re
import time
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import RealDictCursor
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

# Behavior
GUARD_AUTO_LIMIT = os.getenv("GUARD_AUTO_LIMIT", "1") == "1"
GUARD_MAX_ROWS = int(os.getenv("GUARD_MAX_ROWS", "200"))
SCHEMA_REFRESH_SECONDS = int(os.getenv("SCHEMA_REFRESH_SECONDS", "60"))

# Strictness
REQUIRE_TENANT_SCOPE = os.getenv("REQUIRE_TENANT_SCOPE", "1") == "1"
REQUIRE_USER_VERIFICATION = os.getenv("REQUIRE_USER_VERIFICATION", "1") == "1"
DENY_SELECT_STAR = os.getenv("DENY_SELECT_STAR", "1") == "1"

# Allowlist tables (default safe)
DEFAULT_ALLOWED_TABLES = [
    "entreprise",
    "bureau",
    "client",
    "projet",
    "compte_financier",
    "depense",
    "facture",
    "transfert_interne",
    "role",
    "utilisateur",
    "utilisateur_entreprise",
    "audit_event",
]
GUARD_ALLOWED_TABLES = [
    t.strip() for t in os.getenv("GUARD_ALLOWED_TABLES", "").split(",") if t.strip()
]
if not GUARD_ALLOWED_TABLES:
    GUARD_ALLOWED_TABLES = DEFAULT_ALLOWED_TABLES

# RBAC policies (role name -> allowed ops)
ROLE_POLICIES: Dict[str, Dict[str, Any]] = {
    "FinanceAdmin": {"ops": {"SELECT", "INSERT", "UPDATE", "DELETE"}},
    "Manager": {"ops": {"SELECT", "INSERT", "UPDATE"}},       # no DELETE
    "ProjectManager": {"ops": {"SELECT", "INSERT", "UPDATE"}},# if you use this role name
    "ReadOnly": {"ops": {"SELECT"}},
    "System": {"ops": {"SELECT", "INSERT", "UPDATE", "DELETE"}},
}

# Forbidden keywords and dangerous functions
_FORBIDDEN_KEYWORDS = ["DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CREATE"]
_FORBIDDEN_FUNCTIONS = {
    "pg_sleep",
    "pg_read_file",
    "pg_ls_dir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "dblink_connect",
    "dblink",
}

# =========================================================
# Logging
# =========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("sql_guard")
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")


def log_event(event: str, **fields):
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


# =========================================================
# Models (contract with Text2SQL)
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
    columns_ok: bool = True
    no_select_star: bool = True
    limit_ok: bool = True
    functions_ok: bool = True


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


class GuardResponse(BaseModel):
    allowed: bool
    reasons: List[str] = Field(default_factory=list)
    normalized_sql: Optional[str] = None
    normalized_params: Optional[List[Any]] = None
    operation: str
    tables: List[str] = Field(default_factory=list)
    risk_override: Optional[RiskInfo] = None

    # verification / audit
    verified_user: Optional[Dict[str, Any]] = None
    verified_role: Optional[str] = None
    verified_entreprise_id: Optional[int] = None


# =========================================================
# DB helpers
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


# =========================================================
# Schema cache (tables + columns)
# =========================================================
_SCHEMA_CACHE: Dict[str, Any] = {}
_SCHEMA_LAST_LOAD_TS: Optional[float] = None


def load_schema_mapping(retries: int = 30, sleep_s: float = 1.0) -> Dict[str, Any]:
    last_err = None
    for _ in range(retries):
        try:
            conn = _pg_connect()
            cur = conn.cursor(cursor_factory=RealDictCursor)
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

            schema: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                t = r["table_name"]
                c = r["column_name"]
                schema.setdefault(t, {"columns": []})
                schema[t]["columns"].append(c)

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


app = FastAPI(title="SQL Guard Service", version="2.0.0", lifespan=lifespan)


# =========================================================
# SQL parsing & AST helpers (strict)
# =========================================================
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s or "")

def contains_forbidden_keywords(sql: str) -> bool:
    return any(re.search(rf"\b{k}\b", sql or "", re.I) for k in _FORBIDDEN_KEYWORDS)


def parse_sql_single(sql: str) -> exp.Expression:
    statements = sqlglot.parse(sql, read="postgres")
    if len(statements) != 1:
        raise ValueError("Multi-statements interdits")
    return statements[0]


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


def extract_tables(node: exp.Expression) -> List[str]:
    return sorted({t.name for t in node.find_all(exp.Table) if t.name})


def extract_table_aliases(node: exp.Expression) -> Dict[str, str]:
    """
    alias -> table_name based on FROM/JOIN.
    """
    alias_map: Dict[str, str] = {}
    for table in node.find_all(exp.Table):
        if not table.name:
            continue
        alias = table.alias
        if alias and alias != table.name:
            alias_map[alias] = table.name
    return alias_map


def extract_columns(node: exp.Expression) -> List[Tuple[Optional[str], str]]:
    cols: List[Tuple[Optional[str], str]] = []
    for c in node.find_all(exp.Column):
        cols.append((c.table, c.name))
    return cols


def has_where_clause(node: exp.Expression) -> bool:
    return node.args.get("where") is not None

TOP_N_PATTERNS = [
    # EN: top 3 / last 5 / first 10
    r"\b(top|last|first)\s+(\d{1,3})\b",

    # FR: dernières 5 / premières 10
    r"\b(derni[eè]res?|premi[eè]res?)\s+(\d{1,3})\b",

    # FR: les 3 grandes / 3 plus grandes / 3 importantes / 3 plus importantes
    r"\b(\d{1,3})\s+(plus\s+)?(grandes?|grosses?|importantes?)\b",

    # FR: les 3 plus grandes dépenses / 3 plus grosses dépenses (avec nom après)
    r"\b(\d{1,3})\s+(plus\s+)?(grandes?|grosses?|importantes?)\s+\w+\b",

    # FR: 5 dernières / 10 premières (nombre avant)
    r"\b(\d{1,3})\s+derni[eè]res?\b",
    r"\b(\d{1,3})\s+premi[eè]res?\b",
]

def extract_requested_limit(user_input: str) -> Optional[int]:
    if not user_input:
        return None

    s = user_input.lower()

    for pattern in TOP_N_PATTERNS:
        m = re.search(pattern, s, flags=re.I)
        if not m:
            continue

        # On récupère le 1er groupe "numérique" trouvé
        for g in m.groups():
            if g and g.isdigit():
                n = int(g)
                # garde-fous
                if n <= 0:
                    return None
                return n

    return None


def select_has_limit(node: exp.Expression) -> bool:
    sel = node if isinstance(node, exp.Select) else node.find(exp.Select)
    if not sel:
        return False
    return sel.args.get("limit") is not None




def has_select_star(node: exp.Expression) -> bool:
    sel = node if isinstance(node, exp.Select) else node.find(exp.Select)
    if not sel:
        return False
    return any(True for _ in sel.find_all(exp.Star))


def extract_functions(node: exp.Expression) -> List[str]:
    funcs = set()
    for f in node.find_all(exp.Anonymous):
        if f.name:
            funcs.add(f.name.lower())
    for f in node.find_all(exp.Func):
        name = getattr(f, "sql_name", None)
        if name:
            funcs.add(str(name).lower())
    return sorted(funcs)


# =========================================================
# Static checks: allowlist tables/columns/functions
# =========================================================
def schema_allowlist_check(node: exp.Expression, schema: Dict[str, Any]) -> List[str]:
    """
    - All referenced tables must be allowlisted + exist in schema.
    - Qualified columns must exist (alias resolved).
    - Unqualified columns:
        - if only one table in query => validate against it
        - else warn only (ambiguous), do not hard fail
    """
    reasons: List[str] = []

    tables = extract_tables(node)
    alias_map = extract_table_aliases(node)
    schema_cols = {t: set(schema[t]["columns"]) for t in schema}

    # Tables
    for t in tables:
        if t not in GUARD_ALLOWED_TABLES:
            reasons.append(f"Table non autorisée: {t}")
        elif t not in schema:
            reasons.append(f"Table inconnue: {t}")

    # Columns
    cols = extract_columns(node)
    for tbl, col in cols:
        if col == "*" or not col:
            continue

        if tbl:
            real_tbl = alias_map.get(tbl, tbl)
            if real_tbl not in schema_cols:
                reasons.append(f"Table inconnue (col): {real_tbl}")
            else:
                if col not in schema_cols[real_tbl]:
                    reasons.append(f"Colonne inconnue: {real_tbl}.{col}")
        else:
            # Unqualified
            if len(tables) == 1:
                only_tbl = tables[0]
                if only_tbl in schema_cols and col not in schema_cols[only_tbl]:
                    reasons.append(f"Colonne inconnue: {only_tbl}.{col} (non qualifiée)")
            # else ambiguous: do not hard fail

    return reasons


def functions_policy_check(node: exp.Expression) -> List[str]:
    funcs = extract_functions(node)
    bad = [f for f in funcs if f in _FORBIDDEN_FUNCTIONS]
    if bad:
        return [f"Fonctions interdites: {', '.join(bad)}"]
    return []


# =========================================================
# User verification (Twilio From -> utilisateur)
# =========================================================
def get_user_by_actor(actor_id: str) -> Optional[Dict[str, Any]]:
    """
    actor_id expected format: whatsapp:+33....
    Tables:
      utilisateur(id, nom, numero_whatsapp, role_id)
      role(id, nom)
      utilisateur_entreprise(utilisateur_id, entreprise_id)
    """
    if not actor_id:
        return None

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.nom, u.numero_whatsapp, r.nom AS role
                FROM utilisateur u
                LEFT JOIN role r ON r.id = u.role_id
                WHERE u.numero_whatsapp = %s
                LIMIT 1;
                """,
                (actor_id,),
            )
            user = cur.fetchone()
            if not user:
                return None

            cur.execute(
                """
                SELECT entreprise_id
                FROM utilisateur_entreprise
                WHERE utilisateur_id = %s
                ORDER BY entreprise_id;
                """,
                (user["id"],),
            )
            ents = [row["entreprise_id"] for row in cur.fetchall()]
            user["entreprises"] = ents
            return user
    finally:
        conn.close()


def rbac_allows(role: Optional[str], operation: str) -> bool:
    # Default safest: SELECT only
    if not role:
        return operation == "SELECT"
    pol = ROLE_POLICIES.get(role)
    if not pol:
        return operation == "SELECT"
    return operation in pol["ops"]


# =========================================================
# Tenant scope enforcement check (guard-side)
# =========================================================
def ast_has_entreprise_filter(node: exp.Expression) -> bool:
    where = node.args.get("where")
    if not where:
        return False
    for col in where.find_all(exp.Column):
        if col.name and col.name.lower() == "entreprise_id":
            return True
    return False


# =========================================================
# Business validation (before execution) for sensitive writes
# =========================================================
def _extract_insert_target(node: exp.Expression) -> Tuple[Optional[str], List[str], List[exp.Expression]]:
    """
    Returns (table, columns, values_exprs)
    """
    if not isinstance(node, exp.Insert):
        return None, [], []

    table = None
    if isinstance(node.this, exp.Table) and node.this.name:
        table = node.this.name

    cols: List[str] = []
    if node.args.get("columns"):
        for c in node.args["columns"]:
            if isinstance(c, exp.Column) and c.name:
                cols.append(c.name)

    values_exprs: List[exp.Expression] = []
    val = node.args.get("expression")
    if isinstance(val, exp.Values) and val.expressions:
        # single row expected in our pipeline
        row = val.expressions[0]
        if isinstance(row, exp.Tuple):
            values_exprs = list(row.expressions)

    return table, cols, values_exprs


def _extract_update_target(node: exp.Expression) -> Tuple[Optional[str], Dict[str, exp.Expression]]:
    """
    Returns (table, set_map column->expression)
    """
    if not isinstance(node, exp.Update):
        return None, {}

    table = None
    if isinstance(node.this, exp.Table) and node.this.name:
        table = node.this.name

    set_map: Dict[str, exp.Expression] = {}
    for s in node.args.get("expressions") or []:
        if isinstance(s, exp.SetItem):
            left = s.this
            right = s.expression
            if isinstance(left, exp.Column) and left.name:
                set_map[left.name] = right
    return table, set_map


def _placeholders_to_param_indexes(exprs: List[exp.Expression]) -> List[Optional[int]]:
    """
    Our SQL uses psycopg2 %s placeholders. sqlglot typically represents them as exp.Placeholder.
    We map each placeholder occurrence to sequential param index.
    """
    idxs: List[Optional[int]] = []
    cur = 0
    for e in exprs:
        if isinstance(e, exp.Placeholder):
            idxs.append(cur)
            cur += 1
        else:
            # literal, function, etc.
            idxs.append(None)
    return idxs


def _get_param(params: List[Any], index: Optional[int]) -> Any:
    if index is None:
        return None
    if index < 0 or index >= len(params):
        return None
    return params[index]


def validate_depense_write(context: Dict[str, Any], cols: List[str], placeholder_idxs: List[Optional[int]], params: List[Any]) -> List[str]:
    """
    Business rules (before execution):
    - type_depense not null
    - montant > 0
    - date_depense not in the future
    - compte_id and projet_id belong to same entreprise_id
    - devise matches compte.devise (and optionally entreprise devise)
    - solde suffisant (pre-check)
    - budget projet (if column exists) not exceeded (pre-check)
    """
    reasons: List[str] = []
    entreprise_id = context.get("entreprise_id")

    col_to_idx = {c: placeholder_idxs[i] for i, c in enumerate(cols)}

    compte_id = _get_param(params, col_to_idx.get("compte_id"))
    projet_id = _get_param(params, col_to_idx.get("projet_id"))
    montant = _get_param(params, col_to_idx.get("montant"))
    devise = _get_param(params, col_to_idx.get("devise"))
    date_depense = _get_param(params, col_to_idx.get("date_depense"))
    type_depense = _get_param(params, col_to_idx.get("type_depense"))

    if type_depense is None:
        reasons.append("Validation métier: type_depense requis (non null).")

    try:
        if montant is None or float(montant) <= 0:
            reasons.append("Validation métier: montant doit être > 0.")
    except Exception:
        reasons.append("Validation métier: montant invalide.")

    # date_depense: accept None (DB default), else must not be future
    try:
        if date_depense is not None:
            # psycopg2 may pass date as string; compare via SQL in DB (simpler)
            pass
    except Exception:
        pass

    # DB checks
    if entreprise_id is None:
        reasons.append("Validation métier: entreprise_id manquant dans le contexte.")
        return reasons

    if compte_id is None or projet_id is None:
        reasons.append("Validation métier: compte_id et projet_id requis pour une dépense.")
        return reasons

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # compte
            cur.execute(
                "SELECT id, entreprise_id, devise, solde FROM compte_financier WHERE id = %s;",
                (compte_id,),
            )
            compte = cur.fetchone()
            if not compte:
                reasons.append("Validation métier: compte_id inexistant.")
                return reasons

            # projet
            cur.execute(
                "SELECT id, entreprise_id, nom"
                + (", budget" if "budget" in _SCHEMA_CACHE.get("projet", {}).get("columns", []) else "")
                + " FROM projet WHERE id = %s;",
                (projet_id,),
            )
            projet = cur.fetchone()
            if not projet:
                reasons.append("Validation métier: projet_id inexistant.")
                return reasons

            if compte["entreprise_id"] != entreprise_id or projet["entreprise_id"] != entreprise_id:
                reasons.append("Validation métier: projet/compte hors de l'entreprise demandée.")

            if devise is not None and str(devise) != str(compte["devise"]):
                reasons.append("Validation métier: devise de la dépense doit correspondre à la devise du compte.")

            # date_depense check in DB (not future)
            if date_depense is not None:
                cur.execute("SELECT (%s::date <= CURRENT_DATE) AS ok;", (date_depense,))
                ok = cur.fetchone()
                if ok and not ok["ok"]:
                    reasons.append("Validation métier: date_depense ne peut pas être dans le futur.")

            # solde suffisant (pre-check)
            try:
                if montant is not None and float(montant) > float(compte["solde"]):
                    reasons.append("Validation métier: solde insuffisant pour cette dépense.")
            except Exception:
                pass

            # budget (optional)
            if "budget" in (projet.keys() if projet else []):
                try:
                    budget = projet.get("budget")
                    if budget is not None and montant is not None:
                        cur.execute(
                            "SELECT COALESCE(SUM(montant),0) AS spent FROM depense WHERE projet_id = %s;",
                            (projet_id,),
                        )
                        spent = cur.fetchone()["spent"]
                        if float(spent) + float(montant) > float(budget):
                            reasons.append("Validation métier: budget projet dépassé (pré-check).")
                except Exception:
                    # never crash guard on optional
                    pass

    finally:
        conn.close()

    return reasons


def validate_transfert_write(context: Dict[str, Any], cols: List[str], placeholder_idxs: List[Optional[int]], params: List[Any]) -> List[str]:
    """
    Business rules (before execution):
    - montant > 0
    - comptes mêmes entreprise
    - devise match comptes
    - solde suffisant sur compte source
    - date_transfert not future (optional)
    """
    reasons: List[str] = []
    entreprise_id = context.get("entreprise_id")
    col_to_idx = {c: placeholder_idxs[i] for i, c in enumerate(cols)}

    from_id = _get_param(params, col_to_idx.get("compte_source_id") or col_to_idx.get("compte_debit_id") or col_to_idx.get("from_compte_id"))
    to_id = _get_param(params, col_to_idx.get("compte_destination_id") or col_to_idx.get("compte_credit_id") or col_to_idx.get("to_compte_id"))
    montant = _get_param(params, col_to_idx.get("montant"))
    devise = _get_param(params, col_to_idx.get("devise"))
    date_transfert = _get_param(params, col_to_idx.get("date_transfert"))

    try:
        if montant is None or float(montant) <= 0:
            reasons.append("Validation métier: montant transfert doit être > 0.")
    except Exception:
        reasons.append("Validation métier: montant transfert invalide.")

    if entreprise_id is None:
        reasons.append("Validation métier: entreprise_id manquant.")
        return reasons

    if from_id is None or to_id is None:
        reasons.append("Validation métier: comptes source/destination requis.")
        return reasons

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, entreprise_id, devise, solde FROM compte_financier WHERE id = %s;", (from_id,))
            c_from = cur.fetchone()
            cur.execute("SELECT id, entreprise_id, devise, solde FROM compte_financier WHERE id = %s;", (to_id,))
            c_to = cur.fetchone()

            if not c_from or not c_to:
                reasons.append("Validation métier: compte source/destination inexistant.")
                return reasons

            if c_from["entreprise_id"] != entreprise_id or c_to["entreprise_id"] != entreprise_id:
                reasons.append("Validation métier: transfert hors entreprise (comptes).")

            if devise is not None:
                if str(devise) != str(c_from["devise"]) or str(devise) != str(c_to["devise"]):
                    reasons.append("Validation métier: devise transfert doit correspondre aux devises des comptes.")

            try:
                if montant is not None and float(montant) > float(c_from["solde"]):
                    reasons.append("Validation métier: solde insuffisant sur le compte source.")
            except Exception:
                pass

            if date_transfert is not None:
                cur.execute("SELECT (%s::date <= CURRENT_DATE) AS ok;", (date_transfert,))
                ok = cur.fetchone()
                if ok and not ok["ok"]:
                    reasons.append("Validation métier: date_transfert ne peut pas être dans le futur.")

    finally:
        conn.close()

    return reasons


def validate_facture_write(context: Dict[str, Any], cols: List[str], placeholder_idxs: List[Optional[int]], params: List[Any]) -> List[str]:
    """
    Business rules:
    - montant > 0
    - projet_id + client_id exist
    - projet/client scoppés entreprise
    - devise cohérente (à minima entreprise devise ou projet devise si dispo)
    """
    reasons: List[str] = []
    entreprise_id = context.get("entreprise_id")
    col_to_idx = {c: placeholder_idxs[i] for i, c in enumerate(cols)}

    projet_id = _get_param(params, col_to_idx.get("projet_id"))
    client_id = _get_param(params, col_to_idx.get("client_id"))
    montant = _get_param(params, col_to_idx.get("montant"))
    devise = _get_param(params, col_to_idx.get("devise"))
    date_facture = _get_param(params, col_to_idx.get("date_facture"))

    try:
        if montant is None or float(montant) <= 0:
            reasons.append("Validation métier: montant facture doit être > 0.")
    except Exception:
        reasons.append("Validation métier: montant facture invalide.")

    if entreprise_id is None:
        reasons.append("Validation métier: entreprise_id manquant.")
        return reasons

    if projet_id is None or client_id is None:
        reasons.append("Validation métier: projet_id et client_id requis.")
        return reasons

    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, entreprise_id FROM projet WHERE id = %s;", (projet_id,))
            pr = cur.fetchone()
            cur.execute("SELECT id, entreprise_id FROM client WHERE id = %s;", (client_id,))
            cl = cur.fetchone()
            cur.execute("SELECT id, devise_principale FROM entreprise WHERE id = %s;", (entreprise_id,))
            ent = cur.fetchone()

            if not pr:
                reasons.append("Validation métier: projet_id inexistant.")
                return reasons
            if not cl:
                reasons.append("Validation métier: client_id inexistant.")
                return reasons
            if pr["entreprise_id"] != entreprise_id or cl["entreprise_id"] != entreprise_id:
                reasons.append("Validation métier: projet/client hors entreprise.")

            if ent and devise is not None and str(devise) != str(ent["devise_principale"]):
                # si ton modèle autorise multi-devise par facture, retire cette règle
                reasons.append("Validation métier: devise facture doit correspondre à la devise principale de l'entreprise.")

            if date_facture is not None:
                cur.execute("SELECT (%s::date <= CURRENT_DATE) AS ok;", (date_facture,))
                ok = cur.fetchone()
                if ok and not ok["ok"]:
                    reasons.append("Validation métier: date_facture ne peut pas être dans le futur.")
    finally:
        conn.close()

    return reasons

LIMIT_PRESENT_RE = re.compile(r"\bLIMIT\b", re.I)

def inject_limit_safe(sql: str, limit: int) -> str:
    s = (sql or "").strip()

    # Déjà un LIMIT -> ne touche pas
    if LIMIT_PRESENT_RE.search(s):
        return s

    # Enlever ; final si présent
    has_semicolon = s.endswith(";")
    if has_semicolon:
        s = s[:-1].rstrip()

    s = f"{s} LIMIT {int(limit)}"
    if has_semicolon:
        s += ";"
    return s

# =========================================================
# Guard endpoint
# =========================================================
@app.get("/health")
def health():
    ensure_schema_loaded()
    return {
        "status": "ok",
        "env": ENV,
        "db_host": POSTGRES_HOST,
        "schema_loaded": bool(_SCHEMA_CACHE),
        "schema_last_load_ts": _SCHEMA_LAST_LOAD_TS,
        "auto_limit": GUARD_AUTO_LIMIT,
        "max_rows": GUARD_MAX_ROWS,
        "allowed_tables": GUARD_ALLOWED_TABLES,
        "roles": list(ROLE_POLICIES.keys()),
    }


@app.post("/check", response_model=GuardResponse)
def check(plan: SQLPlan):
    ensure_schema_loaded()

    started = time.time()
    reasons: List[str] = []
    normalized_sql: Optional[str] = None
    normalized_params: Optional[List[Any]] = None

    ctx = plan.context or {}
    verified_user = None
    verified_role = None
    verified_entreprise_id = ctx.get("entreprise_id")

    request_id = plan.request_id or "n/a"
    sql = (plan.sql or "").strip()
    sql = strip_ansi(sql)
    params = list(plan.params or [])

    # print("SQL_RECV:", repr(sql)) 

    if re.search(r"\bLIMIT\s*$", sql, re.I):
        return GuardResponse(
            allowed=False,
            reasons=["SQL invalide: LIMIT sans valeur."],
            operation=plan.operation or "UNKNOWN",
            tables=[],
            risk_override=RiskInfo(level="high", needs_approval=True),
            verified_user=verified_user,
            verified_role=verified_role,
            verified_entreprise_id=verified_entreprise_id,
        )

    
    # -------------------------
    # (A) User verification
    # -------------------------
    actor_id = ctx.get("actor_id")
    entreprise_id = ctx.get("entreprise_id")

    verified_user = None
    verified_role = None
    verified_entreprise_id = entreprise_id

    # On ne fait PAS confiance au rôle venant du gateway (ctx["role"])
    role_to_use = None

    if REQUIRE_USER_VERIFICATION:
        if not actor_id:
            reasons.append("User verification: actor_id manquant (From Twilio).")
        else:
            verified_user = get_user_by_actor(actor_id)
            if not verified_user:
                reasons.append("User verification: utilisateur inconnu (numero_whatsapp).")
            else:
                verified_role = verified_user.get("role")
                role_to_use = verified_role

                allowed_ents = verified_user.get("entreprises", [])

                # Si entreprise_id fourni, il doit appartenir à l'utilisateur
                if entreprise_id is not None and entreprise_id not in allowed_ents:
                    reasons.append("Tenant scope: entreprise_id non autorisée pour cet utilisateur.")

                # Déduction SAFE : uniquement si 1 seule entreprise
                if entreprise_id is None:
                    if len(allowed_ents) == 1:
                        entreprise_id = allowed_ents[0]
                        ctx["entreprise_id"] = entreprise_id
                        verified_entreprise_id = entreprise_id
                    else:
                        reasons.append("Tenant scope: entreprise_id obligatoire (utilisateur multi-entreprises).")

    # Exiger le scope tenant si activé
    if REQUIRE_TENANT_SCOPE and entreprise_id is None:
        reasons.append("Tenant scope: entreprise_id obligatoire (manquant).")

    # Si on n'a pas d'utilisateur ou pas de rôle -> on stoppe
    if REQUIRE_USER_VERIFICATION and (not verified_user or not verified_role):
        return GuardResponse(
            allowed=False,
            reasons=reasons or ["User verification: utilisateur non autorisé."],
            operation=plan.operation or "UNKNOWN",
            tables=[],
            risk_override=RiskInfo(level="high", needs_approval=True),
            verified_user=verified_user,
            verified_role=verified_role,
            verified_entreprise_id=verified_entreprise_id,
        )

    # -------------------------
    # (B) Basic SQL presence + keyword safety
    # -------------------------
    if not sql:
        return GuardResponse(
            allowed=False,
            reasons=["SQL vide."],
            operation=plan.operation or "UNKNOWN",
            tables=[],
            risk_override=RiskInfo(level="high", needs_approval=True),
            verified_user=verified_user,
            verified_role=verified_role,
            verified_entreprise_id=verified_entreprise_id,
        )

    if contains_forbidden_keywords(sql):
        reasons.append("Commande interdite détectée (DDL/DCL).")

    # -------------------------
    # (C) Parse strict single statement
    # -------------------------
    node: Optional[exp.Expression] = None
    try:
        node = parse_sql_single(sql)
    except Exception as e:
        reasons.append(f"Parse AST strict échoué: {e}")

    op = plan.operation or "UNKNOWN"
    tables: List[str] = plan.tables or []

    if node is not None:
        op = ast_operation(node)
        tables = extract_tables(node)

    # -------------------------
    # (D) RBAC
    # -------------------------
    if not rbac_allows(role_to_use, op):
        reasons.append(f"RBAC: rôle '{role_to_use}' non autorisé pour opération '{op}'.")

    # -------------------------
    # (E) Static allowlist: tables/columns/functions
    # -------------------------
    if node is not None:
        reasons.extend(schema_allowlist_check(node, _SCHEMA_CACHE))
        reasons.extend(functions_policy_check(node))

        if op in ("UPDATE", "DELETE") and not has_where_clause(node):
            reasons.append(f"{op} sans WHERE (interdit).")

        if op == "SELECT" and not select_has_limit(node):
            if GUARD_AUTO_LIMIT:
                requested = extract_requested_limit(plan.user_input)
                limit = GUARD_MAX_ROWS if requested is None else min(requested, GUARD_MAX_ROWS)

                sql = inject_limit_safe(sql, limit)        # ✅ remplace sql
                try:
                    node = parse_sql_single(sql)           # ✅ remplace node
                    op = ast_operation(node)
                    tables = extract_tables(node)
                except Exception as e:
                    reasons.append(f"SQL invalide après LIMIT auto: {e}")

            
    

    # -------------------------
    # (F) Tenant scope enforcement (guard-side)
    # -------------------------
    if entreprise_id is not None and node is not None:
        if node is not None and op in ("SELECT", "UPDATE", "DELETE"):
            if REQUIRE_TENANT_SCOPE and not ast_has_entreprise_filter(node):
                reasons.append("Tenant scope: filtre entreprise_id manquant dans WHERE.")


        elif op == "INSERT":
            # INSERT: on ne demande pas de WHERE, on s'appuie sur la validation métier.
            # Donc rien ici.
            pass


    # -------------------------
    # (G) Business validation pre-execution
    #     Only if still parsable and only for writes
    # -------------------------
    if node is not None and op in ("INSERT", "UPDATE"):
        # INSERT checks
        if isinstance(node, exp.Insert):
            target_table, cols, values_exprs = _extract_insert_target(node)
            if target_table in ("depense", "transfert_interne", "facture"):
                placeholder_idxs = _placeholders_to_param_indexes(values_exprs)

                if target_table == "depense":
                    reasons.extend(validate_depense_write(ctx, cols, placeholder_idxs, params))
                elif target_table == "transfert_interne":
                    reasons.extend(validate_transfert_write(ctx, cols, placeholder_idxs, params))
                elif target_table == "facture":
                    reasons.extend(validate_facture_write(ctx, cols, placeholder_idxs, params))

        # UPDATE checks (limited, still useful)
        if isinstance(node, exp.Update):
            target_table, set_map = _extract_update_target(node)
            # For professional safety, require WHERE already handled above.
            # You can add deeper validations later when you standardize update templates.
            if target_table in ("depense", "transfert_interne", "facture"):
                # Optional: mark as needs_approval unless you implement full mapping
                reasons.append(f"Validation métier: UPDATE sur {target_table} nécessite validation avancée (mettre en approval).")

    # -------------------------
    # Decision
    # -------------------------
    allowed = len(reasons) == 0

    risk_override: Optional[RiskInfo] = None
    if not allowed:
        risk_override = RiskInfo(level="high", needs_approval=True)

    if allowed:
        normalized_sql = normalized_sql or sql
        normalized_params = normalized_params or params

    duration_ms = int((time.time() - started) * 1000)

    print("SQL_RECV:", repr(sql))
    print("USER_INPUT:", repr(plan.user_input))
    print("CTX:", {k: ctx.get(k) for k in ["entreprise_id","actor_id","role"]})

    log_event(
        "guard_check",
        request_id=request_id,
        allowed=allowed,
        operation=op,
        tables=tables,
        role=role_to_use,
        entreprise_id=entreprise_id,
        duration_ms=duration_ms,
        reasons=reasons,
        reasons_count=len(reasons),
    )

    return GuardResponse(
        allowed=allowed,
        reasons=reasons,
        normalized_sql=normalized_sql if allowed else None,
        normalized_params=normalized_params if allowed else None,
        operation=op,
        tables=tables,
        risk_override=risk_override,
        verified_user=verified_user,
        verified_role=verified_role,
        verified_entreprise_id=verified_entreprise_id,
    )
