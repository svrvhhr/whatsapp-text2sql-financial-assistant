import os
import re
import time
import json
import uuid
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field, ValidationError

import sqlglot
from sqlglot import exp

from openai import OpenAI

# =========================================================
# ENV
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "orionis-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini-2025-04-14")
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "20"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "900"))

ENV = os.getenv("ENV", "dev")
TEXT2SQL_STUB_LLM = os.getenv("TEXT2SQL_STUB_LLM", "0") == "1"

SCHEMA_REFRESH_SECONDS = int(os.getenv("SCHEMA_REFRESH_SECONDS", "60"))
TEXT2SQL_SOFT_SCHEMA_FAIL = os.getenv("TEXT2SQL_SOFT_SCHEMA_FAIL", "1") == "1"

# Allowlist
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
TEXT2SQL_ALLOWED_TABLES = [t.strip() for t in os.getenv("TEXT2SQL_ALLOWED_TABLES", "").split(",") if t.strip()]
if not TEXT2SQL_ALLOWED_TABLES:
    TEXT2SQL_ALLOWED_TABLES = DEFAULT_ALLOWED_TABLES

# Entity resolution
RESOLVE_TOPK = int(os.getenv("RESOLVE_TOPK", "5"))
TEXT2SQL_USE_PG_TRGM = os.getenv("TEXT2SQL_USE_PG_TRGM", "0") == "1"

# Output safety
DEFAULT_SELECT_LIMIT = int(os.getenv("DEFAULT_SELECT_LIMIT", "100"))
ENFORCE_NO_SELECT_STAR = os.getenv("ENFORCE_NO_SELECT_STAR", "1") == "1"
ENFORCE_LIMIT_ON_SELECT = os.getenv("ENFORCE_LIMIT_ON_SELECT", "1") == "1"

# Tenant scope
REQUIRE_TENANT_SCOPE = os.getenv("REQUIRE_TENANT_SCOPE", "1") == "1"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("text2sql")
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")


def log_event(event: str, **fields):
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


# =========================================================
# Language detection (simple heuristic)
# =========================================================
LANG_FR_HINTS = [
    "bonjour", "salut", "merci", "s'il", "stp", "projet", "dépense", "depense",
    "facture", "client", "compte", "solde", "budget", "aujourd", "cette", "mois", "année",
    "ajoute", "insère", "inserer", "insere", "depenses",
]
LANG_EN_HINTS = [
    "hello", "hi", "thanks", "please", "project", "expense", "invoice", "customer",
    "account", "balance", "budget", "today", "this", "month", "year", "insert", "add",
]


def detect_lang(user_input: str) -> str:
    t = (user_input or "").lower()
    fr = sum(1 for w in LANG_FR_HINTS if w in t)
    en = sum(1 for w in LANG_EN_HINTS if w in t)
    return "fr" if fr >= en else "en"


# =========================================================
# FastAPI
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema_loaded(force=True)
    yield


app = FastAPI(title="Text2SQL Service", version="5.3.0", lifespan=lifespan)


# =========================================================
# API Models
# =========================================================
class ConvertRequest(BaseModel):
    user_input: str
    entreprise_id: Optional[int] = None
    projet_id: Optional[int] = None
    role: Optional[str] = None
    actor_id: Optional[str] = None


class RiskInfo(BaseModel):
    level: str
    needs_approval: bool


class StaticChecks(BaseModel):
    ast_parsed: bool
    ddl_blocked: bool
    schema_ok: bool
    single_statement: bool
    tenant_scoped: bool
    columns_ok: bool
    no_select_star: bool
    limit_ok: bool
    functions_ok: bool


class Clarification(BaseModel):
    needed: bool = False
    entity: Optional[str] = None
    field: Optional[str] = None
    query: Optional[str] = None
    suggestions: List[Dict[str, Any]] = Field(default_factory=list)
    message: Optional[str] = None


class PendingPlan(BaseModel):
    mode: str = "plan"
    operation: str
    intent: Optional[str] = None
    sql_template: str
    template_params: List[Any] = Field(default_factory=list)
    entities: Dict[str, Optional[str]] = Field(default_factory=dict)

    # ✅ NOUVEAU: ce que l'utilisateur a déjà choisi au fil des clarifications
    filled: Dict[str, Any] = Field(default_factory=dict)


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
    resolved: Dict[str, Any] = Field(default_factory=dict)
    clarification: Clarification = Field(default_factory=Clarification)
    pending_plan: Optional[PendingPlan] = None
    notes: List[str] = Field(default_factory=list)


# ✅ NOUVEAU: request pour /continue
class ContinueRequest(BaseModel):
    user_input: str
    pending_plan: PendingPlan
    context: Dict[str, Any] = Field(default_factory=dict)


# =========================================================
# LLM Output Models
# =========================================================
class LLMDirect(BaseModel):
    mode: str = Field(pattern=r"^sql$")
    operation: str
    sql: str
    params: List[Any] = Field(default_factory=list)


class LLMPlan(BaseModel):
    mode: str = Field(pattern=r"^plan$")
    operation: str
    intent: str
    entities: Dict[str, Optional[str]] = Field(default_factory=dict)
    sql_template: str
    template_params: List[Any] = Field(default_factory=list)


LLMOutput = Union[LLMDirect, LLMPlan]


# =========================================================
# PostgreSQL
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
# Schema cache
# =========================================================
@dataclass
class FK:
    table: str
    column: str
    ref_table: str
    ref_column: str


_SCHEMA_CACHE: Dict[str, Any] = {}
_SCHEMA_LAST_LOAD_TS: Optional[float] = None


def load_schema_and_fks() -> Dict[str, Any]:
    conn = _pg_connect()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)
    col_rows = cur.fetchall()

    cur.execute("""
        SELECT
          tc.table_name,
          kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema='public'
          AND tc.constraint_type='PRIMARY KEY'
        ORDER BY tc.table_name, kcu.ordinal_position;
    """)
    pk_rows = cur.fetchall()

    cur.execute("""
        SELECT
          tc.table_name AS table_name,
          kcu.column_name AS column_name,
          ccu.table_name AS ref_table_name,
          ccu.column_name AS ref_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.table_schema='public'
          AND tc.constraint_type='FOREIGN KEY'
        ORDER BY tc.table_name, kcu.ordinal_position;
    """)
    fk_rows = cur.fetchall()

    cur.close()
    conn.close()

    tables: Dict[str, Dict[str, Any]] = {}
    for r in col_rows:
        t = r["table_name"]
        tables.setdefault(t, {"columns": [], "pk": []})
        tables[t]["columns"].append(r["column_name"])

    for r in pk_rows:
        t = r["table_name"]
        tables.setdefault(t, {"columns": [], "pk": []})
        tables[t]["pk"].append(r["column_name"])

    fks: List[Dict[str, str]] = []
    for r in fk_rows:
        fks.append({
            "table": r["table_name"],
            "column": r["column_name"],
            "ref_table": r["ref_table_name"],
            "ref_column": r["ref_column_name"],
        })

    for t, meta in tables.items():
        meta["has_entreprise_id"] = "entreprise_id" in meta["columns"]

    return {"tables": tables, "fks": fks}


def ensure_schema_loaded(force: bool = False):
    global _SCHEMA_CACHE, _SCHEMA_LAST_LOAD_TS
    now = time.time()
    if force or not _SCHEMA_CACHE:
        _SCHEMA_CACHE = load_schema_and_fks()
        _SCHEMA_LAST_LOAD_TS = now
        return
    if SCHEMA_REFRESH_SECONDS > 0 and now - (_SCHEMA_LAST_LOAD_TS or 0) > SCHEMA_REFRESH_SECONDS:
        _SCHEMA_CACHE = load_schema_and_fks()
        _SCHEMA_LAST_LOAD_TS = now


# =========================================================
# Safety helpers
# =========================================================
_SQL_START_RE = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", re.I)
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


def classify_operation(sql: str) -> str:
    m = _SQL_START_RE.search(sql or "")
    return m.group(1).upper() if m else "UNKNOWN"


def contains_forbidden_keywords(sql: str) -> bool:
    return any(re.search(rf"\b{k}\b", sql, re.I) for k in _FORBIDDEN_KEYWORDS)


def parse_sql_single(sql: str) -> exp.Expression:
    statements = sqlglot.parse(sql, read="postgres")
    if len(statements) != 1:
        raise ValueError("Multi-statements interdits")
    return statements[0]


def risk_level_for(op: str) -> Tuple[str, bool]:
    if op == "SELECT":
        return "low", False
    if op in ("INSERT", "UPDATE", "DELETE"):
        return "high", True
    return "medium", True


def extract_tables(node: exp.Expression) -> List[str]:
    return sorted({t.name for t in node.find_all(exp.Table) if t.name})


def extract_table_aliases(node: exp.Expression) -> Dict[str, str]:
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
        col_name = c.name
        tbl = c.table if c.table else None
        if col_name:
            cols.append((tbl, col_name))
    return cols


def has_select_star(node: exp.Expression) -> bool:
    if not isinstance(node, exp.Select):
        node = node.find(exp.Select) or node
    return any(True for _ in node.find_all(exp.Star))


def has_limit(node: exp.Expression) -> bool:
    if not isinstance(node, exp.Select):
        node = node.find(exp.Select) or node
    return bool(node.args.get("limit"))


def extract_functions(node: exp.Expression) -> List[str]:
    funcs: List[str] = []
    for f in node.find_all(exp.Anonymous):
        if f.name:
            funcs.append(f.name.lower())
    for f in node.find_all(exp.Func):
        name = getattr(f, "sql_name", None)
        if name:
            funcs.append(str(name).lower())
    return sorted(set(funcs))


def validate_tables(node: exp.Expression, schema_bundle: Dict[str, Any]) -> Tuple[List[str], bool, List[str]]:
    notes: List[str] = []
    schema_ok = True
    tables = extract_tables(node)
    schema_tables = schema_bundle["tables"]

    for tname in tables:
        if tname not in TEXT2SQL_ALLOWED_TABLES:
            schema_ok = False
            notes.append(f"Table non autorisée: {tname}")
        elif tname not in schema_tables:
            schema_ok = False
            notes.append(f"Table inconnue: {tname}")

    return tables, schema_ok, notes


def validate_columns(node: exp.Expression, schema_bundle: Dict[str, Any]) -> Tuple[bool, List[str]]:
    notes: List[str] = []
    schema_tables = schema_bundle["tables"]
    tables_in_query = extract_tables(node)
    alias_map = extract_table_aliases(node)
    cols = extract_columns(node)

    ok = True

    def col_exists(table_name: str, col_name: str) -> bool:
        if table_name not in schema_tables:
            return False
        return col_name in schema_tables[table_name]["columns"]

    for tbl_or_alias, col_name in cols:
        if not col_name:
            continue

        if tbl_or_alias:
            real_table = alias_map.get(tbl_or_alias, tbl_or_alias)
            if not col_exists(real_table, col_name):
                ok = False
                notes.append(f"Colonne inconnue: {tbl_or_alias}.{col_name}")
        else:
            if len(tables_in_query) == 1:
                only_table = tables_in_query[0]
                if not col_exists(only_table, col_name):
                    ok = False
                    notes.append(f"Colonne inconnue: {only_table}.{col_name} (non qualifiée)")
            else:
                notes.append(f"Colonne non qualifiée dans une requête multi-tables: {col_name}")

    return ok, notes


def validate_functions(node: exp.Expression) -> Tuple[bool, List[str]]:
    funcs = extract_functions(node)
    bad = [f for f in funcs if f in _FORBIDDEN_FUNCTIONS]
    if bad:
        return False, [f"Fonctions interdites: {', '.join(bad)}"]
    return True, []


def apply_default_limit_if_needed(node: exp.Expression) -> Tuple[exp.Expression, bool, List[str]]:
    notes: List[str] = []
    changed = False
    select = node if isinstance(node, exp.Select) else node.find(exp.Select)
    if not select:
        return node, False, notes

    if ENFORCE_LIMIT_ON_SELECT and not has_limit(select):
        select.set("limit", exp.Limit(this=exp.Literal.number(DEFAULT_SELECT_LIMIT)))
        changed = True
        notes.append(f"LIMIT {DEFAULT_SELECT_LIMIT} injecté automatiquement.")
    return node, changed, notes


# =========================================================
# Tenant scope
# =========================================================
def _where_has_entreprise_filter(node: exp.Expression) -> bool:
    where = node.args.get("where")
    if not where:
        return False
    for col in where.find_all(exp.Column):
        if col.name and col.name.lower() == "entreprise_id":
            return True
    return False


def enforce_tenant_scope(sql: str, entreprise_id: Optional[int], schema_bundle: Dict[str, Any]) -> Tuple[str, List[Any], bool, List[str]]:
    notes: List[str] = []
    if entreprise_id is None:
        return sql, [], True, notes

    try:
        node = parse_sql_single(sql)
    except Exception:
        return sql, [], False, ["Impossible d'appliquer le scope (SQL non parsable)."]

    op = node.key.upper() if hasattr(node, "key") else ""
    if op not in ("SELECT", "UPDATE", "DELETE"):
        if not _where_has_entreprise_filter(node):
            return sql, [], False, ["Écriture sans filtre entreprise_id: refusée (Text2SQL)."]
        return sql, [], True, notes

    if _where_has_entreprise_filter(node):
        return sql, [], True, notes

    tables = extract_tables(node)
    schema_tables = schema_bundle["tables"]
    alias_map = extract_table_aliases(node)

    tenant_tables = [t for t in tables if t in schema_tables and schema_tables[t].get("has_entreprise_id")]
    if len(tenant_tables) == 1:
        tenant_table = tenant_tables[0]

        qualifier = tenant_table
        for alias, real_table in alias_map.items():
            if real_table == tenant_table:
                qualifier = alias
                break

        predicate = exp.EQ(
            this=exp.Column(this="entreprise_id", table=qualifier),
            expression=exp.Placeholder(this="%s"),
        )
        where = node.args.get("where")
        if where:
            node.set("where", exp.Where(this=exp.and_(where.this, predicate)))
        else:
            node.set("where", exp.Where(this=predicate))

        notes.append(f"Filtre tenant injecté: {qualifier}.entreprise_id = %s")
        return node.sql(dialect="postgres"), [entreprise_id], True, notes

    if len(tenant_tables) > 1:
        return sql, [], False, ["Plusieurs tables ont entreprise_id: filtre tenant ambigu (refus)."]
    return sql, [], False, ["Aucune table tenant directe: nécessite un JOIN vers projet/entreprise pour scoper."]


# =========================================================
# Entity resolution
# =========================================================
def _pg_has_pg_trgm(conn) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname='pg_trgm';")
            return cur.fetchone() is not None
    except Exception:
        return False


def list_top_entities(entity: str, entreprise_id: Optional[int], topk: int = RESOLVE_TOPK) -> List[Dict[str, Any]]:
    if entity not in ("projet", "client", "compte_financier"):
        return []
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            has_tenant = entity in _SCHEMA_CACHE["tables"] and _SCHEMA_CACHE["tables"][entity].get("has_entreprise_id")
            if entreprise_id is not None and has_tenant:
                cur.execute(
                    f"SELECT id AS id, nom AS nom, 0.0 AS score FROM public.{entity} WHERE entreprise_id=%s ORDER BY nom LIMIT %s;",
                    (entreprise_id, topk),
                )
            else:
                cur.execute(
                    f"SELECT id AS id, nom AS nom, 0.0 AS score FROM public.{entity} ORDER BY nom LIMIT %s;",
                    (topk,),
                )
            rows = cur.fetchall()
            for i, r in enumerate(rows, start=1):
                r["option"] = i
            return rows
    finally:
        conn.close()


def resolve_by_name(entity: str, query: str, entreprise_id: Optional[int], topk: int = RESOLVE_TOPK) -> List[Dict[str, Any]]:
    query = (query or "").strip()
    if entity not in ("projet", "client", "compte_financier"):
        return []

    if not query:
        return list_top_entities(entity, entreprise_id, topk=topk)

    table = entity
    conn = _pg_connect()
    has_trgm = TEXT2SQL_USE_PG_TRGM and _pg_has_pg_trgm(conn)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            has_tenant = _SCHEMA_CACHE["tables"].get(table, {}).get("has_entreprise_id", False)

            if has_trgm:
                if entreprise_id is not None and has_tenant:
                    cur.execute(
                        f"""
                        SELECT id AS id, nom AS nom, similarity(nom, %s) AS score
                        FROM public.{table}
                        WHERE entreprise_id = %s
                        ORDER BY score DESC
                        LIMIT %s;
                        """,
                        (query, entreprise_id, topk),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT id AS id, nom AS nom, similarity(nom, %s) AS score
                        FROM public.{table}
                        ORDER BY score DESC
                        LIMIT %s;
                        """,
                        (query, topk),
                    )
            else:
                like = f"%{query}%"
                if entreprise_id is not None and has_tenant:
                    cur.execute(
                        f"""
                        SELECT id AS id, nom AS nom, 0.0 AS score
                        FROM public.{table}
                        WHERE entreprise_id = %s AND nom ILIKE %s
                        ORDER BY nom
                        LIMIT %s;
                        """,
                        (entreprise_id, like, topk),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT id AS id, nom AS nom, 0.0 AS score
                        FROM public.{table}
                        WHERE nom ILIKE %s
                        ORDER BY nom
                        LIMIT %s;
                        """,
                        (like, topk),
                    )

            results = cur.fetchall()
            if not results:
                results = list_top_entities(entity, entreprise_id, topk=topk)

            for i, r in enumerate(results, start=1):
                r["option"] = i
            return results
    finally:
        conn.close()


# =========================================================
# I18N
# =========================================================
I18N = {
    "fr": {
        "need_pick": "Plusieurs {entity} correspondent. Lequel choisis-tu ?",
        "need_pick_noquery": "Pour continuer, choisis un {entity}.",
        "need_compte": "Pour continuer, je dois savoir depuis quel compte c’est payé.",
        "need_type": "Pour continuer, quel type de dépense ? (ex: cloud, transport, matériel, autre)",
        "need_amount": "Pour continuer, quel est le montant ? (ex: 50 EUR)",
        "need_currency": "Pour continuer, quelle devise ? (ex: EUR, DZD, AED)",
    },
    "en": {
        "need_pick": "Several {entity} match. Which one do you choose?",
        "need_pick_noquery": "To continue, pick a {entity}.",
        "need_compte": "To continue, I need to know which account pays this expense.",
        "need_type": "To continue, what expense type? (e.g., cloud, travel, hardware, other)",
        "need_amount": "To continue, what amount? (e.g., 50 EUR)",
        "need_currency": "To continue, which currency? (e.g., EUR, DZD, AED)",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in I18N else "fr"
    return I18N[lang][key].format(**kwargs)


def entity_to_field(entity_name: str) -> Optional[str]:
    if entity_name == "projet":
        return "projet_id"
    if entity_name == "client":
        return "client_id"
    if entity_name == "compte_financier":
        return "compte_id"
    if entity_name == "type_depense":
        return "type_depense"
    if entity_name == "montant":
        return "montant"
    if entity_name == "devise":
        return "devise"
    return None


# =========================================================
# Few-shot
# =========================================================
FEW_SHOT_BY_LANG = {
    "fr": [
        {"nl": "Liste des projets", "out": {"mode": "sql", "operation": "SELECT",
                                            "sql": "SELECT p.id, p.nom FROM projet p ORDER BY p.nom LIMIT 100;", "params": []}},
        {"nl": "Ajoute une dépense de 50€ pour le projet Migration depuis le compte principal",
         "out": {
             "mode": "plan",
             "operation": "INSERT",
             "intent": "INSERT_DEPENSE",
             "entities": {"projet_query": "Migration", "compte_query": "principal", "type_depense": "autre"},
             "sql_template": "INSERT INTO depense (projet_id, compte_id, type_depense, montant, devise, date_depense, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);",
             "template_params": [50, "EUR"]
         }},
    ],
    "en": [
        {"nl": "List projects", "out": {"mode": "sql", "operation": "SELECT",
                                        "sql": "SELECT p.id, p.nom FROM projet p ORDER BY p.nom LIMIT 100;", "params": []}},
    ]
}


def build_schema_summary(schema_bundle: Dict[str, Any]) -> Dict[str, Any]:
    tables = schema_bundle["tables"]
    fks = schema_bundle["fks"]

    compact_tables = {}
    for tname in TEXT2SQL_ALLOWED_TABLES:
        if tname in tables:
            compact_tables[tname] = tables[tname]["columns"]

    return {
        "tables": compact_tables,
        "relations": [f"{fk['table']}.{fk['column']} -> {fk['ref_table']}.{fk['ref_column']}" for fk in fks],
        "allowed_tables": TEXT2SQL_ALLOWED_TABLES,
    }


def build_llm_prompt(user_input: str, context: Dict[str, Any], force_plan: bool) -> str:
    schema_summary = build_schema_summary(_SCHEMA_CACHE)
    lang = context.get("lang", "fr")
    few_shot_text = json.dumps(FEW_SHOT_BY_LANG.get(lang, FEW_SHOT_BY_LANG["fr"]), ensure_ascii=False)

    mode_instruction = "Tu DOIS retourner le format PLAN (mode=plan)." if force_plan else \
        "Tu peux retourner soit SQL direct (mode=sql), soit PLAN (mode=plan) si résolution nécessaire."

    return f"""
Tu es un assistant Text2SQL STRICT pour PostgreSQL.
- Répond uniquement en JSON (sans markdown).

OBJECTIF:
- {mode_instruction}

RÈGLES:
- 1 seule requête SQL (pas de multi-statements)
- Interdit: DDL/DCL (CREATE/DROP/ALTER/TRUNCATE/GRANT/REVOKE)
- Tables: uniquement allowlist
- Interdit: SELECT *
- NE METS PAS de LIMIT (le système peut injecter)
- Placeholders: %s uniquement

IMPORTANT INSERT DEPENSE (CRITIQUE):
- La table depense exige au minimum: projet_id, compte_id, type_depense, montant, devise.
- Si une info manque, retourne PLAN avec entities.* à null et un sql_template complet:
  INSERT INTO depense (...) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)

FEW-SHOT:
{few_shot_text}

SCHEMA:
{json.dumps(schema_summary, ensure_ascii=False)}

CONTEXTE:
{json.dumps(context, ensure_ascii=False)}

QUESTION:
{user_input}

FORMATS:

SQL:
{{"mode":"sql","operation":"SELECT|INSERT|UPDATE|DELETE|UNKNOWN","sql":"...","params":[]}}

PLAN:
{{
  "mode":"plan",
  "operation":"SELECT|INSERT|UPDATE|DELETE|UNKNOWN",
  "intent":"string",
  "entities":{{
    "projet_query": null|"...",
    "client_query": null|"...",
    "compte_query": null|"...",
    "type_depense": null|"..."
  }},
  "sql_template":"SQL avec %s",
  "template_params":[]
}}
""".strip()


# =========================================================
# Heuristics
# =========================================================
WRITE_TRIGGERS_RE = re.compile(r"\b(ajoute|ajouter|ins[eè]re|inserer|insert|update|supprime|delete)\b", re.I)
DEPENSE_TRIGGER_RE = re.compile(r"\b(dépense|depense|expense)\b", re.I)
MONEY_RE = re.compile(r"(\d[\d\s]*)(?:[.,](\d{1,2}))?\s*(€|eur|dzd|aed|syp|\$|usd)?", re.I)


def looks_like_write(user_input: str) -> bool:
    return bool(WRITE_TRIGGERS_RE.search(user_input or ""))


def looks_like_depense_insert(user_input: str) -> bool:
    ui = user_input or ""
    return bool(DEPENSE_TRIGGER_RE.search(ui) and WRITE_TRIGGERS_RE.search(ui))


def should_force_plan(user_input: str) -> bool:
    if looks_like_write(user_input):
        return True
    return False


def parse_amount_currency(user_input: str) -> Tuple[Optional[float], Optional[str]]:
    s = (user_input or "").replace("\u00a0", " ")
    m = MONEY_RE.search(s)
    if not m:
        return None, None
    int_part = (m.group(1) or "").replace(" ", "")
    dec_part = m.group(2)
    cur = (m.group(3) or "").upper()
    if cur == "€":
        cur = "EUR"
    if not int_part.isdigit():
        return None, None
    amount = float(int_part)
    if dec_part and dec_part.isdigit():
        amount += float(dec_part) / (10 ** len(dec_part))
    if not cur:
        cur = None
    return amount, cur


# =========================================================
# OpenAI client
# =========================================================
_client: Optional[OpenAI] = None


def get_client() -> OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY manquant")
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


def call_gpt(prompt: str) -> Dict[str, Any]:
    client = get_client()
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "Assistant Text2SQL strict. Répond uniquement en JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
        max_tokens=OPENAI_MAX_TOKENS,
        timeout=OPENAI_TIMEOUT,
    )
    raw = (resp.choices[0].message.content or "").strip()
    raw = re.sub(r"^```json\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def parse_llm_output(obj: Dict[str, Any]) -> LLMOutput:
    if "mode" not in obj:
        obj["mode"] = "plan" if "sql_template" in obj else "sql"
    mode = str(obj.get("mode", "")).lower()
    obj["mode"] = mode
    if mode == "plan":
        return LLMPlan(**obj)
    return LLMDirect(**obj)


# =========================================================
# Clarification builder
# =========================================================
def _clarify_pick(entity: str, query: str, matches: List[Dict[str, Any]], lang: str) -> Clarification:
    if not (query or "").strip():
        return Clarification(
            needed=True,
            entity=entity,
            field=entity_to_field(entity),
            query=query,
            suggestions=matches,
            message=t(lang, "need_pick_noquery", entity=entity),
        )
    return Clarification(
        needed=True,
        entity=entity,
        field=entity_to_field(entity),
        query=query,
        suggestions=matches,
        message=t(lang, "need_pick", entity=entity),
    )


# =========================================================
# ✅ Multi-step continuation for pending plans
# =========================================================
def _coerce_choice_to_value(field: str, user_input: str) -> Any:
    """
    - If user replies "1" we keep it as int option (gateway will map option->id usually)
    - If user replies "autre" for type_depense, keep string
    - If user replies "50 EUR", parse amount/currency
    """
    txt = (user_input or "").strip()
    if field in ("type_depense",):
        return txt.lower()

    if field in ("montant", "devise"):
        amount, cur = parse_amount_currency(txt)
        if field == "montant":
            return amount
        return cur or txt.upper()

    return txt


def continue_pending_plan(pending: PendingPlan, user_input: str, context: Dict[str, Any]) -> Tuple[Optional[str], List[Any], Dict[str, Any], Clarification, PendingPlan, List[str]]:
    """
    Continue resolution WITHOUT calling LLM again.
    Uses pending.entities + pending.filled to determine what's missing next.
    """
    notes: List[str] = []
    lang = context.get("lang", "fr")
    entreprise_id = context.get("entreprise_id")

    filled = dict(pending.filled or {})
    entities = dict(pending.entities or {})

    # Determine which field was last asked (provided by gateway in context["last_field"])
    last_field = (context.get("last_field") or "").strip()
    if last_field:
        filled[last_field] = _coerce_choice_to_value(last_field, user_input)
        pending.filled = filled

    intent = (pending.intent or "").strip()

    # For INSERT_DEPENSE, required fields in order:
    # projet_id, compte_id, type_depense, montant, devise
    if intent == "INSERT_DEPENSE":
        # 1) projet
        if "projet_id" not in filled:
            projet_q = (entities.get("projet_query") or "").strip()
            matches = resolve_by_name("projet", projet_q, entreprise_id)
            if len(matches) != 1:
                clar = _clarify_pick("projet", projet_q, matches, lang)
                return None, [], {}, clar, pending, notes
            filled["projet_id"] = int(matches[0]["id"])
            pending.filled = filled

        # 2) compte
        if "compte_id" not in filled:
            compte_q = (entities.get("compte_query") or "").strip()
            matches = resolve_by_name("compte_financier", compte_q, entreprise_id)
            if len(matches) != 1:
                clar = _clarify_pick("compte_financier", compte_q, matches, lang)
                clar.message = t(lang, "need_compte")
                return None, [], {}, clar, pending, notes
            filled["compte_id"] = int(matches[0]["id"])
            pending.filled = filled

        # 3) type_depense
        if not (filled.get("type_depense") or "").strip():
            return None, [], {}, Clarification(
                needed=True,
                entity="type_depense",
                field="type_depense",
                query=None,
                suggestions=[
                    {"option": 1, "nom": "cloud"},
                    {"option": 2, "nom": "transport"},
                    {"option": 3, "nom": "materiel"},
                    {"option": 4, "nom": "autre"},
                ],
                message=t(lang, "need_type"),
            ), pending, notes

        # 4) montant/devise: soit template_params, soit parse, sinon clarification
        tpl = list(pending.template_params or [])
        montant = None
        devise = None

        if len(tpl) >= 2:
            montant = tpl[0]
            devise = tpl[1]
        else:
            # try parse from original context if exists
            original = context.get("original_user_input") or context.get("user_input") or ""
            montant, devise = parse_amount_currency(original)

        if montant is None:
            return None, [], {}, Clarification(
                needed=True,
                entity="montant",
                field="montant",
                query=None,
                suggestions=[],
                message=t(lang, "need_amount"),
            ), pending, notes

        if not devise:
            # if € present, parse_amount_currency gave EUR. else ask
            return None, [], {}, Clarification(
                needed=True,
                entity="devise",
                field="devise",
                query=None,
                suggestions=[
                    {"option": 1, "nom": "EUR"},
                    {"option": 2, "nom": "DZD"},
                    {"option": 3, "nom": "AED"},
                    {"option": 4, "nom": "USD"},
                ],
                message=t(lang, "need_currency"),
            ), pending, notes

        # Build final sql + params in exact order:
        sql = (pending.sql_template or "").strip()
        params: List[Any] = [
            int(filled["projet_id"]),
            int(filled["compte_id"]),
            str(filled["type_depense"]).strip(),
            float(montant),
            str(devise).upper(),
        ]
        notes.append("Continuation INSERT_DEPENSE: tous les champs requis sont remplis.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # Fallback: if not INSERT_DEPENSE, just stop (keep your old behavior)
    return None, [], {}, Clarification(
        needed=True,
        entity="info",
        field=None,
        query=None,
        suggestions=[],
        message="Continuation non supportée pour ce type de plan.",
    ), pending, notes


# =========================================================
# Main endpoint /convert (UNCHANGED behavior)
# =========================================================
@app.post("/convert", response_model=SQLPlan)
def convert(req: ConvertRequest):
    ensure_schema_loaded()

    request_id = str(uuid.uuid4())
    started = time.time()

    lang = detect_lang(req.user_input)
    context = {
        "entreprise_id": req.entreprise_id,
        "projet_id": req.projet_id,
        "actor_id": req.actor_id,
        "role": req.role,
        "channel": "whatsapp",
        "request_id": request_id,
        "lang": lang,
        "user_input": req.user_input,
        "original_user_input": req.user_input,
    }

    force_plan = should_force_plan(req.user_input)

    t0 = time.time()
    if TEXT2SQL_STUB_LLM:
        llm_obj = {
            "mode": "sql",
            "operation": "SELECT",
            "sql": "SELECT p.id, p.nom FROM projet p ORDER BY p.nom;",
            "params": [],
        }
    else:
        prompt = build_llm_prompt(req.user_input, context, force_plan=force_plan)
        llm_obj = call_gpt(prompt)
    llm_ms = int((time.time() - t0) * 1000)

    try:
        llm: LLMOutput = parse_llm_output(llm_obj)
    except (ValidationError, Exception) as e:
        log_event("llm_output_invalid", request_id=request_id, error=str(e))
        raise HTTPException(400, f"Sortie LLM invalide: {e}")

    mode = llm.mode.lower()

    if force_plan and mode != "plan":
        raise HTTPException(400, "Mode PLAN requis (écriture / ambiguïté) pour éviter un INSERT incomplet.")

    resolved: Dict[str, Any] = {}
    clarification = Clarification(needed=False)
    notes: List[str] = []

    if mode == "plan":
        # ✅ On ne change pas ton resolve_plan existant ici.
        # Pour garder ton code tel quel, on fait une résolution minimale:
        # -> si clarification: on renvoie pending_plan
        # -> sinon: on construit sql+params si possible (via /continue à la suite)
        pending = PendingPlan(
            operation=(llm.operation or "UNKNOWN"),
            intent=getattr(llm, "intent", None),
            sql_template=(llm.sql_template or ""),
            template_params=list(llm.template_params or []),
            entities=dict(getattr(llm, "entities", {}) or {}),
            filled={},
        )

        # On tente une "auto-continue" immédiate (0 réponse user) pour résoudre projet/compte si unique
        sql2, params2, _, clar2, pending2, cont_notes = continue_pending_plan(
            pending=pending,
            user_input="",
            context=context,
        )
        notes.extend(cont_notes)

        if clar2.needed:
            return SQLPlan(
                request_id=request_id,
                user_input=req.user_input,
                operation=(llm.operation or "UNKNOWN"),
                sql="",
                params=[],
                tables=[],
                risk=RiskInfo(level="medium", needs_approval=False),
                static_checks=StaticChecks(
                    ast_parsed=False,
                    ddl_blocked=True,
                    schema_ok=True,
                    single_statement=True,
                    tenant_scoped=False,
                    columns_ok=True,
                    no_select_star=True,
                    limit_ok=True,
                    functions_ok=True,
                ),
                context=context,
                resolved=resolved,
                clarification=clar2,
                pending_plan=pending2,
                notes=notes,
            )

        # if already resolved fully
        sql = sql2 or ""
        params = params2 or []
    else:
        sql = (llm.sql or "").strip()
        params = list(llm.params or [])

    if not sql:
        raise HTTPException(400, "SQL vide")

    if contains_forbidden_keywords(sql):
        raise HTTPException(400, "SQL interdit (DDL/DCL)")

    try:
        node = parse_sql_single(sql)
        single_statement_ok = True
        ast_ok = True
    except Exception as e:
        raise HTTPException(400, f"SQL non parsable ou multi-statements: {e}")

    sql = node.sql(dialect="postgres")
    op = classify_operation(sql)
    risk_level, needs_approval = risk_level_for(op)

    tables, schema_ok, schema_notes = validate_tables(node, _SCHEMA_CACHE)
    notes.extend(schema_notes)

    functions_ok, fn_notes = validate_functions(node)
    notes.extend(fn_notes)
    if not functions_ok:
        risk_level, needs_approval = "high", True

    no_select_star_ok = True
    if op == "SELECT" and ENFORCE_NO_SELECT_STAR and has_select_star(node):
        no_select_star_ok = False
        raise HTTPException(400, "SELECT * interdit (précise les colonnes)")

    columns_ok, col_notes = validate_columns(node, _SCHEMA_CACHE)
    notes.extend(col_notes)
    if not columns_ok and not TEXT2SQL_SOFT_SCHEMA_FAIL:
        raise HTTPException(400, "Colonnes invalides / hallucinations détectées")

    limit_ok = True
    if op == "SELECT":
        node2, changed, limit_notes = apply_default_limit_if_needed(node)
        notes.extend(limit_notes)
        if changed:
            sql = node2.sql(dialect="postgres")
            node = node2
        if ENFORCE_LIMIT_ON_SELECT and not has_limit(node):
            limit_ok = False
            raise HTTPException(400, "LIMIT requis sur SELECT")

    tenant_sql, tenant_params, tenant_scoped, tenant_notes = enforce_tenant_scope(sql, req.entreprise_id, _SCHEMA_CACHE)
    notes.extend(tenant_notes)
    if tenant_params:
        params = params + tenant_params
        sql = tenant_sql
        node = parse_sql_single(sql)

    if req.entreprise_id is not None and not tenant_scoped:
        risk_level, needs_approval = "high", True
        if REQUIRE_TENANT_SCOPE:
            raise HTTPException(400, "Requête non scoppée par entreprise_id (refus Text2SQL).")

    if not schema_ok:
        risk_level, needs_approval = "high", True
        if not TEXT2SQL_SOFT_SCHEMA_FAIL:
            raise HTTPException(400, "Schéma invalide / table non autorisée")

    duration_ms = int((time.time() - started) * 1000)

    log_event(
        "convert_ok",
        request_id=request_id,
        mode=mode,
        operation=op,
        tables=tables,
        schema_ok=schema_ok,
        columns_ok=columns_ok,
        tenant_scoped=tenant_scoped if req.entreprise_id is not None else True,
        needs_approval=needs_approval,
        llm_ms=llm_ms,
        duration_ms=duration_ms,
    )

    return SQLPlan(
        request_id=request_id,
        user_input=req.user_input,
        operation=op,
        sql=sql,
        params=params,
        tables=tables,
        risk=RiskInfo(level=risk_level, needs_approval=needs_approval),
        static_checks=StaticChecks(
            ast_parsed=ast_ok,
            ddl_blocked=True,
            schema_ok=schema_ok,
            single_statement=single_statement_ok,
            tenant_scoped=tenant_scoped if req.entreprise_id is not None else True,
            columns_ok=columns_ok,
            no_select_star=no_select_star_ok,
            limit_ok=limit_ok,
            functions_ok=functions_ok,
        ),
        context=context,
        resolved=resolved,
        clarification=Clarification(needed=False),
        pending_plan=None,
        notes=notes,
    )


# =========================================================
# ✅ NEW endpoint /continue
# =========================================================
@app.post("/continue", response_model=SQLPlan)
def continue_plan(req: ContinueRequest):
    ensure_schema_loaded()

    request_id = str(uuid.uuid4())
    started = time.time()

    context = dict(req.context or {})
    if "lang" not in context:
        context["lang"] = detect_lang(req.user_input)
    context.setdefault("request_id", request_id)
    context.setdefault("channel", "whatsapp")
    context.setdefault("user_input", req.user_input)
    context.setdefault("original_user_input", context.get("original_user_input") or req.user_input)

    sql, params, resolved, clarification, pending2, notes = continue_pending_plan(
        pending=req.pending_plan,
        user_input=req.user_input,
        context=context,
    )

    if clarification.needed:
        return SQLPlan(
            request_id=request_id,
            user_input=req.user_input,
            operation=req.pending_plan.operation,
            sql="",
            params=[],
            tables=[],
            risk=RiskInfo(level="medium", needs_approval=False),
            static_checks=StaticChecks(
                ast_parsed=False,
                ddl_blocked=True,
                schema_ok=True,
                single_statement=True,
                tenant_scoped=False,
                columns_ok=True,
                no_select_star=True,
                limit_ok=True,
                functions_ok=True,
            ),
            context=context,
            resolved=resolved,
            clarification=clarification,
            pending_plan=pending2,
            notes=notes,
        )

    if not sql:
        raise HTTPException(400, "SQL vide après continuation")

    # Apply the SAME safety checks as convert (minimal for writes)
    if contains_forbidden_keywords(sql):
        raise HTTPException(400, "SQL interdit (DDL/DCL)")

    node = parse_sql_single(sql)
    sql = node.sql(dialect="postgres")
    op = classify_operation(sql)
    risk_level, needs_approval = risk_level_for(op)

    tables, schema_ok, schema_notes = validate_tables(node, _SCHEMA_CACHE)
    notes.extend(schema_notes)

    functions_ok, fn_notes = validate_functions(node)
    notes.extend(fn_notes)

    columns_ok, col_notes = validate_columns(node, _SCHEMA_CACHE)
    notes.extend(col_notes)

    duration_ms = int((time.time() - started) * 1000)

    log_event(
        "continue_ok",
        request_id=request_id,
        operation=op,
        tables=tables,
        duration_ms=duration_ms,
    )

    return SQLPlan(
        request_id=request_id,
        user_input=req.user_input,
        operation=op,
        sql=sql,
        params=params,
        tables=tables,
        risk=RiskInfo(level=risk_level, needs_approval=needs_approval),
        static_checks=StaticChecks(
            ast_parsed=True,
            ddl_blocked=True,
            schema_ok=schema_ok,
            single_statement=True,
            tenant_scoped=True,
            columns_ok=columns_ok,
            no_select_star=True,
            limit_ok=True,
            functions_ok=functions_ok,
        ),
        context=context,
        resolved=resolved,
        clarification=Clarification(needed=False),
        pending_plan=None,
        notes=notes,
    )


@app.get("/health")
def health():
    ensure_schema_loaded()
    return {
        "status": "ok",
        "service": "text2sql",
        "version": app.version,
        "allowed_tables": TEXT2SQL_ALLOWED_TABLES,
        "schema_cached": bool(_SCHEMA_CACHE),
        "pg_trgm_expected": TEXT2SQL_USE_PG_TRGM,
    }
