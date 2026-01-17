import os
import re
import time
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field

import sqlglot
from sqlglot import exp


# =========================================================
# ENV (.env / docker-compose)
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "orionis-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi3:mini")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))

ENV = os.getenv("ENV", "dev")

# Refresh schema cache (seconds). 0 => never refresh after startup.
SCHEMA_REFRESH_SECONDS = int(os.getenv("SCHEMA_REFRESH_SECONDS", "0"))

# If True: Text2SQL returns schema errors as notes (soft-fail) instead of HTTP 400.
TEXT2SQL_SOFT_SCHEMA_FAIL = os.getenv("TEXT2SQL_SOFT_SCHEMA_FAIL", "1") == "1"
TEXT2SQL_STUB_LLM = os.getenv("TEXT2SQL_STUB_LLM", "0") == "1"



# =========================================================
# FastAPI
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema_loaded(force=True)
    yield


app = FastAPI(title="Text2SQL Service", version="2.2.0", lifespan=lifespan)


# =========================================================
# Models: SQLPlan
# =========================================================
class ConvertRequest(BaseModel):
    user_input: str = Field(..., min_length=1)
    entreprise_id: Optional[int] = None
    projet_id: Optional[int] = None
    role: Optional[str] = None
    actor_id: Optional[str] = None  # ex: whatsapp phone


class RiskInfo(BaseModel):
    level: str  # low|medium|high
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


# =========================================================
# Schema cache (read-only)
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


# =========================================================
# Safety helpers
# =========================================================
_SQL_START_RE = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", re.IGNORECASE)
_FORBIDDEN = [
    r"\bDROP\b",
    r"\bTRUNCATE\b",
    r"\bALTER\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCREATE\b",
]
_MULTI_STATEMENT = re.compile(r";\s*\S+", re.DOTALL)


def classify_operation(sql: str) -> str:
    m = _SQL_START_RE.search(sql or "")
    return (m.group(1).upper() if m else "UNKNOWN")


def contains_forbidden(sql: str) -> bool:
    s = (sql or "")
    return any(re.search(pat, s, flags=re.IGNORECASE) for pat in _FORBIDDEN)


def seems_multi_statement(sql: str) -> bool:
    # Heuristic: "something; something"
    if not sql:
        return False
    stripped = sql.strip()
    # allow trailing semicolon only
    if stripped.endswith(";"):
        stripped = stripped[:-1]
    return bool(_MULTI_STATEMENT.search(stripped))


def risk_level_for(op: str) -> Tuple[str, bool]:
    op = op.upper()
    if op == "SELECT":
        return "low", False
    if op in ("INSERT", "UPDATE", "DELETE"):
        return "high", True
    return "medium", True


# =========================================================
# SQLGLOT "light" static analysis
# =========================================================
def parse_sql_ast(sql: str) -> exp.Expression:
    try:
        return sqlglot.parse_one(sql, read="postgres")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL invalide (parse AST): {e}")


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


def static_analyze_ast(sql: str, schema: Dict[str, Any]) -> Tuple[List[str], bool, List[str], bool]:
    """
    Light static checks:
    - AST parse ok
    - Tables exist?
    - Columns exist when qualified (best-effort)
    Returns: (tables, schema_ok, notes, ast_parsed)
    """
    notes: List[str] = []
    schema_ok = True

    try:
        node = parse_sql_ast(sql)
    except HTTPException as e:
        return [], False, [str(e.detail)], False

    tables = extract_tables_ast(node)

    for t in tables:
        if t not in schema:
            schema_ok = False
            notes.append(f"Pré-check schéma: table inconnue '{t}'")

    schema_cols = {t: set(schema[t]["columns"]) for t in schema}
    for tbl, col in extract_columns_ast(node):
        if col == "*":
            continue
        if tbl and tbl in schema_cols:
            if col not in schema_cols[tbl]:
                schema_ok = False
                notes.append(f"Pré-check schéma: colonne inconnue '{tbl}.{col}'")

    return tables, schema_ok, notes, True


# =========================================================
# Schema linking / rewrite (deterministic fixes)
# =========================================================
def rewrite_common_semantic_errors(sql: str, params: List[Any], notes: List[str]) -> Tuple[str, List[Any]]:
    """
    Fix deterministic known mapping issues.
    Example:
    - depense.projet_id compared to project name => rewrite to JOIN projet on p.nom
    """
    if re.search(r"\bFROM\s+depense\b", sql, flags=re.IGNORECASE) and re.search(
        r"\bprojet_id\s*=\s*%s\b", sql, flags=re.IGNORECASE
    ):
        if params and isinstance(params[0], str):
            notes.append(
                "Rewrite: projet_id attendu int, param est str => "
                "JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s"
            )
            sql = "SELECT d.* FROM depense d JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s;"
            return sql, params

    return sql, params


# =========================================================
# LLM prompt (few-shot + schema grounding)
# =========================================================
def build_llm_prompt(user_input: str, schema: Dict[str, Any], context: Dict[str, Any]) -> str:
    schema_min = {t: v["columns"] for t, v in schema.items()}

    few_shot = [
        {
            "user": "Montre les dépenses du projet Alpha",
            "assistant": {
                "operation": "SELECT",
                "sql": "SELECT d.* FROM depense d JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s;",
                "params": ["Alpha"],
            },
        },
        {
            "user": "Montre les factures PAYEE du projet Beta",
            "assistant": {
                "operation": "SELECT",
                "sql": "SELECT f.* FROM facture f JOIN projet p ON p.id = f.projet_id WHERE p.nom = %s AND f.statut = %s;",
                "params": ["Beta", "PAYEE"],
            },
        },
    ]

    rules = [
        "Tu es un assistant Text2SQL pour Postgres.",
        "Tu DOIS répondre avec un JSON STRICT et rien d'autre.",
        "Le champ sql doit être une requête Postgres PARAMÉTRÉE utilisant uniquement des placeholders %s.",
        "Le champ params est une liste ordonnée correspondant aux %s.",
        "N'utilise QUE les tables/colonnes présentes dans le schéma fourni.",
        "N'écris JAMAIS de SQL dangereux: pas de DROP/TRUNCATE/ALTER/GRANT/REVOKE/CREATE.",
        "Une seule requête SQL (pas de multi-statements).",
        "Si l'utilisateur mentionne un projet par NOM, utilise la table projet (JOIN) plutôt que projet_id = 'nom'.",
    ]

    ctx_lines = []
    for k, v in context.items():
        if v is not None:
            ctx_lines.append(f"- {k}: {v}")

    prompt = f"""
RÈGLES:
{chr(10).join("- " + r for r in rules)}

CONTEXTE:
{chr(10).join(ctx_lines) if ctx_lines else "- (aucun)"}

EXEMPLES (few-shot):
{json.dumps(few_shot, ensure_ascii=False, indent=2)}

SCHÉMA (JSON):
{json.dumps(schema_min, ensure_ascii=False)}

USER:
{user_input}

FORMAT DE SORTIE (JSON strict):
{{
  "operation": "SELECT|INSERT|UPDATE|DELETE|UNKNOWN",
  "sql": "string",
  "params": []
}}
""".strip()

    return prompt


def call_ollama(prompt: str) -> Dict[str, Any]:
    if LLM_PROVIDER.lower() != "ollama":
        raise RuntimeError(f"LLM_PROVIDER non supporté: {LLM_PROVIDER}")

    url = f"{OLLAMA_HOST.rstrip('/')}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0},
    }

    r = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Ollama error {r.status_code}: {r.text}")

    data = r.json()
    raw = (data.get("response", "") or "").strip()
    if not raw:
        raise RuntimeError("Ollama a renvoyé une réponse vide.")

    # cleanup fences if any
    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Réponse LLM non-JSON ou invalide: {e}. Raw={raw[:500]}")


def normalize_llm_output(obj: Dict[str, Any]) -> Tuple[str, str, List[Any]]:
    operation = str(obj.get("operation", "UNKNOWN")).upper().strip()
    sql = str(obj.get("sql", "")).strip()
    params = obj.get("params", [])
    if not isinstance(params, list):
        params = []
    if operation not in ("SELECT", "INSERT", "UPDATE", "DELETE", "UNKNOWN"):
        operation = classify_operation(sql)
    return operation, sql, params


# =========================================================
# API endpoints
# =========================================================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "env": ENV,
        "db_host": POSTGRES_HOST,
        "schema_loaded": bool(_SCHEMA_CACHE),
        "schema_last_load_ts": _SCHEMA_LAST_LOAD_TS,
        "llm_provider": LLM_PROVIDER,
        "ollama_model": OLLAMA_MODEL,
        "ollama_timeout": OLLAMA_TIMEOUT,
        "stub_llm": TEXT2SQL_STUB_LLM,
        "soft_schema_fail": TEXT2SQL_SOFT_SCHEMA_FAIL,
        "schema_refresh_seconds": SCHEMA_REFRESH_SECONDS,
    }


@app.post("/convert", response_model=SQLPlan)
def convert(req: ConvertRequest):
    ensure_schema_loaded()

    notes: List[str] = []

    context = {
        "entreprise_id": req.entreprise_id,
        "projet_id": req.projet_id,
        "role": req.role,
        "actor_id": req.actor_id,
        "channel": "whatsapp",
    }

    prompt = build_llm_prompt(req.user_input, _SCHEMA_CACHE, context)

    # Check stub LLM dynamically to allow test monkeypatching
    use_stub = os.getenv("TEXT2SQL_STUB_LLM", "0") == "1"
    
    if use_stub:
        llm_obj = {
            "operation": "SELECT",
            "sql": "SELECT d.* FROM depense d JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s;",
            "params": ["Alpha"],
        }
        notes.append("STUB_LLM actif (pas d'appel Ollama).")
    else:
        try:
            llm_obj = call_ollama(prompt)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur LLM: {e}")

    operation, sql, params = normalize_llm_output(llm_obj)

    if not sql:
        raise HTTPException(status_code=400, detail="Le modèle n'a pas produit de SQL.")

    # Hard safety: forbid DDL/DCL early
    if contains_forbidden(sql):
        raise HTTPException(status_code=400, detail="SQL refusé: contient une commande interdite (DDL/DCL).")

    # Hard safety: multi-statement early
    single_stmt = not seems_multi_statement(sql)
    if not single_stmt:
        raise HTTPException(status_code=400, detail="SQL refusé: multi-statements détectés.")

    # Deterministic rewrite
    sql, params = rewrite_common_semantic_errors(sql, params, notes)

    # Light static analysis (soft)
    tables, schema_ok, static_notes, ast_parsed = static_analyze_ast(sql, _SCHEMA_CACHE)
    notes.extend(static_notes)

    # Operation & risk suggestion
    # If LLM said UNKNOWN, infer from AST when possible
    op_ast = "UNKNOWN"
    if ast_parsed:
        try:
            node = parse_sql_ast(sql)
            op_ast = ast_operation(node)
        except Exception:
            op_ast = "UNKNOWN"

    if operation == "UNKNOWN" and op_ast != "UNKNOWN":
        operation = op_ast

    risk, needs_approval = risk_level_for(operation)

    # If schema looks wrong => bump risk (but do not block: guard will decide final)
    if not schema_ok:
        risk = "high"
        needs_approval = True
        notes.append("Pré-check schéma KO => le sql-guard doit refuser ou demander correction.")

        if not TEXT2SQL_SOFT_SCHEMA_FAIL:
            raise HTTPException(status_code=400, detail="Schéma invalide (pré-check).")

    # Suggest approval if UPDATE/DELETE without WHERE (still: guard must enforce)
    if operation in ("UPDATE", "DELETE") and re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE) is None:
        needs_approval = True
        risk = "high"
        notes.append(f"Pré-check: {operation} sans WHERE détecté (doit être bloqué par sql-guard).")

    plan = SQLPlan(
        request_id=str(uuid.uuid4()),
        user_input=req.user_input,
        operation=operation,
        sql=sql,
        params=params,
        tables=tables,
        risk=RiskInfo(level=risk, needs_approval=needs_approval),
        static_checks=StaticChecks(
            ast_parsed=ast_parsed,
            ddl_blocked=True,
            schema_ok=schema_ok,
            single_statement=single_stmt,
        ),
        context=context,
        notes=notes,
    )
    return plan
