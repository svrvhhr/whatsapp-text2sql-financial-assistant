import os
import re
import time
import json
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

# Mode test (si tu veux plus tard)
TEXT2SQL_STUB_LLM = os.getenv("TEXT2SQL_STUB_LLM", "0") == "1"


# =========================================================
# FastAPI
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    ensure_schema_loaded()
    yield
    # shutdown (rien à faire pour l’instant)

app = FastAPI(title="Text2SQL Service", version="2.1.0", lifespan=lifespan)


# =========================================================
# Models
# =========================================================
class ConvertRequest(BaseModel):
    user_input: str = Field(..., min_length=1)
    entreprise_id: Optional[int] = None
    projet_id: Optional[int] = None
    role: Optional[str] = None


class ConvertResponse(BaseModel):
    operation: str
    sql: str
    params: List[Any]
    tables: List[str]
    needs_approval: bool
    risk_level: str
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


def ensure_schema_loaded() -> None:
    global _SCHEMA_CACHE, _SCHEMA_LAST_LOAD_TS
    if not _SCHEMA_CACHE:
        _SCHEMA_CACHE = load_schema_mapping()
        _SCHEMA_LAST_LOAD_TS = time.time()


# =========================================================
# Safety / Validation helpers
# =========================================================
_SQL_START_RE = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", re.IGNORECASE)
_FORBIDDEN = [r"\bDROP\b", r"\bTRUNCATE\b", r"\bALTER\b", r"\bGRANT\b", r"\bREVOKE\b", r"\bCREATE\b"]


def classify_operation(sql: str) -> str:
    m = _SQL_START_RE.search(sql or "")
    return (m.group(1).upper() if m else "UNKNOWN")


def contains_forbidden(sql: str) -> bool:
    s = (sql or "")
    return any(re.search(pat, s, flags=re.IGNORECASE) for pat in _FORBIDDEN)


def risk_level_for(op: str) -> Tuple[str, bool]:
    op = op.upper()
    if op == "SELECT":
        return "low", False
    if op in ("INSERT", "UPDATE", "DELETE"):
        return "high", True
    return "medium", True


# =========================================================
# SQLGLOT static checks (AST + whitelist)
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


def whitelist_validate_ast(sql: str, schema: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    - Parse SQL with sqlglot
    - Ensure tables exist in schema
    - Ensure columns exist (when resolvable)
    Returns: (tables, notes)
    """
    notes: List[str] = []
    node = parse_sql_ast(sql)

    op = ast_operation(node)
    if op == "UNKNOWN":
        notes.append("Opération AST inconnue.")

    tables = extract_tables_ast(node)
    for t in tables:
        if t not in schema:
            raise HTTPException(status_code=400, detail=f"Table non autorisée/inconnue: {t}")

    # Column checks (best-effort, ignore '*' and unqualified columns without table when ambiguous)
    schema_cols = {t: set(schema[t]["columns"]) for t in schema}
    for tbl, col in extract_columns_ast(node):
        if col == "*":
            continue
        if tbl and tbl in schema_cols:
            if col not in schema_cols[tbl]:
                raise HTTPException(status_code=400, detail=f"Colonne inconnue: {tbl}.{col}")

    return tables, notes


# =========================================================
# Schema linking / rewrite (deterministic fixes)
# =========================================================
def rewrite_common_semantic_errors(sql: str, params: List[Any], notes: List[str]) -> Tuple[str, List[Any]]:
    """
    Fix deterministic known mapping issues:
    - depense.projet_id compared to a project name => rewrite to JOIN projet on p.nom
    """
    # Pattern: SELECT ... FROM depense ... WHERE projet_id = %s
    if re.search(r"\bFROM\s+depense\b", sql, flags=re.IGNORECASE) and re.search(
        r"\bprojet_id\s*=\s*%s\b", sql, flags=re.IGNORECASE
    ):
        if params and isinstance(params[0], str):
            notes.append("Rewrite: projet_id attendu int, param est str => JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s")
            # naive rewrite: replace "depense" alias with d
            # We'll generate deterministic query instead of trying to patch arbitrary SQL
            sql = "SELECT d.* FROM depense d JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s;"
            # keep params as-is (project name)
            return sql, params

    return sql, params


# =========================================================
# LLM prompt (few-shot + schema grounding)
# =========================================================
def build_llm_prompt(user_input: str, schema: Dict[str, Any], context: Dict[str, Any]) -> str:
    schema_min = {t: v["columns"] for t, v in schema.items()}

    # few-shot minimal (deterministic patterns)
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
    }


@app.post("/convert", response_model=ConvertResponse)
def convert(req: ConvertRequest):
    ensure_schema_loaded()

    notes: List[str] = []

    context = {
        "entreprise_id": req.entreprise_id,
        "projet_id": req.projet_id,
        "role": req.role,
    }

    prompt = build_llm_prompt(req.user_input, _SCHEMA_CACHE, context)

    if TEXT2SQL_STUB_LLM:
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

    if contains_forbidden(sql):
        raise HTTPException(status_code=400, detail="SQL refusé: contient une commande interdite (DDL/DCL).")

    # Deterministic rewrite for common schema-link issues
    sql, params = rewrite_common_semantic_errors(sql, params, notes)

    # AST whitelist validation (tables/columns)
    tables, ast_notes = whitelist_validate_ast(sql, _SCHEMA_CACHE)
    notes.extend(ast_notes)

    # Risk / approval
    op_from_ast = ast_operation(parse_sql_ast(sql))
    if operation == "UNKNOWN":
        operation = op_from_ast

    risk, needs_approval = risk_level_for(operation)

    # Guard minimal: UPDATE/DELETE without WHERE
    if operation in ("UPDATE", "DELETE") and re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE) is None:
        needs_approval = True
        risk = "high"
        notes.append(f"{operation} sans WHERE détecté: doit être bloqué côté sql-guard.")

    return ConvertResponse(
        operation=operation,
        sql=sql,
        params=params,
        tables=tables,
        needs_approval=needs_approval,
        risk_level=risk,
        notes=notes,
    )
