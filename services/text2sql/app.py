import os
import re
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# =========================================================
# ENV (.env / docker-compose)
# =========================================================
POSTGRES_DB = os.getenv("POSTGRES_DB", "orionis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "orionis")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "orionis")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "orionis-postgres")  # service name docker
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

ENV = os.getenv("ENV", "dev")


# =========================================================
# FastAPI
# =========================================================
app = FastAPI(title="Text2SQL Service", version="2.0.0")


# =========================================================
# Models
# =========================================================
class ConvertRequest(BaseModel):
    user_input: str = Field(..., min_length=1, description="Message utilisateur en langage naturel")
    # Optionnel: contexte (multi-entité) si besoin plus tard
    entreprise_id: Optional[int] = None
    projet_id: Optional[int] = None
    role: Optional[str] = None  # ex: admin_financier / responsable_projet / lecture_seule


class ConvertResponse(BaseModel):
    operation: str  # SELECT / INSERT / UPDATE / DELETE / UNKNOWN
    sql: str
    params: List[Any]
    tables: List[str]
    needs_approval: bool
    risk_level: str  # low / medium / high
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
    """
    Lit le schéma depuis information_schema (read-only).
    Retry utile au démarrage (postgres pas encore prêt).
    """
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
_FORBIDDEN = [
    r"\bDROP\b",
    r"\bTRUNCATE\b",
    r"\bALTER\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCREATE\b",
]


def classify_operation(sql: str) -> str:
    m = _SQL_START_RE.search(sql or "")
    return (m.group(1).upper() if m else "UNKNOWN")


def extract_tables(sql: str, known_tables: List[str]) -> List[str]:
    """
    Extraction simple basée sur matching de noms connus.
    (On fera plus robuste plus tard avec sqlglot dans sql-guard.)
    """
    s = (sql or "").lower()
    found = []
    for t in known_tables:
        if re.search(rf"\b{re.escape(t.lower())}\b", s):
            found.append(t)
    return sorted(set(found))


def contains_forbidden(sql: str) -> bool:
    s = (sql or "")
    return any(re.search(pat, s, flags=re.IGNORECASE) for pat in _FORBIDDEN)


def risk_level_for(op: str) -> Tuple[str, bool]:
    """
    Policy simple:
      - SELECT => low, pas besoin d'approbation (mais sera quand même validé par sql-guard côté archi)
      - INSERT/UPDATE/DELETE => high, needs_approval=True
    """
    op = op.upper()
    if op == "SELECT":
        return "low", False
    if op in ("INSERT", "UPDATE", "DELETE"):
        return "high", True
    return "medium", True


# =========================================================
# LLM (Ollama) - generation JSON ONLY
# =========================================================
def build_llm_prompt(user_input: str, schema: Dict[str, Any], context: Dict[str, Any]) -> str:
    """
    On force le modèle à produire un JSON strict: {operation, sql, params}
    - SQL paramétré (placeholders %s)
    - params = liste (types simples)
    - SQL limité au schéma fourni
    """
    schema_min = {t: v["columns"] for t, v in schema.items()}

    rules = [
        "Tu es un assistant Text2SQL pour Postgres.",
        "Tu DOIS répondre avec un JSON STRICT et rien d'autre.",
        "Le champ sql doit être une requête Postgres PARAMÉTRÉE utilisant uniquement des placeholders %s.",
        "Le champ params est une liste ordonnée correspondant aux %s.",
        "N'utilise QUE les tables/colonnes présentes dans le schéma fourni.",
        "N'écris JAMAIS de SQL dangereux: pas de DROP/TRUNCATE/ALTER/GRANT/REVOKE/CREATE.",
        "Si la demande est ambiguë, fais au mieux avec un SELECT et ajoute une note dans params? NON: reste en JSON, et mets operation='SELECT' ou 'UNKNOWN' et une requête neutre.",
        "Pour INSERT/UPDATE/DELETE, renvoie quand même la requête paramétrée (sans l'exécuter).",
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
        "format": "json",  # Ollama: tente de forcer JSON (selon versions)
        "options": {
            "temperature": 0.0,
        },
    }

    r = requests.post(url, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Ollama error {r.status_code}: {r.text}")

    data = r.json()

    # Ollama renvoie souvent {"response": "..."} contenant du JSON texte
    raw = data.get("response", "").strip()
    if not raw:
        raise RuntimeError("Ollama a renvoyé une réponse vide.")

    # Certains modèles ajoutent des fences ```json ... ```
    raw = re.sub(r"^```json\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^```\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        obj = json.loads(raw)
        return obj
    except Exception as e:
        raise RuntimeError(f"Réponse LLM non-JSON ou invalide: {e}. Raw={raw[:500]}")


def normalize_llm_output(obj: Dict[str, Any]) -> Tuple[str, str, List[Any]]:
    operation = str(obj.get("operation", "UNKNOWN")).upper().strip()
    sql = str(obj.get("sql", "")).strip()
    params = obj.get("params", [])
    if not isinstance(params, list):
        params = []

    # Fallback si operation manquante
    if operation not in ("SELECT", "INSERT", "UPDATE", "DELETE", "UNKNOWN"):
        operation = classify_operation(sql)

    return operation, sql, params


# =========================================================
# API endpoints
# =========================================================
@app.on_event("startup")
def _startup():
    # Charge le schéma dès le démarrage (avec retry)
    ensure_schema_loaded()


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
    }


@app.get("/schema")
def schema():
    ensure_schema_loaded()
    return {
        "tables": sorted(_SCHEMA_CACHE.keys()),
        "schema": _SCHEMA_CACHE,
        "last_load_ts": _SCHEMA_LAST_LOAD_TS,
    }


@app.post("/refresh-schema")
def refresh_schema():
    global _SCHEMA_CACHE, _SCHEMA_LAST_LOAD_TS
    _SCHEMA_CACHE = load_schema_mapping()
    _SCHEMA_LAST_LOAD_TS = time.time()
    return {"ok": True, "tables": sorted(_SCHEMA_CACHE.keys()), "last_load_ts": _SCHEMA_LAST_LOAD_TS}


@app.post("/convert", response_model=ConvertResponse)
def convert(req: ConvertRequest):
    ensure_schema_loaded()

    context = {
        "entreprise_id": req.entreprise_id,
        "projet_id": req.projet_id,
        "role": req.role,
    }

    prompt = build_llm_prompt(req.user_input, _SCHEMA_CACHE, context)
    try:
        llm_obj = call_ollama(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur LLM: {e}")

    operation, sql, params = normalize_llm_output(llm_obj)

    notes: List[str] = []

    if not sql:
        raise HTTPException(status_code=400, detail="Le modèle n'a pas produit de SQL.")

    if contains_forbidden(sql):
        raise HTTPException(status_code=400, detail="SQL refusé: contient une commande interdite (DDL/DCL).")

    # Tables
    tables = extract_tables(sql, list(_SCHEMA_CACHE.keys()))
    if not tables:
        notes.append("Aucune table détectée (vérifie la requête / schéma).")

    # Risk / approval
    risk, needs_approval = risk_level_for(operation)

    # Guard minimal: si DELETE/UPDATE sans WHERE => toujours risky
    if operation in ("UPDATE", "DELETE"):
        if re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE) is None:
            needs_approval = True
            risk = "high"
            notes.append(f"{operation} sans WHERE détecté: doit être bloqué côté sql-guard.")

    # IMPORTANT: Ce service N'EXÉCUTE JAMAIS la requête.
    return ConvertResponse(
        operation=operation,
        sql=sql,
        params=params,
        tables=tables,
        needs_approval=needs_approval,
        risk_level=risk,
        notes=notes,
    )
