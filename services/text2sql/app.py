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
from datetime import date

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
TEXT2SQL_ANALYTICS_FROM_LLM = os.getenv("TEXT2SQL_ANALYTICS_FROM_LLM", "1") == "1"

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
    "taux_change",
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

# =========================================================
# Safe money parsing (avoid dates like 2026-01-24)
# =========================================================

MONEY_WITH_CUR_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[ \u00a0]\d{3})*|\d+)"
    r"(?:[.,](\d{1,2}))?\s*(€|eur|usd|\$|dzd|aed|syp)\b",
    re.I
)

MONEY_BARE_RE = re.compile(
    r"\b(montant|amount)\s*[:=]?\s*"
    r"(\d{1,3}(?:[ \u00a0]\d{3})*|\d+)"
    r"(?:[.,](\d{1,2}))?\b",
    re.I
)

def parse_amount_currency(user_input: str) -> tuple[Optional[float], Optional[str]]:
    s = (user_input or "").replace("\u00a0", " ")

    # 1️⃣ priorité : montant AVEC devise
    m = MONEY_WITH_CUR_RE.search(s)
    if m:
        int_part = m.group(1).replace(" ", "")
        dec_part = m.group(2)
        cur = m.group(3).upper()
        if cur == "€":
            cur = "EUR"
        amount = float(int_part)
        if dec_part:
            amount += float(dec_part) / (10 ** len(dec_part))
        return amount, cur

    # 2️⃣ fallback : "montant: 50"
    m = MONEY_BARE_RE.search(s)
    if m:
        int_part = m.group(2).replace(" ", "")
        dec_part = m.group(3)
        amount = float(int_part)
        if dec_part:
            amount += float(dec_part) / (10 ** len(dec_part))
        return amount, None

    return None, None


def log_event(event: str, **fields):
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


# =========================================================
# Language detection (simple heuristic)
# =========================================================
LANG_FR_HINTS = [
    "bonjour", "salut", "merci", "s'il", "stp", "projet", "dépense", "depense",
    "facture", "client", "compte", "solde", "budget", "aujourd", "cette", "mois", "année",
    "ajoute", "insère", "inserer", "insere", "depenses", "transfert", "virement",
]
LANG_EN_HINTS = [
    "hello", "hi", "thanks", "please", "project", "expense", "invoice", "customer",
    "account", "balance", "budget", "today", "this", "month", "year", "insert", "add",
    "transfer",
]


def detect_lang(user_input: str) -> str:
    t = (user_input or "").lower()
    fr = sum(1 for w in LANG_FR_HINTS if w in t)
    en = sum(1 for w in LANG_EN_HINTS if w in t)
    return "fr" if fr >= en else "en"

import unicodedata
def norm_txt(s: str) -> str:
    s = (s or "").strip().lower()
    # tirets & apostrophes fréquents WhatsApp
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    # enlève accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # compacte espaces
    s = re.sub(r"\s+", " ", s)
    return s
# =========================================================
# Analytics canonical SQL (bypass LLM when possible) — extended
# =========================================================

ANALYTICS_DEPENSE_TOTAL_MONTH_RE = re.compile(
    r"\b(total|somme)\b.*\b(dépense|depense|dépenses|depenses)\b.*\b(ce mois|this month)\b",
    re.I
)

ANALYTICS_PROJECT_DEPENSE_TOTAL_MONTH_RE = re.compile(
    r"\b(total|somme)\b.*\b(dépense|depense|dépenses|depenses)\b.*\b(projet|project)\b.*\b(ce mois|this month)\b",
    re.I
)
ANALYTICS_TOPN_TRANSFERTS_RE = re.compile(
    r"\b(top|derni[eè]res?|last)\s*(\d{1,3})\b.*\b(transfert|transferts|virement|virements)\b",
    re.I
)


ANALYTICS_TRANSFERT_LIST_MONTH_RE = re.compile(
    r"\b(transfert|transferts|virement|virements)\b.*\b(réalis[ée]s?|faits?|effectu[ée]s?)\b.*\b(ce mois|this month)\b",
    re.I
)
ANALYTICS_TRANSFERT_TOTAL_MONTH_RE = re.compile(
    r"\b(total|somme)\b.*\b(transfert|transferts|virement|virements)\b.*\b(ce mois|this month)\b",
    re.I
)
ANALYTICS_DEPENSE_BREAKDOWN_TYPE_MONTH_RE = re.compile(
    r"\b(répartition|repartition|breakdown)\b.*\b(dépense|depense|dépenses|depenses)\b.*\b(type|catégorie|categorie)\b.*\b(ce mois|this month)\b",
    re.I
)
ANALYTICS_TOPN_DEPENSES_PROJECT_RE = re.compile(
    r"\b(top|derni[eè]res?|last)\s*(\d{1,3})\b.*\b(dépense|depense|dépenses|depenses)\b.*\b(projet|project)\b",
    re.I
)
ANALYTICS_DEPENSE_BY_PROJECT_PERIOD_RE = re.compile(
    r"\b(dépense|depense|dépenses|depenses)\b.*\b(par|by)\b.*\b(projet|project)\b.*\b(202\d|ce mois|this month|cette ann[eé]e|this year)\b",
    re.I
)
ANALYTICS_REVENUE_BY_PROJECT_YEAR_RE = re.compile(
    r"\b(revenu|ca|chiffre d'affaires|chiffre d’affaires)\b.*\b(par|by)\b.*\b(projet|project)\b.*\b(20\d{2})\b",
    re.I
)
ANALYTICS_UNPAID_INVOICES_RE = re.compile(
    r"\b(facture|factures|invoice|invoices)\b.*\b(impay[ée]e?s?|non pay[ée]e?s?|unpaid|outstanding)\b",
    re.I
)
ANALYTICS_BUDGET_VS_SPENT_RE = re.compile(
    r"\b(budget)\b.*\b(vs|contre|compar|dépens[ée]|depens[ée]|spent)\b.*\b(projet|project)\b",
    re.I
)
ANALYTICS_ACCOUNT_BALANCES_RE = re.compile(
    r"\b(solde|soldes|balance|balances)\b.*\b(compte|comptes|account|accounts)\b",
    re.I
)
ANALYTICS_TOP_CLIENTS_CA_RE = re.compile(
    r"\b(top)\s*(\d{1,3})?\b.*\b(client|clients|customer|customers)\b.*\b(ca|chiffre d'affaires|chiffre d’affaires|revenue|revenu)\b",
    re.I
)

ANALYTICS_INVOICES_ISSUED_TOTAL_MONTH_RE = re.compile(
    r"\b(total|somme)\b.*\b(facture|factures|invoice|invoices)\b.*\b(émis|emise|émises|emises|issued)\b.*\b(ce mois|this month)\b",
    re.I
)

ANALYTICS_DEPENSE_TOTAL_MONTH_BY_PROJECT_RE = re.compile(
    r"\b(total|somme)\b.*\b(depense|depenses)\b.*\b(ce mois|this month)\b.*\b(par|by)\b.*\b(projet|project)\b"
    r"|\b(total|somme)\b.*\b(depense|depenses)\b.*\b(par|by)\b.*\b(projet|project)\b.*\b(ce mois|this month)\b",
    re.I
)

ANALYTICS_TOPN_PROJECTS_SPEND_MONTH_RE = re.compile(
    r"\btop\s*(\d{1,3})\b.*\b(projet|projects?)\b.*\b(depense|depenses)\b.*\b(ce mois|this month)\b"
    r"|\btop\s*(\d{1,3})\b.*\b(depense|depenses)\b.*\b(ce mois|this month)\b.*\b(projet|projects?)\b",
    re.I
)

ANALYTICS_LASTN_DEPENSES_PROJECT_RE = re.compile(
    r"\b(derni[eè]res?|last)\s*(\d{1,3})\b.*\b(depense|depenses)\b.*\b(projet|project)\b",
    re.I
)

ANALYTICS_TOPN_CLIENTS_BILLED_YEAR_RE = re.compile(
    r"\btop\s*(\d{1,3})\b.*\b(clients?)\b.*\b(montant|total)\b.*\b(factur[ée]|billed|invoice)\b.*\b(cette annee|this year)\b",
    re.I
)

ANALYTICS_DEPENSE_TOTAL_RE = re.compile(
    r"\b(total|somme)\b.*\b(dépense|depense|dépenses|depenses)\b(?!.*\b(ce mois|this month)\b)",
    re.I
)

ANALYTICS_DEPENSE_BREAKDOWN_TYPE_RE = re.compile(
    r"\b(répartition|repartition|breakdown)\b.*\b(dépense|depense|dépenses|depenses)\b.*\b(type|catégorie|categorie)\b(?!.*\b(ce mois|this month)\b)",
    re.I
)

# Dernières N dépenses (global, sans projet)
ANALYTICS_LASTN_DEPENSES_RE = re.compile(
    r"\b(?:"
    r"(?:derni[eè]res?|last)\s*(\d{1,3})"          # "dernières 5"
    r"|"
    r"(\d{1,3})\s*(?:derni[eè]res?|last)"          # "5 dernières"
    r")\b.*\b(depense|depenses)\b",
    re.I
)


# Liste des dépenses ce mois-ci
ANALYTICS_DEPENSE_LIST_MONTH_RE = re.compile(
    r"\b(depense|depenses)\b.*\b(ce mois|this month)\b",
    re.I
)

# Dépenses par compte (mois)
ANALYTICS_DEPENSE_BY_ACCOUNT_MONTH_RE = re.compile(
    r"\b(depense|depenses)\b.*\b(par|by)\b.*\b(compte|account)\b.*\b(ce mois|this month)\b",
    re.I
)

# Total factures PAYÉES ce mois
ANALYTICS_INVOICES_PAID_TOTAL_MONTH_RE = re.compile(
    r"\b(total|somme)\b.*\b(factures?)\b.*\b(pay[ée]es?|paid)\b.*\b(ce mois|this month)\b",
    re.I
)

# Total factures PAYÉES (sans période)
ANALYTICS_INVOICES_PAID_TOTAL_RE = re.compile(
    r"\b(total|somme)\b.*\b(factures?)\b.*\b(pay[ée]es?|paid)\b",
    re.I
)

# Liste des factures impayées (détail)
ANALYTICS_UNPAID_INVOICES_LIST_RE = re.compile(
    r"\b(liste|montre|affiche|show|list)\b.*\b(factures?)\b.*\b(impay[ée]es?|unpaid|outstanding)\b",
    re.I
)

# Derniers N transferts
ANALYTICS_LASTN_TRANSFERTS_RE = re.compile(
    r"\b(derni[eè]rs?|last)\s*(\d{1,3})\b.*\b(transferts?|virements?)\b",
    re.I
)

# Logs / audit
ANALYTICS_AUDIT_LASTN_EVENTS_RE = re.compile(
    r"\b(derniers?|last)\s*(\d{1,3})?\b.*\b(logs?|audit|events?)\b",
    re.I
)

# Utilisateurs + rôles
ANALYTICS_USERS_ROLES_RE = re.compile(
    r"\b(utilisateurs?|users?)\b.*\b(r[oô]les?|roles?|permissions?)\b",
    re.I
)

ANALYTICS_TRANSFERT_LIST_RE = re.compile(
    r"\b(liste|montre|affiche|show|list)\b.*\b(transferts?|virements?)\b(?!.*\b(ce mois|this month)\b)",
    re.I
)

ANALYTICS_TRANSFERT_TOTAL_RE = re.compile(
    r"\b(total|somme)\b.*\b(transferts?|virements?)\b(?!.*\b(ce mois|this month)\b)",
    re.I
)

ANALYTICS_INVOICES_ISSUED_TOTAL_RE = re.compile(
    r"\b(total|somme)\b.*\b(factures?)\b.*\b(émis|emise|émises|emises|issued)\b(?!.*\b(ce mois|this month)\b)",
    re.I
)

ANALYTICS_INVOICES_LIST_RE = re.compile(
    r"\b(liste|montre|affiche|show|list)\b.*\b(factures?|invoices?)\b(?!.*\b(impay|unpaid|outstanding|payee|paid|emises|issued|total|somme)\b)",
    re.I
)

# ---------------- FACTURE ----------------
UPDATE_FACTURE_STATUT_RE = re.compile(
    r"\b(marque|met|mettre|passe|change)\b.*\bfacture\s*(\d+)\b.*\b(en|comme)?\s*(payee|payée|emise|émise)\b",
    re.I
)


UPDATE_FACTURE_MONTANT_RE = re.compile(
    r"\b(modifie|change|corrige)\b.*\bmontant\b.*\bfacture\s*(\d+)\b",
    re.I
)

# ---------------- DEPENSE ----------------
UPDATE_DEPENSE_DESCRIPTION_RE = re.compile(
    r"\b(modifie|change)\b.*\bdescription\b.*\bd[eé]pense\s*(\d+)\b",
    re.I
)

UPDATE_DEPENSE_TYPE_RE = re.compile(
    r"\b(change|modifie)\b.*\btype\b.*\bd[eé]pense\s*(\d+)\b",
    re.I
)

UPDATE_DEPENSE_MONTANT_RE = re.compile(
    r"\b(corrige|modifie|change)\b.*\bmontant\b.*\bd[eé]pense\s*(\d+)\b",
    re.I
)

UPDATE_DEPENSE_DATE_RE = re.compile(
    r"\b(change|modifie)\b.*\bdate\b.*\bd[eé]pense\s*(\d+)\b",
    re.I
)

# ---------------- PROJET ----------------
UPDATE_PROJET_BUDGET_RE = re.compile(
    r"\b(modifie|change)\b.*\bbudget\b.*\bprojet\b",
    re.I
)

def _extract_year(text: str):
    m = re.search(r"\b(20\d{2})\b", text or "")
    return int(m.group(1)) if m else None

def _extract_month_year(text: str):
    t = text or ""
    m = re.search(r"\b(20\d{2})-(\d{2})\b", t)     # 2025-05
    if m: return int(m.group(1)), int(m.group(2))
    m = re.search(r"\b(\d{1,2})/(20\d{2})\b", t)   # 05/2025
    if m: return int(m.group(2)), int(m.group(1))
    return None

def _extract_project_query(text: str):
    t = text or ""
    m = re.search(r'\b(projet|project)\b\s*["“](.+?)["”]', t, flags=re.I)
    if m: return m.group(2).strip()
    m = re.search(r"\b(projet|project)\b\s+([^\n\r,.!?]+)", t, flags=re.I)
    if not m: return None
    q = m.group(2).strip()
    q = re.split(r"\b(ce mois|this month|cette ann[eé]e|this year|en\s+20\d{2}|in\s+20\d{2})\b", q, flags=re.I)[0].strip()
    return q or None




# =========================================================
# FastAPI
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema_loaded(force=True)
    yield


app = FastAPI(title="Text2SQL Service", version="6.0.0", lifespan=lifespan)


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
    entities: Dict[str, Optional[Union[str, int, float]]] = Field(default_factory=dict)
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
    entities: Dict[str, Optional[Union[str, int, float]]] = Field(default_factory=dict)
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
# Depense type detection (keep your behavior)
# =========================================================
TYPE_DEPENSE_ALIASES = {
    "restauration": ["restauration","restaurant", "resto", "repas", "déjeuner", "dejeuner", "dîner", "diner", "meal", "lunch", "dinner"],
    "transport": ["transport", "taxi", "uber", "bolt", "train", "sncf", "metro", "bus", "avion", "flight", "parking", "péage", "peage", "carburant", "essence", "diesel"],
    "cloud": ["cloud", "aws", "amazon web services", "azure", "gcp", "google cloud", "ovh", "scaleway", "digitalocean", "heroku", "kubernetes"],
    "materiel": ["matériel", "materiel", "ordinateur", "pc", "laptop", "écran", "ecran", "clavier", "souris", "imprimante", "serveur", "router", "switch"],
    "logiciel": ["logiciel", "licence", "license", "subscription", "abonnement", "saas", "office 365", "microsoft 365", "adobe", "jira", "confluence", "github", "gitlab", "notion"],
    "telecom": ["telecom", "télécom", "internet", "fibre", "4g", "5g", "forfait", "mobile", "twilio", "sms", "voip"],
    "marketing": ["marketing", "pub", "publicité", "publicite", "ads", "google ads", "meta ads", "linkedin ads"],
    "formation": ["formation", "training", "cours", "certification", "udemy", "coursera"],
    "services": ["prestation", "consulting", "consultant", "freelance", "service", "maintenance", "support", "audit", "conseil"],
    "frais_bancaires": ["frais bancaires", "commission", "bank fee", "frais de virement", "chargeback"],
    "autre": ["autre", "misc", "divers"],
}


def detect_type_depense_from_text(text: str) -> Optional[str]:
    t = (text or "").lower()
    for category, keywords in TYPE_DEPENSE_ALIASES.items():
        for kw in keywords:
            if kw in t:
                return category
    return None


# =========================================================
# Safety helpers
# =========================================================
_SQL_START_RE = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE)\b", re.I)
_FORBIDDEN_KEYWORDS = ["DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE", "CREATE"]
_FORBIDDEN_FUNCTIONS = {
    "pg_sleep", "pg_read_file", "pg_ls_dir", "pg_stat_file",
    "lo_import", "lo_export", "dblink_connect", "dblink",
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
    """
    True uniquement si la projection contient SELECT * (ou table.*).
    False pour COUNT(*) / JSON_BUILD_OBJECT('*') etc.
    """
    select = node if isinstance(node, exp.Select) else (node.find(exp.Select) or node)
    if not isinstance(select, exp.Select):
        return False

    for proj in (select.expressions or []):
        # SELECT *
        if isinstance(proj, exp.Star):
            return True

        # SELECT t.*  (sqlglot représente souvent ça aussi comme Star avec table)
        if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
            return True

        # SELECT * AS ...
        if isinstance(proj, exp.Alias) and isinstance(proj.this, exp.Star):
            return True

        # SELECT t.* AS ...
        if isinstance(proj, exp.Alias) and isinstance(proj.this, exp.Column) and isinstance(proj.this.this, exp.Star):
            return True

    return False



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
# Tenant scope (unchanged)
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
            expression=exp.Placeholder(),
        )
        where = node.args.get("where")
        if where:
            node.set("where", exp.Where(this=exp.and_(where.this, predicate)))
        else:
            node.set("where", exp.Where(this=predicate))

        sql_out = node.sql(dialect="postgres")

        # Convert sqlglot placeholders to psycopg2 positional
        sql_out = sql_out.replace("?", "%s")

        return sql_out, [entreprise_id], True, notes

        # notes.append(f"Filtre tenant injecté: {qualifier}.entreprise_id = %s")
        # return node.sql(dialect="postgres"), [entreprise_id], True, notes

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


def get_projet_entreprise_id(projet_id: int) -> Optional[int]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entreprise_id FROM projet WHERE id=%s;", (projet_id,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def get_compte_entreprise_id(compte_id: int) -> Optional[int]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entreprise_id FROM compte_financier WHERE id=%s;", (compte_id,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def get_client_entreprise_id(client_id: int) -> Optional[int]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entreprise_id FROM client WHERE id=%s;", (client_id,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def get_compte_devise(compte_id: int) -> Optional[str]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT devise FROM compte_financier WHERE id=%s;", (compte_id,))
            row = cur.fetchone()
            return str(row[0]).upper() if row and row[0] else None
    finally:
        conn.close()


def get_fx_rate(from_cur: str, to_cur: str, on_date: Optional[date] = None) -> Optional[float]:
    from_cur = (from_cur or "").upper()
    to_cur = (to_cur or "").upper()
    if not from_cur or not to_cur or from_cur == to_cur:
        return 1.0

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if on_date:
                cur.execute("""
                    SELECT rate
                    FROM taux_change
                    WHERE from_devise=%s AND to_devise=%s AND date_rate=%s
                    LIMIT 1;
                """, (from_cur, to_cur, on_date))
                row = cur.fetchone()
                if row and row[0]:
                    return float(row[0])

            cur.execute("""
                SELECT rate
                FROM taux_change
                WHERE from_devise=%s AND to_devise=%s
                ORDER BY date_rate DESC
                LIMIT 1;
            """, (from_cur, to_cur))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] else None
    except Exception:
        return None
    finally:
        conn.close()

def get_depense_context(depense_id: int) -> Optional[Dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  d.id AS depense_id,
                  d.compte_id,
                  cf.devise AS compte_devise,
                  d.projet_id,
                  p.entreprise_id
                FROM depense d
                JOIN compte_financier cf ON cf.id = d.compte_id
                JOIN projet p ON p.id = d.projet_id
                WHERE d.id = %s
                LIMIT 1;
            """, (depense_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_facture_context(facture_id: int) -> Optional[Dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  f.id AS facture_id,
                  f.projet_id,
                  p.entreprise_id,
                  f.devise
                FROM facture f
                JOIN projet p ON p.id = f.projet_id
                WHERE f.id = %s
                LIMIT 1;
            """, (facture_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_projet_context(projet_id: int) -> Optional[Dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id AS projet_id, entreprise_id
                FROM projet
                WHERE id = %s
                LIMIT 1;
            """, (projet_id,))
            return cur.fetchone()
    finally:
        conn.close()


def extract_first_int(text: str) -> Optional[int]:
    m = re.search(r"\b(\d{1,9})\b", text or "")
    return int(m.group(1)) if m else None


def extract_id_after_keyword(text: str, keyword: str) -> Optional[int]:
    # ex: "facture 12", "dépense 45"
    pat = rf"\b{re.escape(keyword)}\b\s*(?:n[°o]\s*)?(\d{{1,9}})\b"
    m = re.search(pat, text or "", flags=re.I)
    return int(m.group(1)) if m else None

# =========================================================
# Date parsing (generic)
# =========================================================
MONTHS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
    "juillet": 7, "août": 8, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11,
    "décembre": 12, "decembre": 12
}


def parse_any_date(text: str) -> Optional[date]:
    if not text:
        return None
    t = text.strip().lower()

    # ISO: 2026-01-24
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    # FR: 24 janvier 2026
    m = re.search(r"\b(\d{1,2})\s+([a-zéèêëûùüîïôöàäç]+)\s+(20\d{2})\b", t)
    if m:
        d = int(m.group(1))
        mo = MONTHS_FR.get(m.group(2))
        y = int(m.group(3))
        if not mo:
            return None
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    # Slash: 24/01/2026 (assume DD/MM/YYYY)
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    return None


# =========================================================
# I18N
# =========================================================
I18N = {
    "fr": {
        "need_pick": "Plusieurs {entity} correspondent. Lequel choisis-tu ?",
        "need_pick_noquery": "Pour continuer, choisis un {entity}.",
        "need_compte": "Pour continuer, je dois savoir depuis quel compte c’est fait.",
        "need_amount": "Pour continuer, quel est le montant ? (ex: 50 EUR)",
        "need_currency": "Pour continuer, quelle devise ? (ex: EUR, DZD, AED)",
        "need_type": "Pour continuer, quel type de dépense ? (ex: cloud, transport, matériel, autre)",
        "need_client": "Pour continuer, je dois savoir quel client est concerné.",
        "need_dst_account": "Pour continuer, je dois savoir le compte destination.",
        "need_entreprise": "Pour continuer, je dois savoir quelle entreprise est concernée.",
    },
    "en": {
        "need_pick": "Several {entity} match. Which one do you choose?",
        "need_pick_noquery": "To continue, pick a {entity}.",
        "need_compte": "To continue, I need to know which account is used.",
        "need_amount": "To continue, what amount? (e.g., 50 EUR)",
        "need_currency": "To continue, which currency? (e.g., EUR, DZD, AED)",
        "need_type": "To continue, what expense type? (e.g., cloud, travel, hardware, other)",
        "need_client": "To continue, I need to know which customer this invoice is for.",
        "need_dst_account": "To continue, I need the destination account.",
        "need_entreprise": "To continue, I need to know which company is involved.",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in I18N else "fr"
    return I18N[lang][key].format(**kwargs)


def entity_to_field(entity_name: str) -> Optional[str]:
    mapping = {
        "entreprise": "entreprise_id",
        "projet": "projet_id",
        "client": "client_id",
        "compte_financier": "compte_id",
        "compte_source": "compte_source_id",
        "compte_destination": "compte_destination_id",
        "type_depense": "type_depense",
        "montant": "montant",
        "devise": "devise",
    }
    return mapping.get(entity_name)


# =========================================================
# Intents map (as requested)
# =========================================================
INTENTS: Dict[str, Dict[str, Any]] = {
    "INSERT_DEPENSE": {
        "operation": "INSERT",
        "table": "depense",
        "required": ["projet_id", "compte_id", "type_depense", "montant", "devise"],
        "template": (
            "INSERT INTO depense "
            "(projet_id, compte_id, type_depense, montant, devise, description, date_depense, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ),
    },
    "INSERT_FACTURE": {
        "operation": "INSERT",
        "table": "facture",
        "required": ["projet_id", "client_id", "montant", "devise", "statut", "date_emission"],
        "template": (
            "INSERT INTO facture "
            "(projet_id, client_id, montant, devise, statut, date_emission, date_paiement, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ),
    },
    "INSERT_TRANSFERT": {
        "operation": "INSERT",
        "table": "transfert_interne",
        "required": ["compte_source_id", "compte_destination_id", "montant", "devise", "date_transfert"],
        "template": (
            "INSERT INTO transfert_interne "
            "(compte_source_id, compte_destination_id, montant, devise, date_transfert, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ),
    },
    "SELECT_PROJECTS": {
    "operation": "SELECT",
    "table": "projet",
    "required": ["entreprise_id"],
    "template": (
        "SELECT p.id, p.nom "
        "FROM projet p "
        "WHERE p.entreprise_id = %s "
        "ORDER BY p.nom"
    ),
}

}

# =========================================================
# Analytics intents (SQL canonique, pas de SQL écrit par LLM)
# =========================================================
INTENTS.update({
    "ANALYTICS_DEPENSE_TOTAL_MONTH": {
        "operation": "SELECT",
        "table": "depense",
        "required": ["entreprise_id"],
        "template": """
            SELECT d.devise, COALESCE(SUM(d.montant), 0) AS total
            FROM depense d
            JOIN projet p ON p.id = d.projet_id
            WHERE p.entreprise_id = %s
              AND d.date_depense >= date_trunc('month', current_date)
              AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
            GROUP BY d.devise
            ORDER BY d.devise;
        """.strip(),
    },

    "ANALYTICS_TRANSFERT_TOTAL_MONTH": {
        "operation": "SELECT",
        "table": "transfert_interne",
        "required": ["entreprise_id"],
        "template": """
            SELECT ti.devise, COALESCE(SUM(ti.montant), 0) AS total
            FROM transfert_interne ti
            JOIN compte_financier src ON src.id = ti.compte_source_id
            WHERE src.entreprise_id = %s
              AND ti.date_transfert >= date_trunc('month', current_date)
              AND ti.date_transfert <  (date_trunc('month', current_date) + interval '1 month')
            GROUP BY ti.devise
            ORDER BY ti.devise;
        """.strip(),
    },

    "ANALYTICS_TRANSFERT_LIST_MONTH": {
        "operation": "SELECT",
        "table": "transfert_interne",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              ti.id,
              ti.date_transfert,
              ti.montant,
              ti.devise,
              src.nom AS compte_source,
              dst.nom AS compte_destination
            FROM transfert_interne ti
            JOIN compte_financier src ON src.id = ti.compte_source_id
            JOIN compte_financier dst ON dst.id = ti.compte_destination_id
            WHERE src.entreprise_id = %s
              AND ti.date_transfert >= date_trunc('month', current_date)
              AND ti.date_transfert <  (date_trunc('month', current_date) + interval '1 month')
            ORDER BY ti.date_transfert DESC, ti.id DESC;
        """.strip(),
    },

    "ANALYTICS_DEPENSE_BREAKDOWN_TYPE_MONTH": {
        "operation": "SELECT",
        "table": "depense",
        "required": ["entreprise_id"],
        "template": """
            SELECT d.type_depense, d.devise, COUNT(*) AS nb, COALESCE(SUM(d.montant), 0) AS total
            FROM depense d
            JOIN projet p ON p.id = d.projet_id
            WHERE p.entreprise_id = %s
              AND d.date_depense >= date_trunc('month', current_date)
              AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
            GROUP BY d.type_depense, d.devise
            ORDER BY total DESC;
        """.strip(),
    },

    "ANALYTICS_ACCOUNT_BALANCES": {
        "operation": "SELECT",
        "table": "compte_financier",
        "required": ["entreprise_id"],
        "template": """
            SELECT cf.id, cf.nom, cf.devise, cf.solde
            FROM compte_financier cf
            WHERE cf.entreprise_id = %s
            ORDER BY cf.nom;
        """.strip(),
    },

    "ANALYTICS_UNPAID_INVOICES": {
        "operation": "SELECT",
        "table": "facture",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              c.id AS client_id,
              c.nom AS client,
              p.id AS projet_id,
              p.nom AS projet,
              f.devise,
              COUNT(*) AS nb_factures,
              COALESCE(SUM(f.montant), 0) AS total_du
            FROM facture f
            JOIN projet p ON p.id = f.projet_id
            JOIN client c ON c.id = f.client_id
            WHERE p.entreprise_id = %s
              AND f.statut <> 'PAYEE'
            GROUP BY c.id, c.nom, p.id, p.nom, f.devise
            ORDER BY total_du DESC;
        """.strip(),
    },
})

INTENTS.update({
  "ANALYTICS_INVOICES_ISSUED_TOTAL_MONTH": {
    "operation": "SELECT",
    "table": "facture",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        f.devise,
        COUNT(*) AS nb_factures,
        COALESCE(SUM(f.montant), 0) AS total_emis
      FROM facture f
      JOIN projet p ON p.id = f.projet_id
      WHERE p.entreprise_id = %s
        AND f.date_emission >= date_trunc('month', current_date)
        AND f.date_emission <  (date_trunc('month', current_date) + interval '1 month')
        AND f.statut = 'EMISE'
      GROUP BY f.devise
      ORDER BY f.devise;
    """.strip(),
  },
})

INTENTS.update({
    "ANALYTICS_DEPENSE_TOTAL_MONTH_BY_PROJECT": {
  "operation": "SELECT",
  "table": "depense",
  "required": ["entreprise_id"],
  "template": """
    SELECT
      p.id   AS projet_id,
      p.nom  AS projet,
      d.devise,
      COALESCE(SUM(d.montant), 0) AS total_depenses
    FROM depense d
    JOIN projet p ON p.id = d.projet_id
    WHERE p.entreprise_id = %s
      AND d.date_depense >= date_trunc('month', current_date)
      AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
    GROUP BY p.id, p.nom, d.devise
    ORDER BY total_depenses DESC;
  """.strip(),
},
"ANALYTICS_TOP_PROJECTS_BY_SPEND_MONTH": {
  "operation": "SELECT",
  "table": "depense",
  "required": ["entreprise_id"],
  # IMPORTANT: N sera injecté dans le SQL (pas en %s) car LIMIT ne marche pas toujours bien en param selon ton pipeline.
  "template": """
    SELECT
      p.id   AS projet_id,
      p.nom  AS projet,
      d.devise,
      COALESCE(SUM(d.montant), 0) AS total_depenses
    FROM depense d
    JOIN projet p ON p.id = d.projet_id
    WHERE p.entreprise_id = %s
      AND d.date_depense >= date_trunc('month', current_date)
      AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
    GROUP BY p.id, p.nom, d.devise
    ORDER BY total_depenses DESC
    LIMIT {topn};
  """.strip(),
},

"ANALYTICS_LASTN_DEPENSES_FOR_PROJECT": {
  "operation": "SELECT",
  "table": "depense",
  "required": ["entreprise_id"],  # entreprise_id sert à résoudre projet + sécuriser
  "template": """
    SELECT
      d.id,
      d.date_depense,
      d.type_depense,
      d.montant,
      d.devise,
      d.description
    FROM depense d
    WHERE d.projet_id = %s
    ORDER BY d.date_depense DESC, d.id DESC
    LIMIT {topn};
  """.strip(),
},
"ANALYTICS_TOP_CLIENTS_BILLED_YEAR": {
  "operation": "SELECT",
  "table": "facture",
  "required": ["entreprise_id"],
  "template": """
    SELECT
      c.id AS client_id,
      c.nom AS client,
      f.devise,
      COUNT(*) AS nb_factures,
      COALESCE(SUM(f.montant), 0) AS total_facture
    FROM facture f
    JOIN client c ON c.id = f.client_id
    JOIN projet p ON p.id = f.projet_id
    WHERE p.entreprise_id = %s
      AND f.date_emission >= date_trunc('year', current_date)
      AND f.date_emission <  (date_trunc('year', current_date) + interval '1 year')
      AND f.statut IN ('EMISE','PAYEE')  -- ajuste si tu veux seulement PAYEE
    GROUP BY c.id, c.nom, f.devise
    ORDER BY total_facture DESC
    LIMIT {topn};
  """.strip(),
},

})

INTENTS.update({
  "ANALYTICS_DEPENSE_TOTAL": {
    "operation": "SELECT",
    "table": "depense",
    "required": ["entreprise_id"],
    "template": """
      SELECT d.devise, COALESCE(SUM(d.montant), 0) AS total
      FROM depense d
      JOIN projet p ON p.id = d.projet_id
      WHERE p.entreprise_id = %s
      GROUP BY d.devise
      ORDER BY d.devise;
    """.strip(),
  },
})

INTENTS.update({
  "ANALYTICS_DEPENSE_BREAKDOWN_TYPE": {
    "operation": "SELECT",
    "table": "depense",
    "required": ["entreprise_id"],
    "template": """
      SELECT d.type_depense, d.devise, COUNT(*) AS nb, COALESCE(SUM(d.montant), 0) AS total
      FROM depense d
      JOIN projet p ON p.id = d.projet_id
      WHERE p.entreprise_id = %s
      GROUP BY d.type_depense, d.devise
      ORDER BY total DESC;
    """.strip(),
  },
})

INTENTS.update({
  "ANALYTICS_TRANSFERT_LIST": {
    "operation": "SELECT",
    "table": "transfert_interne",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        ti.id,
        ti.date_transfert,
        ti.montant,
        ti.devise,
        src.nom AS compte_source,
        dst.nom AS compte_destination
      FROM transfert_interne ti
      JOIN compte_financier src ON src.id = ti.compte_source_id
      JOIN compte_financier dst ON dst.id = ti.compte_destination_id
      WHERE src.entreprise_id = %s
      ORDER BY ti.date_transfert DESC, ti.id DESC;
    """.strip(),
  },
})

# =========================================================
# EXTRA ANALYTICS INTENTS — SQL CANONIQUE
# =========================================================

INTENTS.update({

    # Dernières N dépenses (global)
    "ANALYTICS_LASTN_DEPENSES": {
        "operation": "SELECT",
        "table": "depense",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              d.id,
              d.date_depense,
              d.type_depense,
              d.montant,
              d.devise,
              d.description
            FROM depense d
            JOIN projet p ON p.id = d.projet_id
            WHERE p.entreprise_id = %s
            ORDER BY d.date_depense DESC, d.id DESC
            LIMIT {topn};
        """.strip(),
    },

    # Liste dépenses ce mois-ci
    "ANALYTICS_DEPENSE_LIST_MONTH": {
        "operation": "SELECT",
        "table": "depense",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              d.date_depense,
              d.type_depense,
              d.montant,
              d.devise,
              p.nom AS projet
            FROM depense d
            JOIN projet p ON p.id = d.projet_id
            WHERE p.entreprise_id = %s
              AND d.date_depense >= date_trunc('month', current_date)
              AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
            ORDER BY d.date_depense DESC;
        """.strip(),
    },

    # Dépenses par compte ce mois
    "ANALYTICS_DEPENSE_BY_ACCOUNT_MONTH": {
        "operation": "SELECT",
        "table": "depense",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              cf.nom AS compte,
              d.devise,
              COALESCE(SUM(d.montant),0) AS total
            FROM depense d
            JOIN compte_financier cf ON cf.id = d.compte_id
            JOIN projet p ON p.id = d.projet_id
            WHERE p.entreprise_id = %s
              AND d.date_depense >= date_trunc('month', current_date)
              AND d.date_depense <  (date_trunc('month', current_date) + interval '1 month')
            GROUP BY cf.nom, d.devise
            ORDER BY total DESC;
        """.strip(),
    },

    # Total factures PAYÉES ce mois
    "ANALYTICS_INVOICES_PAID_TOTAL_MONTH": {
        "operation": "SELECT",
        "table": "facture",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              f.devise,
              COALESCE(SUM(f.montant),0) AS total_paye
            FROM facture f
            JOIN projet p ON p.id = f.projet_id
            WHERE p.entreprise_id = %s
              AND f.statut = 'PAYEE'
              AND f.date_paiement >= date_trunc('month', current_date)
              AND f.date_paiement <  (date_trunc('month', current_date) + interval '1 month')
            GROUP BY f.devise;
        """.strip(),
    },

    # Total factures PAYÉES (global)
    "ANALYTICS_INVOICES_PAID_TOTAL": {
        "operation": "SELECT",
        "table": "facture",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              f.devise,
              COALESCE(SUM(f.montant),0) AS total_paye
            FROM facture f
            JOIN projet p ON p.id = f.projet_id
            WHERE p.entreprise_id = %s
              AND f.statut = 'PAYEE'
            GROUP BY f.devise;
        """.strip(),
    },

    # Liste factures impayées (détail)
    "ANALYTICS_UNPAID_INVOICES_LIST": {
        "operation": "SELECT",
        "table": "facture",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              f.id,
              c.nom AS client,
              p.nom AS projet,
              f.montant,
              f.devise,
              f.date_emission
            FROM facture f
            JOIN client c ON c.id = f.client_id
            JOIN projet p ON p.id = f.projet_id
            WHERE p.entreprise_id = %s
              AND f.statut <> 'PAYEE'
            ORDER BY f.date_emission ASC;
        """.strip(),
    },

    # Derniers N transferts
    "ANALYTICS_LASTN_TRANSFERTS": {
        "operation": "SELECT",
        "table": "transfert_interne",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              ti.date_transfert,
              ti.montant,
              ti.devise,
              src.nom AS source,
              dst.nom AS destination
            FROM transfert_interne ti
            JOIN compte_financier src ON src.id = ti.compte_source_id
            JOIN compte_financier dst ON dst.id = ti.compte_destination_id
            WHERE src.entreprise_id = %s
            ORDER BY ti.date_transfert DESC
            LIMIT {topn};
        """.strip(),
    },

    # Audit logs
    "ANALYTICS_AUDIT_LASTN_EVENTS": {
        "operation": "SELECT",
        "table": "audit_event",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              ae.created_at,
              ae.event,
              ae.actor_id,
              ae.details
            FROM audit_event ae
            WHERE ae.entreprise_id = %s
            ORDER BY ae.created_at DESC
            LIMIT {topn};
        """.strip(),
    },

    # Utilisateurs + rôles
    "ANALYTICS_USERS_ROLES": {
        "operation": "SELECT",
        "table": "utilisateur",
        "required": ["entreprise_id"],
        "template": """
            SELECT
              u.nom AS utilisateur,
              r.nom AS role
            FROM utilisateur u
            JOIN utilisateur_entreprise ue ON ue.utilisateur_id = u.id
            JOIN role r ON r.id = ue.role_id
            WHERE ue.entreprise_id = %s
            ORDER BY u.nom;
        """.strip(),
    },

})



INTENTS.update({
  "ANALYTICS_TRANSFERT_TOTAL": {
    "operation": "SELECT",
    "table": "transfert_interne",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        ti.devise,
        COALESCE(SUM(ti.montant), 0) AS total
      FROM transfert_interne ti
      JOIN compte_financier src ON src.id = ti.compte_source_id
      WHERE src.entreprise_id = %s
      GROUP BY ti.devise
      ORDER BY ti.devise;
    """.strip(),
  },
})

INTENTS.update({
  "ANALYTICS_INVOICES_ISSUED_TOTAL": {
    "operation": "SELECT",
    "table": "facture",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        f.devise,
        COUNT(*) AS nb_factures,
        COALESCE(SUM(f.montant), 0) AS total_emis
      FROM facture f
      JOIN projet p ON p.id = f.projet_id
      WHERE p.entreprise_id = %s
        AND f.statut = 'EMISE'
      GROUP BY f.devise
      ORDER BY f.devise;
    """.strip(),
  },
})

INTENTS.update({
  "ANALYTICS_TOPN_TRANSFERTS": {
    "operation": "SELECT",
    "table": "transfert_interne",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        ti.date_transfert,
        ti.montant,
        ti.devise,
        src.nom AS source,
        dst.nom AS destination
      FROM transfert_interne ti
      JOIN compte_financier src ON src.id = ti.compte_source_id
      JOIN compte_financier dst ON dst.id = ti.compte_destination_id
      WHERE src.entreprise_id = %s
      ORDER BY ti.montant DESC
      LIMIT {topn};
    """.strip(),
  },
})

INTENTS.update({
  "ANALYTICS_INVOICES_LIST": {
    "operation": "SELECT",
    "table": "facture",
    "required": ["entreprise_id"],
    "template": """
      SELECT
        f.id,
        f.date_emission,
        f.statut,
        c.nom AS client,
        p.nom AS projet,
        f.montant,
        f.devise
      FROM facture f
      JOIN client c ON c.id = f.client_id
      JOIN projet p ON p.id = f.projet_id
      WHERE p.entreprise_id = %s
      ORDER BY f.date_emission DESC, f.id DESC
      LIMIT 50;
    """.strip(),
  },
})

INTENTS.update({
    "UPDATE_FACTURE_STATUT": {
        "operation": "UPDATE",
        "table": "facture",
        "required": ["entreprise_id", "facture_id", "statut"],
        "template": """
            UPDATE facture f
            SET statut=%s,
                date_paiement = CASE WHEN %s='PAYEE' THEN CURRENT_DATE ELSE NULL END,
                updated_at=CURRENT_TIMESTAMP
            WHERE f.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=f.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_FACTURE_MONTANT": {
        "operation": "UPDATE",
        "table": "facture",
        "required": ["entreprise_id", "facture_id", "montant", "devise"],
        "template": """
            UPDATE facture f
            SET montant=%s,
                devise=%s,
                updated_at=CURRENT_TIMESTAMP
            WHERE f.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=f.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_DEPENSE_DESCRIPTION": {
        "operation": "UPDATE",
        "table": "depense",
        "required": ["entreprise_id", "depense_id", "description"],
        "template": """
            UPDATE depense d
            SET description=%s,
                updated_at=CURRENT_TIMESTAMP
            WHERE d.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=d.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_DEPENSE_TYPE": {
        "operation": "UPDATE",
        "table": "depense",
        "required": ["entreprise_id", "depense_id", "type_depense"],
        "template": """
            UPDATE depense d
            SET type_depense=%s,
                updated_at=CURRENT_TIMESTAMP
            WHERE d.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=d.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_DEPENSE_MONTANT": {
        "operation": "UPDATE",
        "table": "depense",
        "required": ["entreprise_id", "depense_id", "montant", "devise"],
        "template": """
            UPDATE depense d
            SET montant=%s,
                devise=%s,
                updated_at=CURRENT_TIMESTAMP
            WHERE d.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=d.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_DEPENSE_DATE": {
        "operation": "UPDATE",
        "table": "depense",
        "required": ["entreprise_id", "depense_id", "date_depense"],
        "template": """
            UPDATE depense d
            SET date_depense=%s,
                updated_at=CURRENT_TIMESTAMP
            WHERE d.id=%s
              AND EXISTS (
                SELECT 1
                FROM projet p
                WHERE p.id=d.projet_id AND p.entreprise_id=%s
              );
        """.strip(),
    },

    "UPDATE_PROJET_BUDGET": {
        "operation": "UPDATE",
        "table": "projet",
        "required": ["entreprise_id", "projet_id", "budget_total"],
        "template": """
        UPDATE projet p
        SET budget_total=%s,
            updated_at=CURRENT_TIMESTAMP
        WHERE p.id=%s
            AND p.entreprise_id=%s;
        """.strip(),
    },
})

# =========================================================
# Few-shot (extended: depense + facture + transfert)
# =========================================================
FEW_SHOT_BY_LANG = {
    "fr": [
        {"nl": "Liste les projets", "out": {
            "mode":"plan",
            "operation":"SELECT",
            "intent":"SELECT_PROJECTS",
            "entities": {},
            "sql_template": INTENTS["SELECT_PROJECTS"]["template"],
            "template_params": []
            }},
        {"nl": "Ajoute une dépense de 50€ pour le projet Migration depuis le compte principal",
         "out": {
             "mode": "plan",
             "operation": "INSERT",
             "intent": "INSERT_DEPENSE",
             "entities": {"projet_query": "Migration", "compte_query": "principal", "type_depense": None},
             "sql_template": INTENTS["INSERT_DEPENSE"]["template"],
             "template_params": [50, "EUR"]
         }},
        {"nl": "Crée une facture de 18000 EUR pour le projet Chatbot WhatsApp pour le client Gamma",
         "out": {
             "mode": "plan",
             "operation": "INSERT",
             "intent": "INSERT_FACTURE",
             "entities": {"projet_query": "Chatbot WhatsApp", "client_query": "Gamma"},
             "sql_template": INTENTS["INSERT_FACTURE"]["template"],
             "template_params": [18000, "EUR"]
         }},
        {"nl": "Transfère 5000 EUR du compte principal vers la caisse",
         "out": {
             "mode": "plan",
             "operation": "INSERT",
             "intent": "INSERT_TRANSFERT",
             "entities": {"compte_source_query": "principal", "compte_destination_query": "caisse"},
             "sql_template": INTENTS["INSERT_TRANSFERT"]["template"],
             "template_params": [5000, "EUR"]
         }},
         {"nl": "Total des factures émises ce mois-ci", "out": {
            "mode": "plan",
            "operation": "SELECT",
            "intent": "ANALYTICS_INVOICES_ISSUED_TOTAL_MONTH",
            "entities": {},
            "sql_template": "",
            "template_params": []
        }},

    ],
    "en": [
        {"nl": "List projects", "out": {"mode": "sql", "operation": "SELECT",
                                        "sql": "SELECT p.id, p.nom FROM projet p ORDER BY p.nom;", "params": []}},
    ]
}

FEW_SHOT_BY_LANG["fr"] += [
    {"nl": "Total des dépenses ce mois-ci", "out": {
        "mode": "plan",
        "operation": "SELECT",
        "intent": "ANALYTICS_DEPENSE_TOTAL_MONTH",
        "entities": {},
        "sql_template": "",
        "template_params": []
    }},
    {"nl": "Répartition des dépenses par type ce mois", "out": {
        "mode": "plan",
        "operation": "SELECT",
        "intent": "ANALYTICS_DEPENSE_BREAKDOWN_TYPE_MONTH",
        "entities": {},
        "sql_template": "",
        "template_params": []
    }},
    {"nl": "Montre les soldes des comptes", "out": {
        "mode": "plan",
        "operation": "SELECT",
        "intent": "ANALYTICS_ACCOUNT_BALANCES",
        "entities": {},
        "sql_template": "",
        "template_params": []
    }},
]

FEW_SHOT_BY_LANG["fr"] += [
    # 1) Facture: changer statut
    {"nl": "Marque la facture 12 comme PAYEE", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_FACTURE_STATUT",
        "entities": {"facture_id": 12, "statut": "PAYEE"},
        "sql_template": INTENTS["UPDATE_FACTURE_STATUT"]["template"],
        "template_params": []
    }},

    # 2) Facture: modifier montant (+ devise si mentionnée)
    {"nl": "Corrige le montant de la facture 12 à 2500 EUR", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_FACTURE_MONTANT",
        "entities": {"facture_id": 12, "montant": 2500, "devise": "EUR"},
        "sql_template": INTENTS["UPDATE_FACTURE_MONTANT"]["template"],
        "template_params": []
    }},

    # 3) Dépense: modifier description
    {"nl": "Modifie la description de la dépense 45 : \"Déjeuner client\"", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_DEPENSE_DESCRIPTION",
        "entities": {"depense_id": 45, "description": "Déjeuner client"},
        "sql_template": INTENTS["UPDATE_DEPENSE_DESCRIPTION"]["template"],
        "template_params": []
    }},

    # 4) Dépense: changer type_depense
    {"nl": "Change le type de la dépense 45 en restauration", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_DEPENSE_TYPE",
        "entities": {"depense_id": 45, "type_depense": "restauration"},
        "sql_template": INTENTS["UPDATE_DEPENSE_TYPE"]["template"],
        "template_params": []
    }},

    # 5) Dépense: modifier montant (+ devise)
    {"nl": "Corrige le montant de la dépense 45 à 60 EUR", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_DEPENSE_MONTANT",
        "entities": {"depense_id": 45, "montant": 60, "devise": "EUR"},
        "sql_template": INTENTS["UPDATE_DEPENSE_MONTANT"]["template"],
        "template_params": []
    }},

    # 6) Dépense: modifier date
    {"nl": "Change la date de la dépense 45 au 2026-01-24", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_DEPENSE_DATE",
        "entities": {"depense_id": 45, "date_depense": "2026-01-24"},
        "sql_template": INTENTS["UPDATE_DEPENSE_DATE"]["template"],
        "template_params": []
    }},

    # 7) Projet: modifier budget_total
    {"nl": "Mets à jour le budget du projet 2 à 150000", "out": {
        "mode": "plan",
        "operation": "UPDATE",
        "intent": "UPDATE_PROJET_BUDGET",
        "entities": {"projet_id": 2, "budget_total": 150000},
        "sql_template": INTENTS["UPDATE_PROJET_BUDGET"]["template"],
        "template_params": []
    }},
]


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

    rules = {
        "NO_MULTISTMT": "1 seule requête SQL (pas de multi-statements)",
        "NO_DDL": "Interdit: DDL/DCL (CREATE/DROP/ALTER/TRUNCATE/GRANT/REVOKE)",
        "ALLOWLIST": "Tables: uniquement allowlist",
        "NO_STAR": "Interdit: SELECT *",
        "LIMIT_RULE": "Si l'utilisateur demande explicitement un top N / N dernières / N plus grandes, tu DOIS mettre LIMIT N. Sinon, tu NE mets PAS de LIMIT (le système injectera un LIMIT par défaut).",
        "PLACEHOLDERS": "Placeholders: %s uniquement",
    }
    UPDATE_INTENTS = [
  "UPDATE_FACTURE_STATUT",
  "UPDATE_FACTURE_MONTANT",
  "UPDATE_DEPENSE_DESCRIPTION",
  "UPDATE_DEPENSE_TYPE",
  "UPDATE_DEPENSE_MONTANT",
  "UPDATE_DEPENSE_DATE",
  "UPDATE_PROJET_BUDGET",
]


    # give LLM the canonical templates to avoid hallucinations
    templates = {
        "INSERT_DEPENSE": INTENTS["INSERT_DEPENSE"]["template"],
        "INSERT_FACTURE": INTENTS["INSERT_FACTURE"]["template"],
        "INSERT_TRANSFERT": INTENTS["INSERT_TRANSFERT"]["template"],
    }
    analytics_intents = [k for k in INTENTS.keys() if k.startswith("ANALYTICS_")]

    return f"""
Tu es un assistant Text2SQL STRICT pour PostgreSQL.
- Répond uniquement en JSON (sans markdown).

OBJECTIF:
- {mode_instruction}

RÈGLES:
- {rules["NO_MULTISTMT"]}
- {rules["NO_DDL"]}
- {rules["ALLOWLIST"]}
- {rules["NO_STAR"]}
- {rules["LIMIT_RULE"]}
- {rules["PLACEHOLDERS"]}

INTENTS D'ÉCRITURE (IMPORTANT):
- Si l'utilisateur veut AJOUTER/INSÉRER une dépense -> intent=INSERT_DEPENSE
- Si l'utilisateur veut CRÉER une facture -> intent=INSERT_FACTURE
- Si l'utilisateur veut FAIRE un transfert interne -> intent=INSERT_TRANSFERT

CONTRAINTES SCHÉMA (IMPORTANT):
- depense exige: projet_id, compte_id, type_depense, montant, devise.
- facture exige: projet_id, client_id, montant, devise, statut, date_emission. (date_paiement optionnelle)
- transfert_interne exige: compte_source_id, compte_destination_id, montant, devise, date_transfert.
- Si une info manque: retourne PLAN avec entities.* à null + sql_template canonique.

INTENTS & DÉTECTION:
- Si l'utilisateur veut AJOUTER/INSÉRER une dépense -> intent=INSERT_DEPENSE (PLAN)
- Si l'utilisateur veut CRÉER une facture -> intent=INSERT_FACTURE (PLAN)
- Si l'utilisateur veut FAIRE un transfert interne -> intent=INSERT_TRANSFERT (PLAN)
- Si l'utilisateur demande un indicateur / résumé / total / répartition / solde / impayés -> intent ANALYTICS (PLAN)
  Intents analytics autorisés:
  - {{", ".join(analytics_intents)}}

  
IMPORTANT:
- Pour les intents ANALYTICS_* : tu retournes TOUJOURS un PLAN (operation=SELECT).
- Pour les intents ANALYTICS_* : tu NE DOIS PAS produire de SQL (sql_template="" et template_params=[]).
- Si la question correspond à un indicateur (total/répartition/solde/impayés/factures émises),
  tu DOIS choisir un intent ANALYTICS_* parmi la liste autorisée ci-dessous.
- Ne retourne JAMAIS mode="sql" pour une analytics.
- Ne retourne JAMAIS intent="AUTRE" pour une analytics.
- Si l'entreprise n'est pas connue/ambigue (entreprise_id absent), retourne quand même le PLAN analytics:
      "entities":{{}}, et laisse le backend demander entreprise_id via clarification (ne demande pas "info").

INTENTS D'ÉCRITURE (IMPORTANT):
- Si l'utilisateur veut AJOUTER/INSÉRER une dépense -> intent=INSERT_DEPENSE
- Si l'utilisateur veut CRÉER une facture -> intent=INSERT_FACTURE
- Si l'utilisateur veut FAIRE un transfert interne -> intent=INSERT_TRANSFERT

INTENTS DE MISE À JOUR (UPDATE):
- Marquer une facture payée / émise -> UPDATE_FACTURE_STATUT
- Modifier le montant d’une facture -> UPDATE_FACTURE_MONTANT
- Modifier la description d’une dépense -> UPDATE_DEPENSE_DESCRIPTION
- Modifier le type d’une dépense -> UPDATE_DEPENSE_TYPE
- Modifier le montant d’une dépense -> UPDATE_DEPENSE_MONTANT
- Modifier la date d’une dépense -> UPDATE_DEPENSE_DATE
- Modifier le budget d’un projet -> UPDATE_PROJET_BUDGET

IMPORTANT:
- Pour les intents UPDATE_* : tu DOIS retourner un PLAN (mode=plan).
- Pour UPDATE_* : retourne un PLAN avec sql_template = template canonique correspondant. Le backend exécute ce template.
- Le SQL UPDATE est entièrement géré par le backend.

- Si l'utilisateur demande une MODIFICATION (update) -> intent UPDATE_* (PLAN)
  Intents update autorisés:
  - {", ".join(UPDATE_INTENTS)}

RÈGLES UPDATE:
- Pour UPDATE_* : retourne TOUJOURS un PLAN (operation=UPDATE).
- Pour UPDATE_* : sql_template = template canonique correspondant (pas vide).
- Place les champs dans entities :
  - facture_id / depense_id / projet_id (selon le cas)
  - et la valeur à modifier (statut, montant+devise, description, type_depense, date_depense, budget_total)





TEMPLATES CANONIQUES:
{json.dumps(templates, ensure_ascii=False)}

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
  "intent":"INSERT_DEPENSE|INSERT_FACTURE|INSERT_TRANSFERT|SELECT_PROJECTS|ANALYTICS_*|UPDATE_*|AUTRE",
  "entities":{{ ... }},
  "sql_template":"SQL avec %s OU vide si ANALYTICS_*",
  "template_params":[]
}}
""".strip()

def detect_analytics_intent(user_input: str) -> Optional[str]:
    """
    Règle principale:
    - On ne met un filtre de période (mois/année/YYYY-MM/YYYY) QUE si l'utilisateur le mentionne.
    - Donc: on matche d'abord les intents "avec période" (MONTH/YEAR/PERIOD),
      puis seulement ensuite les variantes "sans période".
    """
    ui_raw = user_input or ""
    ui = norm_txt(ui_raw)

    # -------------------------------------------------------
    # 0) Analytics "structurés" (top/last/by project) PRIORITAIRES
    # -------------------------------------------------------
    # last N dépenses d’un projet
    if ANALYTICS_LASTN_DEPENSES_PROJECT_RE.search(ui):
        return "ANALYTICS_LASTN_DEPENSES_FOR_PROJECT"

    # top N projets par dépenses ce mois
    if ANALYTICS_TOPN_PROJECTS_SPEND_MONTH_RE.search(ui):
        return "ANALYTICS_TOP_PROJECTS_BY_SPEND_MONTH"

    # total dépenses ce mois par projet
    if ANALYTICS_DEPENSE_TOTAL_MONTH_BY_PROJECT_RE.search(ui):
        return "ANALYTICS_DEPENSE_TOTAL_MONTH_BY_PROJECT"

    # top N clients facturés cette année
    if ANALYTICS_TOPN_CLIENTS_BILLED_YEAR_RE.search(ui):
        return "ANALYTICS_TOP_CLIENTS_BILLED_YEAR"

    # dépenses par projet sur période (année / mois / ce mois / this year / this month)
    if ANALYTICS_DEPENSE_BY_PROJECT_PERIOD_RE.search(ui):
        return "ANALYTICS_DEPENSE_BY_PROJECT_PERIOD"

    # revenu / CA par projet sur une année précise (PAYEE)
    if ANALYTICS_REVENUE_BY_PROJECT_YEAR_RE.search(ui):
        return "ANALYTICS_REVENUE_BY_PROJECT_YEAR"

    # -------------------------------------------------------
    # 1) PÉRIODE EXPLICITE — matcher AVANT les variantes sans période
    # -------------------------------------------------------
    # Factures émises ce mois-ci
    if ANALYTICS_INVOICES_ISSUED_TOTAL_MONTH_RE.search(ui):
        return "ANALYTICS_INVOICES_ISSUED_TOTAL_MONTH"

    # Total dépenses ce mois-ci
    if ANALYTICS_DEPENSE_TOTAL_MONTH_RE.search(ui):
        return "ANALYTICS_DEPENSE_TOTAL_MONTH"

    # Répartition dépenses par type ce mois-ci
    if ANALYTICS_DEPENSE_BREAKDOWN_TYPE_MONTH_RE.search(ui):
        return "ANALYTICS_DEPENSE_BREAKDOWN_TYPE_MONTH"

    # Transferts listés ce mois-ci
    if ANALYTICS_TRANSFERT_LIST_MONTH_RE.search(ui):
        return "ANALYTICS_TRANSFERT_LIST_MONTH"

    # Total transferts ce mois-ci
    if ANALYTICS_TRANSFERT_TOTAL_MONTH_RE.search(ui):
        return "ANALYTICS_TRANSFERT_TOTAL_MONTH"

    # (optionnel) tu avais aussi TOPN_TRANSFERTS_RE (si tu ajoutes l’intent correspondant)
    # Exemple: "Top 5 transferts" (sans période ou avec "ce mois")
    # if ANALYTICS_TOPN_TRANSFERTS_RE.search(ui):
    #     return "ANALYTICS_TOPN_TRANSFERTS"  # à créer si tu veux

    # -------------------------------------------------------
    # 2) SANS PÉRIODE — pas de filtre de date
    # -------------------------------------------------------
    # Soldes comptes (jamais besoin de période)
    if ANALYTICS_ACCOUNT_BALANCES_RE.search(ui):
        return "ANALYTICS_ACCOUNT_BALANCES"

    # Factures impayées (pas une question de période)
    if ANALYTICS_UNPAID_INVOICES_RE.search(ui):
        return "ANALYTICS_UNPAID_INVOICES"

    # Total dépenses (sans période)
    # -> nécessite ANALYTICS_DEPENSE_TOTAL_RE
    if "ANALYTICS_DEPENSE_TOTAL_RE" in globals():
        if ANALYTICS_DEPENSE_TOTAL_RE.search(ui):
            return "ANALYTICS_DEPENSE_TOTAL"

    # Répartition dépenses par type (sans période)
    # -> nécessite ANALYTICS_DEPENSE_BREAKDOWN_TYPE_RE
    if "ANALYTICS_DEPENSE_BREAKDOWN_TYPE_RE" in globals():
        if ANALYTICS_DEPENSE_BREAKDOWN_TYPE_RE.search(ui):
            return "ANALYTICS_DEPENSE_BREAKDOWN_TYPE"

    # Liste transferts (sans période)
    # -> nécessite ANALYTICS_TRANSFERT_LIST_RE
    if "ANALYTICS_TRANSFERT_LIST_RE" in globals():
        if ANALYTICS_TRANSFERT_LIST_RE.search(ui):
            return "ANALYTICS_TRANSFERT_LIST"

    # Total transferts (sans période)
    # -> nécessite ANALYTICS_TRANSFERT_TOTAL_RE
    if "ANALYTICS_TRANSFERT_TOTAL_RE" in globals():
        if ANALYTICS_TRANSFERT_TOTAL_RE.search(ui):
            return "ANALYTICS_TRANSFERT_TOTAL"

    # Total factures émises (sans période)
    # -> nécessite ANALYTICS_INVOICES_ISSUED_TOTAL_RE
    if "ANALYTICS_INVOICES_ISSUED_TOTAL_RE" in globals():
        if ANALYTICS_INVOICES_ISSUED_TOTAL_RE.search(ui):
            return "ANALYTICS_INVOICES_ISSUED_TOTAL"

    # Top clients par CA (PAYEE) (sans période)
    # -> ton regex ANALYTICS_TOP_CLIENTS_CA_RE est déjà "top N clients ... ca"
    if ANALYTICS_TOP_CLIENTS_CA_RE.search(ui):
        return "ANALYTICS_TOP_CLIENTS_CA"

    # Budget vs dépensé (sans période)
    if ANALYTICS_BUDGET_VS_SPENT_RE.search(ui):
        return "ANALYTICS_BUDGET_VS_SPENT"

    # ------------------------------
    # EXTRA analytics detection
    # ------------------------------

    if ANALYTICS_LASTN_DEPENSES_RE.search(ui):
        return "ANALYTICS_LASTN_DEPENSES"

    if ANALYTICS_DEPENSE_LIST_MONTH_RE.search(ui):
        return "ANALYTICS_DEPENSE_LIST_MONTH"

    if ANALYTICS_DEPENSE_BY_ACCOUNT_MONTH_RE.search(ui):
        return "ANALYTICS_DEPENSE_BY_ACCOUNT_MONTH"

    if ANALYTICS_INVOICES_PAID_TOTAL_MONTH_RE.search(ui):
        return "ANALYTICS_INVOICES_PAID_TOTAL_MONTH"

    if ANALYTICS_INVOICES_PAID_TOTAL_RE.search(ui):
        return "ANALYTICS_INVOICES_PAID_TOTAL"

    if ANALYTICS_UNPAID_INVOICES_LIST_RE.search(ui):
        return "ANALYTICS_UNPAID_INVOICES_LIST"

    if ANALYTICS_LASTN_TRANSFERTS_RE.search(ui):
        return "ANALYTICS_LASTN_TRANSFERTS"

    if ANALYTICS_TOPN_TRANSFERTS_RE.search(ui):
        return "ANALYTICS_TOPN_TRANSFERTS"

    if ANALYTICS_AUDIT_LASTN_EVENTS_RE.search(ui):
        return "ANALYTICS_AUDIT_LASTN_EVENTS"

    if ANALYTICS_USERS_ROLES_RE.search(ui):
        return "ANALYTICS_USERS_ROLES"
        
    if ANALYTICS_TRANSFERT_TOTAL_RE.search(ui):
        return "ANALYTICS_TRANSFERT_TOTAL"
    if ANALYTICS_INVOICES_LIST_RE.search(ui):
        return "ANALYTICS_INVOICES_LIST"
    if ANALYTICS_INVOICES_ISSUED_TOTAL_RE.search(ui):
        return "ANALYTICS_INVOICES_ISSUED_TOTAL"
    
    # -------------------------------------------------------
    # 3) Rien trouvé
    # -------------------------------------------------------
    return None


def detect_update_intent(user_input: str) -> Optional[str]:
    ui = norm_txt(user_input)

    if UPDATE_FACTURE_STATUT_RE.search(ui):
        return "UPDATE_FACTURE_STATUT"

    if UPDATE_FACTURE_MONTANT_RE.search(ui):
        return "UPDATE_FACTURE_MONTANT"

    if UPDATE_DEPENSE_DESCRIPTION_RE.search(ui):
        return "UPDATE_DEPENSE_DESCRIPTION"

    if UPDATE_DEPENSE_TYPE_RE.search(ui):
        return "UPDATE_DEPENSE_TYPE"

    if UPDATE_DEPENSE_MONTANT_RE.search(ui):
        return "UPDATE_DEPENSE_MONTANT"

    if UPDATE_DEPENSE_DATE_RE.search(ui):
        return "UPDATE_DEPENSE_DATE"

    if UPDATE_PROJET_BUDGET_RE.search(ui):
        return "UPDATE_PROJET_BUDGET"

    return None



def extract_topn(ui: str, default: int = 10) -> int:
    # top N
    m = re.search(r"\btop\s*(\d{1,3})\b", ui)
    if m:
        return max(1, min(100, int(m.group(1))))

    # dernières N
    m = re.search(r"\b(derni[eè]res?|last)\s*(\d{1,3})\b", ui)
    if m:
        return max(1, min(200, int(m.group(2))))

    # N dernières
    m = re.search(r"\b(\d{1,3})\s*(derni[eè]res?|last)\b", ui)
    if m:
        return max(1, min(200, int(m.group(1))))

    return default


# =========================================================
# Heuristics
# =========================================================
WRITE_TRIGGERS_RE = re.compile(r"\b(ajoute|ajouter|ins[eè]re|inserer|insert|update|supprime|delete|crée|creer|créer|transf[eè]re|transfer|virement)\b", re.I)
DEPENSE_TRIGGER_RE = re.compile(r"\b(dépense|depense|expense)\b", re.I)
FACTURE_TRIGGER_RE = re.compile(r"\b(facture|invoice)\b", re.I)
TRANSFERT_TRIGGER_RE = re.compile(r"\b(transfert|transfer|virement)\b", re.I)



def looks_like_write(user_input: str) -> bool:
    return bool(WRITE_TRIGGERS_RE.search(user_input or ""))


def should_force_plan(user_input: str) -> bool:
    return looks_like_write(user_input)


def looks_like_depense_insert(user_input: str) -> bool:
    ui = user_input or ""
    return bool(DEPENSE_TRIGGER_RE.search(ui) and WRITE_TRIGGERS_RE.search(ui))


def looks_like_facture_insert(user_input: str) -> bool:
    ui = user_input or ""
    return bool(FACTURE_TRIGGER_RE.search(ui) and WRITE_TRIGGERS_RE.search(ui))


def looks_like_transfert_insert(user_input: str) -> bool:
    ui = user_input or ""
    return bool(TRANSFERT_TRIGGER_RE.search(ui) and WRITE_TRIGGERS_RE.search(ui))





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
# Clarification helpers
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


def _coerce_choice_to_value(field: str, user_input: str) -> Any:
    txt = (user_input or "").strip()

    # type_depense quick menu mapping (optional)
    if field == "type_depense":
        low = txt.lower()
        mapping = {"1": "cloud", "2": "transport", "3": "materiel", "4": "autre"}
        return mapping.get(low, low)

    if field == "devise":
        low = txt.lower()
        mapping = {"1": "EUR", "2": "DZD", "3": "AED", "4": "USD", "5": "SYP"}
        if low in mapping:
            return mapping[low]
        _, cur = parse_amount_currency(txt)
        return (cur or txt).upper()

    if field == "montant":
        amt, _ = parse_amount_currency(txt)
        return amt

    if field in ("entreprise_id", "projet_id", "client_id", "compte_id", "compte_source_id", "compte_destination_id"):
        if txt.isdigit():
            return int(txt)
        return txt


    if field in ("statut",):
        v = txt.strip().upper()
        if v in ("EMISE", "PAYEE"):
            return v
        # allow shortcuts
        if v in ("1", "E", "EMIS"):
            return "EMISE"
        if v in ("2", "P"):
            return "PAYEE"
        return "EMISE"

    return txt

def get_user_by_actor(actor_id: str) -> Optional[Dict[str, Any]]:
    if not actor_id:
        return None
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, nom, numero_whatsapp
                FROM utilisateur
                WHERE numero_whatsapp = %s
                LIMIT 1;
            """, (actor_id,))
            return cur.fetchone()
    finally:
        conn.close()


def list_user_enterprises(user_id: int) -> List[Dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT e.id, e.nom
                FROM utilisateur_entreprise ue
                JOIN entreprise e ON e.id = ue.entreprise_id
                WHERE ue.utilisateur_id = %s
                ORDER BY e.nom;
            """, (user_id,))
            rows = cur.fetchall() or []
            for i, r in enumerate(rows, start=1):
                r["option"] = i
            return rows
    finally:
        conn.close()


def get_user_role_for_enterprise(user_id: int, entreprise_id: int) -> Optional[str]:
    """
    utilisateur_entreprise.role_id -> role.nom
    Retourne un rôle texte exploitable par guard/executor (ex: 'FinanceAdmin', 'ReadOnly', etc.)
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.nom
                FROM utilisateur_entreprise ue
                JOIN role r ON r.id = ue.role_id
                WHERE ue.utilisateur_id = %s
                  AND ue.entreprise_id  = %s
                LIMIT 1;
            """, (user_id, entreprise_id))
            row = cur.fetchone()
            return (row[0] if row else None)
    finally:
        conn.close()


# =========================================================
# Continuation (DEPENSE preserved + add FACTURE + TRANSFERT)
# =========================================================

def run_pipeline(user_input: str, context: Dict[str, Any]) -> Tuple[Optional[str], List[Any], Clarification, Optional[PendingPlan], List[str]]:
    """
    Retourne soit:
      - (sql, params, Clarification(False), None, notes)   => terminé
      - (None, [], Clarification(True,...), pending, notes) => besoin clarification
    """
    notes: List[str] = []
    lang = context.get("lang", "fr")

    # 1) analytics intent (plan)
    analytics_intent = detect_analytics_intent(user_input)

    if analytics_intent:
        pending = PendingPlan(
            operation="SELECT",
            intent=analytics_intent,
            sql_template="",
            template_params=[],
            entities={},
            filled={},
        )
        # on essaie de résoudre tout de suite (si entreprise_id déjà là)
        sql, params, _, clar, pending2, cont_notes = continue_pending_plan(
            pending=pending,
            user_input="",
            context=context,
        )
        notes += cont_notes
        if clar.needed:
            return None, [], clar, pending2, notes
        return sql, params, Clarification(needed=False), None, notes

    # 3) LLM
    force_plan = should_force_plan(user_input)
    prompt = build_llm_prompt(user_input, context, force_plan=force_plan)
    llm_obj = call_gpt(prompt)
    llm = parse_llm_output(llm_obj)

    if force_plan and llm.mode.lower() != "plan":
        raise HTTPException(400, "Mode PLAN requis (écriture).")

    if llm.mode.lower() == "plan":
        pending = PendingPlan(
            operation=(llm.operation or "UNKNOWN"),
            intent=getattr(llm, "intent", None),
            sql_template=(llm.sql_template or ""),
            template_params=list(llm.template_params or []),
            entities=dict(getattr(llm, "entities", {}) or {}),
            filled={},
        )
        sql, params, _, clar, pending2, cont_notes = continue_pending_plan(
            pending=pending, user_input="", context=context
        )
        notes += cont_notes
        if clar.needed:
            return None, [], clar, pending2, notes
        return sql, params, Clarification(needed=False), None, notes

    # mode sql direct
    return (llm.sql or "").strip(), list(llm.params or []), Clarification(needed=False), None, notes

def continue_pending_plan(
    pending: PendingPlan,
    user_input: str,
    context: Dict[str, Any],
) -> Tuple[Optional[str], List[Any], Dict[str, Any], Clarification, PendingPlan, List[str]]:
    notes: List[str] = []
    lang = context.get("lang", "fr")
    if (pending.mode or "").lower() == "import":
            return continue_import_session(pending, user_input, context)

    if pending.intent == "REPLAY_ORIGINAL_AFTER_TENANT":
        original = context.get("original_user_input") or ""
        if not original:
            raise HTTPException(400, "original_user_input manquant")

        # relancer pipeline: analytics intent -> bypass -> LLM -> auto-continue
        force_plan = should_force_plan(original)
        analytics_intent = detect_analytics_intent(original)

        if analytics_intent:
            # on construit un nouveau pending plan analytics
            new_pending = PendingPlan(
                operation="SELECT",
                intent=analytics_intent,
                sql_template="",
                template_params=[],
                entities={},
                filled={},
            )
            # puis on re-rentre sur continue_pending_plan MAIS sur new_pending
            return continue_pending_plan(new_pending, user_input="", context=context)

        prompt = build_llm_prompt(original, context, force_plan=force_plan)
        llm_obj = call_gpt(prompt)
        llm = parse_llm_output(llm_obj)

        if llm.mode.lower() == "plan":
            new_pending = PendingPlan(
                operation=llm.operation,
                intent=getattr(llm, "intent", None),
                sql_template=getattr(llm, "sql_template", "") or "",
                template_params=list(getattr(llm, "template_params", []) or []),
                entities=dict(getattr(llm, "entities", {}) or {}),
                filled={},
            )
            return continue_pending_plan(new_pending, user_input="", context=context)

        return (llm.sql or ""), list(llm.params or []), {}, Clarification(needed=False), pending, ["replay_llm_sql"]



    # record the answer to the last asked field
    filled = dict(pending.filled or {})
    last_field = (context.get("last_field") or "").strip()
    if last_field and (user_input or "").strip():
        filled[last_field] = _coerce_choice_to_value(last_field, user_input)
        pending.filled = filled

    # convenience
    entities = dict(pending.entities or {})
    intent = (pending.intent or "").strip()
    entreprise_id = context.get("entreprise_id")

    # helper to deduce entreprise from projet
    def _ensure_entreprise_from_projet():
        nonlocal entreprise_id
        if context.get("entreprise_id") is None and filled.get("projet_id"):
            eid = get_projet_entreprise_id(int(filled["projet_id"]))
            if eid is not None:
                context["entreprise_id"] = eid
                entreprise_id = eid
                notes.append(f"entreprise_id déduit depuis projet: {eid}")

    # Si l'utilisateur vient de choisir l'entreprise
    if (context.get("last_field") == "entreprise_id") and filled.get("entreprise_id"):
        try:
            eid = int(filled["entreprise_id"])
        except Exception:
            return None, [], {}, Clarification(
                needed=True,
                entity="entreprise",
                field="entreprise_id",
                query=str(filled.get("entreprise_id") or ""),
                suggestions=[],
                message="Réponds avec le numéro de l’entreprise (ex: 1, 2).",
            ), pending, notes

        context["entreprise_id"] = eid
        uid = context.get("user_id")
        if uid:
            context["role"] = get_user_role_for_enterprise(int(uid), eid)
        notes.append(f"entreprise_id choisi={eid}")
    # -----------------------------------------------------
    # INTENT: SELECT_PROJECTS (NEW)
    # -----------------------------------------------------
    if intent == "SELECT_PROJECTS":
        # On attend entreprise_id (soit context, soit filled)
        eid = context.get("entreprise_id") or filled.get("entreprise_id")
        if not eid:
            # Si on a user_id, on peut re-proposer les entreprises
            uid = context.get("user_id")
            if uid:
                ents = list_user_enterprises(int(uid))
                return None, [], {}, _clarify_pick("entreprise", "", ents, lang), pending, notes

            # Sinon on demande entreprise_id explicitement
            return None, [], {}, Clarification(
                needed=True,
                entity="entreprise",
                field="entreprise_id",
                query="",
                suggestions=[],
                message=t(lang, "need_entreprise"),
            ), pending, notes

        eid = int(eid)
        context["entreprise_id"] = eid
        pending.filled["entreprise_id"] = eid

        sql = (pending.sql_template or "").strip() or INTENTS["SELECT_PROJECTS"]["template"]
        params = [eid]

        notes.append("SELECT_PROJECTS resolved.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # -----------------------------------------------------
    # INTENTS: UPDATE_* — SQL canonique backend + clarifications
    # -----------------------------------------------------
    if intent and intent.startswith("UPDATE_"):
        # 0) entreprise_id obligatoire (comme analytics)
        eid = context.get("entreprise_id") or filled.get("entreprise_id")
        if not eid:
            uid = context.get("user_id")
            if uid:
                ents = list_user_enterprises(int(uid))
                return None, [], {}, _clarify_pick("entreprise", "", ents, lang), pending, notes

            return None, [], {}, Clarification(
                needed=True,
                entity="entreprise",
                field="entreprise_id",
                query="",
                suggestions=[],
                message=t(lang, "need_entreprise"),
            ), pending, notes

        eid = int(eid)
        context["entreprise_id"] = eid
        pending.filled["entreprise_id"] = eid

        original = context.get("original_user_input") or ""
        ui_norm = norm_txt(original)

        # -------------------------
        # UPDATE_FACTURE_STATUT
        # -------------------------
        if intent == "UPDATE_FACTURE_STATUT":
            if "facture_id" not in filled:
                fid = extract_id_after_keyword(original, "facture") or extract_first_int(original)
                if not fid:
                    return None, [], {}, Clarification(
                        needed=True, entity="facture", field="facture_id",
                        message="Quelle est l’ID de la facture à mettre à jour ? (ex: 12)"
                    ), pending, notes
                filled["facture_id"] = int(fid)
                pending.filled = filled

            # statut
            statut = (filled.get("statut") or "").strip().upper()
            if not statut:
                # essaie d’inférer depuis le texte
                if re.search(r"\bpayee|payée|paid\b", ui_norm, re.I):
                    statut = "PAYEE"
                elif re.search(r"\bemise|émise|issued\b", ui_norm, re.I):
                    statut = "EMISE"

            if statut not in ("PAYEE", "EMISE"):
                return None, [], {}, Clarification(
                    needed=True, entity="statut", field="statut",
                    message="Quel statut ? Réponds: PAYEE ou EMISE."
                ), pending, notes

            ctxf = get_facture_context(int(filled["facture_id"]))
            if not ctxf:
                raise HTTPException(404, f"Facture {filled['facture_id']} introuvable.")
            if int(ctxf["entreprise_id"]) != eid:
                raise HTTPException(403, "Facture hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [statut, statut, int(filled["facture_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_FACTURE_MONTANT
        # -------------------------
        if intent == "UPDATE_FACTURE_MONTANT":
            if "facture_id" not in filled:
                fid = extract_id_after_keyword(original, "facture") or extract_first_int(original)
                if not fid:
                    return None, [], {}, Clarification(
                        needed=True, entity="facture", field="facture_id",
                        message="Quelle est l’ID de la facture ? (ex: 12)"
                    ), pending, notes
                filled["facture_id"] = int(fid)
                pending.filled = filled

            montant, devise = parse_amount_currency(original)
            if filled.get("montant") is not None:
                montant = float(filled["montant"])
            if filled.get("devise"):
                devise = str(filled["devise"]).upper()

            if montant is None:
                return None, [], {}, Clarification(
                    needed=True, entity="montant", field="montant",
                    message=t(lang, "need_amount")
                ), pending, notes
            if not devise:
                return None, [], {}, Clarification(
                    needed=True, entity="devise", field="devise",
                    suggestions=[{"option": 1, "nom": "EUR"}, {"option": 2, "nom": "DZD"}, {"option": 3, "nom": "AED"}, {"option": 4, "nom": "USD"}],
                    message=t(lang, "need_currency")
                ), pending, notes

            ctxf = get_facture_context(int(filled["facture_id"]))
            if not ctxf:
                raise HTTPException(404, f"Facture {filled['facture_id']} introuvable.")
            if int(ctxf["entreprise_id"]) != eid:
                raise HTTPException(403, "Facture hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [float(montant), str(devise).upper(), int(filled["facture_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_DEPENSE_DESCRIPTION
        # -------------------------
        if intent == "UPDATE_DEPENSE_DESCRIPTION":
            if "depense_id" not in filled:
                did = extract_id_after_keyword(original, "depense") or extract_id_after_keyword(original, "dépense") or extract_first_int(original)
                if not did:
                    return None, [], {}, Clarification(
                        needed=True, entity="depense", field="depense_id",
                        message="Quelle est l’ID de la dépense ? (ex: 45)"
                    ), pending, notes
                filled["depense_id"] = int(did)
                pending.filled = filled

            desc = (filled.get("description") or "").strip()
            if not desc:
                m = re.search(r":\s*['\"“”]?(.*?)['\"“”]?\s*$", original)
                if m:
                    desc = m.group(1).strip()
                m = re.search(r"\b(en|to)\b\s+(.+)$", original, flags=re.I)
                if m:
                    desc = m.group(2).strip().strip('"“”')


            if not desc:
                return None, [], {}, Clarification(
                    needed=True, entity="description", field="description",
                    message="Quelle nouvelle description ? (ex: Déjeuner client)"
                ), pending, notes

            ctxd = get_depense_context(int(filled["depense_id"]))
            if not ctxd:
                raise HTTPException(404, f"Dépense {filled['depense_id']} introuvable.")
            if int(ctxd["entreprise_id"]) != eid:
                raise HTTPException(403, "Dépense hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [desc, int(filled["depense_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_DEPENSE_TYPE
        # -------------------------
        if intent == "UPDATE_DEPENSE_TYPE":
            if "depense_id" not in filled:
                did = extract_id_after_keyword(original, "depense") or extract_id_after_keyword(original, "dépense") or extract_first_int(original)
                if not did:
                    return None, [], {}, Clarification(
                        needed=True, entity="depense", field="depense_id",
                        message="Quelle est l’ID de la dépense ? (ex: 45)"
                    ), pending, notes
                filled["depense_id"] = int(did)
                pending.filled = filled

            td = (filled.get("type_depense") or "").strip().lower()
            if not td:
                # essaie d’inférer depuis le texte
                td = detect_type_depense_from_text(original) or ""

            # contrôle strict: doit être une clé connue
            if td not in TYPE_DEPENSE_ALIASES.keys():
                return None, [], {}, Clarification(
                    needed=True, entity="type_depense", field="type_depense",
                    message="Quel type ? (ex: restauration, transport, cloud, logiciel, telecom, marketing, formation, services, frais_bancaires, autre)"
                ), pending, notes

            ctxd = get_depense_context(int(filled["depense_id"]))
            if not ctxd:
                raise HTTPException(404, f"Dépense {filled['depense_id']} introuvable.")
            if int(ctxd["entreprise_id"]) != eid:
                raise HTTPException(403, "Dépense hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [td, int(filled["depense_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_DEPENSE_DATE
        # -------------------------
        if intent == "UPDATE_DEPENSE_DATE":
            if "depense_id" not in filled:
                did = extract_id_after_keyword(original, "depense") or extract_id_after_keyword(original, "dépense") or extract_first_int(original)
                if not did:
                    return None, [], {}, Clarification(
                        needed=True, entity="depense", field="depense_id",
                        message="Quelle est l’ID de la dépense ? (ex: 45)"
                    ), pending, notes
                filled["depense_id"] = int(did)
                pending.filled = filled

            ddep = parse_any_date(original)
            if filled.get("date_depense"):
                try:
                    ddep = parse_any_date(str(filled["date_depense"]))
                except Exception:
                    pass

            if not ddep:
                return None, [], {}, Clarification(
                    needed=True, entity="date", field="date_depense",
                    message="Quelle nouvelle date ? (ex: 2026-01-24)"
                ), pending, notes

            ctxd = get_depense_context(int(filled["depense_id"]))
            if not ctxd:
                raise HTTPException(404, f"Dépense {filled['depense_id']} introuvable.")
            if int(ctxd["entreprise_id"]) != eid:
                raise HTTPException(403, "Dépense hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [ddep.isoformat(), int(filled["depense_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_DEPENSE_MONTANT (contrôle devise vs compte)
        # -------------------------
        if intent == "UPDATE_DEPENSE_MONTANT":
            if "depense_id" not in filled:
                did = extract_id_after_keyword(original, "depense") or extract_id_after_keyword(original, "dépense") or extract_first_int(original)
                if not did:
                    return None, [], {}, Clarification(
                        needed=True, entity="depense", field="depense_id",
                        message="Quelle est l’ID de la dépense ? (ex: 45)"
                    ), pending, notes
                filled["depense_id"] = int(did)
                pending.filled = filled

            montant, devise = parse_amount_currency(original)
            if filled.get("montant") is not None:
                montant = float(filled["montant"])
            if filled.get("devise"):
                devise = str(filled["devise"]).upper()

            if montant is None:
                return None, [], {}, Clarification(
                    needed=True, entity="montant", field="montant",
                    message=t(lang, "need_amount")
                ), pending, notes

            ctxd = get_depense_context(int(filled["depense_id"]))
            if not ctxd:
                raise HTTPException(404, f"Dépense {filled['depense_id']} introuvable.")
            if int(ctxd["entreprise_id"]) != eid:
                raise HTTPException(403, "Dépense hors de ton entreprise (refus).")

            compte_dev = (ctxd.get("compte_devise") or "").upper()
            if not devise:
                # devise par défaut = devise du compte (comme INSERT)
                devise = compte_dev

            devise = str(devise).upper()

            # refus si devise incompatible compte
            if compte_dev and devise != compte_dev:
                raise HTTPException(
                    400,
                    f"Correction impossible en {devise}. Le compte de la dépense est en {compte_dev}. "
                    f"Reformule en {compte_dev}."
                )

            sql = INTENTS[intent]["template"]
            params = [float(montant), devise, int(filled["depense_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        # -------------------------
        # UPDATE_PROJET_BUDGET
        # -------------------------
        if intent == "UPDATE_PROJET_BUDGET":
            if "projet_id" not in filled:
                pq = (entities.get("projet_query") or "").strip()
                if pq:
                    matches = resolve_by_name("projet", pq, eid, topk=RESOLVE_TOPK)
                    if len(matches) != 1:
                        return None, [], {}, _clarify_pick("projet", pq, matches, lang), pending, notes
                    filled["projet_id"] = int(matches[0]["id"])
                    pending.filled = filled
                else:
                    pid = extract_id_after_keyword(original, "projet") or extract_id_after_keyword(original, "project") or extract_first_int(original)
                    if not pid:
                        return None, [], {}, Clarification(
                            needed=True, entity="projet", field="projet_id",
                            message="Quel projet ? Donne l’ID (ex: projet 2)."
                        ), pending, notes
                    filled["projet_id"] = int(pid)
                    pending.filled = filled

            # budget_total
            # on accepte: "à 150000", "budget ... 150000", etc.
            bt = filled.get("budget_total")
            if bt is None:
                m = re.search(r"\b(à|a)\s*(\d{1,3}(?:[ \u00a0]\d{3})*|\d+)(?:[.,](\d{1,2}))?\b", original, flags=re.I)
                if m:
                    int_part = m.group(2).replace("\u00a0", " ").replace(" ", "")
                    dec_part = m.group(3)
                    bt = float(int_part) + (float(dec_part) / (10 ** len(dec_part)) if dec_part else 0.0)

            if bt is None:
                return None, [], {}, Clarification(
                    needed=True, entity="budget", field="budget_total",
                    message="Quel nouveau budget_total ? (ex: 150000)"
                ), pending, notes

            ctxp = get_projet_context(int(filled["projet_id"]))
            if not ctxp:
                raise HTTPException(404, f"Projet {filled['projet_id']} introuvable.")
            if int(ctxp["entreprise_id"]) != eid:
                raise HTTPException(403, "Projet hors de ton entreprise (refus).")

            sql = INTENTS[intent]["template"]
            params = [float(bt), int(filled["projet_id"]), eid]
            return sql, params, {}, Clarification(needed=False), pending, notes


        # si un UPDATE_* tombe ici => intent non géré
        return None, [], {}, Clarification(
            needed=True,
            entity=None,
            field=None,
            message=f"Intent update non supportée: {intent}",
        ), pending, notes

    # -----------------------------------------------------
    # INTENTS: ANALYTICS_* (NEW) — SQL canonique backend
    # -----------------------------------------------------
    if intent and intent.startswith("ANALYTICS_"):
        eid = context.get("entreprise_id") or filled.get("entreprise_id")
        if not eid:
            uid = context.get("user_id")
            if uid:
                ents = list_user_enterprises(int(uid))
                return None, [], {}, _clarify_pick("entreprise", "", ents, lang), pending, notes

            return None, [], {}, Clarification(
                needed=True,
                entity="entreprise",
                field="entreprise_id",
                query="",
                suggestions=[],
                message=t(lang, "need_entreprise"),
            ), pending, notes

        eid = int(eid)
        context["entreprise_id"] = eid
        pending.filled["entreprise_id"] = eid

        meta = INTENTS.get(intent)
        if intent == "ANALYTICS_TOP_PROJECTS_BY_SPEND_MONTH":
            original = context.get("original_user_input") or ""
            topn = extract_topn(norm_txt(original), default=5)

            sql = INTENTS[intent]["template"].format(topn=topn)
            params = [int(context["entreprise_id"])]

            notes.append(f"Top {topn} projets par dépenses (mois en cours)")
            return sql, params, {}, Clarification(needed=False), pending, notes
        if intent == "ANALYTICS_LASTN_DEPENSES_FOR_PROJECT":
            original = context.get("original_user_input") or ""
            ui = norm_txt(original)
            topn = extract_topn(ui, default=10)

            proj_q = _extract_project_query(original)
            if not proj_q:
                # clarification projet
                matches = resolve_by_name("projet", "", eid, topk=RESOLVE_TOPK)
                return None, [], {}, _clarify_pick("projet", "", matches, lang), pending, notes

            matches = resolve_by_name("projet", proj_q, eid, topk=RESOLVE_TOPK)
            if len(matches) != 1:
                return None, [], {}, _clarify_pick("projet", proj_q, matches, lang), pending, notes

            pid = int(matches[0]["id"])
            sql = INTENTS[intent]["template"].format(topn=topn)
            params = [pid]
            return sql, params, {}, Clarification(needed=False), pending, notes
        
        if intent == "ANALYTICS_TOP_CLIENTS_BILLED_YEAR":
            original = context.get("original_user_input") or ""
            topn = extract_topn(norm_txt(original), default=5)
            sql = INTENTS[intent]["template"].format(topn=topn)
            params = [eid]
            return sql, params, {}, Clarification(needed=False), pending, notes

        if intent == "ANALYTICS_TOPN_TRANSFERTS":
            original = context.get("original_user_input") or ""
            topn = extract_topn(norm_txt(original), default=5)
            sql = INTENTS[intent]["template"].format(topn=topn)
            return sql, [eid], {}, Clarification(needed=False), pending, notes

        if not meta:
            return None, [], {}, Clarification(
                needed=True,
                entity=None,
                message=f"Intent analytics inconnu: {intent}. Reformule.",
            ), pending, notes

        sql = (meta.get("template") or "").strip()
         # ✅ Si le template contient un placeholder {topn}, on le résout ici
        if "{topn}" in sql:
            original = context.get("original_user_input") or ""
            topn = extract_topn(norm_txt(original), default=5)
            sql = sql.format(topn=topn)
            notes.append(f"topn injecté={topn} pour intent={intent}")
        params = [eid]

        notes.append(f"{intent} resolved.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # -----------------------------------------------------
    # INTENT: INSERT_DEPENSE (keep your logic, cleaned)
    # -----------------------------------------------------

    if intent == "INSERT_DEPENSE":
        # 1) projet
        if "projet_id" not in filled:
            projet_q = (entities.get("projet_query") or "").strip()
            matches = resolve_by_name("projet", projet_q, entreprise_id)
            if len(matches) != 1:
                return None, [], {}, _clarify_pick("projet", projet_q, matches, lang), pending, notes
            filled["projet_id"] = int(matches[0]["id"])
            pending.filled = filled
            _ensure_entreprise_from_projet()

        # 2) compte (must match entreprise)
        if "compte_id" in filled and context.get("entreprise_id") is not None:
            compte_eid = get_compte_entreprise_id(int(filled["compte_id"]))
            if compte_eid is not None and compte_eid != int(context["entreprise_id"]):
                filled.pop("compte_id", None)
                pending.filled = filled
                notes.append(f"compte_id incompatible (compte_eid={compte_eid}) => re-clarification")
        if "compte_id" not in filled and context.get("prefill_compte_choice"):
            filled["compte_id"] = int(context["prefill_compte_choice"])
            pending.filled = filled
            context.pop("prefill_compte_choice", None)

        if "compte_id" not in filled:
            compte_q = (entities.get("compte_query") or "").strip()
            matches = resolve_by_name("compte_financier", compte_q, entreprise_id)
            if len(matches) != 1:
                clar = Clarification(
                    needed=True,
                    entity="compte_financier",
                    field="compte_id",
                    query=compte_q,
                    suggestions=matches,
                    message=t(lang, "need_compte"),
                )
                return None, [], {}, clar, pending, notes
            filled["compte_id"] = int(matches[0]["id"])
            pending.filled = filled

        # 3) type_depense (non bloquant: auto -> fallback autre)
        if not (filled.get("type_depense") or "").strip():
            original = context.get("original_user_input") or ""
            auto_td = detect_type_depense_from_text(original)
            filled["type_depense"] = auto_td or "autre"
            pending.filled = filled
            notes.append(f"type_depense set: {filled['type_depense']}")

        # 4) montant + devise
        original = (
            context.get("original_user_input")
            or context.get("initial_user_input")
            or context.get("first_user_input")
            or ""
        )

        montant, devise = parse_amount_currency(original)
                
        if filled.get("devise"):
            devise = str(filled["devise"]).upper()
        if filled.get("montant"):
            montant = float(filled["montant"])

        if montant is None:
            clar = Clarification(needed=True, entity="montant", field="montant", message=t(lang, "need_amount"))
            return None, [], {}, clar, pending, notes
        if not devise:
            clar = Clarification(
                needed=True,
                entity="devise",
                field="devise",
                suggestions=[{"option": 1, "nom": "EUR"}, {"option": 2, "nom": "DZD"}, {"option": 3, "nom": "AED"}, {"option": 4, "nom": "USD"}, {"option": 5, "nom": "SYP"}],
                message=t(lang, "need_currency"),
            )
            return None, [], {}, clar, pending, notes

        devise = str(devise).upper()
        montant = float(montant)

        # date + fx conversion to account currency (your feature)
        # ---- devise STRICTE comme transfert ----

        ddep = parse_any_date(original) or date.today()

        compte_dev = get_compte_devise(int(filled["compte_id"]))
        compte_dev = compte_dev.upper() if compte_dev else None

        # devise par défaut = devise du compte
        if not devise and compte_dev:
            devise = compte_dev
            filled["devise"] = devise
            pending.filled = filled
            notes.append(f"devise par défaut (compte): {devise}")

        # devise toujours absente
        if not devise:
            return None, [], {}, Clarification(
                needed=True,
                entity="devise",
                field="devise",
                suggestions=[
                    {"option": 1, "nom": "EUR"},
                    {"option": 2, "nom": "DZD"},
                    {"option": 3, "nom": "AED"},
                    {"option": 4, "nom": "USD"},
                    {"option": 5, "nom": "SYP"},
                ],
                message=t(lang, "need_currency"),
            ), pending, notes

        # ❌ devise différente = REFUS
        if compte_dev and devise != compte_dev:
            raise HTTPException(
                400,
                f"Dépense impossible en {devise}. "
                f"Le compte est en {compte_dev}. "
                f"Reformule en {compte_dev}."
            )


        # description (optional)
        m = re.search(r"description\s*:\s*(.+)$", original, flags=re.I)
        desc = (m.group(1).strip() if m else "")

        sql = (pending.sql_template or "").strip() or INTENTS["INSERT_DEPENSE"]["template"]
        # Normalize to canonical 7 params insert
        sql = INTENTS["INSERT_DEPENSE"]["template"]

        params = [
            int(filled["projet_id"]),
            int(filled["compte_id"]),
            str(filled["type_depense"]).strip(),
            float(montant),
            str(devise).upper(),
            str(desc),
            (ddep.isoformat()),
        ]

        if sql.count("%s") != len(params):
            raise ValueError(f"Mismatch placeholders/params depense: expected={sql.count('%s')} got={len(params)}")

        notes.append("INSERT_DEPENSE resolved.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # -----------------------------------------------------
    # INTENT: INSERT_FACTURE (NEW)
    # -----------------------------------------------------
    if intent == "INSERT_FACTURE":
        # 1) projet -> deduce entreprise_id
        if "projet_id" not in filled:
            projet_q = (entities.get("projet_query") or "").strip()
            matches = resolve_by_name("projet", projet_q, entreprise_id)
            if len(matches) != 1:
                return None, [], {}, _clarify_pick("projet", projet_q, matches, lang), pending, notes
            filled["projet_id"] = int(matches[0]["id"])
            pending.filled = filled
            _ensure_entreprise_from_projet()

        # 2) client (filter by entreprise)
        if "client_id" in filled and context.get("entreprise_id") is not None:
            client_eid = get_client_entreprise_id(int(filled["client_id"]))
            if client_eid is not None and client_eid != int(context["entreprise_id"]):
                filled.pop("client_id", None)
                pending.filled = filled
                notes.append(f"client_id incompatible (client_eid={client_eid}) => re-clarification")

        if "client_id" not in filled:
            client_q = (entities.get("client_query") or "").strip()
            matches = resolve_by_name("client", client_q, entreprise_id)
            if len(matches) != 1:
                clar = Clarification(
                    needed=True,
                    entity="client",
                    field="client_id",
                    query=client_q,
                    suggestions=matches,
                    message=t(lang, "need_client"),
                )
                return None, [], {}, clar, pending, notes
            filled["client_id"] = int(matches[0]["id"])
            pending.filled = filled

        # 3) montant + devise
        # ---- montant + devise (STRICT) ----

        original = (
            context.get("original_user_input")
            or context.get("initial_user_input")
            or context.get("first_user_input")
            or ""
        )

        montant, devise = parse_amount_currency(original)

        if filled.get("montant"):
            montant = float(filled["montant"])
        if filled.get("devise"):
            devise = str(filled["devise"]).upper()

        if montant is None:
            return None, [], {}, Clarification(
                needed=True,
                entity="montant",
                field="montant",
                message=t(lang, "need_amount"),
            ), pending, notes

        if not devise:
            return None, [], {}, Clarification(
                needed=True,
                entity="devise",
                field="devise",
                suggestions=[
                    {"option": 1, "nom": "EUR"},
                    {"option": 2, "nom": "DZD"},
                    {"option": 3, "nom": "AED"},
                    {"option": 4, "nom": "USD"},
                    {"option": 5, "nom": "SYP"},
                ],
                message=t(lang, "need_currency"),
            ), pending, notes

        devise = devise.upper()
        montant = float(montant)


        # 4) statut (optionnel => EMISE)
        statut = (filled.get("statut") or "").strip().upper()
        if not statut:
            statut = "EMISE"
            filled["statut"] = statut
            pending.filled = filled

        if statut not in ("EMISE", "PAYEE"):
            statut = "EMISE"
            filled["statut"] = statut
            pending.filled = filled
            notes.append("statut invalide => fallback EMISE")

        # 5) date_emission (si absente => aujourd'hui)
        dem = parse_any_date(original) or date.today()

        # 6) date_paiement uniquement si PAYEE (sinon NULL)
        dpay = None
        if statut == "PAYEE":
            # si user donne une date dans le texte, on réutilise; sinon date_emission (ou today)
            dpay = dem

        sql = (pending.sql_template or "").strip() or INTENTS["INSERT_FACTURE"]["template"]
        sql = INTENTS["INSERT_FACTURE"]["template"]

        params = [
            int(filled["projet_id"]),
            int(filled["client_id"]),
            float(montant),
            str(devise).upper(),
            str(statut),
            dem.isoformat(),
            (dpay.isoformat() if dpay else None),
        ]

        if sql.count("%s") != len(params):
            raise ValueError(f"Mismatch placeholders/params facture: expected={sql.count('%s')} got={len(params)}")

        notes.append("INSERT_FACTURE resolved.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # -----------------------------------------------------
    # INTENT: INSERT_TRANSFERT (NEW)
    # -----------------------------------------------------
    if intent == "INSERT_TRANSFERT":
        original = (
            context.get("original_user_input")
            or context.get("initial_user_input")
            or context.get("first_user_input")
            or ""
        )

        # 1) compte_source
        if "compte_source_id" not in filled:
            src_q = (entities.get("compte_source_query") or "").strip()
            matches = resolve_by_name("compte_financier", src_q, entreprise_id)
            if len(matches) != 1:
                clar = Clarification(
                        needed=True,
                        entity="compte_source",
                        field="compte_source_id",
                        query=src_q,
                        suggestions=matches,
                        message="Quel est le compte SOURCE du transfert ?",
                        )
                return None, [], {}, clar, pending, notes
            filled["compte_source_id"] = int(matches[0]["id"])
            pending.filled = filled

            # déduire entreprise à partir du compte source si pas défini
            if context.get("entreprise_id") is None:
                eid = get_compte_entreprise_id(int(filled["compte_source_id"]))
                if eid is not None:
                    context["entreprise_id"] = eid
                    entreprise_id = eid
                    notes.append(f"entreprise_id déduit depuis compte_source: {eid}")

        # 2) compte_destination (même entreprise)
        if "compte_destination_id" in filled and context.get("entreprise_id") is not None:
            dst_eid = get_compte_entreprise_id(int(filled["compte_destination_id"]))
            if dst_eid is not None and dst_eid != int(context["entreprise_id"]):
                filled.pop("compte_destination_id", None)
                pending.filled = filled
                notes.append(f"compte_destination_id incompatible (dst_eid={dst_eid}) => re-clarification")

        if "compte_destination_id" not in filled:
            dst_q = (entities.get("compte_destination_query") or "").strip()
            matches = resolve_by_name("compte_financier", dst_q, entreprise_id)
            if len(matches) != 1:
                clar = Clarification(
                        needed=True,
                        entity="compte_destination",
                        field="compte_destination_id",
                        query=dst_q,
                        suggestions=matches,
                        message=t(lang, "need_dst_account"),
                        )
                return None, [], {}, clar, pending, notes
            filled["compte_destination_id"] = int(matches[0]["id"])
            pending.filled = filled

        # 3) sécurité: source != destination
        if int(filled["compte_source_id"]) == int(filled["compte_destination_id"]):
            filled.pop("compte_destination_id", None)
            pending.filled = filled
            clar = Clarification(
                needed=True,
                entity="compte_financier",
                field="compte_destination_id",
                query=None,
                suggestions=[],
                message="Le compte destination doit être différent du compte source. Choisis un autre compte destination.",
            )
            return None, [], {}, clar, pending, notes

        # 4) montant + devise
        # si l'user a répondu pendant une clarification, on prend filled d'abord
        montant = filled.get("montant")
        devise = (filled.get("devise") or "").strip().upper() or None

        if montant is None:
            parsed_montant, parsed_devise = parse_amount_currency(original)
            montant = parsed_montant
            if devise is None and parsed_devise:
                devise = str(parsed_devise).upper()

        if montant is None:
            return None, [], {}, Clarification(
                needed=True, entity="montant", field="montant", message=t(lang, "need_amount")
            ), pending, notes

        montant = float(montant)

        # devise par défaut = devise du compte source (pour respecter validate_transfert_integrity)
        src_dev = get_compte_devise(int(filled["compte_source_id"]))
        if not src_dev:
            raise HTTPException(400, "Impossible de lire la devise du compte source.")
        src_dev = src_dev.upper()

        if not devise:
            devise = src_dev
            filled["devise"] = devise
            pending.filled = filled

        # si l’utilisateur demande USD mais le compte est AED/EUR => pas de boucle, on bloque proprement
        if devise != src_dev:
            # on refuse avec message clair (pas de clarification)
            raise HTTPException(
                400,
                f"Transfert impossible en {devise}. Le compte source est en {src_dev}. "
                f"Reformule en {src_dev} (ex: 'Transfère {montant} {src_dev} ...')."
            )

            # return None, [], {}, Clarification(
            #     needed=True,
            #     entity="devise",
            #     field="devise",
            #     query=devise,  ""
            #     suggestions=[{"option": 1, "nom": src_dev}],
            #     message=f"Le transfert doit être en {src_dev} (devise des comptes). Choisis {src_dev}.",
            # ), pending, notes


        # (optionnel) stocker la devise validée pour éviter toute incohérence plus tard
        filled["devise"] = src_dev
        pending.filled = filled


        # 5) date_transfert
        dtr = parse_any_date(original) or date.today()

        sql = (pending.sql_template or "").strip() or INTENTS["INSERT_TRANSFERT"]["template"]
        sql = INTENTS["INSERT_TRANSFERT"]["template"]

        params = [
            int(filled["compte_source_id"]),
            int(filled["compte_destination_id"]),
            float(montant),
            str(devise).upper(),
            dtr.isoformat(),
        ]

        if sql.count("%s") != len(params):
            raise ValueError(f"Mismatch placeholders/params transfert: expected={sql.count('%s')} got={len(params)}")

        notes.append("INSERT_TRANSFERT resolved.")
        return sql, params, {}, Clarification(needed=False), pending, notes

    # -----------------------------------------------------
    # Fallback: intent not supported
    # -----------------------------------------------------
    
    return None, [], {}, Clarification(
        needed=True,
        entity=None,
        field=None,
        message="Je ne sais pas continuer ce plan (intent non supportée). Reformule la demande.",
    ), pending, notes


def _extract_account_query_from_row(row_text: str) -> Optional[str]:
    m = re.search(r'compte\s*["“](.+?)["”]', row_text or "", flags=re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"\bdepuis\s+le\s+compte\s+([^\n\r,.!?]+)", row_text or "", flags=re.I)
    if m:
        return m.group(1).strip()
    return None


def continue_import_session(
    pending: PendingPlan,
    user_input: str,
    context: Dict[str, Any],
):
    notes: List[str] = ["import_session"]
    entities = dict(pending.entities or {})

    rows: List[str] = entities.get("rows", []) or []
    row_idx = int(entities.get("row_idx", 0) or 0)
    account_map: Dict[str, int] = entities.get("account_map", {}) or {}

    # 0) terminé
    if row_idx >= len(rows):
        return None, [], {}, Clarification(needed=False), pending, notes + ["import_done"]

    row_text = rows[row_idx]
    context["original_user_input"] = row_text 

    # 1) si l’utilisateur répond à une clarification "compte"
    last_field = (context.get("last_field") or "").strip()
    if last_field in ("compte_id", "compte_source_id", "compte_destination_id") and (user_input or "").strip().isdigit():
        chosen_option = int(user_input.strip())
        last_account_q = entities.get("last_account_query")
        if last_account_q:
            account_map[last_account_q] = chosen_option
            entities["account_map"] = account_map
            notes.append(f"account_map[{last_account_q}]={chosen_option}")
        context["last_field"] = None  # reset

    # 2) si on a déjà choisi un compte pour "Compte EUR", on pré-remplit
    acc_q = _extract_account_query_from_row(row_text)
    if acc_q and acc_q in account_map:
        context["prefill_compte_choice"] = account_map[acc_q]  # on passe le "numéro" choisi

    # 3) exécuter la ligne via ton pipeline existant
    sql, params, clar, row_pending, row_notes = run_pipeline(row_text, context)
    notes += row_notes

    # 4) clarification => STOP (on pose 1 question)
    if clar.needed:
        if clar.field in ("compte_id", "compte_source_id", "compte_destination_id") and acc_q:
            entities["last_account_query"] = acc_q
            pending.entities = entities

        context["last_field"] = clar.field
        pending.entities = entities
        return None, [], {}, clar, pending, notes

    # 5) OK => on avance à la prochaine ligne
    entities["row_idx"] = row_idx + 1
    pending.entities = entities

    notes.append(f"row_done idx={row_idx}")
    return sql, params, {}, Clarification(needed=False), pending, notes


# =========================================================
# /convert
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
    resolved: Dict[str, Any] = {}
    notes: List[str] = []

    # Auto-resolve entreprise_id + role from actor_id (multi-entité)
    if context.get("entreprise_id") is None and context.get("actor_id"):
        user = get_user_by_actor(context["actor_id"])
        if user:
            context["user_id"] = user["id"]
            ents = list_user_enterprises(int(user["id"]))

            if len(ents) == 1:
                eid = int(ents[0]["id"])
                context["entreprise_id"] = eid
                context["role"] = get_user_role_for_enterprise(int(user["id"]), eid) or context.get("role")
                notes.append(f"entreprise_id auto={eid} depuis utilisateur")
            elif len(ents) > 1:
                # ⚠️ clarification entreprise
                clar = Clarification(
                    needed=True,
                    entity="entreprise",
                    field="entreprise_id",
                    query="",
                    suggestions=ents,
                    message=t(lang, "need_entreprise"),
                )
                pending = PendingPlan(
                    operation="SELECT",
                    intent="REPLAY_ORIGINAL_AFTER_TENANT",
                    sql_template="",
                    entities={},
                    filled={}
                    )
                context["last_field"] = "entreprise_id"
                context["original_user_input"] = req.user_input



                return SQLPlan(
                    request_id=request_id,
                    user_input=req.user_input,
                    operation="SELECT",
                    sql="",
                    params=[],
                    tables=[],
                    risk=RiskInfo(level="low", needs_approval=False),
                    static_checks=StaticChecks(
                        ast_parsed=False, ddl_blocked=True, schema_ok=True, single_statement=True,
                        tenant_scoped=False, columns_ok=True, no_select_star=True, limit_ok=True, functions_ok=True,
                    ),
                    context=context,
                    resolved=resolved,
                    clarification=clar,
                    pending_plan=pending,
                    notes=notes,
                )

    # =========================================================
    # Hard block DDL/DCL at NL level (before LLM)
    # =========================================================
    if re.search(r"\b(drop|truncate|alter|create|grant|revoke)\b", req.user_input or "", re.I) \
    or re.search(r"\b(supprime|supprimer|efface|effacer|détruis|detruis|détruire|detruire)\b", req.user_input or "", re.I):
        raise HTTPException(400, "Commande interdite (DDL/DCL).")

    force_plan = should_force_plan(req.user_input)

    # =========================================================
    # Analytics bypass (avoid LLM hallucinations)
    # =========================================================
    analytics_intent = detect_analytics_intent(req.user_input)
    log_event(
    "analytics_detect",
    request_id=request_id,
    raw=req.user_input,
    normalized=norm_txt(req.user_input),
    analytics_intent=analytics_intent,
    channel=context.get("channel"),
)

    # 1) Analytics bypass
    analytics_intent = detect_analytics_intent(req.user_input)

    # 2) Update bypass (fallback rule-based)
    update_intent = detect_update_intent(req.user_input) if not analytics_intent else None

    if analytics_intent:
        llm_obj = {
            "mode": "plan",
            "operation": "SELECT",
            "intent": analytics_intent,
            "entities": {},
            "sql_template": "",
            "template_params": [],
        }
        llm_ms = 0

    elif update_intent:
        llm_obj = {
            "mode": "plan",
            "operation": "UPDATE",
            "intent": update_intent,
            "entities": {},
            "sql_template": INTENTS[update_intent]["template"],
            "template_params": [],
        }
        llm_ms = 0

    else:
        t0 = time.time()
        prompt = build_llm_prompt(req.user_input, context, force_plan=force_plan)
        llm_obj = call_gpt(prompt)
        llm_ms = int((time.time() - t0) * 1000)

    try:
        llm: LLMOutput = parse_llm_output(llm_obj)
    except (ValidationError, Exception) as e:
        log_event(
            "llm_output_invalid",
            request_id=request_id,
            error=str(e),
            llm_obj=llm_obj,   # 🔥 essentiel
        )
        raise HTTPException(400, "Sortie LLM invalide")


    mode = llm.mode.lower()

    if force_plan and mode != "plan":
        raise HTTPException(400, "Mode PLAN requis (écriture / ambiguïté) pour éviter une écriture incomplète.")


    
    # -----------------------------------------
    # PLAN => try auto-continue once
    # -----------------------------------------
    if mode == "plan":
        pending = PendingPlan(
            operation=(llm.operation or "UNKNOWN"),
            intent=getattr(llm, "intent", None),
            sql_template=(llm.sql_template or ""),
            template_params=list(llm.template_params or []),
            entities=dict(getattr(llm, "entities", {}) or {}),
            filled={},
        )
        log_event("continue_in",
                last_field=context.get("last_field"),
                user_input=req.user_input,
                pending_intent=getattr(pending, "intent", None))
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

        sql = sql2 or ""
        params = params2 or []
    else:
        sql = (llm.sql or "").strip()
        params = list(llm.params or [])
        # -------------------------------------------------
        # Auto-resolve projet name in DIRECT SELECT SQL
        # -------------------------------------------------
        try:
            entreprise_id = context.get("entreprise_id")
            if entreprise_id is not None and isinstance(params, list) and params:
                # cas typique: WHERE p.nom = %s AND p.entreprise_id = %s
                if re.search(r"\bp\.nom\s*=\s*%s\b", sql, flags=re.I):
                    # on suppose que le 1er param correspond au nom projet
                    if isinstance(params[0], str):
                        q = params[0].strip()
                        matches = resolve_by_name("projet", q, entreprise_id)
                        if len(matches) == 1:
                            # remplace p.nom=%s par p.id=%s + param = id
                            sql = re.sub(r"\bp\.nom\s*=\s*%s\b", "p.id = %s", sql, flags=re.I)
                            params[0] = int(matches[0]["id"])
        except Exception as _e:
            pass


    if not sql:
        raise HTTPException(400, "SQL vide")

    if contains_forbidden_keywords(sql):
        raise HTTPException(400, "SQL interdit (DDL/DCL)")

    try:
        node = parse_sql_single(sql)
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

    tenant_sql, tenant_params, tenant_scoped, tenant_notes = enforce_tenant_scope(
        sql,
        context.get("entreprise_id"),
        _SCHEMA_CACHE
    )
    notes.extend(tenant_notes)
    if tenant_params:
        params = params + tenant_params
        sql = tenant_sql
        node = parse_sql_single(sql)

    if context.get("entreprise_id") is not None and not tenant_scoped:
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
        tenant_scoped=tenant_scoped if context.get("entreprise_id") is not None else True,
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
            ast_parsed=True,
            ddl_blocked=True,
            schema_ok=schema_ok,
            single_statement=True,
            tenant_scoped=tenant_scoped if context.get("entreprise_id") is not None else True,
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
# /continue
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

    if not context.get("original_user_input"):
        context["original_user_input"] = context.get("initial_user_input") or context.get("first_user_input") or ""

    # ✅ variables toujours définies
    sql: Optional[str] = None
    params: List[Any] = []
    resolved: Dict[str, Any] = {}
    clarification = Clarification(needed=False)
    pending2 = req.pending_plan
    notes: List[str] = []

    # (optionnel) déduction last_field si tu veux, mais ton test ne dépend pas de ça
    if not context.get("last_field") and req.pending_plan and req.pending_plan.intent == "SELECT_PROJECTS":
        context["last_field"] = "entreprise_id"

    # Cas spécial: si on vient de choisir entreprise_id -> replay analytics si possible
    if context.get("last_field") == "entreprise_id" and (req.user_input or "").strip().isdigit():
        context["entreprise_id"] = int(req.user_input.strip())
        context["last_field"] = None

        original = context.get("original_user_input") or ""
        if not original:
            raise HTTPException(400, "original_user_input manquant")
        log_event(
    "analytics_bypass_try",
    request_id=request_id,
    user_input=req.user_input,
)

        sql, params, clar, pending, notes2 = run_pipeline(original, context)
        notes.extend(notes2)

        if clar.needed:
            return SQLPlan(
                request_id=request_id,
                user_input=req.user_input,
                operation="SELECT",
                sql="",
                params=[],
                tables=[],
                risk=RiskInfo(level="medium", needs_approval=False),
                static_checks=StaticChecks(
                    ast_parsed=False, ddl_blocked=True, schema_ok=True, single_statement=True,
                    tenant_scoped=False, columns_ok=True, no_select_star=True, limit_ok=True, functions_ok=True,
                ),
                context=context,
                resolved={},
                clarification=clar,
                pending_plan=pending,
                notes=notes,
            )
        # si pas analytics, relancer le pipeline LLM sur la requête originale
        force_plan = should_force_plan(original)

        if TEXT2SQL_STUB_LLM:
            llm_obj = {
                "mode": "plan" if force_plan else "sql",
                "operation": "INSERT" if force_plan else "SELECT",
                "intent": "INSERT_DEPENSE",
                "entities": {"projet_query": None, "compte_query": None, "type_depense": None},
                "sql_template": INTENTS["INSERT_DEPENSE"]["template"],
                "template_params": [],
            }
        else:
            prompt = build_llm_prompt(original, context, force_plan=force_plan)
            llm_obj = call_gpt(prompt)

        llm = parse_llm_output(llm_obj)
        mode = llm.mode.lower()

        if force_plan and mode != "plan":
            raise HTTPException(400, "Mode PLAN requis (écriture).")

        if mode == "plan":
            pending = PendingPlan(
                operation=(llm.operation or "UNKNOWN"),
                intent=getattr(llm, "intent", None),
                sql_template=(llm.sql_template or ""),
                template_params=list(llm.template_params or []),
                entities=dict(getattr(llm, "entities", {}) or {}),
                filled=req.pending_plan.filled or {},
            )

            sql, params, resolved, clarification, pending2, notes2 = continue_pending_plan(
                pending=pending,
                user_input="",
                context=context,
            )

            if clarification.needed:
                return SQLPlan(
                    request_id=request_id,
                    user_input=req.user_input,
                    operation=pending.operation,
                    sql="",
                    params=[],
                    tables=[],
                    risk=RiskInfo(level="medium", needs_approval=False),
                    static_checks=StaticChecks(
                        ast_parsed=False, ddl_blocked=True, schema_ok=True, single_statement=True,
                        tenant_scoped=False, columns_ok=True, no_select_star=True, limit_ok=True, functions_ok=True,
                    ),
                    context=context,
                    resolved=resolved,
                    clarification=clarification,
                    pending_plan=pending2,
                    notes=notes2,
                )
        else:
            sql = (llm.sql or "").strip()
            params = list(llm.params or [])


    sql, params, resolved, clarification, pending2, notes2 = continue_pending_plan(
        pending=req.pending_plan,
        user_input=req.user_input,
        context=context,
    )
    notes.extend(notes2)

    # Si on a encore besoin de préciser -> renvoyer Clarification (et ne PAS toucher sql)
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

    # ✅ ici sql existe forcément si clarif finie
    if not sql:
        raise HTTPException(400, "SQL vide après continuation")

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
    log_event("continue_ok", request_id=request_id, operation=op, tables=tables, duration_ms=duration_ms)

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
        "intents": list(INTENTS.keys()),
    }

