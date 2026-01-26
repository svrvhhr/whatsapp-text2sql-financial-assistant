import os
from typing import Any, Dict, List, Optional, Union

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# =========================================================
# ENV
# =========================================================
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "none")  # "openai" | "ollama" | "openai"
TIMEOUT_S = float(os.getenv("WRITER_TIMEOUT_S", "12"))

# OpenAI-compatible (optionnel)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Safety
MAX_ITEMS = int(os.getenv("WRITER_MAX_ITEMS", "10"))
MAX_TEXT_CHARS = int(os.getenv("WRITER_MAX_TEXT_CHARS", "1200"))

app = FastAPI(title="Response Writer", version="1.0.0")


# =========================================================
# Models
# =========================================================
class WriteRequest(BaseModel):
    request_id: str
    lang: str = Field(default="fr", description="fr or en")
    channel: str = Field(default="whatsapp", description="whatsapp/web/etc")

    # info “métier” déjà décidé par ton pipeline (pas par le LLM)
    intent: str = Field(default="generic", description="ex: total_depenses_par_projet")
    operation: str = Field(default="SELECT", description="SELECT/INSERT/UPDATE/DELETE")

    # Résultats DB structurés
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    columns: List[str] = Field(default_factory=list)

    # Contexte utile
    period_label: Optional[str] = None  # ex: "ce mois-ci", "since Jan 2025"
    currency: Optional[str] = None

    # Messages système / erreurs provenant des services en amont
    user_message: Optional[str] = None  # ex: "Plusieurs comptes correspondent..."
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class WriteResponse(BaseModel):
    request_id: str
    text: str
    used_provider: str
    fallback: bool = False


# =========================================================
# Helpers (fallback deterministic formatting)
# =========================================================
def _truncate(s: str) -> str:
    if len(s) <= MAX_TEXT_CHARS:
        return s
    return s[: MAX_TEXT_CHARS - 1] + "…"


def format_fallback(req: WriteRequest) -> str:
    # 1) Erreurs / messages directs
    if req.user_message:
        return _truncate(req.user_message)

    if req.error_message:
        if req.lang.lower().startswith("fr"):
            return _truncate(f"❌ Impossible de répondre : {req.error_message}")
        return _truncate(f"❌ Unable to answer: {req.error_message}")

    # 2) Aucun résultat
    if not req.rows:
        if req.lang.lower().startswith("fr"):
            return "✅ Aucun résultat."
        return "✅ No results."

    # 3) Affichage Top-N (WhatsApp friendly)
    # On évite d’afficher des "id=" et des pipes.
    period = f" ({req.period_label})" if req.period_label else ""
    title_fr = f"📊 Résultats{period}"
    title_en = f"📊 Results{period}"

    title = title_fr if req.lang.lower().startswith("fr") else title_en
    lines = [title, ""]

    # Choisir colonnes à afficher
    cols = req.columns[:] if req.columns else list(req.rows[0].keys())
    cols = [c for c in cols if c.lower() not in {"id"}]  # masque id si présent
    cols = cols[:4]  # limite pour WhatsApp

    top_rows = req.rows[:MAX_ITEMS]
    for i, r in enumerate(top_rows, start=1):
        parts = []
        for c in cols:
            v = r.get(c)
            if v is None:
                continue
            parts.append(f"{c}: {v}")
        if not parts:
            parts = [str(r)]
        lines.append(f"{i}) " + " | ".join(parts))

    if len(req.rows) > MAX_ITEMS:
        remaining = len(req.rows) - MAX_ITEMS
        if req.lang.lower().startswith("fr"):
            lines.append(f"\n… ({remaining} autres)")
        else:
            lines.append(f"\n… ({remaining} more)")

    return _truncate("\n".join(lines))


# =========================================================
# LLM calls (writer only)
# =========================================================
def build_prompt(req: WriteRequest) -> str:
    # Prompt très “contraignant” : pas d’invention, seulement reformuler / présenter.
    lang = "French" if req.lang.lower().startswith("fr") else "English"

    # On passe les données de façon simple (texte) pour rester robuste.
    # (Tu peux aussi passer du JSON si tu veux, mais attention aux tokens.)
    rows_preview = req.rows[:MAX_ITEMS]

    return f"""
You are a financial assistant response writer for WhatsApp.
Write in {lang}.
Your job: turn structured database results into a clear WhatsApp message.

STRICT RULES:
- Do NOT invent numbers, names, projects, accounts, dates, totals.
- Use ONLY the provided data.
- If data is missing or ambiguous, say so briefly and ask for a clarification.
- Keep it short, WhatsApp-friendly. Avoid technical SQL words, avoid column names like "entreprise_id".
- Never output raw JSON, never output stack traces.

Context:
- intent: {req.intent}
- operation: {req.operation}
- period_label: {req.period_label}
- currency: {req.currency}

If user_message is present, just rewrite it nicer (same meaning).

user_message: {req.user_message}

rows (top {MAX_ITEMS}):
{rows_preview}
""".strip()




def call_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")
    url = f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "You only rewrite/present provided data. No invention."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT_S)
    r.raise_for_status()
    data = r.json()
    text = data["choices"][0]["message"]["content"].strip()
    if not text:
        raise RuntimeError("Empty response from OpenAI")
    return text


# =========================================================
# API
# =========================================================
@app.get("/health")
def health():
    return {"ok": True, "provider": LLM_PROVIDER}


@app.post("/write", response_model=WriteResponse)
def write(req: WriteRequest):
    # Fallback rapide si pas de LLM
    if LLM_PROVIDER == "none":
        return WriteResponse(
            request_id=req.request_id,
            text=format_fallback(req),
            used_provider="none",
            fallback=True,
        )

    prompt = build_prompt(req)

    try:

        if LLM_PROVIDER == "openai":
            text = call_openai(prompt)
            return WriteResponse(request_id=req.request_id, text=_truncate(text), used_provider="openai", fallback=False)

        # Unknown provider -> fallback
        return WriteResponse(
            request_id=req.request_id,
            text=format_fallback(req),
            used_provider=str(LLM_PROVIDER),
            fallback=True,
        )

    except Exception:
        # En cas de panne LLM: on ne casse jamais le bot.
        return WriteResponse(
            request_id=req.request_id,
            text=format_fallback(req),
            used_provider=str(LLM_PROVIDER),
            fallback=True,
        )
