import os
import re
import time
import io
import uuid
import threading
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse, Response

# PDF
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# =========================================================
# ENV
# =========================================================
TEXT2SQL_URL = os.getenv("TEXT2SQL_URL", "http://text2sql:8000").rstrip("/")
TEXT2SQL_CONTINUE_URL = os.getenv("TEXT2SQL_CONTINUE_URL", TEXT2SQL_URL).rstrip("/")

SQL_GUARD_URL = os.getenv("SQL_GUARD_URL", "http://sql-guard:8000")
SQL_EXECUTOR_URL = os.getenv("SQL_EXECUTOR_URL", "http://sql-executor:8000")
AUDIT_URL = os.getenv("AUDIT_URL", "http://audit:8000")

RESPONSE_WRITER_URL = os.getenv("RESPONSE_WRITER_URL", "http://response-writer:8000")
WRITER_TIMEOUT_S = float(os.getenv("WRITER_TIMEOUT_S", "12"))

ENV = os.getenv("ENV", "dev")
TIMEOUT_S = int(os.getenv("GATEWAY_TIMEOUT_S", "20"))

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

REQUIRE_CONFIRMATION_FOR_WRITES = os.getenv("REQUIRE_CONFIRMATION_FOR_WRITES", "1") == "1"
PENDING_TTL_SECONDS = int(os.getenv("PENDING_TTL_SECONDS", "300"))

CLARIFY_TTL_SECONDS = int(os.getenv("CLARIFY_TTL_SECONDS", "300"))
FILES_TTL_SECONDS = int(os.getenv("FILES_TTL_SECONDS", "300"))

# =========================================================
# App
# =========================================================
app = FastAPI(title="API Gateway", version="1.6.0")

# =========================================================
# Security prefilter (block obvious SQL injection markers)
# =========================================================
SQL_INJECTION_MARKERS_RE = re.compile(r"(;|--|/\*|\*/)", re.IGNORECASE)

# =========================================================
# In-memory pending confirmations (actor_id -> payload)
# =========================================================
_PENDING: Dict[str, Dict[str, Any]] = {}
_PENDING_LOCK = Lock()

# =========================================================
# In-memory pending clarifications (actor_id -> payload)
# =========================================================
_CLARIFY: Dict[str, Dict[str, Any]] = {}
_CLARIFY_LOCK = Lock()

# =========================================================
# In-memory file store for PDF exports (file_id -> bytes)
# =========================================================
_FILES: Dict[str, Dict[str, Any]] = {}
FILES_LOCK = Lock()

# =========================================================
# Commands / patterns
# =========================================================
_CONFIRM_YES = re.compile(r"^\s*(oui|o|yes|y|ok)\s*$", re.I)
_CONFIRM_NO = re.compile(r"^\s*(non|n|no|annule|annuler|stop|cancel)\s*$", re.I)

_CMD_CANCEL = re.compile(r"^\s*(annuler|annule|stop|cancel)\s*$", re.I)
_CMD_BACK = re.compile(r"^\s*(retour|back)\s*$", re.I)
_CMD_CHANGE = re.compile(r"^\s*(changer|change)\s*$", re.I)
_CHOICE_NUM = re.compile(r"^\s*(\d+)\s*$")

PDF_EXPLICIT_RE = re.compile(r"\b(pdf|export|fichier|download|télécharger|telecharger)\b", re.IGNORECASE)

# =========================================================
# Pending confirmation helpers
# =========================================================
def _pending_cleanup(now: Optional[float] = None) -> None:
    now = now or time.time()
    with _PENDING_LOCK:
        dead = []
        for actor_id, item in _PENDING.items():
            ts = float(item.get("ts") or 0)
            if now - ts > PENDING_TTL_SECONDS:
                dead.append(actor_id)
        for k in dead:
            _PENDING.pop(k, None)


def _pending_get(actor_id: str) -> Optional[Dict[str, Any]]:
    _pending_cleanup()
    with _PENDING_LOCK:
        return _PENDING.get(actor_id)


def _pending_set(actor_id: str, plan: Dict[str, Any], user_text: str) -> None:
    with _PENDING_LOCK:
        _PENDING[actor_id] = {"ts": time.time(), "plan": plan, "user_text": user_text}


def _pending_clear(actor_id: str) -> None:
    with _PENDING_LOCK:
        _PENDING.pop(actor_id, None)


# =========================================================
# Pending clarification helpers
# =========================================================
def _clarify_cleanup(now: Optional[float] = None) -> None:
    now = now or time.time()
    with _CLARIFY_LOCK:
        dead = []
        for actor_id, item in _CLARIFY.items():
            ts = float(item.get("ts") or 0)
            if now - ts > CLARIFY_TTL_SECONDS:
                dead.append(actor_id)
        for k in dead:
            _CLARIFY.pop(k, None)


def _clarify_get(actor_id: str) -> Optional[Dict[str, Any]]:
    _clarify_cleanup()
    with _CLARIFY_LOCK:
        return _CLARIFY.get(actor_id)


def _clarify_set(actor_id: str, original_text: str, plan: Dict[str, Any]) -> None:
    """
    ✅ On stocke aussi pending_plan + context pour appeler /continue ensuite.
    """
    clar = plan.get("clarification") or {}
    with _CLARIFY_LOCK:
        _CLARIFY[actor_id] = {
            "ts": time.time(),
            "original_text": original_text,
            "plan": plan,
            "pending_plan": plan.get("pending_plan"),
            "context": plan.get("context") or {},
            "entity": clar.get("entity"),
            "field": clar.get("field"),
            "message": clar.get("message"),
            "suggestions": clar.get("suggestions") or [],
        }


def _clarify_update(actor_id: str, plan: Dict[str, Any], original_text: str) -> None:
    """
    ✅ Remplace l’état clarification par celui retourné par /continue
    """
    clar = plan.get("clarification") or {}
    with _CLARIFY_LOCK:
        _CLARIFY[actor_id] = {
            "ts": time.time(),
            "original_text": original_text,
            "plan": plan,
            "pending_plan": plan.get("pending_plan"),
            "context": plan.get("context") or {},
            "entity": clar.get("entity"),
            "field": clar.get("field"),
            "message": clar.get("message"),
            "suggestions": clar.get("suggestions") or [],
        }


def _clarify_clear(actor_id: str) -> None:
    with _CLARIFY_LOCK:
        _CLARIFY.pop(actor_id, None)


# =========================================================
# Files helpers
# =========================================================
def _files_cleanup(now: Optional[float] = None) -> None:
    now = now or time.time()
    with FILES_LOCK:
        dead = []
        for file_id, item in _FILES.items():
            ts = float(item.get("ts") or 0)
            if now - ts > FILES_TTL_SECONDS:
                dead.append(file_id)
        for k in dead:
            _FILES.pop(k, None)


def _save_temp_file(data: bytes, filename: str, content_type: str) -> str:
    _files_cleanup()
    file_id = str(uuid.uuid4())
    with FILES_LOCK:
        _FILES[file_id] = {
            "bytes": data,
            "ts": time.time(),
            "filename": filename,
            "content_type": content_type,
        }
    return file_id


# =========================================================
# Helpers - WhatsApp / Twilio
# =========================================================
def normalize_whatsapp_number(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith("whatsapp:"):
        raw = raw.replace("whatsapp:", "")
    raw = re.sub(r"[^\d+]", "", raw)
    if raw.startswith("33"):
        raw = "+" + raw
    elif raw.startswith("0"):
        raw = "+33" + raw[1:]
    return f"whatsapp:{raw}"


def send_whatsapp_via_twilio(to: str, body: str, media_url: Optional[str] = None) -> None:
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        print("Twilio credentials missing; cannot send message.")
        return

    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    data = {"From": TWILIO_WHATSAPP_FROM, "To": to, "Body": body}
    if media_url:
        data["MediaUrl"] = media_url

    r = requests.post(url, data=data, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN), timeout=10)
    print(f"[twilio] status={r.status_code} body={r.text[:200]}")
    if r.status_code >= 300:
        print("Twilio send failed:", r.status_code, r.text[:500])


def twiml_message(text: str) -> str:
    safe = (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Message>{safe}</Message>
</Response>
"""


# =========================================================
# Helpers - HTTP calls
# =========================================================
def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, json=payload, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()


def post_json_allow_400(url: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    r = requests.post(url, json=payload, timeout=TIMEOUT_S)
    if 200 <= r.status_code < 300:
        return r.json(), None

    if r.status_code == 400:
        try:
            j = r.json()
            detail = j.get("detail") if isinstance(j, dict) else None
            msg = detail or r.text
        except Exception:
            msg = r.text
        return None, (msg or "").strip()

    r.raise_for_status()
    return None, "unknown"


def call_text2sql_continue(user_input: str, pending_plan: Dict[str, Any], context: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    ✅ Appelle /continue au lieu de relancer /convert
    """
    payload = {
        "user_input": user_input,
        "pending_plan": pending_plan,
        "context": context or {},
    }
    return post_json_allow_400(f"{TEXT2SQL_CONTINUE_URL}/continue", payload)


# =========================================================
# Writer (LLM response-writer service) - safe usage
# =========================================================
def detect_lang(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ["hello", "hi", "thanks", "thank you", "bye"]):
        return "en"
    return "fr"


def call_response_writer(payload: Dict[str, Any]) -> Optional[str]:
    try:
        r = requests.post(
            f"{RESPONSE_WRITER_URL.rstrip('/')}/write",
            json=payload,
            timeout=WRITER_TIMEOUT_S,
        )
        if 200 <= r.status_code < 300:
            j = r.json()
            txt = (j.get("text") or "").strip()
            return txt if txt else None
        return None
    except Exception:
        return None


# =========================================================
# Routing
# =========================================================
SMALLTALK_PATTERNS = [
    r"^\s*(bonjour|salut|coucou|hey|hello|hi)\s*$",
    r"^\s*(merci|thanks|thx)\s*$",
    r"^\s*(ok|d'accord|daccord|super|parfait|nickel)\s*$",
    r"^\s*(au revoir|bye|bonne journée|bonne journee|bonne soiree|bonne soirée)\s*$",
]

DB_KEYWORDS = [
    "projet", "projets",
    "depense", "dépense", "depenses", "dépenses",
    "facture", "factures",
    "client", "clients",
    "compte", "comptes", "caisse",
    "solde", "budget",
    "transfert", "transferts",
    "entreprise", "bureau",
    "paiement", "paiements",
]

DB_VERBS = [
    "liste", "lister", "montre", "affiche", "donne",
    "total", "somme", "combien", "top", "moyenne",
    "par mois", "par projet", "par client",
    "aujourd", "cette semaine", "ce mois", "cette année",
]


def route_message(text: str) -> Tuple[str, Optional[str]]:
    t = (text or "").strip().lower()
    SQL_DIRECT_RE = re.compile(r"^\s*(select|insert|update|delete)\b", re.I)

    if SQL_DIRECT_RE.match(t):
        return "db", None

    lang = detect_lang(text)

    for pat in SMALLTALK_PATTERNS:
        if re.match(pat, t, flags=re.IGNORECASE):
            if lang == "en":
                return "smalltalk", (
                    "Hello 👋\n"
                    "I can help you with: projects, expenses, invoices, clients, accounts and balances.\n"
                    "Examples:\n"
                    "- List projects\n"
                    "- Total expenses this month\n"
                    "- Unpaid invoices for client Alpha\n"
                    "- Main account balance"
                )
            return "smalltalk", (
                "Bonjour 👋\n"
                "Je peux t’aider sur : projets, dépenses, factures, clients, comptes et soldes.\n"
                "Exemples :\n"
                "- Liste des projets\n"
                "- Total des dépenses ce mois-ci\n"
                "- Factures du client Alpha\n"
                "- Solde du compte principal"
            )

    if any(k in t for k in DB_KEYWORDS) or any(v in t for v in DB_VERBS):
        return "db", None

    return "out_of_scope", (
        "Je suis dédié à la gestion financière Orionis (projets, dépenses, factures, clients, comptes, soldes).\n"
        "Je ne peux pas répondre à ce type de demande.\n"
        "Exemples de demandes supportées :\n"
        "- Liste des projets\n"
        "- Dépenses du projet Migration\n"
        "- Total des dépenses par projet\n"
        "- Factures non payées du client Alpha"
    )


# =========================================================
# Confirmation helpers
# =========================================================
def plan_needs_confirmation(plan: Dict[str, Any]) -> bool:
    if not REQUIRE_CONFIRMATION_FOR_WRITES:
        return False

    op = (plan.get("operation") or "").upper()
    if op in ("INSERT", "UPDATE", "DELETE"):
        return True

    risk = plan.get("risk") or {}
    if bool(risk.get("needs_approval")):
        return True

    return False


def build_confirmation_message(plan: Dict[str, Any]) -> str:
    op = (plan.get("operation") or "UNKNOWN").upper()
    sql = (plan.get("sql") or "").strip()
    excerpt = sql.replace("\n", " ")
    if len(excerpt) > 220:
        excerpt = excerpt[:220] + "..."

    return (
        "⚠️ Action sensible détectée.\n"
        f"Opération: {op}\n"
        f"Aperçu: {excerpt}\n\n"
        "Réponds:\n"
        "✅ OUI pour confirmer\n"
        "❌ NON pour annuler"
    )


# =========================================================
# Response formatting fallback (only if writer unavailable)
# =========================================================
def _coerce_none(v: Any) -> Any:
    return 0 if v is None else v


def fallback_format_select(columns: List[str], rows: List[List[Any]], max_rows: int = 5) -> str:
    if not rows:
        return "✅ Aucun résultat."
    lines = ["📊 Résultats", ""]
    for idx, r in enumerate(rows[:max_rows], start=1):
        parts = []
        for i, c in enumerate(columns[:4]):
            val = _coerce_none(r[i] if i < len(r) else None)
            parts.append(f"{c}: {val}")
        lines.append(f"{idx}) " + " | ".join(parts))
    if len(rows) > max_rows:
        lines.append(f"\n… ({len(rows) - max_rows} autres)")
    return "\n".join(lines)


# =========================================================
# PDF export helpers
# =========================================================
def to_pdf_bytes(title: str, columns: List[str], rows: List[List[Any]]) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4)
    styles = getSampleStyleSheet()

    story = []
    story.append(Paragraph(title, styles["Title"]))
    story.append(Spacer(1, 10))

    max_rows = 200
    clipped = rows[:max_rows]
    if len(rows) > max_rows:
        story.append(Paragraph(f"(Aperçu: {max_rows} lignes sur {len(rows)})", styles["Normal"]))
        story.append(Spacer(1, 8))

    data = [columns] + [[("" if v is None else str(v)) for v in r] for r in clipped]
    table = Table(data, repeatRows=1)

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
    ]))

    story.append(table)
    doc.build(story)
    return buf.getvalue()


def send_pdf_export(actor_id: str, columns: List[str], rows: List[List[Any]], caption: str) -> None:
    pdf_bytes = to_pdf_bytes("Export Orionis", columns, rows)
    file_id = _save_temp_file(pdf_bytes, filename="export.pdf", content_type="application/pdf")

    if not PUBLIC_BASE_URL:
        send_whatsapp_via_twilio(
            actor_id,
            f"{caption}\n\n⚠️ PUBLIC_BASE_URL non configuré (MediaUrl Twilio requis).",
        )
        return

    media_url = f"{PUBLIC_BASE_URL}/files/{file_id}"
    send_whatsapp_via_twilio(actor_id, caption, media_url=media_url)


def wants_pdf(user_text: str) -> bool:
    return bool(PDF_EXPLICIT_RE.search(user_text or ""))


# =========================================================
# Clarification rendering
# =========================================================
_CMD_SHOW = re.compile(r"^\s*(voir\s*la\s*liste|liste|show\s*list)\s*$", re.I)
_CMD_EDIT = re.compile(r"^\s*(modifier|change|changer|edit)\s*$", re.I)


def render_clarification_message(state: Dict[str, Any]) -> str:
    plan = state.get("plan") or {}
    clar = plan.get("clarification") or {}
    suggestions = state.get("suggestions") or []

    # user_ask = (plan.get("user_input") or "").strip()
    user_ask = (state.get("original_text") or plan.get("user_input") or "").strip()

    entity = (clar.get("entity") or "élément").strip()
    query = (clar.get("query") or "").strip()

    header = f"🧾 Pour continuer, j’ai besoin de préciser le {entity}.\n"
    if user_ask:
        header += f"👉 Ta demande : {user_ask}\n"

    if not suggestions:
        lines = [
            header.strip(),
            "",
            f"Je ne trouve aucun {entity} qui ressemble à “{query}”.",
            "Peux-tu préciser ?",
            "",
            "Tu peux répondre :",
            "• un nom plus précis",
            "• Annuler",
        ]
        return "\n".join(lines)

    lines = [
        header.strip(),
        "",
        f"Plusieurs {entity}s correspondent à “{query}”. Lequel veux-tu ?",
        "",
    ]
    for s in suggestions[:6]:
        lines.append(f"{s.get('option')}) {s.get('nom')}")
    lines += [
        "",
        "Réponds avec le numéro (ex: 1).",
        "Ou réponds : Annuler / Voir la liste / Modifier ma demande",
    ]
    return "\n".join(lines)


# =========================================================
# Files endpoint for Twilio MediaUrl
# =========================================================
@app.get("/files/{file_id}")
def download_file(file_id: str):
    _files_cleanup()
    with FILES_LOCK:
        item = _FILES.get(file_id)
    if not item:
        return Response("Not found", status_code=404)

    return Response(
        content=item["bytes"],
        media_type=item.get("content_type") or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{item.get("filename") or "export.pdf"}"'},
    )


# =========================================================
# Routes
# =========================================================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "env": ENV,
        "text2sql_url": TEXT2SQL_URL,
        "text2sql_continue_url": TEXT2SQL_CONTINUE_URL,
        "sql_guard_url": SQL_GUARD_URL,
        "sql_executor_url": SQL_EXECUTOR_URL,
        "audit_url": AUDIT_URL,
        "response_writer_url": RESPONSE_WRITER_URL,
        "require_confirmation_for_writes": REQUIRE_CONFIRMATION_FOR_WRITES,
        "pending_ttl_seconds": PENDING_TTL_SECONDS,
        "clarify_ttl_seconds": CLARIFY_TTL_SECONDS,
        "public_base_url": PUBLIC_BASE_URL or None,
    }


@app.get("/whatsapp/webhook")
def whatsapp_webhook_get():
    return PlainTextResponse("OK. Use POST from Twilio.", status_code=200)


@app.post("/whatsapp/webhook", response_class=PlainTextResponse)
def whatsapp_webhook(
    Body: str = Form(...),
    From: str = Form(...),
    ProfileName: Optional[str] = Form(None),
):
    user_text = (Body or "").strip()
    actor_id = normalize_whatsapp_number(From)

    if not user_text:
        return PlainTextResponse(twiml_message("Message vide."), media_type="application/xml", status_code=200)

    # Security prefilter
    if SQL_INJECTION_MARKERS_RE.search(user_text):
        return PlainTextResponse(
            twiml_message("❌ Requête refusée : caractères SQL non autorisés détectés (;, --, /* */). Reformule sans symboles SQL."),
            media_type="application/xml",
            status_code=200,
        )

    # =====================================================
    # 0) Clarification flow FIRST (multi-step via /continue)
    # =====================================================
    clarify = _clarify_get(actor_id)
    if clarify:
        if _CMD_CANCEL.match(user_text):
            _clarify_clear(actor_id)
            return PlainTextResponse(twiml_message("✅ Ok, j’ai annulé."), media_type="application/xml")

        if _CMD_SHOW.match(user_text):
            return PlainTextResponse(twiml_message(render_clarification_message(clarify)), media_type="application/xml")

        if _CMD_EDIT.match(user_text):
            _clarify_clear(actor_id)
            return PlainTextResponse(
                twiml_message("D’accord 🙂 Réécris ta demande en précisant le nom (ex: ‘Migration ERP’)."),
                media_type="application/xml",
            )

        # Sinon: on continue le plan via /continue
        ack = PlainTextResponse(twiml_message("✅ Reçu. Je continue…"), media_type="application/xml", status_code=200)

        def continue_job():
            try:
                original_text = clarify.get("original_text") or ""
                pending_plan = clarify.get("pending_plan") or {}
                ctx = dict(clarify.get("context") or {})

                field = clarify.get("field")  # ex: projet_id, compte_id, type_depense, montant, devise
                suggestions = clarify.get("suggestions") or []

                # On indique à text2sql quel champ on est en train de remplir
                ctx["last_field"] = field
                ctx["original_user_input"] = ctx.get("original_user_input") or original_text
                ctx["user_input"] = user_text

                # Si suggestions -> choix numérique => on transforme en valeur canonical
                m = _CHOICE_NUM.match(user_text)
                if suggestions and m:
                    choice = int(m.group(1))
                    selected = next((s for s in suggestions if int(s.get("option", -1)) == choice), None)
                    if not selected:
                        send_whatsapp_via_twilio(actor_id, "Choix invalide. Réponds avec un numéro de la liste, ou Annuler.")
                        return

                    # Pour les entités (projet/compte), on passe l'ID directement
                    # Pour les enums (type_depense/devise), on passe le nom choisi
                    if field in ("projet_id", "compte_id", "client_id"):
                        user_value = str(selected.get("id"))
                    else:
                        user_value = str(selected.get("nom"))

                else:
                    # Si suggestions existent mais user n'a pas répondu un numéro
                    if suggestions and not m:
                        send_whatsapp_via_twilio(actor_id, "Réponds avec un numéro (ex: 1), ou Annuler.")
                        return
                    user_value = user_text

                plan2, err = call_text2sql_continue(
                    user_input=user_value,
                    pending_plan=pending_plan,
                    context=ctx,
                )
                if err:
                    _send_user_message(actor_id, original_text, error_message=f"Requête refusée (Text2SQL/continue): {err}")
                    _clarify_clear(actor_id)
                    return

                clar2 = plan2.get("clarification") or {}
                if clar2.get("needed"):
                    _clarify_update(actor_id, plan2, original_text)
                    msg = render_clarification_message(_clarify_get(actor_id))
                    _send_user_message(actor_id, original_text, user_message=msg)
                    return

                # Clarification terminée -> on exécute (avec confirmation si write)
                _clarify_clear(actor_id)

                if plan_needs_confirmation(plan2):
                    _pending_set(actor_id, plan2, original_text)
                    send_whatsapp_via_twilio(actor_id, build_confirmation_message(plan2))
                    return

                _execute_plan(actor_id, original_text, plan2)

            except Exception as e:
                send_whatsapp_via_twilio(actor_id, f"❌ Erreur interne: {e}")

        threading.Thread(target=continue_job, daemon=True).start()
        return ack

    # =====================================================
    # 1) Confirmation flow (OUI/NON)
    # =====================================================
    pending = _pending_get(actor_id)
    if pending:
        if _CMD_CANCEL.match(user_text) or _CONFIRM_NO.match(user_text):
            _pending_clear(actor_id)
            return PlainTextResponse(twiml_message("✅ Annulé. Rien n’a été exécuté."), media_type="application/xml", status_code=200)

        if _CONFIRM_YES.match(user_text):
            ack = PlainTextResponse(
                twiml_message("✅ Confirmation reçue. J’exécute la requête…"),
                media_type="application/xml",
                status_code=200,
            )

            def confirmed_job():
                try:
                    plan = pending["plan"]
                    ctx = plan.get("context") or {}
                    ctx["confirmed"] = True
                    plan["context"] = ctx
                    _execute_plan(actor_id, pending.get("user_text") or plan.get("user_input") or "", plan)
                except Exception as e:
                    send_whatsapp_via_twilio(actor_id, f"❌ Erreur interne: {e}")
                finally:
                    _pending_clear(actor_id)

            threading.Thread(target=confirmed_job, daemon=True).start()
            return ack

        return PlainTextResponse(
            twiml_message("Je n’ai pas compris. Réponds OUI pour confirmer ou NON/ANNULER pour annuler."),
            media_type="application/xml",
            status_code=200,
        )

    # =====================================================
    # 2) Routing (smalltalk / out_of_scope / db)
    # =====================================================
    route, immediate_reply = route_message(user_text)
    if route in ("smalltalk", "out_of_scope"):
        return PlainTextResponse(twiml_message(immediate_reply or ""), media_type="application/xml", status_code=200)

    # Immediate ACK to Twilio
    ack = PlainTextResponse(
        twiml_message("⏳ Requête reçue. Je traite et je reviens vers toi…"),
        media_type="application/xml",
        status_code=200,
    )

    def job():
        _run_pipeline(actor_id=actor_id, user_text=user_text, original_user_text=user_text)

    threading.Thread(target=job, daemon=True).start()
    return ack


# =========================================================
# Core pipeline function
# =========================================================
def _run_pipeline(actor_id: str, user_text: str, original_user_text: str) -> None:
    """
    Text2SQL(/convert) → (Clarification?) → Confirmation? → Guard → Executor → Writer
    """
    try:
        convert_payload = {"user_input": user_text, "actor_id": actor_id}
        plan, err = post_json_allow_400(f"{TEXT2SQL_URL}/convert", convert_payload)
        if err:
            _send_user_message(actor_id, original_user_text, error_message=f"Requête refusée (Text2SQL): {err}")
            return

        clarification = plan.get("clarification") or {}
        if clarification.get("needed"):
            _clarify_set(actor_id, original_text=original_user_text, plan=plan)
            msg = render_clarification_message(_clarify_get(actor_id))
            _send_user_message(actor_id, original_user_text, user_message=msg)
            return

        if plan_needs_confirmation(plan):
            _pending_set(actor_id, plan, original_user_text)
            send_whatsapp_via_twilio(actor_id, build_confirmation_message(plan))
            return

        _execute_plan(actor_id, original_user_text, plan)

    except Exception as e:
        _send_user_message(actor_id, original_user_text, error_message=f"Erreur interne: {e}")


def _execute_plan(actor_id: str, user_text: str, plan: Dict[str, Any]) -> None:
    guard = post_json(f"{SQL_GUARD_URL.rstrip('/')}/check", plan)
    if not guard.get("allowed", False):
        reasons = guard.get("reasons") or []
        _send_user_message(actor_id, user_text, error_message="Requête refusée:\n- " + "\n- ".join(reasons[:6]))
        return

    exec_payload = dict(plan)
    exec_payload["sql"] = guard.get("normalized_sql") or plan.get("sql")
    exec_payload["params"] = guard.get("normalized_params") or plan.get("params", [])

    result = post_json(f"{SQL_EXECUTOR_URL.rstrip('/')}/execute", exec_payload)
    if result.get("status") != "executed":
        reasons = result.get("reasons") or []
        _send_user_message(actor_id, user_text, error_message=f"Non exécuté ({result.get('status')}).\n" + ("\n".join(reasons) if reasons else ""))
        return

    _send_result_via_writer(actor_id, user_text, plan, result)


# =========================================================
# Writer + fallback dispatch
# =========================================================
def _send_user_message(actor_id: str, original_user_text: str, user_message: Optional[str] = None, error_message: Optional[str] = None) -> None:
    lang = detect_lang(original_user_text)
    payload = {
        "request_id": str(uuid.uuid4()),
        "lang": lang,
        "channel": "whatsapp",
        "intent": "system_message",
        "operation": "MESSAGE",
        "rows": [],
        "columns": [],
        "user_message": user_message,
        "error_message": error_message,
    }
    txt = call_response_writer(payload) or (user_message or error_message or "OK")
    send_whatsapp_via_twilio(actor_id, txt)


def _send_result_via_writer(actor_id: str, user_text: str, plan: Dict[str, Any], result: Dict[str, Any]) -> None:
    op = (result.get("operation") or plan.get("operation") or "").upper()
    lang = detect_lang(user_text)

    if op == "SELECT":
        columns = result.get("columns") or []
        rows = result.get("rows") or []

        if wants_pdf(user_text) or (len(rows) >= 30):
            send_pdf_export(actor_id, columns, rows, f"📎 Export PDF ({len(rows)} lignes).")
            return

        rows_dicts: List[Dict[str, Any]] = []
        for r in rows[:100]:
            d = {}
            for i, c in enumerate(columns):
                d[c] = r[i] if i < len(r) else None
            rows_dicts.append(d)

        writer_payload = {
            "request_id": str(uuid.uuid4()),
            "lang": lang,
            "channel": "whatsapp",
            "intent": plan.get("intent") or "select_result",
            "operation": "SELECT",
            "rows": rows_dicts,
            "columns": columns,
        }
        txt = call_response_writer(writer_payload)
        if not txt:
            txt = fallback_format_select(columns, rows, max_rows=5)

        send_whatsapp_via_twilio(actor_id, txt)
        return

    writer_payload = {
        "request_id": str(uuid.uuid4()),
        "lang": lang,
        "channel": "whatsapp",
        "intent": plan.get("intent") or "write_result",
        "operation": op or "EXECUTED",
        "rows": [],
        "columns": [],
        "user_message": f"✅ {op} exécuté. affected_rows={result.get('affected_rows')}",
    }
    txt = call_response_writer(writer_payload) or f"✅ {op} exécuté. affected_rows={result.get('affected_rows')}"
    send_whatsapp_via_twilio(actor_id, txt)
