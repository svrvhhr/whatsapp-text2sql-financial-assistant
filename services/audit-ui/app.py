import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import streamlit as st
import plotly.express as px


# =========================================================
# ENV
# =========================================================
AUDIT_API_URL = os.getenv("AUDIT_API_URL", "http://audit:8000").rstrip("/")
AUDIT_ADMIN_USER = os.getenv("AUDIT_ADMIN_USER", "admin")
AUDIT_ADMIN_PASS = os.getenv("AUDIT_ADMIN_PASS", "admin")

DEFAULT_RANGE_HOURS = int(os.getenv("AUDIT_UI_DEFAULT_RANGE_HOURS", "24"))
DEFAULT_LIMIT = int(os.getenv("AUDIT_UI_DEFAULT_LIMIT", "200"))

REQUEST_TIMEOUT = float(os.getenv("AUDIT_UI_TIMEOUT_S", "10"))


# =========================================================
# Helpers
# =========================================================
def _auth() -> Tuple[str, str]:
    return (AUDIT_ADMIN_USER, AUDIT_ADMIN_PASS)


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{AUDIT_API_URL}{path}"
    r = requests.get(url, params=params or {}, auth=_auth(), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


def _to_df(events: List[Dict[str, Any]]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()

    # Flatten columns for table
    rows = []
    for e in events:
        payload = e.get("payload") or {}
        ctx = payload.get("context") or {}

        rows.append({
            "id": e.get("id"),
            "created_at": e.get("created_at"),
            "request_id": e.get("request_id"),
            "status": e.get("status"),
            "operation": e.get("operation"),
            "source": e.get("source") or payload.get("source"),
            "stage": e.get("stage") or payload.get("stage"),
            "actor_id": e.get("actor_id") or ctx.get("actor_id"),
            "role": e.get("role") or ctx.get("role"),
            "entreprise_id": e.get("entreprise_id") or ctx.get("entreprise_id"),
            "projet_id": e.get("projet_id") or ctx.get("projet_id"),
            "duration_ms": e.get("duration_ms") or payload.get("duration_ms"),
            "affected_rows": e.get("affected_rows") or payload.get("affected_rows"),
            "row_count": e.get("row_count") or payload.get("row_count"),
            "sql_preview": e.get("sql_preview") or payload.get("sql") or "",
            "error_code": e.get("error_code") or payload.get("error_code"),
            "http_status": e.get("http_status") or payload.get("http_status"),
        })

    df = pd.DataFrame(rows)

    # Normalize timestamps
    if "created_at" in df.columns:
        df["created_at_dt"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    else:
        df["created_at_dt"] = pd.NaT

    # Keep a shorter preview
    def clip(s: Any, n: int = 120) -> str:
        s = "" if s is None else str(s)
        s = " ".join(s.split())
        return s if len(s) <= n else s[:n] + "…"

    df["sql_preview_short"] = df["sql_preview"].apply(lambda x: clip(x, 120))
    return df


def _filter_recent(df: pd.DataFrame, hours: int) -> pd.DataFrame:
    if df.empty or "created_at_dt" not in df.columns:
        return df
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    return df[df["created_at_dt"] >= cutoff].copy()


# =========================================================
# Streamlit UI
# =========================================================
st.set_page_config(
    page_title="Orionis Audit Dashboard",
    page_icon="🛡️",
    layout="wide"
)

st.title("🛡️ Orionis — Audit & Monitoring")
st.caption(f"Audit API: {AUDIT_API_URL}")

# Sidebar controls
with st.sidebar:
    st.header("Filtres")

    range_hours = st.slider("Fenêtre (heures)", min_value=1, max_value=24*30, value=DEFAULT_RANGE_HOURS, step=1)

    limit = st.slider("Nombre d’événements (limit)", min_value=20, max_value=500, value=min(DEFAULT_LIMIT, 500), step=10)

    status = st.selectbox("Status", ["(all)", "executed", "refused", "error"], index=0)
    operation = st.selectbox("Operation", ["(all)", "SELECT", "INSERT", "UPDATE", "DELETE", "UNKNOWN"], index=0)

    source = st.text_input("Source (exact)", value="")
    stage = st.text_input("Stage (exact)", value="")
    actor_id = st.text_input("Actor ID (exact)", value="")
    q = st.text_input("Recherche (request_id / payload / sql_preview)", value="")

    colA, colB = st.columns(2)
    with colA:
        refresh = st.button("🔄 Rafraîchir", use_container_width=True)
    with colB:
        auto = st.toggle("Auto-refresh", value=False)

# Auto refresh every 10s (simple)
if auto:
    st.toast("Auto-refresh actif (10s)")
    st.autorefresh(interval=10_000, key="audit_autorefresh")

# Load stats
try:
    stats = _get("/stats", params={"range_hours": range_hours})
except Exception as e:
    st.error(f"Impossible de récupérer /stats : {e}")
    st.stop()

# KPIs
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total", stats.get("total", 0))
k2.metric("Executed ✅", stats.get("executed", 0))
k3.metric("Refused ⛔", stats.get("refused", 0))
k4.metric("Error ❌", stats.get("error", 0))
den = max(int(stats.get("total", 0)), 1)
k5.metric("Error rate", f"{(100.0 * int(stats.get('error', 0)) / den):.1f}%")

st.divider()

# Load events list
params = {
    "limit": limit,
    "offset": 0,
}
if status != "(all)":
    params["status"] = status
if operation != "(all)":
    params["operation"] = operation
if actor_id.strip():
    params["actor_id"] = actor_id.strip()
if source.strip():
    params["source"] = source.strip()
if stage.strip():
    params["stage"] = stage.strip()
if q.strip():
    params["q"] = q.strip()

try:
    events = _get("/events", params=params)
except Exception as e:
    st.error(f"Impossible de récupérer /events : {e}")
    st.stop()

df = _to_df(events)
df = _filter_recent(df, range_hours)

# Charts row
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.subheader("Status")
    if not df.empty:
        fig = px.pie(df, names="status")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucun événement")

with c2:
    st.subheader("Operations")
    if not df.empty:
        op_counts = df["operation"].value_counts().reset_index()
        op_counts.columns = ["operation", "count"]
        fig = px.bar(op_counts, x="operation", y="count")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucun événement")

with c3:
    st.subheader("Sources")
    if not df.empty:
        src = df["source"].fillna("(null)")
        src_counts = src.value_counts().reset_index()
        src_counts.columns = ["source", "count"]
        fig = px.bar(src_counts.head(10), x="source", y="count")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucun événement")

with c4:
    st.subheader("Latence (ms)")
    if not df.empty:
        lat = df[["duration_ms"]].copy()
        lat["duration_ms"] = pd.to_numeric(lat["duration_ms"], errors="coerce")
        lat = lat.dropna()
        if not lat.empty:
            fig = px.histogram(lat, x="duration_ms", nbins=30)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données duration_ms")
    else:
        st.info("Aucun événement")

st.divider()

# Events table + selection
st.subheader("📜 Événements (liste)")

if df.empty:
    st.warning("Aucun événement pour ces filtres.")
    st.stop()

# Sort by created_at_dt desc
df = df.sort_values("created_at_dt", ascending=False)

display_cols = [
    "id",
    "created_at",
    "status",
    "operation",
    "source",
    "stage",
    "actor_id",
    "role",
    "duration_ms",
    "affected_rows",
    "row_count",
    "sql_preview_short",
    "request_id",
]

# Some columns may not exist depending on your audit-service version
display_cols = [c for c in display_cols if c in df.columns]

st.dataframe(
    df[display_cols],
    use_container_width=True,
    hide_index=True,
)

st.caption("Clique un ID ci-dessous pour voir le détail complet.")

# Detail viewer
left, right = st.columns([1, 2], gap="large")

with left:
    st.subheader("🔎 Détail")
    ids = df["id"].tolist()
    selected_id = st.selectbox("Event ID", ids, index=0)

    try:
        ev = _get(f"/events/id/{int(selected_id)}")
    except Exception as e:
        st.error(f"Impossible de récupérer l’événement {selected_id} : {e}")
        st.stop()

    st.write("**Meta**")
    meta_keys = [
        "created_at", "request_id", "status", "operation",
        "source", "stage", "actor_id", "role", "entreprise_id", "projet_id",
        "duration_ms", "affected_rows", "row_count", "http_status", "error_code",
        "sql_preview", "sql_hash",
    ]
    for k in meta_keys:
        if k in ev and ev.get(k) not in (None, "", []):
            st.write(f"- **{k}**: {ev.get(k)}")

with right:
    st.subheader("🧾 Payload complet (JSON)")
    payload = ev.get("payload") or {}
    st.code(_safe_json(payload), language="json")

    # Convenience sections
    st.subheader("🧠 SQL / Reasons (si présents)")
    sql = payload.get("sql") or ev.get("sql_preview") or ""
    reasons = payload.get("reasons") or []
    params = payload.get("params") or []

    if sql:
        st.code(sql, language="sql")
    if params:
        st.write("**params**")
        st.code(_safe_json(params), language="json")
    if reasons:
        st.write("**reasons**")
        st.code(_safe_json(reasons), language="json")
