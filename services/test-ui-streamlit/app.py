import os
import time
import requests
import streamlit as st

API_URL = os.getenv("API_GATEWAY_SIMULATE_URL", "http://api-gateway:8000/simulate")
ACTOR_ID_DEFAULT = os.getenv("TEST_UI_ACTOR_ID", "whatsapp:+33600000000")
TIMEOUT_S = float(os.getenv("TEST_UI_TIMEOUT_S", "30"))

st.set_page_config(page_title="Orionis – Test WhatsApp UI", layout="centered")

# --- Minimal CSS to look like WhatsApp bubbles ---
st.markdown("""
<style>
.chat-wrap {max-width: 720px; margin: 0 auto;}
.bubble {
  padding: 10px 12px; border-radius: 14px; margin: 6px 0; width: fit-content;
  max-width: 90%; line-height: 1.35; font-size: 0.98rem;
}
.user { background: #DCF8C6; margin-left: auto; border-top-right-radius: 4px;}
.bot  { background: #FFFFFF; margin-right: auto; border-top-left-radius: 4px; border: 1px solid #eee;}
.meta { font-size: 0.75rem; opacity: 0.55; margin-top: 2px;}
.header {
  background: #075E54; color: white; padding: 10px 12px; border-radius: 12px;
  position: sticky; top: 0; z-index: 9;
}
.small {font-size: 0.85rem; opacity: 0.9;}
</style>
""", unsafe_allow_html=True)

# --- Session state ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "actor_id" not in st.session_state:
    st.session_state.actor_id = ACTOR_ID_DEFAULT

def add_message(role: str, text: str):
    st.session_state.messages.append({"role": role, "text": text, "ts": time.strftime("%H:%M:%S")})

def call_backend(actor_id: str, body: str) -> str:
    payload = {"actor_id": actor_id, "body": body}
    r = requests.post(API_URL, json=payload, timeout=TIMEOUT_S)
    r.raise_for_status()
    data = r.json()
    return (data.get("reply") or "").strip() or "✅ (Pas de réponse)"

# --- UI header ---
st.markdown('<div class="chat-wrap">', unsafe_allow_html=True)
st.markdown(f"""
<div class="header">
  <div><b>Orionis Finance Assistant</b></div>
  <div class="small">Mode test (sans Twilio) • actor_id = {st.session_state.actor_id}</div>
</div>
""", unsafe_allow_html=True)

# --- Controls ---
with st.expander("⚙️ Options", expanded=False):
    st.session_state.actor_id = st.text_input("Actor ID (whatsapp:+33...)", st.session_state.actor_id)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧹 Vider la conversation"):
            st.session_state.messages = []
            st.rerun()
    with col2:
        if st.button("👋 Message de départ"):
            add_message("bot", "Bonjour 👋 Envoie une demande (ex: Liste des projets).")
            st.rerun()

# --- Render chat history ---
for m in st.session_state.messages:
    css = "user" if m["role"] == "user" else "bot"
    st.markdown(
        f'<div class="bubble {css}">{m["text"]}<div class="meta">{m["ts"]}</div></div>',
        unsafe_allow_html=True
    )

# --- Input box ---
st.markdown("---")
with st.form("chat_form", clear_on_submit=True):
    user_text = st.text_input("Message", placeholder="Tape ton message…")
    submitted = st.form_submit_button("Envoyer")

if submitted and user_text.strip():
    user_text = user_text.strip()
    add_message("user", user_text)

    try:
        reply = call_backend(st.session_state.actor_id, user_text)
    except Exception as e:
        reply = f"❌ Erreur: {e}"

    add_message("bot", reply)
    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)
