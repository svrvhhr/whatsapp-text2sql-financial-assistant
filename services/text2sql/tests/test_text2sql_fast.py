from fastapi.testclient import TestClient
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app as text2sql_app


client = TestClient(text2sql_app.app)


def test_convert_select_schema_linking(monkeypatch):
    # Mock LLM output (pour tester sans Ollama)
    def fake_call_ollama(prompt: str):
        return {
            "operation": "SELECT",
            "sql": "SELECT d.* FROM depense d JOIN projet p ON p.id = d.projet_id WHERE p.nom = %s;",
            "params": ["Alpha"],
        }

    monkeypatch.setattr(text2sql_app, "call_ollama", fake_call_ollama)

    r = client.post("/convert", json={"user_input": "Montre les dépenses du projet Alpha"})
    assert r.status_code == 200, r.text
    data = r.json()

    assert data["operation"] == "SELECT"
    assert "JOIN projet" in data["sql"]
    assert data["params"] == ["Alpha"]
    assert "depense" in data["tables"]
    assert "projet" in data["tables"]
    assert data["needs_approval"] is False
    assert data["risk_level"] == "low"


def test_convert_insert_needs_approval(monkeypatch):
    # Mock LLM output (INSERT)
    def fake_call_ollama(prompt: str):
        return {
            "operation": "INSERT",
            "sql": "INSERT INTO depense (projet_id, compte_id, type_depense, montant, devise, description, date_depense) VALUES (%s,%s,%s,%s,%s,%s,%s);",
            "params": [1, 1, "cloud", 220.0, "EUR", "test", "2025-05-02"],
        }

    monkeypatch.setattr(text2sql_app, "call_ollama", fake_call_ollama)

    r = client.post("/convert", json={"user_input": "Ajoute une dépense de 220€ pour le projet 1"})
    assert r.status_code == 200, r.text
    data = r.json()

    assert data["operation"] == "INSERT"
    assert "INSERT INTO depense" in data["sql"]
    assert data["needs_approval"] is True
    assert data["risk_level"] == "high"
    assert "depense" in data["tables"]
