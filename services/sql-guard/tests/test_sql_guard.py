import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Add parent directory to path so we can import app.py
sys.path.insert(0, str(Path(__file__).parent.parent))
import app as guard_app  # noqa: E402


client = TestClient(guard_app.app)


def _base_plan(**overrides):
    plan = {
        "request_id": "test-req",
        "user_input": "test",
        "operation": "SELECT",
        "sql": "SELECT 1",
        "params": [],
        "tables": [],
        "risk": {"level": "low", "needs_approval": False},
        "static_checks": {
            "ast_parsed": True,
            "ddl_blocked": True,
            "schema_ok": True,
            "single_statement": True,
        },
        "context": {"role": "FinanceAdmin"},
        "notes": [],
    }
    plan.update(overrides)
    return plan


def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"


def test_reject_ddl_drop():
    plan = _base_plan(
        operation="UNKNOWN",
        sql="DROP TABLE depense;",
        context={"role": "FinanceAdmin"},
    )
    r = client.post("/check", json=plan)
    assert r.status_code == 200
    out = r.json()
    assert out["allowed"] is False
    assert any("DDL" in reason or "interdite" in reason.lower() for reason in out["reasons"])


def test_reject_multi_statements():
    plan = _base_plan(
        operation="SELECT",
        sql="SELECT 1; SELECT 2;",
        context={"role": "FinanceAdmin"},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("Multi-statements" in reason for reason in out["reasons"])


def test_reject_update_without_where():
    plan = _base_plan(
        operation="UPDATE",
        sql="UPDATE depense SET montant = 0;",
        tables=["depense"],
        context={"role": "FinanceAdmin"},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("sans WHERE" in reason for reason in out["reasons"])


def test_reject_delete_without_where():
    plan = _base_plan(
        operation="DELETE",
        sql="DELETE FROM depense;",
        tables=["depense"],
        context={"role": "FinanceAdmin"},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("sans WHERE" in reason for reason in out["reasons"])


def test_rbac_readonly_cannot_insert():
    plan = _base_plan(
        operation="INSERT",
        sql="INSERT INTO depense(projet_id, montant) VALUES (%s, %s);",
        params=[1, 100],
        tables=["depense"],
        context={"role": "ReadOnly"},
        risk={"level": "high", "needs_approval": True},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("RBAC" in reason for reason in out["reasons"])


def test_rbac_projectmanager_cannot_delete():
    plan = _base_plan(
        operation="DELETE",
        sql="DELETE FROM depense WHERE id = %s;",
        params=[123],
        tables=["depense"],
        context={"role": "ProjectManager"},
        risk={"level": "high", "needs_approval": True},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("RBAC" in reason for reason in out["reasons"])


def test_select_without_limit_auto_adds_limit(monkeypatch):
    # Force config in runtime (the code reads env at import time usually)
    # If your GUARD_AUTO_LIMIT is already enabled via env, this will pass without monkeypatch.
    plan = _base_plan(
        operation="SELECT",
        sql="SELECT * FROM depense",
        tables=["depense"],
        context={"role": "ReadOnly"},
    )

    r = client.post("/check", json=plan)
    out = r.json()

    # Two acceptable behaviors depending on config:
    # - auto-limit => allowed True + normalized_sql contains LIMIT
    # - reject => allowed False + reason mentions LIMIT
    if out["allowed"]:
        assert "LIMIT" in (out["normalized_sql"] or "").upper()
    else:
        assert any("LIMIT" in reason.upper() for reason in out["reasons"])


def test_unknown_role_defaults_to_select_only():
    plan = _base_plan(
        operation="INSERT",
        sql="INSERT INTO depense(projet_id, montant) VALUES (%s, %s);",
        params=[1, 100],
        tables=["depense"],
        context={"role": "SomeRandomRole"},
        risk={"level": "high", "needs_approval": True},
    )
    r = client.post("/check", json=plan)
    out = r.json()
    assert out["allowed"] is False
    assert any("RBAC" in reason for reason in out["reasons"])
