# import os
# import requests
# import pytest


# TEXT2SQL_URL = os.getenv("TEXT2SQL_URL", "http://localhost:8001")
# GUARD_URL = os.getenv("SQL_GUARD_URL", "http://localhost:8002")


# def _post_json(url: str, payload: dict, timeout: int = 10):
#     r = requests.post(url, json=payload, timeout=timeout)
#     return r.status_code, r.text, r.json() if r.headers.get("content-type", "").startswith("application/json") else None


# @pytest.mark.integration
# def test_e2e_select_allowed_with_stub_llm():
#     """
#     End-to-end:
#     - text2sql returns SQLPlan
#     - sql-guard allows it (and may add LIMIT)
#     """
#     # Ici on suppose que TEXT2SQL_STUB_LLM=1 est activé dans le service text2sql
#     # (sinon, tu dépends d'Ollama).
#     status, raw, plan = _post_json(
#         f"{TEXT2SQL_URL}/convert",
#         {
#             "user_input": "Montre les dépenses du projet Alpha",
#             "entreprise_id": 1,
#             "role": "ReadOnly",
#             "actor_id": "+33600000000",
#         },
#         timeout=20,
#     )
#     assert status == 200, raw
#     assert plan["operation"] == "SELECT"
#     assert "sql" in plan and "%s" in plan["sql"]
#     assert plan["risk"]["needs_approval"] is False

#     status, raw, out = _post_json(f"{GUARD_URL}/check", plan)
#     assert status == 200, raw
#     assert out["allowed"] is True, out
#     assert out["operation"] == "SELECT"
#     assert out["normalized_sql"]  # should echo or normalized with LIMIT


# @pytest.mark.integration
# def test_e2e_insert_rejected_by_rbac_readonly():
#     """
#     Build a SQLPlan for INSERT (direct plan, not via text2sql) then guard should reject by RBAC.
#     This tests the same schema/AST path used in production.
#     """
#     plan = {
#         "request_id": "e2e-insert-1",
#         "user_input": "Ajoute une depense",
#         "operation": "INSERT",
#         "sql": "INSERT INTO depense(projet_id, montant) VALUES (%s, %s);",
#         "params": [1, 100],
#         "tables": ["depense"],
#         "risk": {"level": "high", "needs_approval": True},
#         "static_checks": {"ast_parsed": True, "ddl_blocked": True, "schema_ok": True, "single_statement": True},
#         "context": {"role": "ReadOnly"},
#         "notes": [],
#     }

#     status, raw, out = _post_json(f"{GUARD_URL}/check", plan)
#     assert status == 200, raw
#     assert out["allowed"] is False
#     assert any("RBAC" in r for r in out["reasons"])


# @pytest.mark.integration
# def test_e2e_update_without_where_blocked():
#     """
#     UPDATE sans WHERE => doit être refusé par guard.
#     """
#     plan = {
#         "request_id": "e2e-update-1",
#         "user_input": "Modifie toutes les depenses",
#         "operation": "UPDATE",
#         "sql": "UPDATE depense SET montant = 0;",
#         "params": [],
#         "tables": ["depense"],
#         "risk": {"level": "high", "needs_approval": True},
#         "static_checks": {"ast_parsed": True, "ddl_blocked": True, "schema_ok": True, "single_statement": True},
#         "context": {"role": "FinanceAdmin"},
#         "notes": [],
#     }

#     status, raw, out = _post_json(f"{GUARD_URL}/check", plan)
#     assert status == 200, raw
#     assert out["allowed"] is False
#     assert any("sans WHERE" in r for r in out["reasons"])


# @pytest.mark.integration
# def test_e2e_text2sql_plan_then_guard_decides():
#     """
#     Test the real coupling: take plan from Text2SQL then guard decides.
#     This is the core integration contract.
#     """
#     status, raw, plan = _post_json(
#         f"{TEXT2SQL_URL}/convert",
#         {
#             "user_input": "Montre les dépenses du projet Alpha",
#             "role": "ReadOnly",
#             "actor_id": "+33600000000",
#         },
#         timeout=20,
#     )
#     assert status == 200, raw

#     # Contract expectations
#     assert "request_id" in plan
#     assert "risk" in plan and "needs_approval" in plan["risk"]
#     assert "static_checks" in plan

#     status, raw, out = _post_json(f"{GUARD_URL}/check", plan)
#     assert status == 200, raw

#     # Allowed true or false depending on schema presence, but must always return a decision payload
#     assert "allowed" in out
#     assert "reasons" in out
#     assert "operation" in out
