import requests

BASE_URL = "http://localhost:8001"

def test_convert_select_http():
    r = requests.post(
        f"{BASE_URL}/convert",
        json={"user_input": "Montre les dépenses du projet Alpha"}
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert "sql" in data
    assert "operation" in data
