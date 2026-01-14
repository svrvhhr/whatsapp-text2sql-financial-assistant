# import requests

# url = "http://localhost:8001/convert"
# payload = {"message": "Show all expenses for project Alpha in 2025"}

# r = requests.post(url, json=payload, timeout=300)
# print(r.status_code, r.text)
# r.raise_for_status()
# print("SQL:", r.json()["sql"])
# # Note: On ne compare pas le SQL exact (LLM ≠ déterministe)
# # On vérifie que la requête est cohérente avec la demande utilisateur