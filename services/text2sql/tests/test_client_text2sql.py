import requests
r = requests.post('http://127.0.0.1:8001/convert', json={'user_input':'Montre les dépenses du projet Alpha'})
print('STATUS:', r.status_code)
print('RESPONSE:', r.text)