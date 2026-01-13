from fastapi import FastAPI, Request
from twilio.twiml.messaging_response import MessagingResponse

app = FastAPI(title="API Gateway")
@app.get("/")
def root():
    return {"message": "API Gateway is running"}


@app.post("/webhook_whatsapp")
async def whatsapp_webhook(request: Request):
    data = await request.form()
    user_number = data.get("From")
    message_body = data.get("Body")

    resp = MessagingResponse()
    resp.message(f"Message reçu de {user_number}: {message_body}")
    return str(resp)
