FROM python:3.10-slim

WORKDIR /app

COPY audit.py .
RUN pip install psycopg2-binary python-dotenv

CMD ["python", "audit.py"]
