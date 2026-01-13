FROM python:3.10-slim

WORKDIR /app

COPY executor.py .
RUN pip install psycopg2-binary python-dotenv

CMD ["python", "executor.py"]
