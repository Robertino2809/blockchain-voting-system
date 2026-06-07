FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

CMD ["sh", "-c", "uvicorn services.voting_service.main:app --host 0.0.0.0 --port ${NODE_PORT:-8001}"]