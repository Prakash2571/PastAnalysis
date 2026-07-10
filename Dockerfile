FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hourly_futures_backfill.py .
COPY generate_token.py .

ENTRYPOINT ["python", "hourly_futures_backfill.py"]
