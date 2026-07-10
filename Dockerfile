FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hourly_futures_backfill.py .

ENTRYPOINT ["python", "hourly_futures_backfill.py"]
