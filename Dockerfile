FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOWNTIME_DB_PATH=/data/gateway.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 单 worker：SQLite 单库多 worker 写仍可串行，但单进程 + BEGIN IMMEDIATE
# 能最直观地保证事务边界与并发更正的版本一致性
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
