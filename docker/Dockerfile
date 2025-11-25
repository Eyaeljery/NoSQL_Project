# docker/Dockerfile.migration

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts ./scripts
COPY data ./data

WORKDIR /app/scripts

CMD ["python", "migrate_to_mongo.py"]
