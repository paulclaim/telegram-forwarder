FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# Persist all runtime state under /app/data (mount a volume there).
# MSG_MAP / RETRY_QUEUE defaulted here so a minimal `-v data:/app/data`
# does not lose live edit maps or parked retries on container recreate.
ENV STATE_PATH=/app/data/watermarks.json \
    PAIRS_PATH=/app/data/pairs.json \
    RUN_LOG_PATH=/app/data/run_log.json \
    MSG_MAP_PATH=/app/data/message_map.json \
    RETRY_QUEUE_PATH=/app/data/retry_queue.json \
    PORT=8080

RUN mkdir -p /app/data

EXPOSE 8080

CMD ["python", "server.py"]
