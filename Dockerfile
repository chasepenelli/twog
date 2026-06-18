# twog web API — the public read surface (and gated routes) served against Neon.
# Lean image: the API only needs the orchestrator + psycopg2 (compute libs live in the Modal image).
FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2-binary are bundled in the wheel; keep the image minimal.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Source + the data files the service reads at runtime (target library, seeds).
COPY src ./src
COPY scripts ./scripts
COPY data ./data

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app

# Railway/Fly inject $PORT; default to 8000 locally. Bind 0.0.0.0 so the platform can route to it.
# NEON_DATABASE_URL must be set as a platform env var (secret).
CMD ["sh", "-c", "python scripts/run_web_api.py --host 0.0.0.0 --port ${PORT:-8000} --allow-origin \"${TWOG_API_ALLOW_ORIGIN:-*}\""]
