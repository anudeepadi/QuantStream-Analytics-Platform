# ── QuantStream API ─────────────────────────────────────────────
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev curl && rm -rf /var/lib/apt/lists/*

# ── Development target (used by docker-compose) ────────────────
FROM base AS development

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "src.dashboard.backend.api.main:app", \
     "--host", "0.0.0.0", "--port", "8000", "--reload"]

# ── Production target ──────────────────────────────────────────
FROM base AS production

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN addgroup --system --gid 1001 appuser && \
    adduser --system --uid 1001 appuser
USER appuser

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "src.dashboard.backend.api.main:app", \
     "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
