FROM python:3.10.21-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/damadjan/scraperack"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/scraperack

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY scraperack_platform ./scraperack_platform

RUN useradd --create-home --uid 1000 scraperack
USER scraperack

CMD ["uvicorn", "scraperack_platform.app:app", "--host", "0.0.0.0", "--port", "8080"]
