FROM python:3.10.21-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/damadjan/scraperack"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/scraperack

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY scraperack_platform ./scraperack_platform
RUN python -m pip install --no-cache-dir --no-deps . \
    && cd / \
    && python -c "import scraperack_platform.runner"

RUN useradd --create-home --uid 1000 scraperack \
    && mkdir -p /home/scraperack/.cache/functions /home/scraperack/.cache/pip /home/scraperack/.cache/working-dirs \
    && chown -R scraperack:scraperack /home/scraperack/.cache
USER scraperack

CMD ["uvicorn", "scraperack_platform.app:app", "--host", "0.0.0.0", "--port", "8080"]
