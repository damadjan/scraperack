FROM python:3.10.21-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY app.py cache_monitor.py ray_nodes.py ray_tasks.py ./

RUN useradd --create-home --uid 1000 scraperack
USER scraperack

EXPOSE 9987
CMD ["python", "app.py"]
