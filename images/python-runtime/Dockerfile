FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates util-linux \
    && ln -s /usr/local/bin/python3 /usr/bin/python3 \
    && pip install --no-cache-dir cloudpickle \
    && rm -rf /var/lib/apt/lists/*