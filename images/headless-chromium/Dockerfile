FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        fonts-liberation \
        python3 \
        socat \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY start-browser /usr/local/bin/start-browser
RUN chmod 0755 /usr/local/bin/start-browser

ENV HOME=/tmp/chromium
EXPOSE 9222
ENTRYPOINT ["/usr/local/bin/start-browser"]
CMD ["about:blank"]
