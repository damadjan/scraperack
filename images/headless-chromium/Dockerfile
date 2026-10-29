FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        chromium \
        fonts-liberation \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

ENV HOME=/tmp/chromium
EXPOSE 9222
ENTRYPOINT ["chromium", "--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--no-first-run", "--no-default-browser-check", "--remote-allow-origins=*", "--remote-debugging-address=0.0.0.0", "--remote-debugging-port=9222", "--user-data-dir=/tmp/chromium/profile"]
CMD ["about:blank"]
