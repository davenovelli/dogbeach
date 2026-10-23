FROM node:24.22.1-bookworm-slim AS node
FROM python:3.12-slim-trixie
COPY --from=node /usr/local/bin/node /usr/local/bin/node
RUN apt-get update && apt-get install -y --no-install-recommends chromium chromium-driver chromium-sandbox ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd -m -u 1000 worker && mkdir -p /app/log /app/data /app/scrapers/data && chown -R worker:worker /app
USER worker
ENV PYTHONUNBUFFERED=1 YEWREVIEW_CHROME_BINARY=/usr/bin/chromium YEWREVIEW_CHROMEDRIVER=/usr/bin/chromedriver PLAYWRIGHT_NODEJS_PATH=/usr/local/bin/node
CMD ["python", "--version"]
