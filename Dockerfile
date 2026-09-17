# Multi-stage reproducible container build
FROM python:3.12-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir build
COPY . /build/
RUN python -m build --wheel

FROM python:3.12-slim AS runtime

# Create isolated non-login system user matching VPS specification
RUN groupadd -g 1001 bitcoin-data && \
    useradd -u 1001 -g bitcoin-data -s /usr/sbin/nologin -M bitcoin-data

WORKDIR /app
COPY --from=builder /build/dist/*.whl /app/
RUN pip install --no-cache-dir /app/*.whl && rm /app/*.whl

# Prepare persistent data mount point
RUN mkdir -p /srv/data/bitcoin-data-platform && \
    chown -R bitcoin-data:bitcoin-data /srv/data/bitcoin-data-platform

USER bitcoin-data
VOLUME ["/srv/data/bitcoin-data-platform"]
ENTRYPOINT ["bitcoin-data"]
CMD ["status", "--format", "text"]
