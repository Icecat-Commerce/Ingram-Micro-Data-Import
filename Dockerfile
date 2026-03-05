# Multi-stage build for smaller production image
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt ./

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 icecat && \
    mkdir -p /app /var/log/icecat && \
    chown -R icecat:icecat /app /var/log/icecat

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY --chown=icecat:icecat icecat_integration ./icecat_integration
COPY --chown=icecat:icecat config/config.example.yaml ./config/config.example.yaml

USER icecat

ENTRYPOINT ["python", "-m", "icecat_integration"]
CMD ["--help"]
