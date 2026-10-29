# Stage 1: Build Rust Coordinator
FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --release

# Stage 2: Runtime Image
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install static FFmpeg 7.0.2
RUN wget -q https://johnvansickle.com/ffmpeg/releases/ffmpeg-7.0.2-amd64-static.tar.xz -O ffmpeg-static.tar.xz && \
    tar -xf ffmpeg-static.tar.xz && \
    cp ffmpeg-7.0.2-amd64-static/ffmpeg /usr/bin/ffmpeg && \
    cp ffmpeg-7.0.2-amd64-static/ffprobe /usr/bin/ffprobe && \
    chmod +x /usr/bin/ffmpeg /usr/bin/ffprobe && \
    rm -rf ffmpeg-7.0.2-amd64-static* ffmpeg-static.tar.xz

WORKDIR /app

# 🚀 Install required Python libraries for the sync service
RUN pip install --no-cache-dir requests

# Copy Python scripts and entrypoint wrapper (explicitly checking files that exist)
COPY node_sa.py ./
COPY arr_sync_service.py ./
COPY entrypoint.sh ./

# Copy the example config template as your container's default config
COPY config_node_sa.multi_example.json ./config_node_sa.json

# Copy compiled Rust coordinator from Stage 1
COPY --from=builder /app/target/release/media_manager /app/media_manager

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Command to run all services via entrypoint wrapper
CMD ["/app/entrypoint.sh"]
