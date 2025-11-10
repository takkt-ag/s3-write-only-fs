FROM rust:latest

RUN apt-get update && apt-get install -y \
    libfuse-dev \
    libsystemd-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && rustup component add clippy

WORKDIR /workspace