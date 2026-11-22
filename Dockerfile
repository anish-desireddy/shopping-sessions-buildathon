# Shopping Sessions starter: DuckDB skeleton.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.

# Build the Rust event/attribution binary for the same glibc as the runtime.
FROM rust:1.85-slim-bookworm AS rust-build
WORKDIR /build
COPY rust/ /build/rust/
RUN cargo build --release --manifest-path rust/shopping-pipeline/Cargo.toml

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=rust-build /build/rust/shopping-pipeline/target/release/shopping-pipeline /app/bin/shopping-pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
#
# Recommended: W8/P128. The documented fallback for a 6-vCPU host is W6/P128:
# set RUST_WORKERS=6, SHOPPING_CLICK_WORKERS=6 and SHOPPING_SESSION_WORKERS=6,
# and keep PARTITION_COUNT=128. This file uses the recommended W8/P128 values.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIPELINE_BACKEND=rust-all \
    RUST_WORKERS=8 \
    PARTITION_COUNT=128 \
    SHOPPING_INMEMORY_PARTS=1 \
    SHOPPING_COMPACT_EVENTS=1 \
    SHOPPING_PARALLEL_CLICKS=1 \
    SHOPPING_CLICK_WORKERS=8 \
    SHOPPING_PARALLEL_SESSIONIZE=1 \
    SHOPPING_SESSION_WORKERS=8 \
    SHOPPING_FUSED_CLICKS=1

WORKDIR /app
CMD ["/app/run.sh"]
