# Shopping Sessions engine: one static Rust binary on an empty base image.
# Pinned by digest (rustc 1.100.0) so the final rebuild uses the exact toolchain we validated.
FROM rust:1-slim-bookworm@sha256:7c7d4eb4d05b7781187a327809466cfddf29753f6defdc3d19a0e291951b7a37 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon (Cascade Lake or newer): AVX2/BMI2 are safe. Static glibc so the
# runtime image can be empty and start fast.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
ENTRYPOINT ["/sessions"]
