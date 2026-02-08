FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/deltat /usr/local/bin/deltat
EXPOSE 5433
VOLUME /data
ENV DELTAT_DATA_DIR=/data
ENV DELTAT_BIND=0.0.0.0
CMD ["deltat"]
