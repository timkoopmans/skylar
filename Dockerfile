FROM rust:1.81.0 AS chef
WORKDIR app
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin skylar

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt install -y openssl

COPY --from=builder /app/target/release/skylar /app/skylar

CMD ["/app/skylar"]