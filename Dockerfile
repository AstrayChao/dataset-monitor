FROM docker.1ms.run/rust:1.89.0 as builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && \
    echo "fn main() { println!(\"if you see this, the build broke\") }" > src/main.rs


RUN cargo build --release

RUN rm -f target/release/deps/dataset_monitor*

COPY src ./src

RUN cargo build --release

FROM docker.1ms.run/debian:bullseye-slim

RUN groupadd -g 10001 app && \
    useradd -u 10001 -g app app

WORKDIR /app

COPY --from=builder --chown=app:app /usr/src/app/target/release/dataset-monitor ./dataset-monitor

COPY --chown=app:app   ./config.yaml ./config.yaml
USER 10001

ENTRYPOINT ["./dataset-monitor"]