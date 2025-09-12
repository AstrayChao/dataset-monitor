FROM docker.1ms.run/rust:1.89.0 as builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./

# 创建临时文件以避免构建错误
RUN mkdir src && \
    echo "fn main() { println!(\"if you see this, the build broke\") }" > src/main.rs && \
    mkdir -p src/bin && \
    echo "fn main() { println!(\"data_fetch placeholder\") }" > src/bin/data_fetch.rs && \
    echo "fn main() { println!(\"data_monitor placeholder\") }" > src/bin/data_monitor.rs


RUN cargo build --release

RUN rm -f target/release/deps/dataset_monitor* && \
    rm -f target/release/deps/data_fetch* && \
    rm -f target/release/deps/data_monitor*


COPY src ./src

RUN cargo build --release

FROM docker.1ms.run/debian:bullseye-slim

WORKDIR /app

COPY --from=builder --chown=app:app /usr/src/app/target/release/data_fetch ./data_fetch
COPY --from=builder --chown=app:app /usr/src/app/target/release/data_monitor ./data_monitor

# 复制配置文件
COPY --chown=app:app ./config.yaml ./config.yaml

USER root

