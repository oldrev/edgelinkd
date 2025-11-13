FROM rust:latest AS builder
RUN apt update && apt install -y nodejs npm
WORKDIR /app

COPY . /app/
RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release --all-targets

RUN strip /app/target/release/edgelinkd
RUN mkdir /export && \
  cp $(ldd /app/target/release/edgelinkd | grep -ve 'libc\.so' -e 'ld-linux' -e 'vdso\.so' | sed -e s/'.*=> '// -e s/' (0x.*'//) /export

FROM debian:bookworm-slim as release
RUN mkdir -p /app/target/
WORKDIR /app
COPY --from=builder /app/target/release/edgelinkd .
COPY --from=builder /app/target/ui_static /app/target/ui_static 
COPY --from=builder /export /usr/local/lib
RUN ldconfig
EXPOSE 1888

CMD ["./edgelinkd"]
