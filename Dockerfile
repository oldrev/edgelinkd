FROM rust:latest AS builder
WORKDIR /app

COPY . /app/
RUN cargo build --release

RUN strip target/release/edgelinkd
RUN mkdir /export && \
  cp $(ldd /app/target/release/edgelinkd | grep -ve 'libc\.so' -e 'ld-linux' -e 'vdso\.so' | sed -e s/'.*=> '// -e s/' (0x.*'//) /export

FROM debian:bookworm-slim as release
WORKDIR /app
COPY --from=builder /app/target/release/edgelinkd .
COPY --from=builder /export /usr/local/lib
RUN ldconfig
EXPOSE 1888

CMD ["./edgelinkd"]
