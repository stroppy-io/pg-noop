# syntax=docker/dockerfile:1

# A static musl build, in the image that has the toolchain for it, and then
# nothing else: the server has no runtime dependencies to install.
FROM rust:1.85-bookworm AS build

# musl-gcc is the C compiler jemalloc-sys needs for a musl target. The repo's
# .cargo/config.toml sets `+crt-static` for the target; nothing else is required
# to produce a binary that runs on any x86-64 Linux.
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl

WORKDIR /src
COPY . .

# `--locked` so the image is built from the dependency set the repository
# pins, and `--bin dbnoop` so a stray binary name cannot become the entrypoint.
RUN cargo build --locked --release --target x86_64-unknown-linux-musl --bin dbnoop

FROM scratch

# One static binary, nothing else. The image is the server.
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/dbnoop /dbnoop

# PostgreSQL and MySQL/MariaDB.
EXPOSE 5432 3306

# Bound on every interface: a container's loopback is not the caller's. The
# MySQL listener is on because an image that has to be told which protocols to
# serve is an image whose second protocol nobody finds.
ENTRYPOINT ["/dbnoop"]
CMD ["--host", "0.0.0.0", "--port", "5432", "--mysql-port", "3306"]
