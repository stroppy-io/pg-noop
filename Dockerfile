# syntax=docker/dockerfile:1

# A static musl build, in the image that has the toolchain for it, and then
# nothing else: the server has no runtime dependencies to install.
#
# **Pinned to a toolchain the dependencies accept.** `Cargo.toml` declares
# `rust-version = "1.89"` because pgwire 0.38 and smol_str 0.3 both require it,
# and an image older than that fails at the first `cargo build` with cargo's
# "rustc 1.85 is not supported" -- which is how this line was found.
FROM rust:1.90-bookworm AS build

# The target this stage builds, so that `scripts/release-local.sh` can build
# another architecture out of the same stage instead of a Dockerfile of its own.
ARG TARGET=x86_64-unknown-linux-musl

# musl-gcc is the C compiler jemalloc-sys needs for a musl target. The repo's
# .cargo/config.toml sets `+crt-static` for the target; nothing else is required
# to produce a binary that runs on any Linux of that architecture.
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add "${TARGET}"

WORKDIR /src
COPY . .

# `--locked` so the image is built from the dependency set the repository pins,
# and the two binary names explicitly so a stray target cannot become the
# entrypoint. Both are built: `pgnoop` is what existing scripts call, and the
# release script copies both out of this stage.
RUN cargo build --locked --release --target "${TARGET}" --bin dbnoop --bin pgnoop

FROM scratch

ARG TARGET=x86_64-unknown-linux-musl

# One static binary, nothing else. The image is the server.
COPY --from=build /src/target/${TARGET}/release/dbnoop /dbnoop

# PostgreSQL and MySQL/MariaDB.
EXPOSE 5432 3306

# Bound on every interface: a container's loopback is not the caller's. The
# MySQL listener is on because an image that has to be told which protocols to
# serve is an image whose second protocol nobody finds.
ENTRYPOINT ["/dbnoop"]
CMD ["--host", "0.0.0.0", "--port", "5432", "--mysql-port", "3306"]
