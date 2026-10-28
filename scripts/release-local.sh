#!/usr/bin/env bash
# Build the release artifacts locally.
#
# GitHub Actions minutes are not always available, and the artifacts are the same
# either way: one static binary per target, a tarball each, and a checksum file.
# This script builds what this host can build and says what it could not.
#
#     scripts/release-local.sh
#
# Output: dist/dbnoop-<target>.tar.gz, dist/pgnoop-<target>.tar.gz and
# dist/SHA256SUMS. Nothing is tagged, pushed or published here.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
dist="$root/dist"
mkdir -p "$dist"

say() { printf 'release-local: %s\n' "$*" >&2; }

# The targets `dist-workspace.toml` declares. Linux is static musl (the binary
# runs anywhere); the macOS ones cannot be cross-built from Linux and are
# reported rather than skipped silently.
targets=(x86_64-unknown-linux-musl aarch64-unknown-linux-musl)
native_uname=$(uname -sm)

installed() { rustup target list --installed 2>/dev/null | grep -qx "$1"; }

for target in "${targets[@]}"; do
    if ! installed "$target"; then
        say "SKIPPED $target: not installed (rustup target add $target)"
        continue
    fi
    case "$target" in
        aarch64-unknown-linux-musl)
            # A cross C compiler for jemalloc's build script. Without one, the
            # target is declared and not built -- which is a fact about this
            # host, not about the release.
            if ! command -v aarch64-linux-musl-gcc >/dev/null 2>&1; then
                say "SKIPPED $target: no aarch64-linux-musl-gcc on this host"
                continue
            fi
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc
            export CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc
            ;;
    esac

    say "building $target"
    cargo build --locked --release --target "$target" --bin dbnoop --bin pgnoop

    for bin in dbnoop pgnoop; do
        src="target/$target/release/$bin"
        pkg="db-noop-$version-$target"
        stage="$dist/$pkg"
        rm -rf "$stage"
        mkdir -p "$stage"
        cp "$src" "$stage/"
        cp README.md LICENSE-MIT LICENSE-APACHE "$stage/"
        # The name dist publishes: the one the README's download URL uses, so a
        # local build can be uploaded as it is.
        tar -czf "$dist/$bin-$target.tar.gz" -C "$stage" .
        rm -rf "$stage"
        say "wrote dist/$bin-$target.tar.gz"
    done
done

# The macOS targets `dist-workspace.toml` also declares. Only a macOS host can
# build them, and this script says so rather than pretending.
case "$native_uname" in
    Darwin*) say "NOTE: the macOS targets are not built here; dist will build them in CI" ;;
    *)       say "SKIPPED the macOS targets: only a macOS host can build them" ;;
esac

if compgen -G "$dist/*.tar.gz" >/dev/null; then
    ( cd "$dist" && sha256sum ./*.tar.gz > SHA256SUMS )
    say "wrote dist/SHA256SUMS"
    cat "$dist/SHA256SUMS" >&2
else
    say "FAILED: nothing was built"
    exit 1
fi
