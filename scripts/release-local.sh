#!/usr/bin/env bash
# Build the release artifacts.
#
# GitHub Actions minutes are not always available, and a host with no musl
# toolchain cannot build a static binary locally -- jemalloc's configure refuses
# to run without one ("CC=x86_64-linux-musl-gcc ... exit status 77"). So the
# build happens in the container the Dockerfile already describes: `--target
# build` is its toolchain stage, and the binaries are copied out of it.
#
#     scripts/release-local.sh                 # x86_64 musl, and aarch64 if it can
#     TARGETS=x86_64-unknown-linux-musl scripts/release-local.sh
#
# Output: dist/dbnoop-<target>.tar.gz, dist/pgnoop-<target>.tar.gz and
# dist/SHA256SUMS -- the names the README's download URLs use. Nothing is
# tagged, pushed or published here.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

dist="$root/dist"
mkdir -p "$dist"

say() { printf 'release-local: %s\n' "$*" >&2; }

if ! command -v docker >/dev/null 2>&1; then
    say "FAILED: docker is needed to build the static binaries on this host"
    say "        jemalloc's configure needs a musl C compiler, which is what the"
    say "        build image carries. Install docker, or build on a host with a"
    say "        musl toolchain: cargo build --release --target <target>"
    exit 1
fi

# The version comes from the manifest, so the artifacts and the crate cannot
# disagree.
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
say "db-noop $version"

# The targets `dist-workspace.toml` declares. The macOS ones cannot be built
# from Linux at all; aarch64 needs an emulated build, which needs binfmt.
targets=${TARGETS:-"x86_64-unknown-linux-musl aarch64-unknown-linux-musl"}

image_for() { echo "db-noop-build-${1}"; }

build_stage() { # target, and any extra docker build arguments
    local target=$1
    shift
    docker build \
        --target build \
        --build-arg "TARGET=$target" \
        -t "$(image_for "$target")" \
        "$@" \
        .
}

extract() { # target, image
    local target=$1 image=$2
    local stage="$dist/db-noop-$version-$target"
    rm -rf "$stage"
    mkdir -p "$stage"

    # **A stopped container is how the binaries come out.** The build stage has
    # no entrypoint to run (its job is finished when the image exists), so the
    # container is created and never started, and `docker cp` reads the paths
    # cargo wrote.
    local id
    id=$(docker create "$image")
    for bin in dbnoop pgnoop; do
        if ! docker cp "$id:/src/target/$target/release/$bin" "$stage/$bin"; then
            docker rm -f "$id" >/dev/null 2>&1 || true
            say "FAILED: the build image has no $bin for $target"
            return 1
        fi
        chmod +x "$stage/$bin"
    done
    docker rm -f "$id" >/dev/null 2>&1 || true

    cp README.md LICENSE-MIT LICENSE-APACHE "$stage/"
    # The name dist publishes: the one the README's download URLs use, so a
    # local build can be uploaded as it is.
    for bin in dbnoop pgnoop; do
        tar -czf "$dist/$bin-$target.tar.gz" -C "$stage" .
        say "wrote dist/$bin-$target.tar.gz"
    done
    rm -rf "$stage"
}

built=0
for target in $targets; do
    case "$target" in
        x86_64-unknown-linux-musl)
            say "building $target"
            if build_stage "$target" && extract "$target" "$(image_for "$target")"; then
                built=$((built + 1))
            else
                say "FAILED: $target did not build"
            fi
            ;;
        aarch64-unknown-linux-musl)
            # Cross-built in an arm64 image, which needs binfmt/qemu registered
            # on this host. A failure here is the host's emulation rather than
            # the source, so it is reported and the rest carries on.
            say "building $target (emulated; needs binfmt)"
            if build_stage "$target" --platform linux/arm64 2>/dev/null &&
                extract "$target" "$(image_for "$target")"; then
                built=$((built + 1))
            else
                say "SKIPPED $target: this host cannot run an arm64 build"
                say "        docker run --privileged --rm tonistiigi/binfmt --install arm64"
            fi
            ;;
        *)
            say "SKIPPED $target: not a target this script knows"
            ;;
    esac
done

say "SKIPPED the macOS targets: only a macOS host can build them (dist builds them in CI)"

if compgen -G "$dist/*.tar.gz" >/dev/null; then
    ( cd "$dist" && sha256sum ./*.tar.gz > SHA256SUMS )
    say "wrote dist/SHA256SUMS"
    cat "$dist/SHA256SUMS" >&2
else
    say "FAILED: nothing was built"
    exit 1
fi

say "built $built target(s)"
