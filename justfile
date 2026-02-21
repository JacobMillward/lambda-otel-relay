arch_flag := if arch() == "aarch64" { "--arm64" } else { "" }

_default:
    @just --list

# Run all CI checks
ci: fmt lint test-all

# Check formatting
fmt:
    cargo fmt --check

# Lint with clippy
lint:
    cargo clippy --all-targets -- -D warnings

# Build the extension binary for Linux (matches host architecture for Docker tests)
build-extension:
    cargo lambda build --release --extension {{ arch_flag }}

# Build mock-rie Docker image (proxy + wrapper scripts baked in)
build-mock-rie: build-extension
    #!/usr/bin/env bash
    # Not a Lambda, but cargo-lambda is a convenient cross-compiler (via Zig) for Linux
    cargo lambda build --release --bin telemetry-proxy {{ arch_flag }}

    # Copy the proxy binary and wrapper scripts into a temp context directory for Docker
    mkdir -p target/mock-rie-context
    cp target/lambda/telemetry-proxy/bootstrap target/mock-rie-context/telemetry-proxy
    cp tests/fixtures/mock-rie/ext-wrapper.sh target/mock-rie-context/
    cp tests/fixtures/mock-rie/entrypoint.sh target/mock-rie-context/
    docker build -t mock-rie:latest -f tests/fixtures/mock-rie/Dockerfile target/mock-rie-context/

# Run unit tests
test:
    cargo test --bin lambda-otel-relay

# Run integration tests (builds mock-rie image first)
integration-test: build-mock-rie
    cargo test --test integration -- --nocapture

# Run all tests
test-all: test integration-test
