_default:
    @just --list

# Build the extension binary for Linux (matches host architecture for Docker tests)
build-extension:
    cargo lambda build --release --extension {{ if arch() == "aarch64" { "--arm64" } else { "" } }}

# Run unit tests
test:
    cargo test --bin lambda-otel-relay

# Run integration tests (builds extension first)
integration-test: build-extension
    cargo test --test integration -- --nocapture

# Run all tests
test-all: test integration-test
