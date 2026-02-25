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
    cargo clippy --workspace --all-targets -- -D warnings

# Build the extension binary for Linux (matches host architecture for Docker tests)
build-extension:
    cargo lambda build --release -p lambda-otel-relay --extension {{ arch_flag }}

# Build the test handler binary for Linux
build-test-handler:
    cargo lambda build --release -p test-handler {{ arch_flag }}

# Build mock-rie Docker image (proxy + wrapper scripts baked in)
build-mock-rie: build-extension build-test-handler
    #!/usr/bin/env bash
    # Not a Lambda, but cargo-lambda is a convenient cross-compiler (via Zig) for Linux
    cargo lambda build --release -p telemetry-proxy {{ arch_flag }}

    # Copy the proxy binary and wrapper scripts into a temp context directory for Docker
    mkdir -p target/mock-rie-context
    cp target/lambda/telemetry-proxy/bootstrap target/mock-rie-context/telemetry-proxy
    cp tests/fixtures/mock-rie/ext-wrapper.sh target/mock-rie-context/
    cp tests/fixtures/mock-rie/entrypoint.sh target/mock-rie-context/
    docker build -t mock-rie:latest -f tests/fixtures/mock-rie/Dockerfile target/mock-rie-context/

# Run unit tests
test:
    cargo test -p lambda-otel-relay

# Run integration tests (builds mock-rie image first)
integration-test: build-mock-rie
    cargo test --test '*' -- --nocapture

# Run all tests
test-all: test integration-test

# Download vendored proto files at the version in proto/.version
vendor:
    #!/usr/bin/env bash
    set -euo pipefail
    tag=$(cat proto/.version | tr -d '[:space:]')
    echo "Vendoring opentelemetry-proto at ${tag}"
    while IFS= read -r file || [[ -n "${file}" ]]; do
        [[ -z "${file}" ]] && continue
        dest="proto/opentelemetry/proto/${file}"
        mkdir -p "$(dirname "${dest}")"
        curl -sfL "https://raw.githubusercontent.com/open-telemetry/opentelemetry-proto/${tag}/opentelemetry/proto/${file}" -o "${dest}"
        echo "  ${dest}"
    done < proto/.files
    echo "Done — ${tag}"
    # Check for newer releases
    latest=$(gh api repos/open-telemetry/opentelemetry-proto/releases/latest --jq '.tag_name' 2>/dev/null || true)
    if [[ -n "${latest}" && "${latest}" != "${tag}" ]]; then
        echo "Newer version available: ${latest}"
    fi

# Upgrade vendored proto version (omit tag for latest)
vendor-upgrade tag="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -n "{{ tag }}" ]]; then
        echo "{{ tag }}" > proto/.version
    else
        latest=$(gh api repos/open-telemetry/opentelemetry-proto/releases/latest --jq '.tag_name')
        echo "${latest}" > proto/.version
        echo "Resolved latest: ${latest}"
    fi
    just vendor
