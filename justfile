arch_flag := if arch() == "aarch64" { "--arm64" } else { "" }
proto_version := trim(read("proto/.version"))

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
    echo "Vendoring opentelemetry-proto at {{ proto_version }}"
    while IFS= read -r file || [[ -n "${file}" ]]; do
        [[ -z "${file}" ]] && continue
        dest="proto/opentelemetry/proto/${file}"
        mkdir -p "$(dirname "${dest}")"
        curl -sfL "https://raw.githubusercontent.com/open-telemetry/opentelemetry-proto/{{ proto_version }}/opentelemetry/proto/${file}" -o "${dest}"
        echo "  ${dest}"
    done < proto/.files
    echo "Done — {{ proto_version }}"

# Upgrade vendored proto version (omit tag for latest)
vendor-upgrade tag="":
    #!/usr/bin/env bash
    set -euo pipefail
    latest=$(gh api repos/open-telemetry/opentelemetry-proto/releases/latest --jq '.tag_name')
    if [[ -n "{{ tag }}" ]]; then
        if ! gh api "repos/open-telemetry/opentelemetry-proto/releases/tags/{{ tag }}" --silent 2>/dev/null; then
            echo "Error: '{{ tag }}' is not a valid release of open-telemetry/opentelemetry-proto" >&2
            echo "  current: {{ proto_version }}" >&2
            echo "  latest:  ${latest}" >&2
            exit 1
        fi
        echo "{{ tag }}" > proto/.version
    else
        echo "${latest}" > proto/.version
        echo "Resolved latest: ${latest}"
    fi
    just vendor

# Tag a release: just release 0.5.0
release version:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! [[ "{{ version }}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.+)?$ ]]; then
        echo "Error: '{{ version }}' is not valid semver (expected: X.Y.Z or X.Y.Z-pre)" >&2
        exit 1
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "Error: working tree is not clean" >&2
        exit 1
    fi
    if [[ "$(git branch --show-current)" != "main" ]]; then
        echo "Error: not on main branch" >&2
        exit 1
    fi
    tmp=$(mktemp)
    sed 's/^version = ".*"/version = "{{ version }}"/' crates/extension/Cargo.toml > "$tmp" && mv "$tmp" crates/extension/Cargo.toml
    cargo generate-lockfile --quiet
    git add crates/extension/Cargo.toml Cargo.lock
    git commit -m "Release v{{ version }}"
    git tag "v{{ version }}"
    echo ""
    echo "Created commit and tag v{{ version }}."
    # Temporarily disable kitty keyboard protocol so read works in all terminals
    printf '\e[>0u'
    trap 'printf "\e[<u"' EXIT
    printf "Push to origin? [y/N] "
    read -r answer < /dev/tty
    if [[ "${answer}" =~ ^[Yy]$ ]]; then
        git push origin main "v{{ version }}"
    else
        echo "Push with: git push origin main v{{ version }}"
    fi
