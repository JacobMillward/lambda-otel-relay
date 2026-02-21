# lambda-otel-relay

[![CI](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml)

A Lambda extension that acts as a lifecycle-aware OTLP proxy. It accepts telemetry from in-process OpenTelemetry SDKs over localhost, buffers it, and forwards it to an external OTLP collector. Uses the Lambda shutdown grace period to flush telemetry that would otherwise be lost on timeouts or crashes.

## Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)
- [cargo-lambda](https://www.cargo-lambda.info/)
- [just](https://github.com/casey/just)
- Docker (for integration tests)

## Build

```sh
just build-extension
```

## Test

```sh
just test              # unit tests
just integration-test  # builds extension + runs integration tests (requires Docker)
just test-all          # both
```
