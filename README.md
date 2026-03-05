# lambda-otel-relay

A lightweight, lifecycle-aware OTLP proxy for AWS Lambda.

![GitHub Release](https://img.shields.io/github/v/release/JacobMillward/lambda-otel-relay)
[![CI](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml)

## The problem

Lambda can freeze or shut down execution environments at any time. Telemetry sitting in batch processor buffers is silently lost.

## What this does

- **~1.4 MB layer, near-zero cold start impact.** Written in Rust.
- **Accepts OTLP on localhost, buffers, forwards to your collector.** Your SDK exports to `localhost:4318`.
- **Lifecycle-aware.** Hooks into the [Lambda Extensions API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html) and [Telemetry API](https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api.html) to track invocations and flush on shutdown.
- **HTTP/protobuf and gRPC export.** Choose the protocol your collector speaks.
- **gzip, custom headers, mTLS, SigV4.** Works with AWS-native backends like Amazon Managed Grafana and AWS X-Ray.
- **Per-signal control.** Enable or disable traces, metrics, and logs independently.

## vs ADOT

The [AWS Distro for OpenTelemetry (ADOT) Lambda Layer](https://aws-otel.github.io/docs/getting-started/lambda) runs a full OpenTelemetry Collector as a Lambda extension. It supports receivers, processors, exporters, and pipelines. It's also large and adds cold start latency.

This relay is smaller and starts faster. Use ADOT if you need collector-level processing (sampling, tail-based routing, attribute mutation). This relay is for getting telemetry out of Lambda reliably.

## Quick start

1. Download the layer zip for your architecture from the [latest release](https://github.com/JacobMillward/lambda-otel-relay/releases).
2. Add it to your Lambda function as a layer.
3. Set the `LAMBDA_OTEL_RELAY_ENDPOINT` environment variable to your collector URL (e.g. `https://collector.example.com:4318`).
4. Configure your OTel SDK to export to `http://localhost:4318`.

## How it works

1. Your function's OpenTelemetry SDK exports telemetry to `http://localhost:4318` (the relay's local listener).
2. The relay buffers incoming OTLP payloads in memory.
3. Based on the configured flush strategy, the relay forwards buffered data to your external OTLP collector.
4. On shutdown, the relay drains all remaining buffers during the Lambda shutdown grace period.

> [!IMPORTANT]
> Configure your function's OTel SDK to use `SimpleSpanProcessor` (and the equivalent simple/synchronous exporters for metrics and logs) instead of the default `BatchSpanProcessor`. The batch processor holds spans in an internal buffer and flushes on its own schedule. In Lambda, the execution environment can freeze between invocations, so spans sitting in that buffer may never be exported. `SimpleSpanProcessor` exports each span to the relay immediately. The relay is on localhost so the overhead is negligible, and the relay itself handles all buffering and batched export to the remote collector.

- [Configuration Reference](#configuration-reference)
  - [Flush Strategies](#flush-strategies)
- [Development](#development)
- [Releasing](#releasing)

## Configuration Reference

All configuration is via environment variables on your Lambda function. The relay reads these at startup.

| Variable                                   | Default               | Description                                                                                                                          |
| ------------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `LAMBDA_OTEL_RELAY_ENDPOINT`               | _(required)_          | Base URL of the external OTLP collector (e.g. `https://collector.example.com:4318`). Must be a valid HTTP/HTTPS URL.                 |
| `LAMBDA_OTEL_RELAY_PROTOCOL`               | `http/protobuf`       | Export protocol. `http/protobuf` or `grpc`.                                                                                          |
| `LAMBDA_OTEL_RELAY_LISTENER_PORT`          | `4318`                | Port for the local OTLP listener on `localhost`. Your function's SDK exports to this port.                                           |
| `LAMBDA_OTEL_RELAY_TELEMETRY_PORT`         | `4319`                | Port for the Lambda Telemetry API listener. Used internally to receive lifecycle events.                                             |
| `LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS`      | `5000`                | Timeout in milliseconds for each outbound export request.                                                                            |
| `LAMBDA_OTEL_RELAY_COMPRESSION`            | `gzip`                | Compression for outbound requests. `gzip` or `none`.                                                                                 |
| `LAMBDA_OTEL_RELAY_EXPORT_HEADERS`         | _(none)_              | Custom headers for outbound requests. Comma-separated `key=value` pairs (e.g. `Authorization=Bearer token,X-Org-Id=12345`).          |
| `LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES`       | `4194304` (4 MiB)     | Maximum buffer size in bytes before triggering a background flush. `0` to disable.                                                   |
| `LAMBDA_OTEL_RELAY_FLUSH_STRATEGY`         | `default`             | When to forward buffered telemetry. See [Flush Strategies](#flush-strategies).                                                       |
| `LAMBDA_OTEL_RELAY_CERTIFICATE`            | _(none)_              | Path to a custom CA certificate (PEM) for verifying the collector's TLS certificate.                                                 |
| `LAMBDA_OTEL_RELAY_CLIENT_CERT`            | _(none)_              | Path to a client certificate (PEM) for mTLS. Must be set together with `CLIENT_KEY`.                                                 |
| `LAMBDA_OTEL_RELAY_CLIENT_KEY`             | _(none)_              | Path to a client private key (PEM) for mTLS. Must be set together with `CLIENT_CERT`.                                                |
| `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE` | _(none)_              | AWS service code to sign requests for (e.g. `aps`, `xray`). Enables SigV4 signing. Requires AWS credentials from the Lambda runtime. |
| `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION`  | _(none)_              | AWS region for SigV4 signing. Falls back to `AWS_REGION`, then `AWS_DEFAULT_REGION`.                                                 |
| `LAMBDA_OTEL_RELAY_SIGNALS`                | `traces,metrics,logs` | Comma-separated list of signal types to accept and forward. Disabled signals return 404. At least one required.                      |
| `LAMBDA_OTEL_RELAY_LOG_LEVEL`              | `WARN`                | Log level for the extension. `DEBUG`, `INFO`, `WARN`, or `ERROR`.                                                                    |

### Flush Strategies

The flush strategy controls when buffered telemetry is forwarded to the collector. All strategies also flush on shutdown. All strategies include a 100ms dedup window to prevent redundant flushes when a timer and a boundary fire close together.

| Strategy            | Boundary flush   | Background timer | Blocking | Description                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------- | ---------------- | ---------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `default`           | After 60s gap    | Every 60s        | No       | Recommended for most workloads. Runs a non-blocking background flush on a 60-second timer. Also flushes at invocation boundaries when 60+ seconds have passed since the last flush. Under sustained load, the boundary check rarely triggers because the background timer keeps the buffer drained. During idle periods with sporadic invocations, the boundary check ensures telemetry is still exported promptly. |
| `end`               | Every invocation | None             | Yes      | Blocks after every invocation and flushes synchronously before the next one starts. Adds latency equal to the export round-trip. Suitable for low-throughput functions where delivery latency matters more than function duration.                                                                                                                                                                                  |
| `end,<ms>`          | Every invocation | Every `<ms>`     | Yes      | Combines per-invocation flushing with a synchronous periodic timer. The timer exports telemetry produced mid-execution by long-running handlers without waiting for the handler to return. Both the boundary flush and the timer flush block the event loop.                                                                                                                                                        |
| `periodically,<ms>` | After `<ms>` gap | None             | Yes      | Flushes at invocation boundaries, but only when `<ms>` milliseconds have elapsed since the last flush. Caps export frequency for high-throughput functions. `periodically,60000` exports at most once per minute regardless of invocation rate.                                                                                                                                                                     |
| `continuously,<ms>` | None             | Every `<ms>`     | No       | Runs a non-blocking background flush every `<ms>` milliseconds. Does not flush at invocation boundaries. Designed for long-running invocations (e.g. streaming handlers) where invocation boundaries are infrequent and you want periodic export throughout execution.                                                                                                                                              |

## Development

### Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)
- [cargo-lambda](https://www.cargo-lambda.info/)
- [just](https://github.com/casey/just)
- Docker (for integration tests)

### Build

```sh
just build-extension
```

### Test

```sh
just test              # unit tests
just integration-test  # builds extension + runs integration tests (requires Docker)
just test-all          # both
```

### Releasing

```sh
just release 0.5.0       # stable release
just release 0.5.0-rc.1  # pre-release
```

This validates the version, bumps `crates/extension/Cargo.toml`, commits, tags `v<version>`, and prompts to push. Pushing the tag triggers a GitHub Actions workflow that cross-compiles Lambda layer zips for arm64 and x86_64 and publishes them as a GitHub Release. Pre-release versions (anything with a hyphen, e.g. `-rc.1`, `-dev.1`) are marked as pre-releases on GitHub.

### Vendored Protos

OTLP `.proto` files are vendored from [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto). The pinned version lives in `proto/.version`.

```sh
just vendor                # re-download protos at the pinned version
just vendor-upgrade        # upgrade to latest release
just vendor-upgrade v1.5.0 # upgrade to a specific release
```
