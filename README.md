# lambda-otel-relay

[![CI](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobMillward/lambda-otel-relay/actions/workflows/ci.yml)

- [How it works](#how-it-works)
- [Configuration Reference](#configuration-reference)
  - [Flush Strategies](#flush-strategies)
- [Prerequisites](#prerequisites)
- [Build](#build)
- [Test](#test)
- [Vendored Protos](#vendored-protos)

An AWS Lambda extension that acts as a lifecycle-aware OTLP proxy. It runs as an external extension alongside your Lambda function, accepting OpenTelemetry telemetry (traces, metrics, and logs) over a localhost HTTP endpoint, buffering it in memory, and forwarding it to an external OTLP collector.

Because Lambda can freeze or shut down the execution environment at any time, telemetry exported directly from in-process SDKs is often lost. This extension hooks into the [Lambda Extensions API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html) and the [Lambda Telemetry API](https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api.html) to track invocation boundaries and uses the shutdown grace period to flush any remaining data before the environment is destroyed.

### How it works

1. Your function's OpenTelemetry SDK exports telemetry to `http://localhost:4318` (the relay's local listener).
2. The relay buffers incoming OTLP payloads in memory.
3. Based on the configured flush strategy, the relay forwards buffered data to your external OTLP collector.
4. On shutdown, the relay drains all remaining buffers during the Lambda shutdown grace period.

> [!IMPORTANT]
> Configure your function's OTel SDK to use `SimpleSpanProcessor` (and the equivalent simple/synchronous exporters for metrics and logs) instead of the default `BatchSpanProcessor`. The batch processor holds spans in an internal buffer and flushes on its own schedule. In Lambda, the execution environment can freeze between invocations, so spans sitting in that buffer may never be exported. `SimpleSpanProcessor` exports each span to the relay immediately. The relay is on localhost so the overhead is negligible, and the relay itself handles all buffering and batched export to the remote collector.

The relay supports gzip compression, custom headers, mTLS, and AWS SigV4 request signing for integration with AWS-native backends like Amazon Managed Grafana or AWS X-Ray.

## Configuration Reference

All configuration is via environment variables on your Lambda function. The relay reads these at startup.

| Variable | Default | Description |
|---|---|---|
| `LAMBDA_OTEL_RELAY_ENDPOINT` | *(required)* | Base URL of the external OTLP collector (e.g. `https://collector.example.com:4318`). Must be a valid HTTP/HTTPS URL. |
| `LAMBDA_OTEL_RELAY_LISTENER_PORT` | `4318` | Port for the local OTLP listener on `localhost`. Your function's SDK exports to this port. |
| `LAMBDA_OTEL_RELAY_TELEMETRY_PORT` | `4319` | Port for the Lambda Telemetry API listener. Used internally to receive lifecycle events. |
| `LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS` | `5000` | Timeout in milliseconds for each outbound export request. |
| `LAMBDA_OTEL_RELAY_COMPRESSION` | `gzip` | Compression for outbound requests. `gzip` or `none`. |
| `LAMBDA_OTEL_RELAY_EXPORT_HEADERS` | *(none)* | Custom headers for outbound requests. Comma-separated `key=value` pairs (e.g. `Authorization=Bearer token,X-Org-Id=12345`). |
| `LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES` | `4194304` (4 MiB) | Maximum buffer size in bytes before triggering a background flush. `0` to disable. |
| `LAMBDA_OTEL_RELAY_FLUSH_STRATEGY` | `default` | When to forward buffered telemetry. See [Flush Strategies](#flush-strategies). |
| `LAMBDA_OTEL_RELAY_CERTIFICATE` | *(none)* | Path to a custom CA certificate (PEM) for verifying the collector's TLS certificate. |
| `LAMBDA_OTEL_RELAY_CLIENT_CERT` | *(none)* | Path to a client certificate (PEM) for mTLS. Must be set together with `CLIENT_KEY`. |
| `LAMBDA_OTEL_RELAY_CLIENT_KEY` | *(none)* | Path to a client private key (PEM) for mTLS. Must be set together with `CLIENT_CERT`. |
| `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE` | *(none)* | AWS service code to sign requests for (e.g. `aps`, `xray`). Enables SigV4 signing. Requires AWS credentials from the Lambda runtime. |
| `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION` | *(none)* | AWS region for SigV4 signing. Falls back to `AWS_REGION`, then `AWS_DEFAULT_REGION`. |
| `LAMBDA_OTEL_RELAY_LOG_LEVEL` | `WARN` | Log level for the extension. `DEBUG`, `INFO`, `WARN`, or `ERROR`. |

### Flush Strategies

The flush strategy controls when buffered telemetry is forwarded to the collector. All strategies also flush on shutdown.

All strategies include a 100ms dedup window to prevent redundant flushes when a timer and a boundary fire close together.

#### `default`

Runs a non-blocking background flush on a 60-second timer. Also flushes at invocation boundaries when 60+ seconds have passed since the last flush. Because background flushes don't block the event loop, the function's response latency is unaffected.

Recommended for most workloads. Under sustained load, the boundary check rarely triggers because the background timer keeps the buffer drained. During idle periods with sporadic invocations, the boundary check ensures telemetry is still exported promptly.

#### `end`

Blocks after every invocation and flushes synchronously before the next invocation starts. No background timer. Every invocation's telemetry is fully exported before the runtime reports completion.

This adds latency to each invocation equal to the export round-trip. Suitable for low-throughput functions where delivery latency matters more than function duration.

#### `end,<ms>`

Combines per-invocation flushing with a synchronous periodic timer that fires every `<ms>` milliseconds. The timer runs between invocation boundaries, so telemetry produced mid-execution by long-running handlers is exported without waiting for the handler to return.

Both the boundary flush and the timer flush block the event loop.

#### `periodically,<ms>`

Flushes at invocation boundaries, but only when `<ms>` milliseconds have elapsed since the last flush. No background timer. The interval is checked each time an invocation ends; if the interval hasn't elapsed, the buffer is left as-is.

Caps export frequency for high-throughput functions. `periodically,60000` exports at most once per minute regardless of invocation rate.

#### `continuously,<ms>`

Runs a non-blocking background flush every `<ms>` milliseconds. Does not flush at invocation boundaries. Telemetry is exported on the timer schedule only.

Designed for long-running invocations (e.g. streaming handlers) where invocation boundaries are infrequent and you want periodic export throughout execution.

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

## Vendored Protos

OTLP `.proto` files are vendored from [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto). The pinned version lives in `proto/.version`.

```sh
just vendor                # re-download protos at the pinned version
just vendor-upgrade        # upgrade to latest release
just vendor-upgrade v1.5.0 # upgrade to a specific release
```
