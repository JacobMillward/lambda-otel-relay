# telemetry-proxy

A reverse HTTP proxy that stubs the AWS Lambda Telemetry API for integration testing.

## Problem

The Lambda Runtime Interface Emulator (RIE) doesn't implement the Telemetry API (`PUT /2022-08-01/telemetry`). When the lambda-otel-relay extension tries to subscribe during init, the RIE returns an error and the extension crashes.

## Solution

The proxy sits between the extension and the RIE, intercepting Telemetry API calls and returning `200 OK` while forwarding everything else transparently:

```
Extension → Proxy (:9002) → RIE (:9001)
                ↓
          PUT /telemetry → 200 OK (stubbed)
          everything else → forwarded
```

## Usage

This binary is only used inside the `mock-rie` Docker image built by `just build-mock-rie`. It is not deployed to Lambda.

1. The container entrypoint starts the proxy in the background
2. The proxy writes `/tmp/telemetry-proxy-ready` once listening
3. The entrypoint waits for that file, then starts the RIE on `:9001`
4. The extension wrapper sets `AWS_LAMBDA_RUNTIME_API=127.0.0.1:9002` to route traffic through the proxy
