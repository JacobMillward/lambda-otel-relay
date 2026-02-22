#!/bin/sh
# Wrapper that the RIE discovers at /opt/extensions/lambda-otel-relay.
# Redirects the extension to talk to the telemetry proxy (:9002) instead
# of the RIE directly (:9001), then execs the real extension binary.
export AWS_LAMBDA_RUNTIME_API=127.0.0.1:9002
exec /opt/lambda-otel-relay-bin "$@"
