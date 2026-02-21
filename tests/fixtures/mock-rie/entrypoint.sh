#!/bin/sh
# Start the telemetry proxy in the background, then hand off to the stock
# Lambda entrypoint which starts the RIE on its default port (:9001).
# The proxy must be listening before the RIE spawns extensions.
/usr/local/bin/telemetry-proxy &

# Wait for the proxy to signal readiness (it creates this file after binding).
while [ ! -f /tmp/telemetry-proxy-ready ]; do sleep 0.05; done

exec /lambda-entrypoint.sh "$@"
