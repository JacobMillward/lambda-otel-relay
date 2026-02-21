# Mock RIE image for integration tests.
#
# Layers the telemetry-proxy and a wrapper script on top of the stock Lambda
# base image. The real extension binary is bind-mounted at runtime to
# /opt/lambda-otel-relay-bin; the wrapper at /opt/extensions/lambda-otel-relay
# redirects it through the proxy.
#
# Build context (staged by justfile into target/mock-rie-context/):
#   telemetry-proxy   — compiled proxy binary
#   ext-wrapper.sh    — extension wrapper script
#   entrypoint.sh     — container entrypoint

FROM public.ecr.aws/lambda/provided:al2023
COPY telemetry-proxy /usr/local/bin/telemetry-proxy
COPY ext-wrapper.sh /opt/extensions/lambda-otel-relay
COPY entrypoint.sh /usr/local/bin/mock-entrypoint.sh
RUN chmod +x /usr/local/bin/telemetry-proxy \
    /opt/extensions/lambda-otel-relay \
    /usr/local/bin/mock-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/mock-entrypoint.sh"]
