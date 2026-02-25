use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");

    let protos = [
        "opentelemetry/proto/collector/trace/v1/trace_service.proto",
        "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        "opentelemetry/proto/collector/logs/v1/logs_service.proto",
    ];

    let proto_paths: Vec<PathBuf> = protos.iter().map(|p| proto_dir.join(p)).collect();

    let fds = protox::compile(proto_paths, [&proto_dir])?;

    prost_build::Config::new().compile_fds(fds)?;

    println!("cargo:rerun-if-changed={}", proto_dir.display());
    Ok(())
}
