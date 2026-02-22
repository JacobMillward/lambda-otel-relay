use std::path::PathBuf;

#[test]
fn extension_binary_is_valid_linux_elf() {
    let extension_path = std::fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/lambda/extensions/lambda-otel-relay"),
    )
    .expect("Extension binary not found. Run `cargo lambda build --release --extension` first.");

    let bytes = std::fs::read(&extension_path).expect("failed to read extension binary");
    assert!(
        bytes.len() > 1000,
        "Binary suspiciously small: {} bytes",
        bytes.len()
    );
    assert_eq!(
        &bytes[..4],
        b"\x7fELF",
        "Binary is not a Linux ELF executable. Got magic bytes: {:?}",
        &bytes[..4]
    );
}
