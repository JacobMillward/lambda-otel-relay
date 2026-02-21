mod support;

use support::lambda::setup;

#[test]
fn extension_binary_is_valid_linux_elf() {
    let ctx = setup();

    let bytes = std::fs::read(&ctx.extension_path).expect("failed to read extension binary");
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
