#[cfg(target_os = "linux")]
use std::{fmt::Write, os::unix::ffi::OsStrExt, path::Path};

#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
pub fn dreamina_secret_service_bus_address(home: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/dreamina-secret-service/v1\0");
    digest.update(home.as_os_str().as_bytes());
    let digest = digest.finalize();
    let mut suffix = String::with_capacity(32);
    for byte in &digest[..16] {
        write!(&mut suffix, "{byte:02x}").expect("writing to a String cannot fail");
    }
    format!("unix:abstract=ai-image-factory-dreamina-{suffix}")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn bus_addresses_are_stable_and_bound_to_the_account_home() {
        let first = dreamina_secret_service_bus_address(Path::new("/srv/accounts/first"));
        let repeated = dreamina_secret_service_bus_address(Path::new("/srv/accounts/first"));
        let second = dreamina_secret_service_bus_address(Path::new("/srv/accounts/second"));

        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert!(first.starts_with("unix:abstract=ai-image-factory-dreamina-"));
        assert_eq!(
            first.len(),
            "unix:abstract=ai-image-factory-dreamina-".len() + 32
        );
    }
}
