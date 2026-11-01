//! Explicit operator only. A dry run cannot create an authorization or release.
use std::{
    env,
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use gpt_image_2_gateway::{
    capacity_risk_waivers::{
        parse_capacity_risk_waiver_manifest, review_or_apply_capacity_risk_waiver,
    },
    database::{
        connect_pool_with_schema, database_schema_from_env, database_url_from_env,
        verify_migrations,
    },
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<_> = env::args().skip(1).collect();
    let (apply, authorization_id, manifest_path, reviewed_sha256) = match args.as_slice() {
        [path, hash] => (false, None, path, hash),
        [mode, path, hash] if mode == "dry-run" => (false, None, path, hash),
        [mode, flag, id, path, hash] if mode == "--apply" && flag == "--authorization-id" => {
            let id = uuid::Uuid::parse_str(id).map_err(|_| "invalid authorization ID")?;
            (true, Some(id), path, hash)
        }
        _ => return Err("usage: capacity-risk-waiver [dry-run | --apply --authorization-id UUID] <private-manifest.json> <reviewed-sha256> (default: dry-run)".into()),
    };
    if unsafe { libc::geteuid() } != 0 {
        return Err("capacity risk waiver requires the root operator".into());
    }
    let path = Path::new(manifest_path);
    if !path.is_absolute() {
        return Err("manifest must have an absolute path".into());
    }
    // Reject symlinks and mutable/untrusted parent directories as well as the file.
    for parent in path.ancestors().skip(1) {
        let metadata = std::fs::symlink_metadata(parent)
            .map_err(|_| "manifest parent cannot be inspected".to_string())?;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            return Err("manifest parents must be root-owned and not group/world writable".into());
        }
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "manifest cannot be opened safely".to_string())?;
    let metadata = file
        .metadata()
        .map_err(|_| "manifest metadata unavailable".to_string())?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.mode() & 0o077 != 0
        || metadata.len() > 32_768
    {
        return Err("manifest must be a private root-owned regular file".into());
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(32_769)
        .read_to_end(&mut bytes)
        .map_err(|_| "manifest cannot be read".to_string())?;
    let manifest = parse_capacity_risk_waiver_manifest(&bytes, reviewed_sha256)
        .map_err(|_| "manifest does not match the reviewed scope".to_string())?;
    if apply && authorization_id != Some(manifest.authorization_id) {
        return Err("explicit authorization ID does not match the reviewed manifest".into());
    }
    // No migrations, provider calls, background workers, or fallback actions.
    let database_url =
        database_url_from_env().map_err(|_| "database configuration unavailable".to_string())?;
    let schema =
        database_schema_from_env().map_err(|_| "database schema unavailable".to_string())?;
    let pool = connect_pool_with_schema(&database_url, 1, &schema)
        .await
        .map_err(|_| "database connection unavailable".to_string())?;
    verify_migrations(&pool)
        .await
        .map_err(|_| "database migrations do not match this operator".to_string())?;
    let receipt = review_or_apply_capacity_risk_waiver(&pool, &bytes, reviewed_sha256, apply)
        .await
        .map_err(|error| format!("{error:?}"))?;
    println!(
        "{}",
        serde_json::to_string(&receipt).map_err(|_| "receipt encoding failed".to_string())?
    );
    pool.close().await;
    Ok(())
}
