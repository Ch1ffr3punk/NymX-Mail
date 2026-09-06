use std::env;
use std::path::PathBuf;
use nym_sdk::mixnet::{MixnetClientBuilder, StoragePaths};
use filetime::{set_file_times, FileTime};

pub fn epoch_filetime() -> FileTime {
    FileTime::from_unix_time(0, 0)
}

pub fn set_epoch_times(path: &std::path::Path) {
    let epoch = epoch_filetime();
    let _ = set_file_times(path, epoch, epoch);
}

pub fn get_base_dir() -> PathBuf {
    let exe_dir = env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| env::current_dir().unwrap_or_default());
    if cfg!(target_os = "windows") {
        return exe_dir;
    }
    if let Some(home) = env::var_os("HOME") {
        return PathBuf::from(home);
    }
    exe_dir
}

pub fn get_storage_dir() -> PathBuf {
    get_base_dir().join("nymx-mail")
}

pub fn create_dir_all_epoch(path: &std::path::Path) -> std::io::Result<()> {
    if path.exists() {
        set_epoch_times(path);
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            create_dir_all_epoch(parent)?;
        }
    }
    std::fs::create_dir_all(path)?;
    set_epoch_times(path);
    let mut current = path.to_path_buf();
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() || !parent.exists() {
            break;
        }
        set_epoch_times(parent);
        current = parent.to_path_buf();
    }
    Ok(())
}

pub async fn init_client(gateway: &str) -> Result<(), Box<dyn std::error::Error>> {
    let storage_dir = get_storage_dir();
    create_dir_all_epoch(&storage_dir)?;

    let paths = StoragePaths::new_from_dir(storage_dir.to_str().unwrap())?;

    let client = MixnetClientBuilder::new_with_default_storage(paths)
        .await?
        .request_gateway(gateway.to_string())
        .build()?
        .connect_to_mixnet()
        .await?;

    println!("{}", client.nym_address().to_string());

    client.disconnect().await;

    stamp_dir_recursive_epoch(&storage_dir);

    Ok(())
}

pub fn stamp_dir_recursive_epoch(dir: &std::path::Path) {
    set_epoch_times(dir);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            set_epoch_times(&path);
            if path.is_dir() {
                stamp_dir_recursive_epoch(&path);
            }
        }
    }
}
