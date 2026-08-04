use crate::config::NymXConfig;
use crate::ssh::connect_ssh;
use crate::TaskMessage;
use filetime::{set_file_times, FileTime};
use ssh2::Session;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

macro_rules! gui_log {
    ($tx:expr, $($arg:tt)*) => {
        { let _ = $tx.send(TaskMessage::Log(format!($($arg)*))); }
    };
}

pub async fn run_get_mode(
    password: &str,
    save_path: PathBuf,
    log_tx: mpsc::UnboundedSender<TaskMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app_config = NymXConfig::load();
    let ssh_config = app_config.ssh;
    
    let host = &ssh_config.host;
    let port = ssh_config.port;
    let username = &ssh_config.username;

    if host.is_empty() || username.is_empty() {
        return Err("Username and host are required in nymx.json".into());
    }

    gui_log!(log_tx, "Connecting to {}@{}:{}...", username, host, port);
    let session = connect_ssh(host, port, username, password)?;
    gui_log!(log_tx, "Connected successfully");

    let remote_dir = format!("/home/{}/received", username);
    let check_cmd = format!("test -d {} && echo 'exists' || echo 'not'", remote_dir);
    let output = run_remote_command(&session, &check_cmd).await?;

    if output.trim() != "exists" {
        gui_log!(log_tx, "Directory {} does not exist on remote server", remote_dir);
        return Ok(());
    }

    gui_log!(log_tx, "Checking for files...");
    let in_use_files = get_in_use_files(&session, &remote_dir).await?;
    let files = list_remote_files(&session, &remote_dir).await?;
    gui_log!(log_tx, "Found {} files", files.len());

    if files.is_empty() {
        gui_log!(log_tx, "No files to download");
        return Ok(());
    }

    let local_dir = save_path;
    if let Err(e) = std::fs::create_dir_all(&local_dir) {
        gui_log!(log_tx, "Failed to create inbox directory: {}", e);
        return Err(format!("Cannot create inbox folder: {}", e).into());
    }

    gui_log!(log_tx, "Saving files to: {}", local_dir.display());

    let mut downloaded = 0;
    let mut failed = 0;
    let mut deleted = 0;
    let mut skipped = 0;

    for filename in &files {
        let is_in_use = in_use_files.iter().any(|path| path.ends_with(filename));
        if is_in_use {
            gui_log!(log_tx, "Skipping {} (in use)", filename);
            skipped += 1;
            continue;
        }

        gui_log!(log_tx, "Downloading {}...", filename);
        match download_file(&session, &remote_dir, &local_dir, filename).await {
            Ok(_) => {
                downloaded += 1;
                gui_log!(log_tx, "Downloaded {}", filename);
                let remote_file = format!("{}/{}", remote_dir, filename);
                match delete_file_remote(&session, &remote_file).await {
                    Ok(_) => {
                        deleted += 1;
                        gui_log!(log_tx, "Deleted remote {}", filename);
                    }
                    Err(e) => {
                        failed += 1;
                        gui_log!(log_tx, "✗ Failed to delete {}: {}", filename, e);
                    }
                }
            }
            Err(e) => {
                failed += 1;
                gui_log!(log_tx, "Failed to download {}: {}", filename, e);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }

    gui_log!(log_tx, "\n--- Summary ---");
    gui_log!(log_tx, "Downloaded: {}", downloaded);
    gui_log!(log_tx, "Deleted: {}", deleted);
    gui_log!(log_tx, "Failed: {}", failed);
    gui_log!(log_tx, "Skipped: {}", skipped);
    gui_log!(log_tx, "Total: {}", files.len());
    Ok(())
}

async fn run_remote_command(
    session: &Session,
    command: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut channel = session.channel_session()?;
    channel.exec(command)?;
    let mut output = String::new();
    channel.read_to_string(&mut output)?;
    channel.wait_close()?;
    Ok(output)
}

async fn get_in_use_files(
    session: &Session,
    remote_path: &str,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let command = format!(
        "lsof +D {} | awk 'NR >1 {{print $9}}' | sort -u",
        remote_path
    );
    let output = run_remote_command(session, &command).await?;
    Ok(output
        .lines()
        .filter(|line| !line.is_empty() && line.starts_with('/'))
        .map(String::from)
        .collect())
}

async fn list_remote_files(
    session: &Session,
    remote_path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let command = format!("find {} -maxdepth 1 -type f -printf '%f\n'", remote_path);
    let output = run_remote_command(session, &command).await?;
    Ok(output
        .lines()
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect())
}

async fn delete_file_remote(
    session: &Session,
    remote_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let command = format!("rm -f {}", remote_path);
    let _ = run_remote_command(session, &command).await?;
    Ok(())
}

async fn download_file(
    session: &Session,
    remote_path: &str,
    local_dir: &PathBuf,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let remote_full = format!("{}/{}", remote_path, filename);
    let local_path = local_dir.join(filename);

    let (mut channel, _) = session.scp_recv(Path::new(&remote_full))?;
    {
        let mut local_file = std::fs::File::create(&local_path)?;
        let mut buffer = vec![0u8; 32768];
        loop {
            let n = channel.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            local_file.write_all(&buffer[..n])?;
        }
        local_file.sync_all()?;
    }
    channel.send_eof()?;
    channel.wait_eof()?;
    channel.close()?;
    channel.wait_close()?;

    let epoch_time = FileTime::from_unix_time(0, 0);
    set_file_times(&local_path, epoch_time, epoch_time)?;
    Ok(())
}
