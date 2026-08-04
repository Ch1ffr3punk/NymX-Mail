use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use filetime::{set_file_times, FileTime};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SshConfig {
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    #[serde(default)]
    pub username: String,
}

fn default_ssh_port() -> u16 {
    22
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct NymXConfig {
    #[serde(default)]
    pub aliases: HashMap<String, String>,
    #[serde(default)]
    pub ssh: SshConfig,
}

pub fn get_base_dir() -> PathBuf {
    let exe_dir = env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| env::current_dir().unwrap_or_default());

    if cfg!(target_os = "windows") {
        return exe_dir;
    }

    let config_in_exe = exe_dir.join("nymx-mail.json");
    if config_in_exe.exists() {
        return exe_dir;
    }

    if let Some(home) = env::var_os("HOME") {
        let home_path = PathBuf::from(home);
        let config_in_home = home_path.join("nymx-mail.json");
        if config_in_home.exists() {
            return home_path;
        }
    }

    if let Some(home) = env::var_os("HOME") {
        return PathBuf::from(home);
    }

    exe_dir
}

impl NymXConfig {
    pub fn load() -> Self {
        let base_dir = get_base_dir();
        let config_path = base_dir.join("nymx-mail.json");
        if config_path.exists() {
            let content = std::fs::read_to_string(config_path).unwrap_or_default();
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            NymXConfig::default()
        }
    }

    pub fn resolve(&self, input: &str) -> Option<String> {
        self.aliases.get(input).cloned()
    }

    pub fn save(&self) -> Result<(), std::io::Error> {
        let base_dir = get_base_dir();
        let config_path = base_dir.join("nymx-mail.json");
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(&config_path, content)?;
        
        let epoch_time = FileTime::from_unix_time(0, 0);
        let _ = set_file_times(&config_path, epoch_time, epoch_time);
        
        Ok(())
    }
}
