use std::env;
use std::path::PathBuf;

pub const CA_FILE: &str = "ca.pem";
pub const SERVER_CERT_FILE: &str = "server.pem";
pub const SERVER_KEY_FILE: &str = "server-key.pem";

pub fn config_file(filename: &str) -> String {
    if let Ok(dir) = env::var("CONFIG_DIR") {
        if !dir.is_empty() {
            return (PathBuf::from(dir).join(filename).to_string_lossy()).to_string();
        }
    }

    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    (home_dir.join(".proglog").join(filename).to_string_lossy()).to_string()
}
