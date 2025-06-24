use std::env;
use std::path::PathBuf;

pub const CA_FILE: &str = "ca.pem";
pub const SERVER_CERT_FILE: &str = "server.pem";
pub const SERVER_KEY_FILE: &str = "server-key.pem";
pub const ROOT_CLIENT_CERT_FILE: &str = "root-client.pem";
pub const ROOT_CLIENT_KEY_FILE: &str = "root-client-key.pem";
pub const NOBODY_CLIENT_CERT_FILE: &str = "nobody-client.pem";
pub const NOBODY_CLIENT_KEY_FILE: &str = "nobody-client-key.pem";
pub const ACL_MODEL_FILE: &str = "model.conf";
pub const ACL_POLICY_FILE: &str = "policy.csv";

pub fn config_file(filename: &str) -> String {
    if let Ok(dir) = env::var("CONFIG_DIR") {
        if !dir.is_empty() {
            return (PathBuf::from(dir).join(filename).to_string_lossy()).to_string();
        }
    }

    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    (home_dir.join(".proglog").join(filename).to_string_lossy()).to_string()
}
