use anyhow::Result;
use std::fs::read;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

pub struct TlsConfig {
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: String,
    pub server: bool,
}

impl TlsConfig {
    pub fn setup(self) -> Result<ConfigType> {
        let cert = read(self.cert_file)?;
        let key = read(self.key_file)?;
        let ca = read(self.ca_file)?;
        let identity = Identity::from_pem(&cert, &key);
        let ca_cert = Certificate::from_pem(ca);
        match self.server {
            true => {
                let config = ServerTlsConfig::new()
                    .identity(identity)
                    .client_ca_root(ca_cert)
                    .client_auth_optional(false);
                Ok(ConfigType::Server(config))
            }
            false => {
                let config = ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(ca_cert);
                Ok(ConfigType::Client(config))
            }
        }
    }
}

pub enum ConfigType {
    Server(ServerTlsConfig),
    Client(ClientTlsConfig),
}

impl ConfigType {
    pub fn server_config(self) -> Option<ServerTlsConfig> {
        match self {
            ConfigType::Server(x) => Some(x),
            _ => None,
        }
    }

    pub fn client_config(self) -> Option<ClientTlsConfig> {
        match self {
            ConfigType::Client(x) => Some(x),
            _ => None,
        }
    }
}
