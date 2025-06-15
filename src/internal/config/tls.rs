use anyhow::Result;
use std::fs::read;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

pub fn setup_server_tls_config(cert_file: String, key_file: String) -> Result<ServerTlsConfig> {
    let cert = read(cert_file)?;
    let key = read(key_file)?;
    let config = ServerTlsConfig::new().identity(Identity::from_pem(&cert, &key));
    Ok(config)
}

pub fn setup_client_tls_config(ca_file: String) -> Result<ClientTlsConfig> {
    let cert = read(ca_file)?;
    let config = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert));
    Ok(config)
}
