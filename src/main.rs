#![allow(dead_code)]
mod internal;

use anyhow::Result;

pub mod api {
    pub mod v1 {
        tonic::include_proto!("log.v1");
    }
}

use internal::server::http::new_http_server;

#[tokio::main]
async fn main() -> Result<()> {
    new_http_server("127.0.0.1:8080").await?;
    Ok(())
}
