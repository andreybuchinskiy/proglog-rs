mod internal;

use internal::server::http::new_http_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    new_http_server("127.0.0.1:8080").await?;
    Ok(())
}
