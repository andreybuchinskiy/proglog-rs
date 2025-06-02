use crate::internal::server::log::{Log, Record};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

struct HttpServer {
    log: Log,
}

impl HttpServer {
    fn new() -> Self {
        HttpServer {
            log: Log::default(),
        }
    }

    async fn handle_produce(
        State(srv): State<Arc<HttpServer>>,
        Json(req): Json<ProduceRequest>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let offset = srv
            .log
            .append(req.record)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let res = ProduceResponse { offset };

        Ok(Json(res))
    }

    async fn handle_consume(
        State(srv): State<Arc<HttpServer>>,
        Json(req): Json<ConsumeRequest>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let record = srv
            .log
            .read(req.offset)
            .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
        let res = ConsumeResponse { record };
        Ok(Json(res))
    }
}

#[derive(Serialize, Deserialize)]
struct ProduceRequest {
    record: Record,
}

#[derive(Serialize, Deserialize)]
struct ProduceResponse {
    offset: u64,
}

#[derive(Serialize, Deserialize)]
struct ConsumeRequest {
    offset: u64,
}

#[derive(Serialize, Deserialize)]
struct ConsumeResponse {
    record: Record,
}

pub async fn new_http_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_srv = Arc::new(HttpServer::new());

    let app = Router::new()
        .route("/", post(HttpServer::handle_produce))
        .route("/", get(HttpServer::handle_consume))
        .with_state(http_srv);

    let addr: SocketAddr = addr.parse()?;

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}



