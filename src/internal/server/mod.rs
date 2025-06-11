pub mod http;
pub mod log;

use crate::api::v1::log_server::{Log, LogServer};
use crate::api::v1::{ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse, Record};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Streaming;
use tonic::{transport::server::Router, transport::Server, Request, Response, Status};

#[derive(Clone)]
struct LogServerService {
    pub config: ServerConfig,
}

impl LogServerService {
    pub fn new(config: ServerConfig) -> LogServerService {
        LogServerService { config }
    }
}

#[derive(Clone)]
struct ServerConfig {
    pub commit_log: Arc<Mutex<dyn CommitLog>>,
}

impl ServerConfig {
    pub fn new(commit_log: impl CommitLog + 'static) -> ServerConfig {
        ServerConfig {
            commit_log: Arc::new(Mutex::new(commit_log)),
        }
    }
}

#[async_trait]
pub trait CommitLog: Send + Sync + 'static {
    async fn append(&mut self, record: Record) -> anyhow::Result<u64>;
    async fn read(&mut self, offset: u64) -> anyhow::Result<Record>;
}

pub async fn new_grpc_server(commit_log: impl CommitLog + 'static) -> anyhow::Result<Router> {
    let config = ServerConfig::new(commit_log);
    let srv = LogServerService::new(config);
    let service = LogServer::new(srv);
    let server = Server::builder().add_service(service);
    Ok(server)
}

#[tonic::async_trait]
impl Log for LogServerService {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        if let Some(record) = request.into_inner().record {
            let offset = self
                .config
                .commit_log
                .lock()
                .await
                .append(record)
                .await
                .unwrap();
            Ok(Response::new(ProduceResponse { offset }))
        } else {
            Err(Status::new(
                tonic::Code::InvalidArgument,
                "Invalid record provided",
            ))
        }
    }

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        let record = self
            .config
            .commit_log
            .lock()
            .await
            .read(request.into_inner().offset)
            .await;
        match record {
            Ok(r) => Ok(Response::new(ConsumeResponse { record: Some(r) })),
            Err(_) => Err(Status::new(tonic::Code::OutOfRange, "record not found")),
        }
    }

    type ConsumeStreamStream = ReceiverStream<Result<ConsumeResponse, Status>>;

    async fn consume_stream(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStreamStream>, Status> {
        let mut req = request.into_inner();
        let consumer = self.clone();
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            loop {
                match consumer.consume(Request::new(req)).await {
                    Ok(res) => {
                        if (tx.send(Ok(res.into_inner())).await).is_err() {
                            return;
                        }
                        req.offset += 1;
                    }
                    Err(e) if e.code() == tonic::Code::OutOfRange => {
                        continue;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::new(tonic::Code::Unknown, e.to_string())))
                            .await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ProduceStreamStream = ReceiverStream<Result<ProduceResponse, Status>>;

    async fn produce_stream(
        &self,
        request: Request<Streaming<ProduceRequest>>,
    ) -> Result<Response<Self::ProduceStreamStream>, Status> {
        let mut req_stream = request.into_inner();
        let producer = self.clone();
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            while let Some(req) = req_stream.next().await {
                match req {
                    Ok(req) => {
                        let res = producer.produce(Request::new(req)).await;
                        match res {
                            Ok(r) => {
                                let response = r.into_inner();
                                tx.send(Ok(response)).await.unwrap();
                            }
                            Err(e) => {
                                tx.send(Err(Status::new(tonic::Code::Unknown, e.to_string())))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    Err(e) => {
                        tx.send(Err(Status::new(tonic::Code::Unknown, e.to_string())))
                            .await
                            .unwrap();
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {}
