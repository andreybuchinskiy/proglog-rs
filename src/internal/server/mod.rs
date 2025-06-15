pub mod http;
pub mod log;

use crate::api::v1::log_server::{Log, LogServer};
use crate::api::v1::{ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse, Record};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::ServerTlsConfig;
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

pub async fn new_grpc_server(
    commit_log: impl CommitLog + 'static,
    tls_config: ServerTlsConfig,
) -> anyhow::Result<Router> {
    let config = ServerConfig::new(commit_log);
    let srv = LogServerService::new(config);
    let service = LogServer::new(srv);
    let server = Server::builder()
        .tls_config(tls_config)?
        .add_service(service);
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
mod tests {
    use super::new_grpc_server;
    use crate::api::v1::log_client::LogClient;
    use crate::api::v1::{ConsumeRequest, ProduceRequest, Record};
    use crate::internal::config::files::{config_file, CA_FILE, SERVER_CERT_FILE, SERVER_KEY_FILE};
    use crate::internal::config::tls::{setup_client_tls_config, setup_server_tls_config};
    use crate::internal::log::config::Config;
    use crate::internal::log::Log;
    use anyhow::Result;
    use assert2::check;
    use assert2::let_assert;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use tempfile::{tempdir, TempDir};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};
    use tonic::transport::Channel;
    use tonic::Request;

    struct TestSetup {
        client: LogClient<Channel>,
        server_handle: JoinHandle<Result<(), tonic::transport::Error>>,
        temp_dir: TempDir,
        addr: SocketAddr,
        shutdown_tx: Option<oneshot::Sender<()>>,
    }

    impl TestSetup {
        async fn new() -> Result<TestSetup> {
            let port = get_random_port()?;
            let addr = SocketAddr::new("127.0.0.1".parse()?, port);

            let temp_dir = tempdir()?;
            let dir = temp_dir.path().to_path_buf();

            let cfg = Config::default();
            let clog = Log::new((dir.to_string_lossy()).to_string(), cfg).await?;

            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let server_tls_config = setup_server_tls_config(
                config_file(SERVER_CERT_FILE),
                config_file(SERVER_KEY_FILE),
            )?;
            let server = new_grpc_server(clog, server_tls_config).await?;
            let server_handle = tokio::spawn(async move {
                server
                    .serve_with_shutdown(addr, async {
                        shutdown_rx.await.unwrap_or(());
                    })
                    .await
            });

            let _ = tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let client_tls_config = setup_client_tls_config(config_file(CA_FILE))?;
            let endpoint = format!("https://localhost:{}", port);
            let channel = Channel::from_shared(endpoint)?
                .tls_config(client_tls_config)?
                .connect()
                .await?;
            let client = LogClient::new(channel);
            Ok(TestSetup {
                client,
                server_handle,
                temp_dir,
                addr,
                shutdown_tx: Some(shutdown_tx),
            })
        }
    }

    impl Drop for TestSetup {
        fn drop(&mut self) {
            if let Some(shutdown_tx) = self.shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
        }
    }

    fn create_record(value: Vec<u8>, offset: u64) -> Record {
        Record { value, offset }
    }

    fn get_random_port() -> std::io::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        Ok(addr.port())
    }

    #[tokio::test]
    async fn test_produce_consume() -> Result<()> {
        let mut test = TestSetup::new().await?;
        let record = create_record("hello world".into(), 0);
        let request = Request::new(ProduceRequest {
            record: Some(record.clone()),
        });
        let produce = test.client.produce(request).await?;
        let consume_req = Request::new(ConsumeRequest {
            offset: produce.into_inner().offset,
        });
        let consume_res = test.client.consume(consume_req).await?;
        let consume = consume_res.into_inner().record.unwrap();
        assert_eq!(consume.value, record.value);
        assert_eq!(consume.offset, record.offset);
        Ok(())
    }

    #[tokio::test]
    async fn test_consume_past_boundry() -> Result<()> {
        let mut test = TestSetup::new().await?;
        let record = create_record("hello world".into(), 0);
        let produce_req = Request::new(ProduceRequest {
            record: Some(record.clone()),
        });
        let produce = test.client.produce(produce_req).await?;
        let consume_req = Request::new(ConsumeRequest {
            offset: produce.into_inner().offset + 1,
        });
        let_assert!(Err(_) = test.client.consume(consume_req).await);

        Ok(())
    }

    #[tokio::test]
    async fn test_produce_consume_stream() -> Result<()> {
        let mut test = TestSetup::new().await?;
        let messages = ["message1", "message2"];
        let records: Vec<Record> = messages
            .iter()
            .enumerate()
            .map(|(i, x)| create_record((*x).into(), i as u64))
            .collect();

        {
            let (tx, rx) = mpsc::channel(1000);
            let stream_req = ReceiverStream::new(rx);
            let mut stream = test.client.produce_stream(stream_req).await?.into_inner();
            for rec in records.clone() {
                let req = ProduceRequest {
                    record: Some(rec.clone()),
                };
                tx.send(req).await?;
                let res = stream.next().await;
                let_assert!(Ok(r) = res.clone().unwrap());
                check!(
                    r.offset == rec.offset,
                    "Wanted offset: {}, got: {}",
                    rec.offset,
                    r.offset
                );
            }
        }
        {
            let (tx, rx) = mpsc::channel(1000);
            let _stream = ReceiverStream::new(rx);
            let req = ConsumeRequest { offset: 0 };
            let mut stream = test.client.consume_stream(req).await?.into_inner();
            for rec in records {
                tx.send(req).await?;
                let res = stream.next().await;
                let_assert!(Ok(r) = res.clone().unwrap());
                let_assert!(Some(record) = r.record);
                check!(
                    record.offset == rec.offset,
                    "Wanted offset: {}, got: {}",
                    rec.offset,
                    record.offset,
                );
                check!(
                    record.value == rec.value,
                    "Wanted value: {}, got: {}",
                    String::from_utf8(rec.value.clone())?,
                    String::from_utf8(record.value.clone())?,
                );
            }
        }
        Ok(())
    }
}
