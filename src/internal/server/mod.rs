pub mod http;
pub mod log;

use crate::api::v1::log_server::{Log, LogServer};
use crate::api::v1::{ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse, Record};
use crate::internal::auth::authorizer::Authorizer;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::Streaming;
use tonic::transport::CertificateDer;
use tonic::transport::ServerTlsConfig;
use tonic::{Request, Response, Status, transport::Server, transport::server::Router};
use x509_parser::prelude::{FromDer, X509Certificate};

pub const OBJECT_WILDCARD: &str = "*";
pub const PRODUCE_ACTION: &str = "produce";
pub const CONSUME_ACTION: &str = "consume";

#[derive(Clone)]
pub struct LogServerService {
    pub config: ServerConfig,
}

impl LogServerService {
    pub fn new(config: ServerConfig) -> LogServerService {
        LogServerService { config }
    }

    pub fn auth(
        &self,
        peer_certs: Arc<Vec<CertificateDer<'static>>>,
        object: String,
        action: String,
    ) -> Result<(), Box<Status>> {
        let names: Vec<String> = peer_certs
            .as_ref()
            .iter()
            .filter_map(|c| extract_subject_name(c.clone()))
            .collect();
        let result = names.iter().find_map(|n| {
            match self.config.authorizer.authorize(
                (*n.clone()).to_string(),
                object.clone(),
                action.clone(),
            ) {
                Ok(()) => Some(Ok(())),
                Err(_) => None,
            }
        });
        result.unwrap_or_else(|| {
            Err(Box::new(Status::new(
                tonic::Code::Unauthenticated,
                "certificate authentication failed",
            )))
        })
    }
}

#[derive(Clone)]
pub struct ServerConfig {
    pub commit_log: Arc<Mutex<dyn CommitLog>>,
    pub authorizer: Authorizer,
}

impl ServerConfig {
    pub fn new(commit_log: impl CommitLog + 'static, authorizer: Authorizer) -> ServerConfig {
        ServerConfig {
            commit_log: Arc::new(Mutex::new(commit_log)),
            authorizer,
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
    authorizer: Authorizer,
    tls_config: ServerTlsConfig,
) -> anyhow::Result<Router> {
    let config = ServerConfig::new(commit_log, authorizer);
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
        let peer_certs = request.peer_certs().ok_or(Status::new(
            tonic::Code::Unauthenticated,
            "No certificate provided",
        ))?;
        self.auth(
            peer_certs,
            OBJECT_WILDCARD.to_string(),
            PRODUCE_ACTION.to_string(),
        )
        .map_err(|e| *e)?;
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
        let peer_certs = request.peer_certs().ok_or(Status::new(
            tonic::Code::Unauthenticated,
            "No certificate provided",
        ))?;
        self.auth(
            peer_certs,
            OBJECT_WILDCARD.to_string(),
            CONSUME_ACTION.to_string(),
        )
        .map_err(|e| *e)?;
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
        let (metadata, extensions, mut request) = request.into_parts();
        let consumer = self.clone();
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            loop {
                match consumer
                    .consume(Request::from_parts(
                        metadata.clone(),
                        extensions.clone(),
                        request,
                    ))
                    .await
                {
                    Ok(res) => {
                        if (tx.send(Ok(res.into_inner())).await).is_err() {
                            return;
                        }
                        request.offset += 1;
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
        let (metadata, extensions, mut stream) = request.into_parts();
        let producer = self.clone();
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(req) => {
                        let res = producer
                            .produce(Request::from_parts(
                                metadata.clone(),
                                extensions.clone(),
                                req,
                            ))
                            .await;
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

fn extract_subject_name(cert: CertificateDer<'_>) -> Option<String> {
    let (_rem, x509) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| eprintln!("Failed to parse certificate: {}", e))
        .ok()?;

    x509.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::new_grpc_server;
    use crate::api::v1::log_client::LogClient;
    use crate::api::v1::{ConsumeRequest, ProduceRequest, Record};
    use crate::internal::auth::authorizer::Authorizer;
    use crate::internal::config::files::{
        ACL_MODEL_FILE, ACL_POLICY_FILE, CA_FILE, NOBODY_CLIENT_CERT_FILE, NOBODY_CLIENT_KEY_FILE,
        ROOT_CLIENT_CERT_FILE, ROOT_CLIENT_KEY_FILE, SERVER_CERT_FILE, SERVER_KEY_FILE,
        config_file,
    };
    use crate::internal::config::tls::TlsConfig;
    use crate::internal::log::Log;
    use crate::internal::log::config::Config;
    use anyhow::Result;
    use assert2::check;
    use assert2::let_assert;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use tempfile::{TempDir, tempdir};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    use tonic::Request;
    use tonic::transport::Channel;

    struct TestSetup {
        root_client: LogClient<Channel>,
        nobody_client: LogClient<Channel>,
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
            let ca_file = config_file(CA_FILE);

            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let server_tls_config = TlsConfig {
                cert_file: config_file(SERVER_CERT_FILE),
                key_file: config_file(SERVER_KEY_FILE),
                ca_file: ca_file.clone(),
                server: true,
            }
            .setup()?
            .server_config()
            .unwrap();
            let model: &'static str = Box::leak(config_file(ACL_MODEL_FILE).into_boxed_str());
            let policy: &'static str = Box::leak(config_file(ACL_POLICY_FILE).into_boxed_str());
            let authorizer = Authorizer::new(model, policy).await?;
            let server = new_grpc_server(clog, authorizer, server_tls_config).await?;
            let server_handle = tokio::spawn(async move {
                server
                    .serve_with_shutdown(addr, async {
                        shutdown_rx.await.unwrap_or(());
                    })
                    .await
            });

            let _ = tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let endpoint = format!("https://localhost:{}", port);
            let root_client_tls_config = TlsConfig {
                cert_file: config_file(ROOT_CLIENT_CERT_FILE),
                key_file: config_file(ROOT_CLIENT_KEY_FILE),
                ca_file: ca_file.clone(),
                server: false,
            }
            .setup()?
            .client_config()
            .unwrap();
            let root_channel = Channel::from_shared(endpoint.clone())?
                .tls_config(root_client_tls_config)?
                .connect()
                .await?;
            let root_client = LogClient::new(root_channel);
            let nobody_client_tls_config = TlsConfig {
                cert_file: config_file(NOBODY_CLIENT_CERT_FILE),
                key_file: config_file(NOBODY_CLIENT_KEY_FILE),
                ca_file: ca_file.clone(),
                server: false,
            }
            .setup()?
            .client_config()
            .unwrap();
            let nobody_channel = Channel::from_shared(endpoint)?
                .tls_config(nobody_client_tls_config)?
                .connect()
                .await?;
            let nobody_client = LogClient::new(nobody_channel);
            Ok(TestSetup {
                root_client,
                nobody_client,
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
        let produce = test.root_client.produce(request).await?;
        let consume_req = Request::new(ConsumeRequest {
            offset: produce.into_inner().offset,
        });
        let consume_res = test.root_client.consume(consume_req).await?;
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
        let produce = test.root_client.produce(produce_req).await?;
        let consume_req = Request::new(ConsumeRequest {
            offset: produce.into_inner().offset + 1,
        });
        let_assert!(Err(_) = test.root_client.consume(consume_req).await);

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
            let mut stream = test
                .root_client
                .produce_stream(stream_req)
                .await?
                .into_inner();
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
            let mut stream = test.root_client.consume_stream(req).await?.into_inner();
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

    #[tokio::test]
    async fn test_unauthorized() -> Result<()> {
        let mut test = TestSetup::new().await?;
        let record = create_record("hello world".into(), 0);
        let produce_req = Request::new(ProduceRequest {
            record: Some(record.clone()),
        });
        let_assert!(Err(_) = test.nobody_client.produce(produce_req).await);
        let consume_req = Request::new(ConsumeRequest { offset: 0 });
        let_assert!(Err(_) = test.nobody_client.consume(consume_req).await);
        Ok(())
    }
}
