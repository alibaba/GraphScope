//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::generated::protocol as pb;
use crate::service::{Output, Service};
use crate::AnyData;
use prost::Message;
use std::io::Write;
use std::net::SocketAddr;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::Instant;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct RpcOutput {
    tx: UnboundedSender<Result<pb::JobResponse, Status>>,
    timer: Option<Instant>,
    job_id: u64,
}

impl RpcOutput {
    pub fn new(tx: UnboundedSender<Result<pb::JobResponse, Status>>, job_id: u64) -> Self {
        RpcOutput { tx, timer: None, job_id }
    }

    pub fn with_timer(tx: UnboundedSender<Result<pb::JobResponse, Status>>, job_id: u64) -> Self {
        RpcOutput { tx, timer: Some(Instant::now()), job_id }
    }
}

impl Output for RpcOutput {
    fn send(&self, res: pb::JobResponse) {
        if let Err(_) = self.tx.send(Ok(res)) {
            error!("Job[{}]: send result into rpc output failure;", self.job_id);
        }
    }

    fn close(&self) {
        if let Some(start) = self.timer {
            let now = Instant::now();
            let latency = now.duration_since(start).as_micros();
            info!("[Job-{}] Execution Time: {:?} ms", self.job_id, latency as f64 / 1000.0);
        }
    }
}

#[derive(Clone)]
pub struct RpcService<D: AnyData> {
    inner: Service<D>,
    report: bool,
}

#[derive(Clone)]
pub struct DebugRpcService<D: AnyData> {
    inner: Service<D>,
    report: bool,
}

#[tonic::async_trait]
impl<D: AnyData> pb::job_service_server::JobService for RpcService<D> {
    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(
        &self, req: Request<pb::JobRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let job_req = req.into_inner();
        let job_id = if let Some(job_conf) = job_req.conf.as_ref() {
            job_conf.job_id
        } else {
            return Err(Status::invalid_argument("job conf not specified;"));
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let output = if self.report {
            // let _g = report_memory(job_id);
            RpcOutput::with_timer(tx, job_id)
        } else {
            RpcOutput::new(tx, job_id)
        };
        self.inner.accept(job_req, output);
        let rx = UnboundedReceiverStream::new(rx);
        Ok(Response::new(rx))
    }
}

#[tonic::async_trait]
impl<D: AnyData> pb::job_service_server::JobService for DebugRpcService<D> {
    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(
        &self, req: Request<pb::JobRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let job_req = req.into_inner();
        let job_id = if let Some(job_conf) = job_req.conf.as_ref() {
            job_conf.job_id
        } else {
            return Err(Status::invalid_argument("job conf not specified;"));
        };
        let mut temp_dir = std::env::temp_dir().join("gaia_queries");
        if !temp_dir.exists() {
            if std::fs::create_dir_all(&temp_dir).is_err() {
                temp_dir = std::path::PathBuf::from("");
            }
        }
        if let Some(ref conf) = job_req.conf {
            let name = if conf.job_name.is_empty() {
                "unknown_query".to_owned()
            } else {
                conf.job_name.clone()
            };

            let query_file = temp_dir.join(name);
            match std::fs::File::create(&query_file) {
                Ok(f) => {
                    let size = job_req.encoded_len();
                    let mut buf = Vec::with_capacity(size);
                    if let Err(e) = job_req.encode(&mut buf) {
                        error!("encode job request failure: {}", e);
                    } else {
                        let mut writer = std::io::BufWriter::new(f);
                        if let Err(e) = writer.write_all(&buf[..]) {
                            error!("write binary job request failure: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("debug job request to file failure: {}", e)
                }
            }
        } else {
            return Err(Status::invalid_argument("job conf not specified;"));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let output = if self.report {
            // let _g = report_memory(job_id);
            RpcOutput::with_timer(tx, job_id)
        } else {
            RpcOutput::new(tx, job_id)
        };
        self.inner.accept(job_req, output);
        let rx = UnboundedReceiverStream::new(rx);
        Ok(Response::new(rx))
    }
}

pub struct RpcServer<S: pb::job_service_server::JobService> {
    service: S,
    addr: SocketAddr,
}

pub async fn start_rpc_server<D: AnyData>(
    addr: SocketAddr, service: Service<D>, report: bool, blocking: bool,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let rpc_service = RpcService { inner: service, report };
    let server = RpcServer::new(addr, rpc_service);
    let local_addr = server.run(blocking).await?;
    Ok(local_addr)
}

pub async fn start_debug_rpc_server<D: AnyData>(
    addr: SocketAddr, service: Service<D>, report: bool,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let rpc_service = DebugRpcService { inner: service, report };
    let server = RpcServer::new(addr, rpc_service);
    let local_addr = server.run(true).await?;
    Ok(local_addr)
}

impl<S: pb::job_service_server::JobService> RpcServer<S> {
    pub fn new(addr: SocketAddr, service: S) -> Self {
        RpcServer { service, addr }
    }

    pub async fn run(self, blocking: bool) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        let RpcServer { service, addr } = self;
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        info!("Rpc server started on {}", local_addr);
        let serve = Server::builder()
            .add_service(pb::job_service_server::JobServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
        if blocking {
            serve.await?;
        } else {
            tokio::spawn(async move {
                serve.await.expect("server await nonblocking failed");
            });
        }
        Ok(local_addr)
    }
}
