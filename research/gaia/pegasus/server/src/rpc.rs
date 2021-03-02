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

use crate::generated::protobuf as pb;
use crate::service::{Output, Service};
use crate::AnyData;
use prost::Message;
use std::io::Write;
use std::net::SocketAddr;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RpcOutput {
    tx: UnboundedSender<Result<pb::JobResponse, Status>>,
}

impl RpcOutput {
    pub fn new(tx: UnboundedSender<Result<pb::JobResponse, Status>>) -> Self {
        RpcOutput { tx }
    }
}

impl Output for RpcOutput {
    fn send(&self, res: pb::JobResponse) {
        let id = res.job_id;
        if let Err(_) = self.tx.send(Ok(res)) {
            error!("Job[{}]: send result into rpc output failure;", id);
        }
    }

    fn close(&self) {}
}

#[derive(Clone)]
pub struct RpcService<D: AnyData> {
    inner: Service<D>,
}

#[derive(Clone)]
pub struct DebugRpcService<D: AnyData> {
    inner: Service<D>,
}

#[tonic::async_trait]
impl<D: AnyData> pb::job_service_server::JobService for RpcService<D> {
    type SubmitStream = UnboundedReceiver<Result<pb::JobResponse, Status>>;

    async fn submit(
        &self, req: Request<pb::JobRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let job_req = req.into_inner();
        let (tx, rx) = mpsc::unbounded_channel();
        let output = RpcOutput::new(tx);
        self.inner.accept(job_req, output);
        Ok(Response::new(rx))
    }
}

#[tonic::async_trait]
impl<D: AnyData> pb::job_service_server::JobService for DebugRpcService<D> {
    type SubmitStream = UnboundedReceiver<Result<pb::JobResponse, Status>>;

    async fn submit(
        &self, req: Request<pb::JobRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let job_req = req.into_inner();
        if let Some(ref conf) = job_req.conf {
            let name = if conf.job_name.is_empty() {
                "unknown_query".to_owned()
            } else {
                conf.job_name.clone()
            };

            match std::fs::File::create(&name) {
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
        let output = RpcOutput::new(tx);
        self.inner.accept(job_req, output);
        Ok(Response::new(rx))
    }
}

pub struct RpcServer<S: pb::job_service_server::JobService> {
    service: S,
    addr: SocketAddr,
}

pub async fn start_rpc_server<D: AnyData>(
    addr: SocketAddr, service: Service<D>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_service = RpcService { inner: service };
    let server = RpcServer::new(addr, rpc_service);
    server.run().await?;
    Ok(())
}

pub async fn start_debug_rpc_server<D: AnyData>(
    addr: SocketAddr, service: Service<D>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_service = DebugRpcService { inner: service };
    let server = RpcServer::new(addr, rpc_service);
    server.run().await?;
    Ok(())
}

impl<S: pb::job_service_server::JobService> RpcServer<S> {
    pub fn new(addr: SocketAddr, service: S) -> Self {
        RpcServer { service, addr }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let RpcServer { service, addr } = self;
        info!("Rpc server started on {}", addr);
        Server::builder()
            .add_service(pb::job_service_server::JobServiceServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
