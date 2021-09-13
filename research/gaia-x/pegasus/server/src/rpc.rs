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

use std::error::Error;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use pegasus::api::function::FnResult;
use pegasus::api::FromStream;
use pegasus::result::{FromStreamExt, ResultSink};
use pegasus::{Data, JobConf, ServerConf};
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::generated::protocol as pb;
use crate::service::{JobParser, Service};
use tokio::net::TcpListener;

pub struct RpcSink {
    pub job_id: u64,
    had_error: Arc<AtomicBool>,
    peers: Arc<AtomicUsize>,
    tx: UnboundedSender<Result<pb::JobResponse, Status>>,
}

impl RpcSink {
    pub fn new(job_id: u64, tx: UnboundedSender<Result<pb::JobResponse, Status>>) -> Self {
        RpcSink {
            tx,
            had_error: Arc::new(AtomicBool::new(false)),
            peers: Arc::new(AtomicUsize::new(1)),
            job_id,
        }
    }
}

impl<T: Message> FromStream<T> for RpcSink {
    fn on_next(&mut self, next: T) -> FnResult<()> {
        let bytes = next.encode_to_vec();
        let res = pb::JobResponse { job_id: self.job_id, data: bytes };
        self.tx.send(Ok(res)).ok();
        Ok(())
    }
}

impl Clone for RpcSink {
    fn clone(&self) -> Self {
        self.peers.fetch_add(1, Ordering::SeqCst);
        RpcSink {
            job_id: self.job_id,
            had_error: self.had_error.clone(),
            peers: self.peers.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<T: Message> FromStreamExt<T> for RpcSink {
    fn on_error(&mut self, error: Box<dyn Error + Send>) {
        self.had_error.store(true, Ordering::SeqCst);
        let status = Status::unknown(format!("execution_error: {}", error));
        self.tx.send(Err(status)).ok();
    }
}

impl Drop for RpcSink {
    fn drop(&mut self) {
        let before_sub = self.peers.fetch_sub(1, Ordering::SeqCst);
        if before_sub == 1 {
            if !self.had_error.load(Ordering::SeqCst) {
                self.tx.send(Err(Status::ok("ok"))).ok();
            }
        }
    }
}

#[derive(Clone)]
pub struct RpcService<I: Data, O, P> {
    inner: Service<I, O, P>,
    report: bool,
}

impl<I: Data, O, P> RpcService<I, O, P> {
    pub fn new(service: Service<I, O, P>, report: bool) -> RpcService<I, O, P> {
        RpcService { inner: service, report }
    }
}

#[tonic::async_trait]
impl<I, O, P> pb::job_service_server::JobService for RpcService<I, O, P>
where
    I: Data,
    O: Send + Debug + Message + 'static,
    P: JobParser<I, O>,
{
    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(&self, req: Request<pb::JobRequest>) -> Result<Response<Self::SubmitStream>, Status> {
        let mut job_req = req.into_inner();
        if job_req.conf.is_none() {
            return Err(Status::new(Code::InvalidArgument, "job configuration not found"));
        }

        let conf_req = job_req.conf.take().unwrap();
        let conf = parse_conf_req(conf_req);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rpc_sink = RpcSink::new(conf.job_id, tx);
        let sink = ResultSink::<O>::with(rpc_sink);
        let service = self.inner.clone();
        let submitted =
            pegasus::run_opt(conf, sink, move |worker| worker.dataflow(service.accept(&job_req)));

        if let Err(e) = submitted {
            return Err(Status::invalid_argument(format!("submit job error {}", e)));
        }

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

pub struct RpcServer<S: pb::job_service_server::JobService> {
    service: S,
    addr: SocketAddr,
}

pub async fn start_rpc_server<I, O, P>(
    addr: SocketAddr, service: RpcService<I, O, P>, blocking: bool,
) -> Result<SocketAddr, Box<dyn std::error::Error>>
where
    I: Data,
    O: Send + Debug + Message + 'static,
    P: JobParser<I, O>,
{
    let server = RpcServer::new(addr, service);
    let local_addr = server.run(blocking).await?;
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
                serve.await.expect("Rpc server start error");
            });
        }
        Ok(local_addr)
    }
}

fn parse_conf_req(conf: pb::JobConfig) -> JobConf {
    let mut job_conf = JobConf::with_id(conf.job_id, conf.job_name, conf.workers);
    if conf.time_limit != 0 {
        job_conf.time_limit = conf.time_limit;
    }
    if conf.batch_size != 0 {
        job_conf.batch_size = conf.batch_size;
    }
    if conf.output_capacity != 0 {
        job_conf.batch_capacity = conf.output_capacity;
    }
    if conf.memory_limit != 0 {
        job_conf.memory_limit = conf.memory_limit;
    }
    job_conf.plan_print = conf.plan_print;
    if !conf.servers.is_empty() {
        job_conf.reset_servers(ServerConf::Partial(conf.servers.clone()));
    }
    job_conf
}
