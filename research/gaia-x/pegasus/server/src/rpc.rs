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
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
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
use crate::generated::protocol::job_config::Servers;
use crate::service::{JobParser, Service};

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
        let data = next.encode_to_vec();
        let res = pb::JobResponse { job_id: self.job_id, res: Some(pb::BinaryResource { resource: data }) };
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

#[tonic::async_trait]
impl<I, O, P> pb::job_service_server::JobService for RpcService<I, O, P>
where
    I: Data,
    O: Send + Debug + Message + 'static,
    P: JobParser<I, O>,
{
    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(&self, req: Request<pb::JobRequest>) -> Result<Response<Self::SubmitStream>, Status> {
        info!("accept new request from {:?};", req.remote_addr());
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

pub struct RpcServerConfig {
    rpc_host: Option<String>,
    rpc_port: Option<u16>,
    rpc_concurrency_limit_per_connection: Option<usize>,
    rpc_timeout: Option<Duration>,
    rpc_initial_stream_window_size: Option<u32>,
    rpc_initial_connection_window_size: Option<u32>,
    rpc_max_concurrent_streams: Option<u32>,
    rpc_keep_alive_interval: Option<Duration>,
    rpc_keep_alive_timeout: Option<Duration>,
    tcp_keep_alive: Option<Duration>,
    tcp_nodelay: Option<bool>,
}

pub struct RpcServer<S: pb::job_service_server::JobService> {
    service: S,
    config: RpcServerConfig,
}

pub async fn start_rpc_server<I, O, P>(
    config: RpcServerConfig, service: RpcService<I, O, P>,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: Data,
    O: Send + Debug + Message + 'static,
    P: JobParser<I, O>,
{
    let server = RpcServer::new(config, service);
    server.run().await?;
    Ok(())
}

impl<S: pb::job_service_server::JobService> RpcServer<S> {
    pub fn new(config: RpcServerConfig, service: S) -> Self {
        RpcServer { service, config }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let RpcServer { service, mut config } = self;
        let host = config
            .rpc_host
            .clone()
            .unwrap_or("0.0.0.0".to_owned());
        let addr = SocketAddr::new(host.parse()?, config.rpc_port.unwrap_or(0));
        let incoming = TcpIncoming::new(addr, config.tcp_nodelay.unwrap_or(true), config.tcp_keep_alive)?;
        info!("Rpc server started on {}", incoming.inner.local_addr());
        let mut builder = Server::builder();
        if let Some(limit) = config.rpc_concurrency_limit_per_connection {
            builder = builder.concurrency_limit_per_connection(limit);
        }

        if let Some(dur) = config.rpc_timeout.take() {
            builder.timeout(dur);
        }

        if let Some(size) = config.rpc_initial_stream_window_size {
            builder = builder.initial_stream_window_size(Some(size));
        }

        if let Some(size) = config.rpc_initial_connection_window_size {
            builder = builder.initial_connection_window_size(Some(size));
        }

        if let Some(size) = config.rpc_max_concurrent_streams {
            builder = builder.max_concurrent_streams(Some(size));
        }

        if let Some(dur) = config.rpc_keep_alive_interval.take() {
            builder = builder.http2_keepalive_interval(Some(dur));
        }

        if let Some(dur) = config.rpc_keep_alive_timeout.take() {
            builder = builder.http2_keepalive_timeout(Some(dur));
        }

        builder
            .add_service(pb::job_service_server::JobServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await?;
        Ok(())
    }
}

pub(crate) struct TcpIncoming {
    inner: AddrIncoming,
}

impl TcpIncoming {
    pub(crate) fn new(addr: SocketAddr, nodelay: bool, keepalive: Option<Duration>) -> hyper::Result<Self> {
        let mut inner = AddrIncoming::bind(&addr)?;
        inner.set_nodelay(nodelay);
        inner.set_keepalive(keepalive);
        Ok(TcpIncoming { inner })
    }
}

impl Stream for TcpIncoming {
    type Item = Result<AddrStream, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_accept(cx)
    }
}

fn parse_conf_req(mut req: pb::JobConfig) -> JobConf {
    let mut conf = JobConf::new(req.job_name);
    if req.job_id != 0 {
        conf.job_id = req.job_id;
    }

    if req.workers != 0 {
        conf.workers = req.workers;
    }

    if req.time_limit != 0 {
        conf.time_limit = req.time_limit;
    }

    if req.batch_size != 0 {
        conf.batch_size = req.batch_size;
    }

    if req.batch_capacity != 0 {
        conf.batch_capacity = req.batch_capacity;
    }

    if req.plan_print {
        conf.plan_print = true;
    }

    if let Some(servers) = req.servers.take() {
        match servers {
            Servers::Local(_) => conf.reset_servers(ServerConf::Local),
            Servers::Part(mut p) => {
                if !p.servers.is_empty() {
                    let vec = std::mem::replace(&mut p.servers, vec![]);
                    conf.reset_servers(ServerConf::Partial(vec))
                }
            }
            Servers::All(_) => conf.reset_servers(ServerConf::All),
        }
    }
    conf
}
