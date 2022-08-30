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
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use pegasus::api::function::FnResult;
use pegasus::api::FromStream;
use pegasus::result::{FromStreamExt, ResultSink};
use pegasus::{Configuration, Data, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use pegasus_network::ServerDetect;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::generated::protocol as pb;
use crate::generated::protocol::job_config::Servers;
use crate::job::{JobAssembly, JobDesc};
use crate::pb::{BinaryResource, Empty, Name};

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

impl FromStream<Vec<u8>> for RpcSink {
    fn on_next(&mut self, resp: Vec<u8>) -> FnResult<()> {
        // todo: use bytes to alleviate copy & allocate cost;
        let res = pb::JobResponse { job_id: self.job_id, resp };
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

impl FromStreamExt<Vec<u8>> for RpcSink {
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

#[allow(dead_code)]
#[derive(Clone)]
pub struct JobServiceImpl<I> {
    inner: Arc<dyn JobAssembly<I>>,
    report: bool,
}

#[tonic::async_trait]
impl<I> pb::job_service_server::JobService for JobServiceImpl<I>
where
    I: Data,
{
    async fn add_library(&self, request: Request<BinaryResource>) -> Result<Response<Empty>, Status> {
        let BinaryResource { name, resource } = request.into_inner();
        let mut path = PathBuf::from("./lib");
        path.push(&name);
        path = path.with_extension("so");

        match std::fs::File::create(path.as_path()) {
            Ok(mut f) => {
                if let Err(e) = f.write_all(&resource[..]) {
                    return Err(Status::aborted(format!("write lib failure: {}", e)));
                }
                if let Err(e) = f.flush() {
                    return Err(Status::aborted(format!("write lib failure: {}", e)));
                }
            }
            Err(e) => {
                return Err(Status::aborted(format!("create lib failure: {}", e)));
            }
        }
        match unsafe { libloading::Library::new(&path) } {
            Ok(lib) => {
                info!("add library with name {}", name);
                if let Some((name, _)) = pegasus::resource::add_global_resource(name, lib) {
                    return Err(Status::aborted(format!("resource {} already exists;", name)));
                }
                Ok(Response::new(Empty {}))
            }
            Err(err) => {
                let msg = format!("fail to load library {:?}", path);
                error!("{}, caused by {} ;", msg, err);
                Err(Status::aborted(msg))
            }
        }
    }

    async fn remove_library(&self, request: Request<Name>) -> Result<Response<Empty>, Status> {
        let name = request.into_inner().name;
        pegasus::resource::remove_global_resource(&name);
        Ok(Response::new(Empty {}))
    }

    type SubmitStream = UnboundedReceiverStream<Result<pb::JobResponse, Status>>;

    async fn submit(&self, req: Request<pb::JobRequest>) -> Result<Response<Self::SubmitStream>, Status> {
        debug!("accept new request from {:?};", req.remote_addr());
        let pb::JobRequest { conf, source, plan, resource } = req.into_inner();
        if conf.is_none() {
            return Err(Status::new(Code::InvalidArgument, "job configuration not found"));
        }

        let conf = parse_conf_req(conf.unwrap());
        pegasus::wait_servers_ready(conf.servers());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rpc_sink = RpcSink::new(conf.job_id, tx);
        let sink = ResultSink::<Vec<u8>>::with(rpc_sink);
        if conf.trace_enable {
            info!("submitting job({}) with id {}", conf.job_name, conf.job_id);
        }
        let job_id = conf.job_id;
        let service = &self.inner;
        let job = JobDesc { input: source, plan, resource };

        if let Err(e) = pegasus::run_opt(conf, sink, move |worker| service.assemble(&job, worker)) {
            error!("submit job {} failure: {:?}", job_id, e);
            Err(Status::unknown(format!("submit job error {}", e)))
        } else {
            Ok(Response::new(UnboundedReceiverStream::new(rx)))
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RPCServerConfig {
    pub rpc_host: Option<String>,
    pub rpc_port: Option<u16>,
    pub rpc_concurrency_limit_per_connection: Option<usize>,
    pub rpc_timeout_ms: Option<u64>,
    pub rpc_initial_stream_window_size: Option<u32>,
    pub rpc_initial_connection_window_size: Option<u32>,
    pub rpc_max_concurrent_streams: Option<u32>,
    pub rpc_keep_alive_interval_ms: Option<u64>,
    pub rpc_keep_alive_timeout_ms: Option<u64>,
    pub tcp_keep_alive_ms: Option<u64>,
    pub tcp_nodelay: Option<bool>,
}

impl RPCServerConfig {
    pub fn new(rpc_host: Option<String>, rpc_port: Option<u16>) -> Self {
        RPCServerConfig {
            rpc_host,
            rpc_port,
            rpc_concurrency_limit_per_connection: None,
            rpc_timeout_ms: None,
            rpc_initial_stream_window_size: None,
            rpc_initial_connection_window_size: None,
            rpc_max_concurrent_streams: None,
            rpc_keep_alive_interval_ms: None,
            rpc_keep_alive_timeout_ms: None,
            tcp_keep_alive_ms: None,
            tcp_nodelay: None,
        }
    }

    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(&content)
    }
}

pub struct RPCJobServer<S: pb::job_service_server::JobService> {
    service: S,
    rpc_config: RPCServerConfig,
}

/// start both rpc server and pegasus server
pub async fn start_all<I: Data, P, D, E>(
    rpc_config: RPCServerConfig, server_config: Configuration, assemble: P, server_detector: D,
    mut listener: E,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: JobAssembly<I>,
    D: ServerDetect + 'static,
    E: ServiceStartListener,
{
    let server_id = server_config.server_id();
    if let Some(server_addr) = pegasus::startup_with(server_config, server_detector)? {
        listener.on_server_start(server_id, server_addr)?;
    }
    start_rpc_server(server_id, rpc_config, assemble, listener).await?;
    Ok(())
}

/// startup rpc server
pub async fn start_rpc_server<I: Data, P, E>(
    server_id: u64, rpc_config: RPCServerConfig, assemble: P, listener: E,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: JobAssembly<I>,
    E: ServiceStartListener,
{
    let service = JobServiceImpl { inner: Arc::new(assemble), report: true };
    let server = RPCJobServer::new(rpc_config, service);
    server.run(server_id, listener).await?;
    Ok(())
}

impl<S: pb::job_service_server::JobService> RPCJobServer<S> {
    pub fn new(rpc_config: RPCServerConfig, service: S) -> Self {
        RPCJobServer { service, rpc_config }
    }

    pub async fn run<E>(self, server_id: u64, mut listener: E) -> Result<(), Box<dyn std::error::Error>>
    where
        E: ServiceStartListener,
    {
        let RPCJobServer { service, mut rpc_config } = self;
        let mut builder = Server::builder();
        if let Some(limit) = rpc_config.rpc_concurrency_limit_per_connection {
            builder = builder.concurrency_limit_per_connection(limit);
        }

        if let Some(dur) = rpc_config.rpc_timeout_ms.take() {
            builder.timeout(Duration::from_millis(dur));
        }

        if let Some(size) = rpc_config.rpc_initial_stream_window_size {
            builder = builder.initial_stream_window_size(Some(size));
        }

        if let Some(size) = rpc_config.rpc_initial_connection_window_size {
            builder = builder.initial_connection_window_size(Some(size));
        }

        if let Some(size) = rpc_config.rpc_max_concurrent_streams {
            builder = builder.max_concurrent_streams(Some(size));
        }

        if let Some(dur) = rpc_config.rpc_keep_alive_interval_ms.take() {
            builder = builder.http2_keepalive_interval(Some(Duration::from_millis(dur)));
        }

        if let Some(dur) = rpc_config.rpc_keep_alive_timeout_ms.take() {
            builder = builder.http2_keepalive_timeout(Some(Duration::from_millis(dur)));
        }

        let service = builder.add_service(pb::job_service_server::JobServiceServer::new(service));

        let rpc_host = rpc_config
            .rpc_host
            .clone()
            .unwrap_or("0.0.0.0".to_owned());
        let rpc_port = rpc_config.rpc_port.unwrap_or(0);
        let rpc_server_addr = ServerAddr::new(rpc_host, rpc_port);
        let addr = rpc_server_addr.to_socket_addr()?;
        let ka = rpc_config
            .tcp_keep_alive_ms
            .map(|d| Duration::from_millis(d));
        let incoming = TcpIncoming::new(addr, rpc_config.tcp_nodelay.unwrap_or(true), ka)?;
        info!("starting RPC job server on {} ...", incoming.inner.local_addr());
        listener.on_rpc_start(server_id, incoming.inner.local_addr())?;

        service.serve_with_incoming(incoming).await?;
        Ok(())
    }
}

pub trait ServiceStartListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()>;

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()>;
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

    if req.trace_enable {
        conf.trace_enable = true;
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
