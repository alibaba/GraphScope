use std::cell::RefCell;
use std::fmt::{Debug, Display, Formatter};
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{BoxStream, SelectAll};
use futures::{Stream, StreamExt};
use pegasus::{JobConf, ServerConf};

use crate::job::JobDesc;
use crate::pb::job_config::Servers;
use crate::pb::job_service_client::JobServiceClient;
use crate::pb::{BinaryResource, Empty, JobConfig, JobRequest, ServerList};

pub enum JobError {
    InvalidConfig(String),
    RPCError(tonic::Status),
}

impl Debug for JobError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
            JobError::RPCError(status) => write!(f, "RPCError: {}", status),
        }
    }
}

impl Display for JobError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

pub struct RPCJobClient {
    conns: Vec<Option<RefCell<JobServiceClient<tonic::transport::Channel>>>>,
}

impl RPCJobClient {
    pub fn new() -> Self {
        RPCJobClient { conns: vec![] }
    }

    pub async fn connect<D>(&mut self, server_id: u64, url: D) -> Result<(), tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let client = JobServiceClient::connect(url).await?;
        while server_id as usize >= self.conns.len() {
            self.conns.push(None);
        }
        self.conns[server_id as usize] = Some(RefCell::new(client));
        Ok(())
    }

    pub async fn add_library<P: AsRef<Path>>(&mut self, name: &str, path: P) -> Result<(), JobError> {
        if self.conns.len() == 0 {
            return Ok(());
        }

        match std::fs::File::open(path) {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                match file.read_to_end(&mut buffer) {
                    Ok(_size) => {
                        buffer.shrink_to_fit();
                        let mut tasks = Vec::new();
                        for conn in self.conns.iter() {
                            if let Some(client) = conn {
                                let res =
                                    BinaryResource { name: name.to_string(), resource: buffer.clone() };
                                tasks.push(async move {
                                    let mut client = client.borrow_mut();
                                    client.add_library(res.clone()).await
                                });
                            }
                        }
                        if tasks.len() == 0 {
                            return Ok(());
                        }

                        if tasks.len() == 1 {
                            if let Err(e) = tasks.pop().unwrap().await {
                                return Err(JobError::RPCError(e));
                            }
                        } else {
                            for result in futures::future::join_all(tasks).await {
                                if let Err(e) = result {
                                    return Err(JobError::RPCError(e));
                                }
                            }
                        }
                        Ok(())
                    }
                    Err(e) => Err(JobError::InvalidConfig(format!("read lib fail {}", e))),
                }
            }
            Err(e) => Err(JobError::InvalidConfig(format!("open lib file failure: {}", e))),
        }
    }

    pub async fn submit(
        &mut self, config: JobConf, job: JobDesc,
    ) -> Result<BoxStream<'static, Result<Vec<u8>, tonic::Status>>, JobError> {
        let mut remotes = vec![];
        let servers = match config.servers() {
            ServerConf::Local => {
                for conn in self.conns.iter() {
                    if let Some(c) = conn {
                        remotes.push(c);
                        break;
                    }
                }
                Servers::Local(Empty {})
            }
            ServerConf::Partial(ref list) => {
                for id in list.iter() {
                    let index = *id as usize;
                    if index >= self.conns.len() {
                        return Err(JobError::InvalidConfig(format!("server[{}] not connect;", index)));
                    }
                    if let Some(ref c) = self.conns[index] {
                        remotes.push(c);
                    } else {
                        return Err(JobError::InvalidConfig(format!("server[{}] not connect;", index)));
                    }
                }
                Servers::Part(ServerList { servers: list.clone() })
            }
            ServerConf::All => {
                for (index, conn) in self.conns.iter().enumerate() {
                    if let Some(c) = conn {
                        remotes.push(c);
                    } else {
                        return Err(JobError::InvalidConfig(format!("server[{}] not connect;", index)));
                    }
                }
                Servers::All(Empty {})
            }
        };

        let r_size = remotes.len();
        if r_size == 0 {
            warn!("No remote server selected;");
            return Ok(futures::stream::empty().boxed());
        }

        let JobDesc { input, plan, resource } = job;

        let conf = JobConfig {
            job_id: config.job_id,
            job_name: config.job_name,
            workers: config.workers,
            time_limit: config.time_limit,
            batch_size: config.batch_size,
            batch_capacity: config.batch_capacity,
            memory_limit: config.memory_limit,
            trace_enable: config.trace_enable,
            servers: Some(servers),
        };
        let req = JobRequest { conf: Some(conf), source: input, plan, resource };

        if r_size == 1 {
            match remotes[0].borrow_mut().submit(req).await {
                Ok(resp) => Ok(resp
                    .into_inner()
                    .map(|r| r.map(|jr| jr.resp))
                    .boxed()),
                Err(status) => Err(JobError::RPCError(status)),
            }
        } else {
            let mut tasks = Vec::with_capacity(r_size);
            for r in remotes {
                let req = req.clone();
                tasks.push(async move {
                    let mut conn = r.borrow_mut();
                    conn.submit(req).await
                })
            }
            let results = futures::future::join_all(tasks).await;
            let mut stream_res = Vec::with_capacity(results.len());
            for res in results {
                match res {
                    Ok(resp) => {
                        let stream = resp.into_inner();
                        stream_res.push(stream);
                    }
                    Err(status) => {
                        return Err(JobError::RPCError(status));
                    }
                }
            }
            Ok(futures::stream::select_all(stream_res)
                .map(|r| r.map(|jr| jr.resp))
                .boxed())
        }
    }
}

pub enum Either<T: Stream + Unpin> {
    Single(T),
    Select(SelectAll<T>),
}

impl<T: Stream + Unpin> Stream for Either<T> {
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match *self {
            Either::Single(ref mut s) => Pin::new(s).poll_next(cx),
            Either::Select(ref mut s) => Pin::new(s).poll_next(cx),
        }
    }
}
