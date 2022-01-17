use std::cell::RefCell;
use std::fmt::{Debug, Display, Formatter};
use futures::{Stream};
use tonic::Status;
use crate::JobResponse;
use crate::pb::{BinaryResource, JobConfig, JobRequest};
use crate::pb::job_config::Servers;
use crate::pb::job_service_client::JobServiceClient;
use crate::service::JobDesc;


pub enum JobError {
    InvalidConfig(String),
    RPCError(tonic::Status)
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
    conns: Vec<Option<RefCell<JobServiceClient<tonic::transport::Channel>>>>
}

impl RPCJobClient {
    pub fn new() -> Self {
        RPCJobClient { conns: vec![] }
    }

    pub async fn connect<D>(&mut self, server_id: u64, url: D) -> Result<(), tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<tonic::codegen::StdError>
    {
        let client = JobServiceClient::connect(url).await?;
        while server_id as usize >= self.conns.len() {
            self.conns.push(None);
        }
        self.conns[server_id as usize] = Some(RefCell::new(client));
        Ok(())
    }

    pub async fn submit(&mut self, config: JobConfig, job: JobDesc) -> Result<Box<dyn Stream<Item = Result<JobResponse, Status>>>, JobError> {
        let mut remotes = vec![];
        match config.servers {
            None | Some(Servers::Local(_)) => {
                for conn in self.conns.iter() {
                    if let Some(c) = conn {
                        remotes.push(c);
                        break
                    }
                }
            },
            Some(Servers::Part(ref list)) => {
                for id in list.servers.iter() {
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
            }
            Some(Servers::All(_)) => {
                for (index, conn) in self.conns.iter().enumerate() {
                    if let Some(c) = conn {
                        remotes.push(c);
                    } else {
                        return Err(JobError::InvalidConfig(format!("server[{}] not connect;", index)));
                    }
                }
            }
        }
        let JobDesc { input, plan, resource } = job;
        let req = JobRequest {
            conf: Some(config),
            source: if input.is_empty() { None } else { Some(BinaryResource { resource: input }) },
            plan : if plan.is_empty() { None } else { Some(BinaryResource { resource: plan } ) },
            resource: if resource.is_empty() { None } else { Some(BinaryResource { resource })}
        };

        let r_size = remotes.len();
        if r_size == 0 {
            warn!("No remote server selected;");
            return Ok(Box::new(futures::stream::empty()));
        }

        if r_size == 1 {
            match remotes[0].borrow_mut().submit(req).await {
                Ok(resp) => {
                   Ok(Box::new(resp.into_inner()))
                },
                Err(status) => {
                    Err(JobError::RPCError(status))
                }
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
                        let stream  = resp.into_inner();
                        stream_res.push(stream);
                    },
                    Err(status) => {
                        return Err(JobError::RPCError(status));
                    }
                }
            }
            Ok(Box::new(futures::stream::select_all(stream_res)))
        }
    }
}

