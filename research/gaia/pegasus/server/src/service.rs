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

use crate::desc::{JobDesc, OpKind, OperatorDesc, EMPTY};
use crate::factory::JobCompiler;
use crate::generated::protobuf as pb;
use crate::AnyData;
use crossbeam_utils::sync::ShardedLock;
use pegasus::api::{ResultSet, Sink};
use pegasus::{BuildJobError, JobConf, JobGuard};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

pub trait JobPreprocess: Send + Sync + 'static {
    fn preprocess(&self, job: &mut JobDesc) -> Result<(), BuildJobError>;
}

pub trait Output: Send + 'static {
    fn send(&self, res: pb::JobResponse);

    fn close(&self);
}

pub struct JobResultSink<O: Output> {
    job_id: u64,
    output: O,
}

impl<O: Output> JobResultSink<O> {
    pub fn new(job_id: u64, output: O) -> Self {
        JobResultSink { job_id, output }
    }

    pub fn on_next(&self, data: Vec<u8>) {
        let result = Some(pb::job_response::Result::Data(data));
        let res = pb::JobResponse { job_id: self.job_id, result };
        self.output.send(res);
    }

    pub fn on_error(&self, err: &dyn std::error::Error) {
        error!("job[{}] get error {}", self.job_id, err);
        let err_msg = format!("{}", err);
        let result = Some(pb::job_response::Result::Err(pb::JobError { err_code: 0, err_msg }));
        let res = pb::JobResponse { job_id: self.job_id, result };
        self.output.send(res);
    }

    pub fn close(&self) {
        self.output.close();
    }
}

impl<O: Output + Clone> Clone for JobResultSink<O> {
    fn clone(&self) -> Self {
        JobResultSink { job_id: self.job_id, output: self.output.clone() }
    }
}

#[derive(Clone)]
pub struct Service<D: AnyData> {
    preprocess: Option<Arc<dyn JobPreprocess>>,
    factory: Arc<dyn JobCompiler<D>>,
    job_guards: Arc<ShardedLock<HashMap<u64, JobGuard>>>,
}

impl<D: AnyData> Service<D> {
    pub fn new<F: JobCompiler<D>>(factory: F) -> Self {
        Service {
            preprocess: None,
            factory: Arc::new(factory),
            job_guards: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub fn register_preprocess<P: JobPreprocess>(&mut self, p: P) {
        self.preprocess = Some(Arc::new(p));
    }

    pub fn accept<O: Output + Clone>(&self, mut req: pb::JobRequest, output: O) {
        if let Some(conf) = req.conf.take() {
            let conf = parse_job_conf(conf);
            let source = req.source.into();
            let mut plan = Vec::with_capacity(req.plan.len());
            let output = JobResultSink::new(conf.job_id, output);
            for op in req.plan {
                match OperatorDesc::parse(op) {
                    Ok(desc) => {
                        plan.push(desc);
                    }
                    Err(e) => {
                        output.on_error(&e);
                        output.close();
                        return;
                    }
                }
            }
            let mut job = JobDesc::new(conf, source, plan);
            info!("finish decode job request {}", job.job_id());
            if let Some(ref p) = self.preprocess {
                if let Err(e) = p.preprocess(&mut job) {
                    output.on_error(&e);
                    output.close();
                    return;
                }
            }
            self.submit(job, output);
        } else {
            error!("job config not found, ignore this job;");
            output.close();
        }
    }

    fn submit<O: Output + Clone>(&self, desc: JobDesc, output: JobResultSink<O>) {
        let JobDesc { conf, source, plan } = desc;
        let result = pegasus::run(conf, |worker| {
            let source = source.clone();
            let plan = plan.clone();
            let factory = self.factory.clone();
            let output = output.clone();
            let worker_index = worker.id.index;
            worker.dataflow(move |builder| {
                let src = factory.source(worker_index, &*source)?.fuse();
                let source = builder.input_from_iter(src)?;
                let stream = if plan.len() > 0 {
                    crate::materialize::exec(&source, plan.deref(), &factory)?
                } else {
                    source
                };
                let ec = if let Some(op) = plan.last() {
                    if op.op_kind == OpKind::Sink {
                        factory.sink(&*op.resource)
                    } else {
                        factory.sink(&EMPTY)
                    }
                } else {
                    factory.sink(&EMPTY)
                }?;
                stream.sink_by(|_meta| {
                    move |_tag, result| match result {
                        ResultSet::Data(data) => {
                            let bytes = ec.encode(data);
                            output.on_next(bytes);
                        }
                        ResultSet::End => {
                            output.close();
                        }
                    }
                })?;
                Ok(())
            })
        });

        match result {
            Ok(Some(guard)) => {
                let mut w = self.job_guards.write().expect("fetch write lock failure;");
                w.insert(guard.job_id, guard);
            }
            Err(err) => {
                output.on_error(&err);
            }
            _ => (),
        }
    }
}

#[inline]
fn parse_job_conf(conf: pb::JobConfig) -> JobConf {
    let mut job_conf = JobConf::new(conf.job_id, conf.job_name, conf.workers);
    if !conf.servers.is_empty() {
        job_conf.add_servers(&conf.servers);
    }
    // TODO: more job configurations;
    job_conf
}
