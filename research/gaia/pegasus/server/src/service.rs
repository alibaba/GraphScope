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

use crate::factory::JobCompiler;
use crate::generated::protocol as pb;
use crate::materialize::ShadeMapFactory;
use crate::AnyData;
use crossbeam_utils::sync::ShardedLock;
use pegasus::api::accum::{Accumulator, ToListAccum};
use pegasus::api::function::EncodeFunction;
use pegasus::api::{Count, Fold, Group, KeyBy, ResultSet, Sink, RANGES};
use pegasus::codec::ShadeCodec;
use pegasus::stream::Stream;
use pegasus::{BuildJobError, Data, JobConf, JobGuard, NeverClone, ServerConf};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

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
        self.on_err_msg(0, err_msg);
    }

    pub fn on_err_msg(&self, err_code: i32, err_msg: impl Into<String>) {
        let err_msg = err_msg.into();
        error!("job[{}] get error {}", self.job_id, err_msg);
        let result = Some(pb::job_response::Result::Err(pb::JobError { err_code, err_msg }));
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
    factory: Arc<dyn JobCompiler<D>>,
    pub job_guards: Arc<ShardedLock<HashMap<u64, JobGuard>>>,
}

impl<D: AnyData> Service<D> {
    pub fn new<F: JobCompiler<D>>(factory: F) -> Self {
        Service {
            factory: Arc::new(factory),
            job_guards: Arc::new(ShardedLock::new(HashMap::new())),
        }
    }

    pub fn accept<O: Output + Clone>(&self, req: pb::JobRequest, output: O) {
        // validate request;
        // check if job conf lost;
        let pb::JobRequest { conf, source, plan, sink } = req;
        if let Some(conf) = conf {
            let conf = parse_job_conf(conf);
            info!("job conf: {:?}", conf);
            let output = JobResultSink::new(conf.job_id, output);
            if let Some(source) = source {
                if plan.is_some() && !plan.as_ref().unwrap().plan.is_empty() {
                    self.submit(conf, source, plan, sink, output);
                } else {
                    let ec = if let Some(sink) = sink {
                        match sink.sinker {
                            None => self.factory.sink(&vec![]),
                            Some(pb::sink::Sinker::Resource(res)) => self.factory.sink(&res),
                            _ => {
                                self.submit(conf, source, None, Some(sink), output);
                                return;
                            }
                        }
                    } else {
                        self.factory.sink(&vec![])
                    };
                    match self.factory.source(&source.resource) {
                        Ok(src) => match ec {
                            Ok(ec) => {
                                let mut vec = Vec::new();
                                vec.extend(src);
                                if !vec.is_empty() {
                                    let result = ec.encode(vec);
                                    output.on_next(result);
                                }
                            }
                            Err(err) => output.on_error(&err),
                        },
                        Err(e) => {
                            output.on_error(&e);
                        }
                    }
                    output.close();
                }
            } else {
                output.on_err_msg(0, "source of job not found;");
                output.close();
            }
        } else {
            error!("job config not found, ignore this job;");
            output.close();
        }
    }

    fn submit<O: Output + Clone>(
        &self, conf: JobConf, source: pb::Source, task: Option<pb::TaskPlan>,
        sink: Option<pb::Sink>, output: JobResultSink<O>,
    ) {
        let task = Arc::new(task);
        let source = Arc::new(source);
        let sink = Arc::new(sink);
        let result = pegasus::run(conf, |worker| {
            let source = source.clone();
            let task = task.clone();
            let sink = sink.clone();
            let factory = self.factory.clone();
            let output = output.clone();
            worker.dataflow(move |builder| {
                let src = factory.source(&source.resource)?.fuse();
                let source = builder.input_from_iter(src)?;
                let stream = if let Some(task) = task.as_ref() {
                    crate::materialize::exec(&source, &task.plan, &factory)?
                } else {
                    source
                };

                if let Some(sink) = sink.as_ref() {
                    match &sink.sinker {
                        Some(pb::sink::Sinker::Fold(fold)) => {
                            let range = RANGES[fold.range as usize];
                            let accum_kind: pb::AccumKind =
                                unsafe { std::mem::transmute(fold.accum) };
                            match accum_kind {
                                pb::AccumKind::Cnt => {
                                    let funcs = factory.fold(&vec![], &vec![], &vec![])?;
                                    let ec = funcs.fold_sink()?;
                                    let s = stream.count(range)?;
                                    sink_fold(&s, ec, output)?;
                                }
                                pb::AccumKind::ToList => {
                                    let funcs = factory.fold(&vec![], &vec![], &vec![])?;
                                    let ec = funcs.fold_sink()?;
                                    let s = stream.fold_with_accum(range, ToListAccum::new())?;
                                    sink_fold(&s, ec, output)?;
                                }
                                _ => unimplemented!(),
                            }
                        }
                        Some(pb::sink::Sinker::Group(group)) => {
                            let range = RANGES[group.range as usize];
                            let funcs = factory.group(&group.map, &vec![], &vec![])?;
                            let key_func = funcs.key()?;
                            let map_factory = funcs.map_factory()?;
                            let ec = funcs.sink()?;
                            let shade_map = ShadeMapFactory::new(map_factory);
                            let s = stream.key_by(key_func)?.group_with_map(range, shade_map)?;
                            sink_shade(&s, ec, output)?;
                        }
                        Some(pb::sink::Sinker::Resource(res)) => {
                            let ec = factory.sink(&res)?;
                            sink_with_encoder(&stream, ec, output)?;
                        }
                        None => {
                            let ec = factory.sink(&vec![])?;
                            sink_with_encoder(&stream, ec, output)?;
                        }
                    }
                } else {
                    let ec = factory.sink(&vec![])?;
                    sink_with_encoder(&stream, ec, output)?;
                }
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
fn sink_with_encoder<D: Data, O: Output + Clone>(
    stream: &Stream<D>, ec: Box<dyn EncodeFunction<D>>, output: JobResultSink<O>,
) -> Result<(), BuildJobError> {
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
    })
}

#[inline]
fn sink_fold<D: Data + Accumulator<A>, O: Output + Clone, A: 'static>(
    stream: &Stream<D>, ec: Box<dyn EncodeFunction<Box<dyn Accumulator<A>>>>,
    output: JobResultSink<O>,
) -> Result<(), BuildJobError> {
    stream.sink_by(|_meta| {
        move |_tag, result| match result {
            ResultSet::Data(data) => {
                let bytes = ec.encode(
                    data.into_iter()
                        .map(|fold| Box::new(fold) as Box<dyn Accumulator<A>>)
                        .collect(),
                );
                output.on_next(bytes);
            }
            ResultSet::End => {
                output.close();
            }
        }
    })
}

#[inline]
fn sink_shade<D: Send + Debug + 'static, O: Output + Clone>(
    stream: &Stream<NeverClone<ShadeCodec<D>>>, ec: Box<dyn EncodeFunction<D>>,
    output: JobResultSink<O>,
) -> Result<(), BuildJobError> {
    stream.sink_by(|_meta| {
        move |_tag, result| match result {
            ResultSet::Data(data) => {
                let bytes = ec.encode(data.into_iter().map(|shade| shade.take().take()).collect());
                output.on_next(bytes);
            }
            ResultSet::End => {
                output.close();
            }
        }
    })
}

#[inline]
fn parse_job_conf(conf: pb::JobConfig) -> JobConf {
    let mut job_conf = JobConf::with_id(conf.job_id, conf.job_name, conf.workers);
    if conf.time_limit != 0 {
        job_conf.time_limit = conf.time_limit;
    }
    if conf.batch_size != 0 {
        job_conf.batch_size = conf.batch_size;
    }
    if conf.output_capacity != 0 {
        job_conf.output_capacity = conf.output_capacity;
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
