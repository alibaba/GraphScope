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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use crate::api::meta::OperatorInfo;
use crate::channel_id::ChannelInfo;
use crate::communication::output::OutputBuilderImpl;
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPull, GeneralPush};
use crate::errors::{BuildJobError, IOResult, JobExecError};
use crate::event::emitter::EventEmitter;
use crate::graph::{Dependency, DotGraph, Edge, Port};
use crate::operator::{GeneralOperator, NotifiableOperator, Operator, OperatorBuilder, OperatorCore};
use crate::schedule::Schedule;
use crate::{Data, JobConf, Tag, WorkerId};

pub struct DataflowBuilder {
    pub worker_id: WorkerId,
    pub config: Arc<JobConf>,
    pub(crate) event_emitter: EventEmitter,
    ch_index: Rc<RefCell<u32>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    edges: Rc<RefCell<Vec<Edge>>>,
    sinks: Rc<RefCell<Vec<usize>>>,
}

impl DataflowBuilder {
    pub(crate) fn new(worker_id: WorkerId, event_emitter: EventEmitter, config: &Arc<JobConf>) -> Self {
        DataflowBuilder {
            worker_id,
            config: config.clone(),
            operators: Rc::new(RefCell::new(vec![])),
            edges: Rc::new(RefCell::new(vec![])),
            event_emitter,
            ch_index: Rc::new(RefCell::new(1)),
            sinks: Rc::new(RefCell::new(vec![])),
        }
    }

    #[inline]
    pub fn job_conf(&self) -> &Arc<JobConf> {
        &self.config
    }

    pub fn get_operator(&self, index: usize) -> OperatorRef {
        let operators = self.operators.clone();
        assert!(index < operators.borrow().len(), "invalid operator index;");
        OperatorRef::new(index, operators, self.config.clone())
    }

    #[inline]
    pub(crate) fn next_channel_index(&self) -> u32 {
        let mut idx = self.ch_index.borrow_mut();
        *idx += 1;
        *idx - 1
    }

    pub(crate) fn add_operator<F, O>(&self, name: &str, scope_level: u32, construct: F) -> OperatorRef
    where
        O: OperatorCore,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let index = self.operators.borrow().len() + 1;
        let info = OperatorInfo::new(name, index, scope_level);
        let core = Box::new(construct(&info));
        let op_b = OperatorBuilder::new(info, GeneralOperator::Simple(core));
        self.operators.borrow_mut().push(op_b);
        OperatorRef::new(index, self.operators.clone(), self.config.clone())
    }

    pub(crate) fn add_notify_operator<F, O>(
        &self, name: &str, scope_level: u32, construct: F,
    ) -> OperatorRef
    where
        O: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let index = self.operators.borrow().len() + 1;
        let info = OperatorInfo::new(name, index, scope_level);
        let core = Box::new(construct(&info));
        let op_b = OperatorBuilder::new(info, GeneralOperator::Notifiable(core));
        self.operators.borrow_mut().push(op_b);
        OperatorRef::new(index, self.operators.clone(), self.config.clone())
    }

    pub(crate) fn add_edge(&self, edge: Edge) {
        self.edges.borrow_mut().push(edge);
    }

    pub(crate) fn add_sink(&self, index: usize) {
        self.sinks.borrow_mut().push(index);
    }

    pub(crate) fn build(self, sch: &mut Schedule) -> Result<Dataflow, BuildJobError> {
        let report = self.worker_id.index == 0
            && (self.config.plan_print || self.config.trace_enable || log_enabled!(log::Level::Trace));
        let mut plan_desc = String::new();
        if report {
            writeln!(plan_desc, "\n============ Build Dataflow ==============").ok();
            writeln!(plan_desc, "Peers:\t{}", self.worker_id.total_peers()).ok();
            writeln!(plan_desc, "{}", "Operators: ").ok();
        }

        let mut builds = self.operators.replace(vec![]);
        builds.sort_by_key(|op| op.index());
        let mut operators = Vec::with_capacity(builds.len() + 1);
        // place holder;
        operators.push(None);
        let mut op_names = vec![];
        op_names.push("root".to_owned());
        let mut depends = Dependency::default();
        sch.add_schedule_op(0, 0, vec![], vec![]);
        let sinks = self.sinks.replace(vec![]);
        depends.set_sinks(sinks);
        for e in self.edges.borrow().iter() {
            depends.add(e);
        }

        for (i, mut op_b) in builds.drain(..).enumerate() {
            let op_index = op_b.index();
            assert_eq!(i + 1, op_index, "{:?}", op_b.info);
            let inputs_notify = op_b.take_inputs_notify();
            let outputs_cancel = op_b.build_outputs_cancel();
            sch.add_schedule_op(op_index, op_b.info.scope_level, inputs_notify, outputs_cancel);
            let op = op_b.build();
            op_names.push(op.info.name.clone());
            if report {
                writeln!(plan_desc, "\t{}\t{}({})", op.info.index, op.info.name, op.info.index).ok();
            }
            operators.push(Some(op));
        }
        let edges = self.edges.replace(vec![]);
        if report {
            writeln!(plan_desc, "Channels ").ok();
            for e in edges.iter() {
                writeln!(plan_desc, "\t{:?}", e).ok();
            }
        }

        writeln!(plan_desc, "==========================================").ok();
        if report {
            info!("crate job[{}] with configuration : {:?}", self.config.job_id, self.config);
            info!("{}", plan_desc);
            let dot_g = DotGraph::new(self.config.job_name.clone(), self.config.job_id, op_names, edges);
            if let Ok(mut f) = File::create(format!("{}_{}.dot", self.config.job_name, self.config.job_id))
            {
                if let Err(e) = dot::render(&dot_g, &mut f) {
                    error!("create dot file failure: {}", e);
                }
            }
        }

        Ok(Dataflow {
            worker_id: self.worker_id,
            operators: RefCell::new(operators),
            conf: self.config,
            depends,
        })
    }
}

impl Clone for DataflowBuilder {
    fn clone(&self) -> Self {
        DataflowBuilder {
            worker_id: self.worker_id,
            config: self.config.clone(),
            operators: self.operators.clone(),
            event_emitter: self.event_emitter.clone(),
            ch_index: self.ch_index.clone(),
            edges: self.edges.clone(),
            sinks: self.sinks.clone(),
        }
    }
}

pub struct OperatorRef {
    index: usize,
    borrow: Rc<RefCell<Vec<OperatorBuilder>>>,
    conf: Arc<JobConf>,
}

impl OperatorRef {
    fn new(index: usize, borrow: Rc<RefCell<Vec<OperatorBuilder>>>, conf: Arc<JobConf>) -> Self {
        OperatorRef { index, borrow, conf }
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn next_input_port(&self) -> Port {
        let b = self.borrow.borrow();
        b[self.index - 1].next_input_port()
    }

    pub fn add_input<T: Data>(
        &self, ch_info: ChannelInfo, pull: GeneralPull<MicroBatch<T>>,
        notify: Option<GeneralPush<MicroBatch<T>>>, event_emitter: &EventEmitter,
    ) {
        let mut b = self.borrow.borrow_mut();
        b[self.index - 1].add_input(ch_info, pull, notify, event_emitter)
    }

    pub fn new_output<D: Data>(&self) -> OutputBuilderImpl<D> {
        let mut b = self.borrow.borrow_mut();
        let batch_size = self.conf.batch_size as usize;
        let batch_capacity = self.conf.batch_capacity;
        b[self.index - 1].new_output_port(batch_size, batch_capacity)
    }
}

pub struct Dataflow {
    pub conf: Arc<JobConf>,
    pub worker_id: WorkerId,
    operators: RefCell<Vec<Option<Operator>>>,
    depends: Dependency,
}

impl Dataflow {
    pub fn dependency(&self) -> &Dependency {
        &self.depends
    }

    pub fn operator_length(&self) -> usize {
        self.operators.borrow().len()
    }

    pub fn try_fire(&self, index: usize) -> Result<bool, JobExecError> {
        let mut operators = self.operators.borrow_mut();
        if let Some(op_opt) = operators.get_mut(index) {
            if let Some(mut op) = op_opt.take() {
                if !op.is_idle()? {
                    let result = op.fire();
                    if op.is_finished() {
                        op.close();
                        // debug_worker!("operator {:?} finished;", op.meta);
                    } else {
                        *op_opt = Some(op);
                    }
                    return result.map(|_| true);
                } else {
                    *op_opt = Some(op)
                }
            }
        }
        Ok(false)
    }

    pub fn is_idle(&self) -> IOResult<bool> {
        let operators = self.operators.borrow();
        for op in operators.iter() {
            if let Some(op) = op {
                if !op.is_idle()? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn check_finish(&self) -> bool {
        let mut operators = self.operators.borrow_mut();
        for op_opt in operators.iter_mut() {
            if let Some(op) = op_opt.take() {
                if op.is_finished() {
                    op.close();
                    // debug_worker!("operator {:?} finished;", op.meta);
                } else {
                    debug_worker!("operator {:?} is unfinished;", op.info);
                    op_opt.replace(op);
                    if self.conf.debug {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    return false;
                }
            }
        }
        true
    }

    pub fn try_cancel(
        &self, index: usize, discards: &mut VecDeque<(Port, Tag)>,
    ) -> Result<(), JobExecError> {
        let mut operators = self.operators.borrow_mut();
        if let Some(op_opt) = operators.get_mut(index) {
            if let Some(op) = op_opt {
                while let Some((port, tag)) = discards.pop_front() {
                    op.cancel(port.port, tag)?;
                }
            } else {
                discards.clear();
            }
        }
        Ok(())
    }
}
