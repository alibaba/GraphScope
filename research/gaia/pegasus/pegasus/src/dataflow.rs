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

use crate::api::meta::{OperatorMeta, ScopePrior};
use crate::errors::BuildJobError;
use crate::event::EventBus;
use crate::graph::{DotGraph, Edge, LogicalGraph};
use crate::operator::{OperatorBuilder, OperatorCore};
use crate::schedule::OpRuntime;
use crate::{JobConf, WorkerId};
use std::cell::{RefCell, RefMut};
use std::fmt::Write;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

pub struct DataflowBuilder {
    pub worker_id: WorkerId,
    pub config: Arc<JobConf>,
    pub event_bus: EventBus,
    ch_index: Rc<RefCell<u32>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    edges: Rc<RefCell<Vec<Edge>>>,
}

impl DataflowBuilder {
    pub fn new(worker_id: WorkerId, config: &Arc<JobConf>, event_bus: &EventBus) -> Self {
        DataflowBuilder {
            worker_id,
            config: config.clone(),
            operators: Rc::new(RefCell::new(vec![])),
            edges: Rc::new(RefCell::new(vec![])),
            event_bus: event_bus.clone(),
            ch_index: Rc::new(RefCell::new(1)),
        }
    }

    #[inline]
    pub fn job_conf(&self) -> &Arc<JobConf> {
        &self.config
    }

    pub fn get_operator(&self, index: OperatorIndex) -> OperatorRef {
        let operators = self.operators.borrow_mut();
        assert!(index.index < operators.len(), "invalid operator index;");
        OperatorRef::new(index.index, operators)
    }

    #[inline]
    pub fn next_channel_index(&self) -> u32 {
        let mut idx = self.ch_index.borrow_mut();
        *idx += 1;
        *idx - 1
    }

    pub fn construct_operator<F>(
        &self, name: &str, scope_depth: usize, order: ScopePrior, construct: F,
    ) -> OperatorRef
    where
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        let index = self.operators.borrow().len();
        let mut meta = OperatorMeta::new(name, self.worker_id, &self.config);
        meta.set_scope_depth(scope_depth).set_scope_order(order.clone()).set_index(index);
        let core = construct(&mut meta);
        let op_b = OperatorBuilder::new(meta, core, &self.event_bus);
        let mut borrow = self.operators.borrow_mut();
        borrow.push(op_b);
        OperatorRef::new(index, borrow)
    }

    #[inline]
    pub fn add_edge(&self, edge: Edge) {
        self.edges.borrow_mut().push(edge);
    }

    pub(crate) fn build(self) -> Result<Dataflow, BuildJobError> {
        let report =
            self.worker_id.index == 0 && (self.config.plan_print || self.config.trace_enable);
        let mut plan_desc = String::new();
        if report {
            writeln!(plan_desc, "\n============ Build Dataflow ==============").ok();
            writeln!(plan_desc, "Peers:\t{}", self.worker_id.peers).ok();
            writeln!(plan_desc, "{}", "Operators: ").ok();
        }

        let mut builds = self.operators.replace(vec![]);
        builds.sort_by_key(|op| op.index());
        let mut operators = Vec::with_capacity(builds.len());
        for (i, op_b) in builds.drain(..).enumerate() {
            assert_eq!(i, op_b.index());
            let op = op_b.build();
            if report {
                writeln!(plan_desc, "\t{}\t{}", op.meta.index, op.meta.name).ok();
            }
            operators.push(Some(OpRuntime::new(op)));
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
            let mut op_names = Vec::with_capacity(operators.len());
            for op in operators.iter() {
                let name = op.as_ref().unwrap().meta.name.clone();
                op_names.push(name);
            }
            let graph = DotGraph::new(
                self.config.job_name.clone(),
                self.config.job_id,
                op_names,
                edges.clone(),
            );
            if let Ok(mut f) =
                File::create(format!("{}_{}.dot", self.config.job_name, self.config.job_id))
            {
                if let Err(e) = dot::render(&graph, &mut f) {
                    error!("create dot file failure: {}", e);
                }
            }
        }
        let graph = LogicalGraph::new(edges, operators.len());
        Ok(Dataflow { worker_id: self.worker_id, graph, operators })
    }
}

impl Clone for DataflowBuilder {
    fn clone(&self) -> Self {
        DataflowBuilder {
            worker_id: self.worker_id,
            config: self.config.clone(),
            operators: self.operators.clone(),
            edges: self.edges.clone(),
            event_bus: self.event_bus.clone(),
            ch_index: self.ch_index.clone(),
        }
    }
}

#[derive(Copy, Clone)]
pub struct OperatorIndex {
    index: usize,
}

pub struct OperatorRef<'a> {
    index: usize,
    borrow: RefMut<'a, Vec<OperatorBuilder>>,
}

impl<'a> OperatorRef<'a> {
    pub(crate) fn new(index: usize, borrow: RefMut<'a, Vec<OperatorBuilder>>) -> Self {
        OperatorRef { index, borrow }
    }

    pub fn get_index(&self) -> OperatorIndex {
        OperatorIndex { index: self.index }
    }
}

impl<'a> std::ops::Deref for OperatorRef<'a> {
    type Target = OperatorBuilder;

    fn deref(&self) -> &Self::Target {
        &self.borrow[self.index]
    }
}

impl<'a> std::ops::DerefMut for OperatorRef<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.borrow[self.index]
    }
}

pub(crate) struct Dataflow {
    pub worker_id: WorkerId,
    pub operators: Vec<Option<OpRuntime>>,
    pub graph: LogicalGraph,
}

impl Dataflow {
    #[allow(dead_code)]
    pub fn check_active(&self) -> bool {
        self.operators.iter().any(|op| op.as_ref().map(|op| op.has_actives()).unwrap_or(false))
    }

    pub fn check_finish(&mut self) -> bool {
        for op in self.operators.iter_mut() {
            if let Some(op) = op {
                if op.is_finished() {
                    op.close();
                } else {
                    trace_worker!("operator {:?} is unfinished;", op.meta);
                    if cfg!(debug_assertions) && log_enabled!(log::Level::Trace) {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                    return false;
                }
            }
        }
        true
    }
}
