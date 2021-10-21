//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use crate::allocate::{ThreadPush, ThreadPull, ParallelConfig};
use crate::{WorkerId, ChannelId, Data, AsAny};
use crate::worker::Worker;
use crate::stream::DataflowBuilder;
use crate::channel::{Push, Pull, Edge, DataSet};
use crate::channel::eventio::EventsBuffer;
use crate::channel::output::OutputBuilder;
use crate::operator::{OperatorBuilder, Operator};
use crate::common::Port;
use crate::stream::StreamMeta;


/// Thread local dataflow plan builder;
pub struct DataflowBuilderImpl<'a> {
    pub name: Rc<String>,
    pub(crate) peers: usize,
    pub(crate) batch_size: usize,
    pub(crate) worker_id: WorkerId,
    operator_count: Rc<RefCell<usize>>,
    operators: Rc<RefCell<Vec<OperatorBuilder>>>,
    edge_stash: Rc<RefCell<Vec<Edge>>>,
    channel_count: Rc<RefCell<usize>>,
    worker: &'a Worker,
    events_buf: EventsBuffer,
    streams: Rc<RefCell<HashMap<Port, Box<dyn AsAny>>>>,
}

impl<'a> Clone for DataflowBuilderImpl<'a> {
    fn clone(&self) -> Self {
        DataflowBuilderImpl {
            name: self.name.clone(),
            peers: self.peers,
            batch_size: self.batch_size,
            worker_id: self.worker_id,
            operator_count: self.operator_count.clone(),
            operators: self.operators.clone(),
            edge_stash: self.edge_stash.clone(),
            channel_count: self.channel_count.clone(),
            worker: self.worker,
            events_buf: self.events_buf.clone(),
            streams: self.streams.clone(),
        }
    }
}

impl<'a> DataflowBuilder for DataflowBuilderImpl<'a> {

    #[inline]
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[inline]
    fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    #[inline]
    fn peers(&self) -> usize {
        self.peers
    }

    #[inline]
    fn parallel(&self) -> ParallelConfig {
        self.worker.parallel
    }

    #[inline]
    fn allocate_channel_index(&self) -> ChannelId {
        *self.channel_count.borrow_mut() += 1;
        let index = *self.channel_count.borrow() - 1;
        ChannelId(index)
    }

    #[inline]
    fn allocate_channel<T: Data>(&self, id: ChannelId) -> (Vec<Box<dyn Push<DataSet<T>>>>, Box<dyn Pull<DataSet<T>>>) {
        let size = ::std::mem::size_of::<T>();
        self.worker.allocate::<DataSet<T>>(id.0, size)
    }

    #[inline]
    fn pipeline<T: Data>(&self, id: ChannelId) -> (ThreadPush<DataSet<T>>, ThreadPull<DataSet<T>>) {
        let size = ::std::mem::size_of::<T>();
        self.worker.pipeline(id.0, size)
    }

    #[inline]
    fn allocate_operator_index(&self) -> usize {
        *self.operator_count.borrow_mut() += 1;
        *self.operator_count.borrow() - 1
    }

    #[inline]
    fn add_operator_builder(&self, operator: OperatorBuilder) {
        self.operators.borrow_mut().push(operator);
    }

    #[inline]
    fn add_edge(&self, edge: Edge) {
        self.edge_stash.borrow_mut().push(edge);
    }

    #[inline]
    fn get_event_buffer(&self) -> &EventsBuffer {
        &self.events_buf
    }

    #[inline]
    fn new_stream_meta<D: Data>(&self, port: Port) -> StreamMeta<D> {
        let output = OutputBuilder::new(self.batch_size(), self.worker_id(), port, self.get_event_buffer());
        let meta = StreamMeta::new(self.worker_id(), port, output);
        self.streams.borrow_mut().insert(port, Box::new(meta.clone()) as Box<dyn AsAny>);
        meta
    }

    #[inline]
    fn fetch_stream_meta<D: Data>(&self, port: &Port) -> Option<StreamMeta<D>> {
        let borrow = self.streams.borrow();
        borrow.get(port).map(|m| StreamMeta::<D>::downcast(m).clone())
    }
}

/// Dataflow execution plan consists of operators and channels;
pub struct Dataflow {
    pub(crate) peers: usize,
    pub(crate) name: String,
    pub(crate) worker_id: WorkerId,
    edges: Vec<Edge>,
    operators: Vec<Option<Operator>>,
    events_buf: EventsBuffer,
}

impl<'a> DataflowBuilderImpl<'a> {
    pub fn new(name: &str, batch_size: usize, worker: &'a Worker, events: &EventsBuffer) -> Self {
        let peers = worker.parallel.total_peers();
        DataflowBuilderImpl {
            name: Rc::new(name.to_owned()),
            peers,
            batch_size,
            worker_id: worker.id,
            operator_count: Rc::new(RefCell::new(0)),
            operators: Rc::new(RefCell::new(Vec::new())),
            edge_stash: Rc::new(RefCell::new(Vec::new())),
            channel_count: Rc::new(RefCell::new(1)),
            worker,
            events_buf: events.clone(),
            streams: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    #[inline]
    pub fn build(self, print: bool) -> Result<Dataflow, String> {
        let mut builds = self.operators.replace(Vec::new());
        builds.sort_by(|op1, op2|
            op1.index().cmp(&op2.index()));
        let builds = builds.into_iter()
            .map(|op_b| op_b.build())
            .collect::<Vec<_>>();

        let mut operators = Vec::with_capacity(builds.len());
        let index = self.worker_id.index();
        if print && index == 0 {
            info!("============ Build Dataflow ==============");
            info!("Operators: ");
        }
        for result in builds {
            match result {
                Ok(op) => {
                    let info = op.info();
                    if print && index == 0 {
                        info!("\t{}\t{}", info.index, info.name);
                    }
                    debug_assert_eq!(operators.len(), info.index);
                    operators.push(Some(op));
                },
                Err(msg) => {
                    error!("build operator failure: {}", msg);
                    return Err(msg);
                }
            }
        };

        let edges = self.edge_stash.replace(Vec::new());
        if print && index == 0 {
            info!("Edges: ");
            for e in edges.iter() {
                info!("\t{:?}", e);
            }

            info!("==========================================");
        }

        Ok(Dataflow {
            name: (*self.name).clone(),
            peers: self.peers,
            worker_id: self.worker_id,
            operators,
            edges,
            events_buf: self.events_buf.clone()
        })
    }
}



impl Dataflow {

    #[inline]
    pub fn edges(&self) -> &[Edge] {
        self.edges.as_slice()
    }

    #[inline]
    pub fn get_event_buffer(&self) -> &EventsBuffer {
        &self.events_buf
    }

    #[inline]
    pub fn operators(&mut self) -> &mut [Option<Operator>] {
        self.operators.as_mut_slice()
    }

    #[inline]
    pub fn return_op(&mut self, op: Operator) {
        let index = op.info().index;
        debug_assert!(self.operators[index].is_none());
        self.operators[index].replace(op);
    }

    #[inline]
    pub fn next_actives(&mut self) -> Option<(Operator, bool)> {
        let len = self.operators.len();
        let mut index = 0;
        for op_opt in self.operators.iter_mut() {
            index += 1;
            if let Some(op) = op_opt.take() {
                if op.is_active() {
                    return Some((op, index == len));
                } else {
                    op_opt.replace(op);
                }
            }
        }
        None
    }

    #[inline]
    pub fn check_finished(&self) -> bool {
        self.operators.iter().all(|op| {
            if let Some(op) = op {
                debug!("Job[{}]-Worker[{}] : check job state: '{}' is running...", self.name, self.worker_id, op.info());
                false
            } else { true }
        })
    }

    #[inline]
    pub fn has_active(&self) -> bool {
        self.operators.iter().any(|opt| {
            opt.as_ref().map(|op| op.is_active()).unwrap_or(false)
        })
    }
}

pub struct Dcg {
    vertex_out: HashMap<Port, ChannelId>,
    vertex_in: HashMap<Port, ChannelId>,
    edges: Vec<Edge>,
}

impl Dcg {
    pub fn new() -> Self {
        Dcg {
            vertex_out: HashMap::new(),
            vertex_in: HashMap::new(),
            edges: Vec::new()
        }
    }

    pub fn init(&mut self, edges: &[Edge]) {
        for e in edges {
            self.vertex_out.insert(e.source, e.id);
            self.vertex_in.insert(e.target, e.id);
        }
        self.edges= edges.to_vec();
        self.edges.sort_by(|a, b| a.id.cmp(&b.id));
    }

    pub fn get_out_channel(&self, port: &Port) -> Option<&ChannelId> {
        self.vertex_out.get(port)
    }

    pub fn get_input_channels(&self, port: &Port) -> Option<&ChannelId> {
        self.vertex_in.get(port)
    }

    pub fn get_edge(&self, ch: &ChannelId) -> Option<&Edge> {
        let index = ch.0 - 1;
        if index >= self.edges.len() {
            None
        } else {
            Some(&self.edges[index])
        }
    }

    #[inline]
    pub fn is_last(&self, op: &Operator) -> bool {
        let index = op.info().index;
        (0..op.output_len()).into_iter().map(|i| Port::new(index, i))
            .all(|p| self.get_out_channel(&p).is_none())
    }
}
