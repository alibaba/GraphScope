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
use std::cell::{Cell, RefCell};
use super::*;
use crate::channel::output::OutputBuilder;
use crate::channel::{Push, Pull, Edge, DataSet};
use crate::channel::input::InputHandle;
use crate::channel::eventio::EventsBuffer;
use crate::operator::{OperatorInfo, OperatorBuilder};
use crate::communication::Communicate;
use crate::common::Port;
use crate::allocate::{ThreadPush, ThreadPull};

pub trait DataflowBuilder: Clone {

    fn batch_size(&self) -> usize;

    fn worker_id(&self) -> WorkerId;

    fn peers(&self) -> usize;

    fn parallel(&self) -> ParallelConfig;

    fn allocate_channel_index(&self) -> ChannelId;

    fn allocate_channel<T: Data>(&self, id: ChannelId) -> (Vec<Box<dyn Push<DataSet<T>>>>, Box<dyn Pull<DataSet<T>>>);

    fn pipeline<T: Data>(&self, id: ChannelId) -> (ThreadPush<DataSet<T>>, ThreadPull<DataSet<T>>);

    fn allocate_operator_index(&self) -> usize;

    fn add_operator_builder(&self, operator: OperatorBuilder);

    fn add_edge(&self, edge: Edge);

    fn get_event_buffer(&self) -> &EventsBuffer;

    fn new_stream_meta<D: Data>(&self, port: Port) -> StreamMeta<D>;

    fn fetch_stream_meta<D: Data>(&self, port: &Port) -> Option<StreamMeta<D>>;
}

pub struct StreamMeta<D: Data> {
    pub(crate) worker: WorkerId,
    pub(crate) source: Port,
    output: OutputBuilder<D>,
    local: Rc<Cell<bool>>,
    scopes: Rc<RefCell<usize>>,
}

impl<D: Data> AsAny for StreamMeta<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> StreamMeta<D> {
    pub fn new(worker: WorkerId, source: Port, output: OutputBuilder<D>) -> Self {
        StreamMeta {
            worker,
            source,
            output,
            local: Rc::new(Cell::new(false)),
            scopes: Rc::new(RefCell::new(1)),
        }
    }

    pub fn downcast(origin: &Box<dyn AsAny>) -> &Self {
        origin.as_any_ref().downcast_ref::<Self>().expect("downcast failure")
    }

    #[inline]
    pub fn output(&self) -> &OutputBuilder<D> {
        &self.output
    }

    #[inline]
    pub fn is_local(&self) -> bool {
        self.local.get()
    }

    #[inline]
    pub fn scopes(&self) -> usize {
        *self.scopes.borrow()
    }

    #[inline]
    pub fn set_local(&self, local: bool) {
        if !local {
            self.local.replace(local);
        }
    }

    #[inline]
    pub fn into_scope(&self) {
        *self.scopes.borrow_mut() += 1
    }

    #[inline]
    pub fn out_scope(&self) {
        *self.scopes.borrow_mut() -= 1
    }
}

/// The clone of the meta share the same content with the origin;
impl<D: Data> Clone for StreamMeta<D> {
    fn clone(&self) -> Self {
        StreamMeta {
            worker: self.worker,
            source: self.source,
            output: self.output.clone(),
            local: self.local.clone(),
            scopes: self.scopes.clone()
        }
    }
}

/// The `Stream` describes an unbound data stream abstraction;
/// It will produces data with type `D` continuously;
pub struct Stream< D: Data, B> {
    pub(crate) builder: B,
    pub(crate) meta: StreamMeta<D>,
}

impl<D: Data, B: DataflowBuilder> Stream<D, B> {

    pub fn new(source: Port, builder: &B) -> Self {
        let meta = builder.new_stream_meta(source);
        Stream {
            builder: builder.clone(),
            meta,
        }
    }

    pub fn with(meta: StreamMeta<D>, builder: &B) -> Self {
        Stream {
            builder: builder.clone(),
            meta
        }
    }

    pub fn from<R: Data>(source: Port, up: &Stream<R, B>) -> Self {
        let meta = up.builder.new_stream_meta(source);
        meta.local.replace(up.is_local());
        meta.scopes.replace(up.scopes());
        Stream {
            meta,
            builder: up.builder.clone()
        }
    }

    #[inline]
    pub fn get_output(&self) -> &OutputBuilder<D> {
        &self.meta.output()
    }

    #[inline]
    pub fn port(&self) -> Port {
        self.meta.source
    }

    #[inline]
    pub fn is_local(&self) -> bool {
        self.meta.is_local()
    }

    #[inline]
    pub fn scopes(&self) -> usize {
        self.meta.scopes()
    }

    #[inline]
    pub fn into_scope(&self) {
       self.meta.into_scope()
    }

    #[inline]
    pub fn out_scope(&self) {
        self.meta.out_scope()
    }

    #[inline]
    pub fn set_local(&self, local: bool) {
        self.meta.set_local(local)
    }

    #[inline]
    pub fn get_event_buffer(&self) -> &EventsBuffer {
        self.builder.get_event_buffer()
    }

    #[inline]
    pub fn allocate_operator_info(&self, name: &str) -> OperatorInfo {
        let op_index = self.builder.allocate_operator_index();
        OperatorInfo::new(self.meta.worker, name, op_index, self.builder.peers(), self.scopes())
    }

    #[inline]
    pub fn add_operator(&self, operator: OperatorBuilder) {
        self.builder.add_operator_builder(operator);
    }

    #[inline]
    pub fn worker_id(&self) -> WorkerId {
        self.meta.worker
    }

    #[inline]
    pub fn connect<C: Communicate<D>>(&self, target: Port, channel: C) -> InputHandle<D>
    {
        let ch_id = self.builder.allocate_channel_index();
        let local = channel.is_local();
        let mode = channel.get_comm_type();
        let (push, pull) = channel.connect(&self.builder, ch_id);
        self.meta.output().add_push(ch_id, local, push);

        let e = Edge::new(ch_id, self.meta.source, target, local,
                          self.scopes(), mode);
        self.builder.add_edge(e);

        let events_buf = self.builder.get_event_buffer();
        let batch = self.builder.batch_size();
        InputHandle::new(self.meta.worker, ch_id, pull, batch, events_buf)
    }
}
