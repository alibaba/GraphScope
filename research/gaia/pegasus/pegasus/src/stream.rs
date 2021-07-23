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
use crate::communication::output::{OutputBuilderImpl, OutputEntry};
use crate::communication::Channel;
use crate::dataflow::{DataflowBuilder, OperatorIndex, OperatorRef};
use crate::errors::BuildJobError;
use crate::graph::{Edge, Port};
use crate::operator::{OperatorBuilder, OperatorCore};
use crate::Data;

pub struct Stream<D: Data> {
    pub(crate) source: Port,
    pub(crate) scope_depth: usize,
    pub(crate) scope_order: Vec<ScopePrior>,
    outputs: OutputBuilderImpl<D>,
    dfb: DataflowBuilder,
}

impl<D: Data> Stream<D> {
    pub fn new(outputs: OutputBuilderImpl<D>, dfb: &DataflowBuilder) -> Self {
        Stream {
            source: outputs.port,
            scope_depth: 0,
            scope_order: vec![ScopePrior::None],
            outputs,
            dfb: dfb.clone(),
        }
    }

    pub fn inherit<I: Data>(parent: &Stream<I>, outputs: OutputBuilderImpl<D>) -> Self {
        Stream {
            source: outputs.port,
            scope_depth: parent.scope_depth,
            scope_order: parent.scope_order.clone(),
            outputs,
            dfb: parent.dfb.clone(),
        }
    }

    pub fn peers(&self) -> u32 {
        self.dfb.worker_id.peers
    }

    pub fn index(&self) -> u32 {
        self.dfb.worker_id.index
    }

    pub fn spawn<O: Data>(&self, op: &mut OperatorBuilder) -> Stream<O> {
        let outputs = op.new_output::<O>();
        Stream::inherit(self, outputs)
    }

    pub fn outputs(&self) -> &OutputBuilderImpl<D> {
        &self.outputs
    }

    pub fn add_operator<C, F>(
        &self, name: &str, channel: C, op_builder: F,
    ) -> Result<OperatorRef, BuildJobError>
    where
        C: Into<Channel<D>>,
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        let mut op = self.dfb.construct_operator(
            name,
            self.scope_depth,
            self.scope_order().clone(),
            op_builder,
        );
        self.connect(&mut op, channel.into())?;
        Ok(op)
    }

    pub fn concat<F, C, O>(
        &self, name: &str, channel: C, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<D>>,
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        let mut op = self.add_operator(name, channel, op_builder)?;
        let output = op.new_output::<O>();
        Ok(Stream::inherit(self, output))
    }

    pub fn sink_stream<C, F>(
        &self, name: &str, channel: C, op_builder: F,
    ) -> Result<(), BuildJobError>
    where
        C: Into<Channel<D>>,
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        self.add_operator(name, channel, op_builder)?;
        Ok(())
    }

    pub fn join<CL, CR, R, F, O>(
        &self, name: &str, ch_left: CL, other: &Stream<R>, ch_right: CR, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        CL: Into<Channel<D>>,
        CR: Into<Channel<R>>,
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        if self.scope_depth != other.scope_depth {
            BuildJobError::unsupported(format!("Build {} operator failure, left and right stream are in different scope hierarchy({}/{});",
                        name, self.scope_depth, other.scope_depth))
        } else {
            let mut op = self.add_operator(name, ch_left, op_builder)?;
            other.connect(&mut op, ch_right.into())?;
            let output = op.new_output::<O>();
            Ok(Stream::inherit(self, output))
        }
    }

    pub fn make_branch<C, F, L, R>(
        &self, name: &str, channel: C, op_builder: F,
    ) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        C: Into<Channel<D>>,
        F: FnOnce(&mut OperatorMeta) -> Box<dyn OperatorCore>,
    {
        let mut op = self.add_operator(name, channel, op_builder)?;
        let left = op.new_output::<L>();
        let right = op.new_output::<R>();
        let left = Stream::inherit(self, left);
        let right = Stream::inherit(self, right);
        Ok((left, right))
    }

    pub fn enter_scope(mut self) -> Self {
        self.scope_depth += 1;
        self.scope_order.push(ScopePrior::None);
        self
    }

    pub fn leave_scope(mut self) -> Self {
        self.scope_depth -= 1;
        self.scope_order.pop();
        self
    }

    pub fn connect_to(
        &self, op_index: OperatorIndex, channel: Channel<D>,
    ) -> Result<(), BuildJobError> {
        let channel = channel.materialize(&self.dfb)?;
        let meta = channel.meta;
        let (push, pull) = channel.take();
        let output = OutputEntry::new(meta.id.index(), meta.is_local, push);
        self.outputs.add_push(output);
        let input = crate::communication::input::new_input(
            meta,
            self.scope_depth,
            &self.dfb.event_bus,
            pull,
        );
        let target = self.dfb.get_operator(op_index).add_input(input);
        let mut edge: Edge = meta.into();
        edge.source = self.source;
        edge.target = target;
        edge.scope_depth = self.scope_depth;
        self.dfb.add_edge(edge);
        Ok(())
    }

    fn connect(&self, op: &mut OperatorBuilder, channel: Channel<D>) -> Result<(), BuildJobError> {
        let channel = channel.materialize(&self.dfb)?;
        let meta = channel.meta;
        let (push, pull) = channel.take();
        let output = OutputEntry::new(meta.id.index(), meta.is_local, push);
        self.outputs.add_push(output);
        let input = crate::communication::input::new_input(
            meta,
            self.scope_depth,
            &self.dfb.event_bus,
            pull,
        );
        let target = op.add_input(input);
        let mut edge: Edge = meta.into();
        edge.source = self.source;
        edge.target = target;
        edge.scope_depth = self.scope_depth;
        self.dfb.add_edge(edge);
        Ok(())
    }

    #[inline]
    fn scope_order(&self) -> &ScopePrior {
        &self.scope_order[self.scope_depth]
    }
}
