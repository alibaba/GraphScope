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

use crate::api::function::FnResult;
use crate::api::meta::OperatorInfo;
use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::api::{Map, Unary};
use crate::communication::channel::ChannelKind;
use crate::communication::output::OutputBuilderImpl;
use crate::communication::Channel;
use crate::dataflow::{DataflowBuilder, OperatorRef};
use crate::errors::BuildJobError;
use crate::graph::{Edge, Port};
use crate::macros::route::*;
use crate::operator::{NotifiableOperator, OperatorCore};
use crate::{Data, JobConf};
use pegasus_common::codec::ShadeCodec;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[must_use = "this `Stream` may be consumed"]
pub struct Stream<D: Data> {
    /// the level of scope of data that through this stream;
    pub scope_level: usize,
    has_cycles: Vec<Arc<AtomicBool>>,
    batch_size: Option<usize>,
    delta: MergedScopeDelta,
    output: OutputBuilderImpl<D>,
    ch_kind: ChannelKind<D>,
    dfb: DataflowBuilder,
}

impl<D: Data> Stream<D> {
    pub(crate) fn create(output: OutputBuilderImpl<D>, dfb: &DataflowBuilder) -> Self {
        Stream {
            scope_level: 0,
            has_cycles: vec![Arc::new(Default::default())],
            batch_size: None,
            delta: MergedScopeDelta::new(0),
            output,
            ch_kind: ChannelKind::Pipeline,
            dfb: dfb.clone(),
        }
    }

    pub fn copied(self) -> Result<(Stream<D>, Stream<D>), BuildJobError> {
        if self.ch_kind.is_local() {
            let copy = Stream {
                scope_level: self.scope_level,
                has_cycles: self.has_cycles.clone(),
                batch_size: None,
                delta: self.delta.clone(),
                output: self.output.copy_data(),
                ch_kind: ChannelKind::Pipeline,
                dfb: self.dfb.clone(),
            };
            Ok((self, copy))
        } else {
            let shuffled = self.unary("shuffle_clone", |_| {
                |input, output| {
                    input.for_each_batch(|dataset| {
                        output
                            .new_session(&dataset.tag)?
                            .forward_batch(dataset)?;
                        Ok(())
                    })
                }
            })?;

            let copy = Stream {
                scope_level: shuffled.scope_level,
                has_cycles: shuffled.has_cycles.clone(),
                batch_size: None,
                delta: shuffled.delta.clone(),
                output: shuffled.output.copy_data(),
                ch_kind: ChannelKind::Pipeline,
                dfb: shuffled.dfb.clone(),
            };
            Ok((shuffled, copy))
        }
    }

    pub fn get_conf(&self) -> Arc<JobConf> {
        self.dfb.config.clone()
    }

    pub fn aggregate(mut self) -> Stream<D> {
        if self.scope_level == 0 {
            self = self.aggregate_to(0);
        } else {
            self.ch_kind = ChannelKind::ShuffleScope;
        }
        self
    }

    pub fn repartition<F>(mut self, route: F) -> Stream<D>
    where
        F: Fn(&D) -> FnResult<u64> + Send + 'static,
    {
        self.ch_kind = ChannelKind::Shuffle(box_route!(route));
        self
    }

    pub fn broadcast(mut self) -> Stream<D> {
        self.ch_kind = ChannelKind::Broadcast;
        self
    }

    fn aggregate_to(mut self, target: u32) -> Stream<D> {
        self.ch_kind = ChannelKind::Aggregate(target);
        self
    }

    pub fn set_batch_size(&mut self, batch_size: u32) {
        self.batch_size = Some(batch_size as usize)
    }

    pub(crate) fn port(&self) -> Port {
        self.output.port
    }

    pub fn add_operator<O, F>(&mut self, name: &str, builder: F) -> Result<OperatorRef, BuildJobError>
    where
        O: OperatorCore,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let mut op = self
            .dfb
            .add_operator(name, self.scope_level, builder);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        Ok(op)
    }

    pub fn add_notify_operator<O, F>(
        &mut self, name: &str, builder: F,
    ) -> Result<OperatorRef, BuildJobError>
    where
        O: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let mut op = self
            .dfb
            .add_notify_operator(name, self.scope_level, builder);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        Ok(op)
    }

    pub fn and_then<F, O, T>(mut self, name: &str, op_builder: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op = self.add_operator(name, op_builder)?;
        let output = op.new_output::<O>();
        let dfb = self.dfb.clone();
        Ok(Stream {
            scope_level: self.scope_level,
            has_cycles: self.has_cycles.clone(),
            batch_size: None,
            delta: MergedScopeDelta::new(self.scope_level),
            output,
            ch_kind: ChannelKind::Pipeline,
            dfb,
        })
    }

    pub fn sink_by<F, T>(mut self, name: &str, op_builder: F) -> Result<(), BuildJobError>
    where
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op_ref = self.add_operator(name, op_builder)?;
        self.dfb.add_sink(op_ref.get_index());
        Ok(())
    }

    pub fn union_branches<R, O, F, T>(
        mut self, name: &str, mut other: Stream<R>, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        if self.scope_level != other.scope_level {
            BuildJobError::unsupported(format!(
                "Build {} operator failure, can't union streams with different scope level;",
                name
            ))
        } else {
            let mut op = self.add_operator(name, op_builder)?;
            let edge = other.connect(&mut op)?;
            self.dfb.add_edge(edge);
            let dfb = self.dfb.clone();
            let output = op.new_output::<O>();
            Ok(Stream {
                scope_level: self.scope_level,
                has_cycles: self.has_cycles.clone(),
                batch_size: None,
                delta: MergedScopeDelta::new(self.scope_level),
                output,
                ch_kind: ChannelKind::Pipeline,
                dfb,
            })
        }
    }

    pub fn make_branches<F, L, R, T>(
        mut self, name: &str, op_builder: F,
    ) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op = self.add_operator(name, op_builder)?;
        let left = op.new_output::<L>();
        let right = op.new_output::<R>();
        let dfb = self.dfb.clone();
        let left = Stream {
            scope_level: self.scope_level,
            has_cycles: self.has_cycles.clone(),
            batch_size: None,
            delta: MergedScopeDelta::new(self.scope_level),
            output: left,
            ch_kind: ChannelKind::Pipeline,
            dfb: dfb.clone(),
        };
        let right = Stream {
            scope_level: self.scope_level,
            has_cycles: self.has_cycles.clone(),
            batch_size: None,
            delta: MergedScopeDelta::new(self.scope_level),
            output: right,
            ch_kind: ChannelKind::Pipeline,
            dfb,
        };
        Ok((left, right))
    }

    pub fn make_branches_notify<F, L, R, T>(
        mut self, name: &str, op_builder: F,
    ) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        T: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op = self.add_notify_operator(name, op_builder)?;
        let left = op.new_output::<L>();
        let right = op.new_output::<R>();
        let scope_level = self.scope_level;
        let dfb = self.dfb.clone();
        let left = Stream {
            scope_level,
            has_cycles: self.has_cycles.clone(),
            batch_size: None,
            delta: MergedScopeDelta::new(scope_level),
            output: left,
            ch_kind: ChannelKind::Pipeline,
            dfb: dfb.clone(),
        };
        let right = Stream {
            scope_level,
            has_cycles: self.has_cycles.clone(),
            batch_size: None,
            delta: MergedScopeDelta::new(scope_level),
            output: right,
            ch_kind: ChannelKind::Pipeline,
            dfb,
        };
        Ok((left, right))
    }

    pub fn union_branches_notify<R, O, F, T>(
        mut self, name: &str, mut other: Stream<R>, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        T: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> T,
    {
        if self.scope_level != other.scope_level {
            BuildJobError::unsupported(format!(
                "Build {} operator failure, can't union streams with different scope level;",
                name
            ))
        } else {
            let mut op = self.add_notify_operator(name, op_builder)?;
            let edge = other.connect(&mut op)?;
            self.dfb.add_edge(edge);
            let dfb = self.dfb.clone();
            let scope_level = self.scope_level;
            let output = op.new_output::<O>();
            Ok(Stream {
                scope_level,
                has_cycles: self.has_cycles.clone(),
                batch_size: None,
                delta: MergedScopeDelta::new(scope_level),
                output,
                ch_kind: ChannelKind::Pipeline,
                dfb,
            })
        }
    }

    pub fn feedback_to(mut self, op_index: usize) -> Result<(), BuildJobError> {
        if self.scope_level == 0 {
            Err("can't create feedback stream on root scope;")?;
        }
        self.delta.add_delta(ScopeDelta::ToSibling(1));
        let mut op = self.dfb.get_operator(op_index);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        let cyclic = self.has_cycles.last().expect("unreachable");
        cyclic.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn enter(mut self) -> Result<Self, BuildJobError> {
        self.delta.add_delta(ScopeDelta::ToChild(1));
        self.scope_level += 1;
        self.output.scope_level += 1;
        self.has_cycles
            .push(Arc::new(Default::default()));
        Ok(self)
    }

    pub fn leave(mut self) -> Result<Self, BuildJobError> {
        self.delta.add_delta(ScopeDelta::ToParent(1));
        self.scope_level -= 1;
        self.output.scope_level -= 1;
        self.has_cycles.pop();
        Ok(self)
    }

    fn connect(&mut self, op: &OperatorRef) -> Result<Edge, BuildJobError> {
        let target = op.next_input_port();
        let kind = std::mem::replace(&mut self.ch_kind, ChannelKind::Pipeline);
        let mut ch = Channel::new(kind);
        if let Some(batch_size) = self.batch_size {
            ch.set_batch_size(batch_size as usize);
        }
        let port = self.output.port;
        let scope_level = self.scope_level as usize;
        let has_cycles = self.has_cycles[scope_level].clone();
        let channel = ch.materialize(port, target, scope_level, has_cycles, &self.dfb)?;
        let (ch_info, push, pull, notify) = channel.take();
        let delta = std::mem::replace(&mut self.delta, Default::default());
        self.output
            .set_push(ch_info, delta.clone(), push);
        op.add_input(ch_info, pull, notify, &self.dfb.event_emitter, delta);
        let edge = Edge::new(ch_info);
        Ok(edge)
    }
}

pub struct Single<T>(pub T);

impl<T> ShadeCodec for Single<T> {}

impl<T> Clone for Single<T> {
    fn clone(&self) -> Self {
        unimplemented!("clone not need;")
    }
}

impl<T: Debug> Debug for Single<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub struct SingleItem<D: Send + Debug + 'static> {
    pub(crate) inner: Stream<Single<D>>,
}

impl<D: Debug + Send + 'static> SingleItem<D> {
    pub fn new(stream: Stream<Single<D>>) -> Self {
        SingleItem { inner: stream }
    }

    pub fn unfold<Iter, F>(self, f: F) -> Result<Stream<Iter::Item>, BuildJobError>
    where
        Iter: Iterator + Send + 'static,
        Iter::Item: Data,
        F: Fn(D) -> FnResult<Iter> + Send + 'static,
    {
        self.inner.flat_map(move |single| f(single.0))
    }

    pub fn map<T, F>(self, f: F) -> Result<SingleItem<T>, BuildJobError>
    where
        T: Debug + Send + 'static,
        F: Fn(D) -> FnResult<T> + Send + 'static,
    {
        let stream = self
            .inner
            .map(move |single| f(single.0).map(|o| Single(o)))?;
        Ok(SingleItem::new(stream))
    }
}

impl<D: Data> SingleItem<D> {
    pub fn into_stream(self) -> Result<Stream<D>, BuildJobError> {
        self.inner.map(|single| Ok(single.0))
    }
}
