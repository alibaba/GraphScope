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

use crate::api::function::{MultiRouteFunction, RouteFunction};
use crate::channel_id::{ChannelId, SubChannelId};
use crate::communication::decorator::{count::CountedPush, exchange::ExchangePush, DataPush};
use crate::data::{Data, DataSet};
use crate::data_plane::{GeneralPull, GeneralPush, Push};
use crate::dataflow::DataflowBuilder;
use crate::errors::BuildJobError;
use crate::graph::Edge;

enum ChannelKind<T: Data> {
    Pipeline,
    Shuffle(Box<dyn RouteFunction<T>>),
    Broadcast(Option<Box<dyn MultiRouteFunction<T>>>),
    Aggregate(u64),
}

pub struct Channel<T: Data> {
    kind: ChannelKind<T>,
    allow_cancel: bool,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ChannelMeta {
    pub id: SubChannelId,
    pub is_local: bool,
    pub push_peers: usize,
    pub forbid_cancel: bool,
    pub is_aggregate: bool,
}

impl Into<Edge> for ChannelMeta {
    fn into(self) -> Edge {
        Edge {
            id: self.id.index() as usize,
            source: Default::default(),
            target: Default::default(),
            scope_depth: 0,
            src_peers: self.push_peers,
            dst_peers: if self.is_aggregate { 1 } else { self.push_peers },
            is_local: self.is_local,
        }
    }
}

pub(crate) struct MaterializedChannel<T: Data> {
    pub meta: ChannelMeta,
    push: DataPush<T>,
    pull: GeneralPull<DataSet<T>>,
}

impl<T: Data> MaterializedChannel<T> {
    pub fn take(self) -> (DataPush<T>, GeneralPull<DataSet<T>>) {
        (self.push, self.pull)
    }
}

impl<T: Data> Channel<T> {
    fn new(kind: ChannelKind<T>, allow_cancel: bool) -> Self {
        Channel { kind, allow_cancel }
    }

    pub fn forbid_cancel(&mut self) {
        self.allow_cancel = false;
    }

    pub(crate) fn materialize(
        self, dfb: &DataflowBuilder,
    ) -> Result<MaterializedChannel<T>, BuildJobError> {
        let index = dfb.next_channel_index();
        let ch_id =
            (ChannelId { job_seq: dfb.config.job_id as u64, index }, dfb.worker_id.index).into();
        match self.kind {
            ChannelKind::Pipeline => {
                let (tx, rx) = crate::data_plane::pipeline::<DataSet<T>>(ch_id);
                let meta = ChannelMeta {
                    id: ch_id,
                    is_local: true,
                    push_peers: 1,
                    forbid_cancel: !self.allow_cancel,
                    is_aggregate: false,
                };
                let push = CountedPush::new(
                    ch_id,
                    dfb.worker_id,
                    dfb.worker_id,
                    tx.into(),
                    &dfb.event_bus,
                );
                Ok(MaterializedChannel { meta, push: DataPush::Count(push), pull: rx.into() })
            }
            ChannelKind::Shuffle(r) => {
                let (raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let meta = ChannelMeta {
                    id: ch_id,
                    is_local: false,
                    push_peers: raw.len(),
                    forbid_cancel: !self.allow_cancel,
                    is_aggregate: false,
                };
                let pushes = decorate_to_count(ch_id, raw, &dfb);
                let push =
                    ExchangePush::exchange_to_one(dfb.config.batch_size as usize, ch_id, pushes, r);
                Ok(MaterializedChannel { meta, push: DataPush::Exchange(push), pull: pull.into() })
            }
            ChannelKind::Broadcast(r) => {
                let (raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let meta = ChannelMeta {
                    id: ch_id,
                    is_local: false,
                    push_peers: raw.len(),
                    forbid_cancel: !self.allow_cancel,
                    is_aggregate: false,
                };
                let pushes = decorate_to_count(ch_id, raw, &dfb);
                let push = if let Some(r) = r {
                    ExchangePush::exchange_to_some(dfb.config.batch_size as usize, ch_id, pushes, r)
                } else {
                    ExchangePush::broadcast(dfb.config.batch_size as usize, ch_id, pushes)
                };
                Ok(MaterializedChannel { meta, push: DataPush::Exchange(push), pull: pull.into() })
            }
            ChannelKind::Aggregate(id) => {
                let (mut raw, pull) =
                    super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let meta = ChannelMeta {
                    id: ch_id,
                    is_local: false,
                    push_peers: raw.len(),
                    forbid_cancel: !self.allow_cancel,
                    is_aggregate: true,
                };
                let push = raw.swap_remove(id as usize);
                let mut target = dfb.worker_id;
                target.index = id as u32;
                let push = CountedPush::new(ch_id, dfb.worker_id, target, push, &dfb.event_bus);
                for mut unused in raw {
                    unused.close().ok();
                }
                Ok(MaterializedChannel { meta, push: DataPush::Count(push), pull: pull.into() })
            }
        }
    }
}

#[inline]
fn decorate_to_count<T: Data>(
    ch_id: SubChannelId, raw: Vec<GeneralPush<DataSet<T>>>, dfb: &DataflowBuilder,
) -> Vec<CountedPush<T>> {
    let mut counts = Vec::with_capacity(raw.len());
    let source = dfb.worker_id;

    for (idx, p) in raw.into_iter().enumerate() {
        let mut target = source;
        target.index = idx as u32;
        let push = CountedPush::new(ch_id, source, target, p, &dfb.event_bus);
        counts.push(push);
    }
    counts
}

pub struct Pipeline;

impl<T: Data> From<Pipeline> for Channel<T> {
    fn from(_: Pipeline) -> Self {
        Channel::new(ChannelKind::Pipeline, true)
    }
}

impl<T: Data, R: RouteFunction<T>> From<Box<R>> for Channel<T> {
    fn from(route: Box<R>) -> Self {
        let kind = ChannelKind::Shuffle(route as Box<dyn RouteFunction<T>>);
        Channel::new(kind, true)
    }
}

impl<T: Data> From<Box<dyn RouteFunction<T>>> for Channel<T> {
    fn from(route: Box<dyn RouteFunction<T>>) -> Self {
        let kind = ChannelKind::Shuffle(route);
        Channel::new(kind, true)
    }
}

pub struct Broadcast;

impl<T: Data> From<Broadcast> for Channel<T> {
    fn from(_: Broadcast) -> Self {
        Channel::new(ChannelKind::Broadcast(None), true)
    }
}

impl<T: Data> From<Box<dyn MultiRouteFunction<T>>> for Channel<T> {
    fn from(route: Box<dyn MultiRouteFunction<T>>) -> Self {
        let kind = ChannelKind::Broadcast(Some(route));
        Channel::new(kind, true)
    }
}

pub struct Aggregate(pub u64);

impl<T: Data> From<Aggregate> for Channel<T> {
    fn from(a: Aggregate) -> Self {
        let kind = ChannelKind::Aggregate(a.0);
        Channel::new(kind, true)
    }
}
