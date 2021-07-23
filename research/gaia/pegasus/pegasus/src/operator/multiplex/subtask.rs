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

use crate::api::function::*;
use crate::api::meta::OperatorMeta;
use crate::api::notify::Notification;
use crate::api::state::StateMap;
use crate::api::{
    Binary, BinaryInput, BinaryNotification, BinaryNotify, Exchange, LeaveScope, Multiplexing,
    ResultSet, SubTask, SubtaskResult,
};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputProxy};
use crate::communication::{Output, Pipeline};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, JobConf, Tag};
use std::collections::HashMap;

impl<D: Data> SubTask<D> for Stream<D> {
    fn fork_subtask<F, T>(&self, func: F) -> Result<Stream<SubtaskResult<T>>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<Stream<T>, BuildJobError> + Send,
    {
        let m = self.scope_by_size(1)?;
        let sub = func(m)?;
        sub.concat("subtask_sink", Pipeline, |meta| Box::new(SubtaskSink::<T>::new(meta)))?
            .owned_leave()?
            .exchange(route!(|item: &SubtaskResult<T>| item.seq as u64))
    }

    fn fork_detached_subtask<F, T>(
        &self, _conf: JobConf, _func: F,
    ) -> Result<Stream<SubtaskResult<T>>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<Stream<T>, BuildJobError> + Send,
    {
        unimplemented!()
    }

    fn join_subtask<T, R, F>(
        &self, subtask: Stream<SubtaskResult<T>>, func: F,
    ) -> Result<Stream<R>, BuildJobError>
    where
        T: Data,
        R: Data,
        F: Fn(&D, T) -> Option<R> + Send + 'static,
    {
        self.binary_notify("join_subtask", &subtask, Pipeline, Pipeline, |meta| {
            SubtaskJoin::new(meta, func)
        })
    }
}

struct SubtaskSink<D: Data> {
    scope_depth: usize,
    state: StateMap<()>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> SubtaskSink<D> {
    fn new(meta: &OperatorMeta) -> Self {
        SubtaskSink {
            scope_depth: meta.scope_depth,
            state: StateMap::new(meta),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D: Data> OperatorCore for SubtaskSink<D> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        let mut output = new_output_session::<SubtaskResult<D>>(&outputs[0], tag);
        self.state.entry(tag).or_insert(());
        let seq = tag.current_uncheck();
        input.for_each_batch(|dataset| {
            if !dataset.is_empty() {
                let data = std::mem::replace(dataset.data(), vec![]);
                output.give(SubtaskResult::new(seq, ResultSet::Data(data)))?;
            }
            Ok(())
        })?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        if n.tag.len() == self.scope_depth {
            self.state.insert(n.tag.clone(), ());
        }
        self.state.notify(&n);
        for (tag, _) in self.state.extract_notified().drain(..) {
            let seq = tag.current_uncheck();
            let data = SubtaskResult::new(seq, ResultSet::End);
            new_output_session::<SubtaskResult<D>>(&outputs[0], &tag).give(data)?;
        }
        Ok(())
    }
}

struct SubtaskJoin<L, R, O, F> {
    peers: u32,
    parent_data: HashMap<Tag, Vec<Option<L>>>,
    func: F,
    _ph: std::marker::PhantomData<(R, O)>,
}

impl<L, R, O, F> SubtaskJoin<L, R, O, F> {
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        SubtaskJoin {
            peers: meta.worker_id.peers,
            parent_data: HashMap::new(),
            func,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<L, R, O, F> BinaryNotify<L, SubtaskResult<R>, O> for SubtaskJoin<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: Fn(&L, R) -> Option<O> + Send + 'static,
{
    type NotifyResult = Vec<O>;

    fn on_receive(
        &mut self, input: &mut BinaryInput<L, SubtaskResult<R>>, output: &mut Output<O>,
    ) -> Result<(), JobExecError> {
        input.subscribe_left_notify();
        input.subscribe_right_notify();

        let mut p = std::mem::replace(&mut self.parent_data, HashMap::new());
        let parent_data = p.entry(input.tag().clone()).or_insert_with(|| vec![]);

        input.left_for_each(|dataset| {
            for item in dataset.drain(..) {
                parent_data.push(Some(item));
            }
            Ok(())
        })?;

        input.right_for_each(|dataset| {
            for data in dataset.drain(..) {
                let offset = (data.seq / self.peers) as usize;
                if let Some(parent) = parent_data.get_mut(offset) {
                    if let Some(p) = parent.take() {
                        match data.take() {
                            ResultSet::Data(s_data) => {
                                for r in s_data {
                                    if let Some(join) = (self.func)(&p, r) {
                                        output.give(join)?;
                                    }
                                }
                                parent.replace(p);
                            }
                            ResultSet::End => (),
                        }
                    } else {
                        Err(format!("join subtask={} error: internal;", data.seq))?;
                    }
                } else {
                    Err(format!("join subtask={} error: parent lost;", data.seq))?;
                }
            }
            Ok(())
        })?;
        self.parent_data = p;
        Ok(())
    }

    fn on_notify(&mut self, n: BinaryNotification) -> Self::NotifyResult {
        match n {
            BinaryNotification::Left(t) => {
                self.parent_data.get_mut(&t).map(|p| p.shrink_to_fit());
            }
            BinaryNotification::Right(t) => {
                self.parent_data.remove(&t);
            }
        }
        vec![]
    }
}
