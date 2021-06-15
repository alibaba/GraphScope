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

use crate::api::meta::{OperatorKind, OperatorMeta};
use crate::api::notify::Notification;
use crate::api::state::StateMap;
use crate::api::{ResultSet, Sink};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::OutputProxy;
use crate::communication::Pipeline;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};

pub struct SinkOperator<D, F> {
    scope_depth: usize,
    func: F,
    state: StateMap<()>,
    _ph: std::marker::PhantomData<D>,
}

impl<D, F> SinkOperator<D, F> {
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        SinkOperator {
            scope_depth: meta.scope_depth,
            func,
            state: StateMap::new(meta),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D, F> OperatorCore for SinkOperator<D, F>
where
    D: Data,
    F: FnMut(&Tag, ResultSet<D>) + Send,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], _: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        self.state.entry(tag).or_insert(());
        input.for_each_batch(|dataset| {
            if !dataset.is_empty() {
                let data = std::mem::replace(dataset.data(), vec![]);
                (self.func)(tag, ResultSet::Data(data));
            }
            Ok(())
        })?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, _: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        if n.tag.len() == self.scope_depth {
            self.state.insert(n.tag.clone(), ());
        }
        self.state.notify(&n);
        for (t, _) in self.state.extract_notified().drain(..) {
            (self.func)(&t, ResultSet::End)
        }
        Ok(())
    }
}

impl<D: Data> Sink<D> for Stream<D> {
    fn sink_by<B, F>(&self, construct: B) -> Result<(), BuildJobError>
    where
        B: FnOnce(&OperatorMeta) -> F,
        F: FnMut(&Tag, ResultSet<D>) + Send + 'static,
    {
        self.sink_stream("sink", Pipeline, |meta| {
            meta.set_kind(OperatorKind::Sink);
            meta.enable_notify();
            let func = construct(meta);
            Box::new(SinkOperator::new(meta, func))
        })?;
        Ok(())
    }
}
