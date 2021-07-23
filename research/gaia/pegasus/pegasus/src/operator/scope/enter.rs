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
use crate::api::scope::enter::{CURRENT_SCOPE, EXTRA_COMPLETES};
use crate::api::{EnterScope, ScopeInput, ScopeInputEmitter};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputDelta, OutputProxy};
use crate::communication::Pipeline;
use crate::errors::{BuildJobError, ErrorKind, IOResult, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};
use hibitset::BitSet;
use std::collections::{HashMap, HashSet, VecDeque};

struct DefaultEnterOperator<D> {
    _ph: std::marker::PhantomData<D>,
}

impl<D> DefaultEnterOperator<D> {
    fn new() -> Self {
        DefaultEnterOperator { _ph: std::marker::PhantomData }
    }
}

impl<D: Data> OperatorCore for DefaultEnterOperator<D> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        let mut output = new_output_session::<D>(&outputs[0], tag);
        input.for_each_batch(|dataset| {
            output.forward(dataset)?;
            Ok(())
        })?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let tag = Tag::inherit(&n.tag, 0);
        outputs[0].scope_end(tag);
        Ok(())
    }
}

struct DynEnterScopeOperator<D, F> {
    completes: HashMap<Tag, IdSet>,
    func: Option<F>,
    emits: HashMap<Tag, F>,
    _ph: std::marker::PhantomData<D>,
}

impl<D, F> DynEnterScopeOperator<D, F> {
    pub fn new(func: Option<F>) -> Self {
        DynEnterScopeOperator {
            completes: HashMap::new(),
            func,
            emits: HashMap::new(),
            _ph: std::marker::PhantomData,
        }
    }

    pub fn is_complete(&self, parent: &Tag, child: u32) -> bool {
        self.completes.get(parent).map(|set| set.contains(child)).unwrap_or(false)
    }

    pub fn add_complete(&mut self, parent: &Tag, child: u32) {
        if let Some(set) = self.completes.get_mut(parent) {
            set.insert(child);
        } else {
            let mut set = IdSet::new();
            set.insert(child);
            self.completes.insert(parent.clone(), set);
        }
    }
}

impl<D, F> OperatorCore for DynEnterScopeOperator<D, F>
where
    D: Data,
    F: ScopeInputEmitter<D>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        CURRENT_SCOPE.with(|cur| cur.borrow_mut().replace(tag.clone()));
        let mut input = new_input_session::<D>(&inputs[0], tag);
        if let Some(emitter) = self.func.take() {
            let mut emits = std::mem::replace(&mut self.emits, HashMap::new());
            let tagged_emit = emits.entry(tag.clone()).or_insert_with(|| emitter.clone());
            let is_global = !emitter.has_peers();
            let mut session = AutoRefreshSession::<D>::new(is_global, tag, &outputs[0]);
            input.for_each_batch(|dataset| {
                for data in dataset.drain(..) {
                    if let Some(input) = tagged_emit.get_scope(data) {
                        let is_last = input.is_last;
                        let id = input.id;
                        if !self.is_complete(tag, id) {
                            session.give(input)?;
                        } else {
                            throw_user_error!(ErrorKind::IllegalScopeInput)
                        }
                        if is_last {
                            self.add_complete(tag, id);
                        }
                    }
                }
                Ok(())
            })?;
            self.func = Some(emitter);
            self.emits = emits;
            std::mem::drop(session);
            EXTRA_COMPLETES.with(|cpe| {
                let mut cpe = cpe.borrow_mut();
                for end in cpe.drain(..) {
                    if is_global {
                        outputs[0].global_scope_end(end);
                    } else {
                        outputs[0].scope_end(end);
                    }
                }
            });
        } else {
            let mut session = new_output_session::<D>(&outputs[0], tag);
            input.for_each_batch(|dataset| {
                session.forward(dataset)?;
                Ok(())
            })?;
        }
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, _: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        self.completes.retain(|k, _| !(&n.tag == k || n.tag.is_parent_of(k)));
        Ok(())
    }
}

impl<D: Data> EnterScope<D> for Stream<D> {
    fn enter(&self) -> Result<Stream<D>, BuildJobError> {
        Ok(self
            .concat("enter", Pipeline, |meta| {
                meta.enable_notify();
                meta.set_kind(OperatorKind::Map);
                meta.set_output_delta(OutputDelta::ToChild);
                Box::new(DefaultEnterOperator::<D>::new())
            })?
            .enter_scope())
    }

    fn dyn_enter<B, F>(&self, builder: B) -> Result<Stream<D>, BuildJobError>
    where
        B: FnOnce(&OperatorMeta) -> F,
        F: ScopeInputEmitter<D> + 'static,
    {
        Ok(self
            .concat("enter_dyn", Pipeline, |meta| {
                meta.set_kind(OperatorKind::Map);
                meta.set_output_delta(OutputDelta::ToChild);
                meta.enable_notify();
                let func = builder(meta);
                Box::new(DynEnterScopeOperator::<D, F>::new(Some(func)))
            })?
            .enter_scope())
    }
}

struct AutoRefreshSession<'a, D: Data> {
    global: bool,
    tag: &'a Tag,
    output: &'a Box<dyn OutputProxy>,
    buffer: HashMap<u32, Vec<D>>,
    reused: VecDeque<Vec<D>>,
}

impl<'a, D: Data> AutoRefreshSession<'a, D> {
    pub fn new(global: bool, tag: &'a Tag, output: &'a Box<dyn OutputProxy>) -> Self {
        AutoRefreshSession { global, tag, output, buffer: HashMap::new(), reused: VecDeque::new() }
    }

    pub fn give(&mut self, input: ScopeInput<D>) -> IOResult<()> {
        if input.is_last {
            let mut session = new_output_session(self.output, self.tag);
            session.advance(input.id)?;
            if let Some(mut buffer) = self.buffer.remove(&input.id) {
                buffer.push(input.take());
                session.give_batch(&mut buffer)?;
                if buffer.capacity() > 0 {
                    self.reused.push_back(buffer);
                }
            } else {
                session.give(input.take())?;
            }
            let tag = session.tag.clone();
            std::mem::drop(session);
            if self.global {
                self.output.global_scope_end(tag);
            } else {
                self.output.scope_end(tag);
            }
            Ok(())
        } else {
            let id = input.id;
            if let Some(mut buffer) = self.buffer.remove(&id) {
                buffer.push(input.take());
                if buffer.len() >= self.output.batch_size() {
                    self.flush(id, &mut buffer)?;
                    if buffer.capacity() > 0 {
                        self.reused.push_back(buffer);
                    }
                } else {
                    self.buffer.insert(id, buffer);
                }
            } else {
                let mut buffer = self.reused.pop_front().unwrap_or_else(|| Vec::new());
                let id = input.id;
                buffer.push(input.take());
                self.buffer.insert(id, buffer);
            }
            Ok(())
        }
    }

    fn flush(&mut self, id: u32, buffer: &mut Vec<D>) -> IOResult<()> {
        let mut session = new_output_session(self.output, self.tag);
        session.advance(id)?;
        session.give_batch(buffer)?;
        Ok(())
    }
}

impl<'a, D: Data> Drop for AutoRefreshSession<'a, D> {
    fn drop(&mut self) {
        let mut buffer = std::mem::replace(&mut self.buffer, HashMap::new());
        for (id, mut batch) in buffer.drain() {
            if !batch.is_empty() {
                self.flush(id, &mut batch).expect("flush failre;");
            }
        }
    }
}

const BIT_BOUNDARY: u32 = (BitSet::BITS_PER_USIZE
    * BitSet::BITS_PER_USIZE
    * BitSet::BITS_PER_USIZE
    * BitSet::BITS_PER_USIZE) as u32;

struct IdSet {
    bit_set: BitSet,
    hash_set: HashSet<u32>,
}

impl IdSet {
    pub fn new() -> Self {
        IdSet { bit_set: BitSet::new(), hash_set: HashSet::new() }
    }

    pub fn insert(&mut self, id: u32) -> bool {
        if id >= BIT_BOUNDARY {
            self.hash_set.insert(id)
        } else {
            self.bit_set.add(id)
        }
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, id: u32) -> bool {
        if id >= BIT_BOUNDARY {
            self.hash_set.remove(&id)
        } else {
            self.bit_set.remove(id)
        }
    }

    pub fn contains(&self, id: u32) -> bool {
        if id >= BIT_BOUNDARY {
            self.hash_set.contains(&id)
        } else {
            self.bit_set.contains(id)
        }
    }
}
