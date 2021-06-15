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

use crate::api::function::FlatMapFunction;
use crate::api::meta::{OperatorKind, OperatorMeta};
use crate::api::LazyUnary;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputProxy};
use crate::communication::Channel;
use crate::data::{DataSet, DataSetIter};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore, FIRED_STATE};
use crate::stream::Stream;
use crate::{Data, Tag};
use pegasus_common::rc::RcPointer;
use std::collections::HashMap;

struct Transformer<I, O, T: FlatMapFunction<I, O>> {
    source: DataSetIter<I>,
    target: Option<T::Target>,
    func: RcPointer<T>,
}

impl<I, O, T: FlatMapFunction<I, O>> Transformer<I, O, T> {
    pub fn new(input: &mut DataSet<I>, func: &RcPointer<T>) -> Self {
        Transformer { source: input.drain_into(), target: None, func: func.clone() }
    }
}

impl<I, O, T: FlatMapFunction<I, O>> Iterator for Transformer<I, O, T> {
    type Item = <T::Target as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(result) = self.target.as_mut() {
                if let Some(r) = result.next() {
                    return Some(r);
                }
            }

            if let Some(next_req) = self.source.next() {
                match self.func.exec(next_req) {
                    Ok(resp) => self.target = Some(resp),
                    Err(e) => return Some(Err(e)),
                }
            } else {
                return None;
            }
        }
    }
}

enum LazyIterator<I, O, T: FlatMapFunction<I, O>> {
    LazyTransform(Transformer<I, O, T>),
    Iter(T::Target),
}

impl<I, O, T: FlatMapFunction<I, O>> Iterator for LazyIterator<I, O, T> {
    type Item = <T::Target as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LazyIterator::LazyTransform(t) => t.next(),
            LazyIterator::Iter(i) => i.next(),
        }
    }
}

struct LazyUnaryOperator<I, O, T: FlatMapFunction<I, O>> {
    func: RcPointer<T>,
    actives: HashMap<Tag, LazyIterator<I, O, T>>,
}

impl<I, O, T: FlatMapFunction<I, O>> LazyUnaryOperator<I, O, T> {
    pub fn new(func: T) -> Self {
        LazyUnaryOperator { func: RcPointer::new(func), actives: HashMap::new() }
    }
}

impl<I, O, T> OperatorCore for LazyUnaryOperator<I, O, T>
where
    I: Data,
    O: Data,
    T: FlatMapFunction<I, O>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<I>(&inputs[0], tag);
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let mut active = false;
        input.for_each_batch(|dataset| {
            let mut resp = if dataset.len() == 1 {
                let data = dataset.pop().expect("dataset size wrong;");
                LazyIterator::Iter(self.func.exec(data)?)
            } else {
                let trans = Transformer::new(dataset, &self.func);
                LazyIterator::LazyTransform(trans)
            };
            let result = output.give_result_set(&mut resp);
            if let Err(ref err) = result {
                if err.can_be_retried() {
                    active = true;
                    self.actives.insert(tag.clone(), resp);
                }
            }

            result
        })?;
        Ok(FIRED_STATE[active as usize])
    }

    fn on_active(
        &mut self, active: &Tag, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut is_active = false;
        if let Some(iter) = self.actives.get_mut(active) {
            let mut session = new_output_session::<O>(&outputs[0], active);
            if let Err(err) = session.give_result_set(iter) {
                if err.can_be_retried() {
                    is_active = true;
                } else {
                    return Err(err);
                }
            }
        }

        if !is_active {
            self.actives.remove(active);
        }
        Ok(FIRED_STATE[is_active as usize])
    }
}

impl<I: Data> LazyUnary<I> for Stream<I> {
    fn lazy_unary<O, C, B, F>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: FlatMapFunction<I, O>,
    {
        self.concat(name, channel, |meta| {
            meta.set_kind(OperatorKind::Expand);
            let func = construct(meta);
            Box::new(LazyUnaryOperator::new(func))
        })
    }
}
