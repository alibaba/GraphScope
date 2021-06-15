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

use crate::api::meta::{OperatorKind, ScopePrior};
use crate::api::{ExternSource, IntoStream};
use crate::communication::input::InputProxy;
use crate::communication::output::{new_output_session, OutputProxy};
use crate::dataflow::DataflowBuilder;
use crate::errors::{BuildJobError, IOError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};
use std::iter::FusedIterator;

struct SourceOperator<D, E: ExternSource<Item = D>> {
    src: E,
    is_exhaust: bool,
}

impl<D, E: ExternSource<Item = D>> SourceOperator<D, E> {
    pub fn new(src: E) -> Self {
        SourceOperator { src, is_exhaust: false }
    }
}

impl<D: Data, E: ExternSource<Item = D>> OperatorCore for SourceOperator<D, E> {
    fn on_receive(
        &mut self, _: &Tag, _: &[Box<dyn InputProxy>], _: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        unreachable!("source operator on receive");
    }

    fn on_active(
        &mut self, active: &Tag, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        assert!(active.is_root());
        let mut session = new_output_session::<D>(&outputs[0], active);
        loop {
            match self.src.pull_next() {
                Ok(Some(data)) => session.give(data)?,
                Ok(None) => break,
                Err(err) => {
                    if err.is_source_exhaust() {
                        self.is_exhaust = true;
                        break;
                    } else {
                        return Err(err)?;
                    }
                }
            }
        }

        if self.is_exhaust {
            std::mem::drop(session);
            outputs[0].scope_end(active.clone());
            info_worker!("source has been exhausted;");
            Ok(FiredState::Idle)
        } else {
            Ok(FiredState::Active)
        }
    }
}

impl<E: ExternSource + 'static> IntoStream<E::Item> for E
where
    E::Item: Data,
{
    fn into_stream(self, dfb: &DataflowBuilder) -> Result<Stream<E::Item>, BuildJobError> {
        let src = SourceOperator::new(self);
        let mut op = dfb.construct_operator("source", 0, ScopePrior::None, move |meta| {
            meta.set_kind(OperatorKind::Source);
            Box::new(src)
        });
        let output = op.new_output::<E::Item>();
        Ok(Stream::new(output, dfb))
    }
}

impl DataflowBuilder {
    pub fn input_from_iter<I: FusedIterator + Send + 'static>(
        &self, iter: I,
    ) -> Result<Stream<<I as Iterator>::Item>, BuildJobError>
    where
        <I as Iterator>::Item: Data,
    {
        let source = WrapIterator::new(iter);
        source.into_stream(self)
    }

    pub fn input_from<E: ExternSource + 'static>(
        &self, extern_src: E,
    ) -> Result<Stream<E::Item>, BuildJobError>
    where
        E::Item: Data,
    {
        extern_src.into_stream(self)
    }
}

struct WrapIterator<D, I: FusedIterator<Item = D>> {
    iter: I,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send, I: FusedIterator<Item = D> + Send> ExternSource for WrapIterator<D, I> {
    type Item = D;

    fn pull_next(&mut self) -> Result<Option<Self::Item>, IOError> {
        match self.iter.next() {
            Some(data) => Ok(Some(data)),
            None => Err(IOError::source_exhaust()),
        }
    }
}

impl<D, I: FusedIterator<Item = D>> WrapIterator<D, I> {
    pub fn new(iter: I) -> Self {
        WrapIterator { iter, _ph: std::marker::PhantomData }
    }
}
