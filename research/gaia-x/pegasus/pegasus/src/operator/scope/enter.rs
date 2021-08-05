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

// use crate::api::meta::OperatorInfo;
// use crate::api::{Notification};
// use crate::communication::input::{new_input_session, InputProxy};
// use crate::communication::output::{new_output, OutputProxy, RefWrapOutput};
// use crate::communication::{Channel, Pipeline};
// use crate::data::DataSet;
// use crate::errors::{BuildJobError, IOResult, JobExecError};
// use crate::operator::{Notifiable, NotifiableOperator, OperatorCore};
// use crate::progress::{EndSignal, Weight};
// use crate::stream::Stream;
// use crate::tag::tools::map::TidyTagMap;
// use crate::{Data, Tag};
// use hibitset::DrainableBitSet;
// use nohash_hasher::IntSet;
// use pegasus_common::rc::RcPointer;
// use pegasus_common::utils::Factory;
// use std::cell::RefCell;
// use std::ops::Deref;

// struct DynScopeEnter<D: Data, F, E> {
//     factory: F,
//     actives: Vec<Tag>,
//     emits: TidyTagMap<X<D, E>>,
//     _ph: std::marker::PhantomData<D>,
// }
//
// struct X<D: Data, E> {
//     emitter: E,
//     un_complete: IntSet<u32>,
//     complete: RcPointer<RefCell<IntSet<u32>>>,
// }
//
// impl<D: Data, E: ScopeInputEmitter<D>> X<D, E> {
//     fn new<F>(factory: &F) -> Self
//     where
//         F: Factory<Target = E>,
//     {
//         let emitter = factory.create();
//         X { emitter, un_complete: Default::default(), complete: RcPointer::new(RefCell::new(Default::default())) }
//     }
// }
//
// impl<D: Data, F, E> DynScopeEnter<D, F, E> {
//     pub fn new(scope_level: usize, factory: F) -> Self {
//         DynScopeEnter { factory, actives: vec![], emits: TidyTagMap::new(scope_level), _ph: std::marker::PhantomData }
//     }
// }

// struct DynScopeOutput<'a, D: Data> {
//     output: &'a RefWrapOutput<D>,
// }
//
// impl<'a, D: Data> Deref for DynScopeOutput<'a, D> {
//     type Target = RefWrapOutput<D>;
//
//     fn deref(&self) -> &Self::Target {
//         self.output
//     }
// }
//
// impl<'a, D: Data> DynScopeOutput<'a, D> {
//     pub fn new(output: &'a Box<dyn OutputProxy>) -> Self {
//         let output = new_output::<D>(output);
//         output.disable_auto_flush();
//         DynScopeOutput { output }
//     }
//
//     pub fn give<E>(&mut self, tag: &Tag, x: &mut X<D, E>) -> IOResult<()>
//     where
//         E: ScopeInputEmitter<D>,
//     {
//         loop {
//             let next = x.emitter.next();
//             if let Some(input) = next.take() {
//                 let scope_id = input.id;
//                 let tag = Tag::inherit(tag, scope_id);
//                 if let Err(e) = self.output.ensure_capacity(&tag) {
//                     *next = Some(input);
//                     return Err(e);
//                 }
//                 if input.is_last {
//                     if !x.un_complete.is_empty() {
//                         x.un_complete.remove(&scope_id);
//                     }
//                     let end = x.emitter.weight();
//                     let end = EndSignal::new(tag, end);
//                     self.output.push_last(input.take(), end)?;
//                 } else {
//                     self.output.new_session_without_evolve(tag)?.give(input.take())?;
//                     x.un_complete.insert(scope_id);
//                 }
//             } else {
//                 return Ok(());
//             }
//         }
//     }
//
//     pub fn flush(&mut self) -> IOResult<()> {
//         self.output.flush()
//     }
// }
//
// impl<D, F, E> OperatorCore for DynScopeEnter<D, F, E>
// where
//     D: Data,
//     E: ScopeInputEmitter<D>,
//     F: Factory<Target = E> + Send + 'static,
// {
//     fn on_receive(&mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
//         self.actives.clear();
//         let mut input = new_input_session::<D>(&inputs[0]);
//         let mut emits = std::mem::replace(&mut self.emits, TidyTagMap::new(0));
//         let mut output = DynScopeOutput::new(&outputs[0]);
//         input.for_each_batch(|dataset| {
//             if !dataset.is_empty() {
//                 let tag = dataset.tag();
//                 let x = emits.get_mut_or_else(&tag, || X::new(&self.factory));
//                 let _t = reset(x.complete.clone());
//                 for item in dataset.drain() {
//                     if let Some(next) = x.emitter.emit(item) {}
//                 }
//
//                 match output.give(&tag, &mut x) {
//                     Err(e) => {
//                         if e.is_would_block() || e.is_interrupted() {
//                             if x.emitter.has_next() {
//                                 actives.push(tag.clone());
//                                 emits.insert(tag, x);
//                             } else {
//                                 if !check_complete(&tag, &mut x, &output)? {
//                                     emits.insert(tag, x);
//                                 }
//                             }
//                         }
//                         Err(e)?
//                     }
//                     _ => {
//                         if !check_complete(&tag, &mut x, &output)? {
//                             emits.insert(tag, x);
//                         }
//                         Ok(())
//                     }
//                 }
//             } else {
//                 Ok(())
//             }
//         })?;
//
//         output.flush()?;
//
//         self.actives = actives;
//         self.emits = emits;
//     }
//
//     fn on_active(&mut self, active: &Tag, outputs: &[Box<dyn OutputProxy>]) -> Result<bool, JobExecError> {
//         if let Some(x) = self.emits.get_mut(active) {
//             let mut output = DynScopeOutput::new(&outputs[0]);
//             match output.give(active, x) {
//                 Err(err) => {
//                     if err.is_interrupted() || err.is_would_block() {
//                         if x.emitter.has_next() {
//                             Ok(true)
//                         } else {
//                             check_complete(active, x, &output)?;
//                             Ok(false)
//                         }
//                     } else {
//                         Err(err)?
//                     }
//                 }
//                 _ => {
//                     check_complete(active, x, &output)?;
//                     Ok(false)
//                 }
//             }
//         } else {
//             Ok(false)
//         }
//     }
// }

// pub struct StaticScopeEnter<D> {
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D: Data> OperatorCore for StaticScopeEnter<D> {
//     fn on_receive(&mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
//         let mut input = new_input_session::<D>(&inputs[0]);
//         let output = new_output::<D>(&outputs[0]);
//         input.for_each_batch(|dataset| {
//             let mut session = output.new_session(&dataset.tag)?;
//             for data in dataset.drain() {
//                 session.give(data)?;
//             }
//             Ok(())
//         })
//     }
// }
//
// impl<D: Data> Notifiable for StaticScopeEnter<D> {
//     fn on_notify(&mut self, n: Notification, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
//         let end = n.take_end();
//         let mut sub_end = end.clone();
//         sub_end.tag = Tag::inherit(&end.tag, 0);
//         outputs[0].notify_end(sub_end)?;
//         outputs[0].notify_end(end)?;
//         Ok(())
//     }
// }

// impl<D> StaticScopeEnter<D> {
//     pub fn new() -> Self {
//         StaticScopeEnter { _ph: std::marker::PhantomData }
//     }
// }
//
// impl<D: Data> EnterScope<D> for Stream<D> {
//     fn enter(self) -> Result<Stream<D>, BuildJobError> {
//         self.enter_scope(|_info| StaticScopeEnter::<D>::new())
//     }
//     fn dyn_enter<B, F, E>(self, func: B) -> Result<Stream<D>, BuildJobError>
//     where
//         E: ScopeInputEmitter<D>,
//         B: FnOnce(&OperatorInfo) -> F,
//         F: Factory<Target = E> + Send + 'static,
//     {
//         unimplemented!()
//     }
// }

// struct ExtraReset;
//
// fn reset(x: RcPointer<RefCell<IntSet<u32>>>) -> ExtraReset {
//     EXTRA_COMPLETES.with(|p| *p.borrow_mut() = Some(x));
//     ExtraReset
// }
//
// impl Drop for ExtraReset {
//     fn drop(&mut self) {
//         EXTRA_COMPLETES.with(|p| p.borrow_mut().take());
//     }
// }

// #[inline]
// fn check_complete<'a, D: Data, E: ScopeInputEmitter<D>>(
//     tag: &Tag, x: &mut X<D, E>, output: &DynScopeOutput<'a, D>,
// ) -> IOResult<bool> {
//     if let Some(end) = x.emitter.take_end() {
//         for end in x.end_all_of(tag.clone()) {
//             output.notify_end(end)?;
//         }
//         output.notify_end(end)?;
//         Ok(true)
//     } else if !x.complete.borrow().is_empty() {
//         let mut bk = std::mem::replace(&mut x.un_complete, Default::default());
//         let weight = x.emitter.weight();
//         let mut borrow = x.complete.borrow_mut();
//         for id in borrow.drain() {
//             bk.remove(&id);
//             let t = Tag::inherit(&tag, id);
//             let e = EndSignal::new(t, weight.clone());
//             output.notify_end(e)?;
//         }
//         x.un_complete = bk;
//         Ok(false)
//     } else {
//         Ok(false)
//     }
// }
