// //
// //! Copyright 2020 Alibaba Group Holding Limited.
// //!
// //! Licensed under the Apache License, Version 2.0 (the "License");
// //! you may not use this file except in compliance with the License.
// //! You may obtain a copy of the License at
// //!
// //! http://www.apache.org/licenses/LICENSE-2.0
// //!
// //! Unless required by applicable law or agreed to in writing, software
// //! distributed under the License is distributed on an "AS IS" BASIS,
// //! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //! See the License for the specific language governing permissions and
// //! limitations under the License.
//
// use crate::api::state::StateMap;
// use crate::api::{Dedup, Range, Unary};
// use crate::communication::{Aggregate, Input, Output, Pipeline};
// use crate::errors::JobExecError;
// use crate::stream::Stream;
// use crate::{BuildJobError, Data};
// use pegasus_common::collections::{Collection, CollectionFactory, DefaultCollectionFactory, Set};
//
// struct DedupHandle<D: Data + Eq, C: CollectionFactory<D>> {
//     factory: C,
//     container: StateMap<C::Target>,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D: Data + Eq, C: CollectionFactory<D>> DedupHandle<D, C> {
//     pub fn new(factory: C, container: StateMap<C::Target>) -> Self {
//         DedupHandle { factory, container, _ph: std::marker::PhantomData }
//     }
// }
//
// impl<D: Data + Eq, C: CollectionFactory<D> + 'static> UnaryNotify<D, D> for DedupHandle<D, C>
// where
//     C::Target: Set<D>,
// {
//     type NotifyResult = Vec<D>;
//
//     fn on_receive(&mut self, input: &mut Input<D>, output: &mut Output<D>) -> Result<(), JobExecError> {
//         input.subscribe_notify();
//         let factory = &self.factory;
//         let container = self.container.entry(&input.tag).or_insert_with(|| factory.create());
//
//         input.for_each_batch(|data| {
//             for datum in data.drain(..) {
//                 if !container.contains(&datum) {
//                     container.add(datum.clone())?;
//                     output.give(datum)?;
//                 }
//             }
//             Ok(())
//         })?;
//
//         Ok(())
//     }
//
//     fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
//         self.container.notify(n);
//         let notified = self.container.extract_notified();
//         assert_eq!(notified.len(), 1);
//         notified.remove(0).1;
//         vec![]
//     }
// }
//
// impl<D: Data + Eq> Dedup<D> for Stream<D> {
//     fn dedup<S>(&self, range: Range) -> Result<Stream<D>, BuildJobError>
//     where
//         S: Set<D> + Default + 'static,
//     {
//         let factory = DefaultCollectionFactory::new();
//         match range {
//             Range::Local => self.unary_with_notify("dedup", Pipeline, |meta| {
//                 let state = StateMap::new(meta);
//                 DedupHandle::<D, DefaultCollectionFactory<D, S>>::new(factory, state)
//             }),
//             Range::Global => self.unary_with_notify("dedup", Aggregate(0), |meta| {
//                 let state = StateMap::new(meta);
//                 DedupHandle::<D, DefaultCollectionFactory<D, S>>::new(factory, state)
//             }),
//         }
//     }
//
//     fn dedup_with<S>(&self, range: Range, factory: S) -> Result<Stream<D>, BuildJobError>
//     where
//         S: CollectionFactory<D> + 'static,
//         S::Target: Set<D>,
//     {
//         match range {
//             Range::Local => self.unary_with_notify("dedup", Pipeline, |meta| {
//                 let state = StateMap::new(meta);
//                 DedupHandle::<D, S>::new(factory, state)
//             }),
//             Range::Global => self.unary_with_notify("dedup", Aggregate(0), |meta| {
//                 let state = StateMap::new(meta);
//                 DedupHandle::<D, S>::new(factory, state)
//             }),
//         }
//     }
// }
