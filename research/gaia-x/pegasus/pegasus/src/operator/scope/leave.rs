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
// use crate::api::function::{MapFunction, NotifyFunction};
// use crate::api::{LeaveScope, Notification};
// use crate::communication::input::{new_input_session, InputProxy};
// use crate::communication::output::{new_output, OutputProxy};
// use crate::errors::{BuildJobError, JobExecError};
// use crate::operator::{Notifiable, NotifiableOperator, OperatorCore};
// use crate::stream::Stream;
// use crate::{Data, Tag};
// use pegasus_common::utils::Factory;
// use std::collections::HashMap;
//
// struct LeaveOperator<D> {
//     scope_level: usize,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D: Data> OperatorCore for LeaveOperator<D> {
//     fn on_receive(&mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
//         let mut input = new_input_session::<D>(&inputs[0]);
//         let output = new_output::<D>(&outputs[0]);
//
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
// impl<D: Data> Notifiable for LeaveOperator<D> {
//     fn on_notify(&mut self, n: Notification, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
//         if n.tag().len() < self.scope_level {
//             outputs[0].notify_end(n.take_end())?;
//         }
//         Ok(())
//     }
// }
//
// impl<D: Data> LeaveScope<D> for Stream<D> {
//     fn leave(self) -> Result<Stream<D>, BuildJobError> {
//         if self.scope_level == 0 {
//             return BuildJobError::unsupported("can't leave root scope;");
//         }
//         self.leave_scope(|info| {
//             let scope_level = info.scope_level;
//             let op: LeaveOperator<D> = LeaveOperator { scope_level, _ph: std::marker::PhantomData };
//             Box::new(op) as Box<dyn NotifiableOperator>
//         })
//     }
// }
