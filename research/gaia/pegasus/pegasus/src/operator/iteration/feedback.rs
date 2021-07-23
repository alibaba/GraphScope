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

use crate::api::notify::Notification;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputProxy};
use crate::errors::{IOError, IOErrorKind, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::{Data, Tag};
use std::collections::HashMap;
use std::io;

pub struct Feedback<D: Data> {
    pub scope_depth: usize,
    pub max_iters: u32,
    /// The key is the tag of data which had entered a loop context, the value is the largest iteration rounds
    /// the data are going;
    in_loop: HashMap<Tag, u32>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> Feedback<D> {
    pub fn new(scope_depth: usize, max_iters: u32) -> Self {
        Feedback { scope_depth, max_iters, in_loop: HashMap::new(), _ph: std::marker::PhantomData }
    }

    #[inline]
    fn try_to_vote(output: &Box<dyn OutputProxy>, tag: &Tag) -> Result<(), IOError> {
        let mut output = new_output_session::<u32>(output, tag);
        let index =
            crate::worker_id::get_current_worker().map(|w| w.index).expect("worker id lost;");
        if let Err(err) = output.give(index) {
            match err.kind() {
                IOErrorKind::Common(kind) if kind == io::ErrorKind::NotConnected => Ok(()),
                _ => Err(err),
            }
        } else {
            Ok(())
        }
    }
}

impl<D: Data> OperatorCore for Feedback<D> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        let mut output = new_output_session::<D>(&outputs[0], tag);
        input.for_each_batch(|data_set| {
            output.forward(data_set)?;
            Ok(())
        })?;
        let p = tag.to_parent_uncheck();
        let current = tag.current_uncheck() + 1;
        let pre = self.in_loop.entry(p).or_insert(0);
        if *pre < current {
            *pre = current
        };
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        if n.tag.len() == self.scope_depth {
            outputs[1].ignore(&n.tag);
            // get new end notification of data stream;
            // as it is in a loop context, the end notification indicates a end of an iteration;
            let p = n.tag.to_parent_uncheck();
            let round = n.tag.current_uncheck();
            if round < self.max_iters {
                // it means that the data of scope `p` had finished the nth iteration;
                if let Some(cur) = self.in_loop.get(&p) {
                    // check if data with this tag has entered next iteration;
                    if *cur <= n.tag.current_uncheck() {
                        // not enter;
                        trace_worker!("try to vote iteration terminate {:?}", &n.tag);
                        Self::try_to_vote(&outputs[1], &n.tag)?;
                    }
                } else {
                    // iteration data of scope `p` has never appear in this worker's loop stream, but it's
                    // data may had appeared in other workers' loop stream;
                    // self.in_loop.insert(p, n.stream_end.current_uncheck());
                    // Self::try_to_vote(&outputs[0], self.worker_id.index, &n.stream_end)?;
                }
            }
        } else if n.tag.len() + 1 == self.scope_depth {
            self.in_loop.remove(&n.tag);
            if !n.tag.is_root() {
                outputs[0].ignore(&n.tag);
                outputs[1].ignore(&n.tag);
            }
        //outputs[0].retain(&n.stream_end);
        } else {
            // ignore
        }

        Ok(())
    }
}
