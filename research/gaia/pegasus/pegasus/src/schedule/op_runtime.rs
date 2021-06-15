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

use crate::api::meta::ScopePrior;
use crate::errors::JobExecError;
use crate::operator::Operator;
use crate::Tag;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::time::Instant;

pub struct OpRuntime {
    op: Operator,
    receives: Vec<Tag>,
    is_closed: bool,
    elapse: [u128; 3],
    start: Instant,
    dedup: HashSet<Tag>,
}

impl OpRuntime {
    pub fn new(op: Operator) -> Self {
        OpRuntime {
            op,
            receives: vec![],
            is_closed: false,
            elapse: [0, 0, 0],
            start: Instant::now(),
            dedup: HashSet::new(),
        }
    }

    pub fn check_ready(&mut self) -> bool {
        self.op.has_actives() || self.op.has_outstanding() || self.op.has_notifications()
    }

    pub fn fire(&mut self) -> Result<bool, JobExecError> {
        let start = Instant::now();
        self.op.fire_actives()?;
        self.elapse[0] += start.elapsed().as_micros();

        let len = self.inputs().len();
        if len > 0 {
            if self.op.has_outstanding() && self.op.has_output_capacity() {
                let start = Instant::now();
                let mut dedup = std::mem::replace(&mut self.dedup, HashSet::new());
                let mut receives = std::mem::replace(&mut self.receives, vec![]);
                match &self.meta.scope_order {
                    ScopePrior::None => {
                        assert!(len < 64, "too much inputs");
                        for input in self.op.inputs() {
                            input.get_state().for_each_outstanding(|t| {
                                dedup.insert(t.clone());
                            });
                        }
                        let mut x = (1u64 << len) - 1;
                        while x > 0 && !dedup.is_empty() {
                            for i in 0..len {
                                let mask = 1u64 << i;
                                if x & mask > 0 {
                                    match self.inputs()[i].next(&dedup) {
                                        Ok(Some(tag)) => {
                                            self.op.fire_on_receive(&tag)?;
                                            dedup.remove(&tag);
                                        }
                                        Ok(None) => {
                                            x ^= mask;
                                        }
                                        Err(err) => {
                                            if err.is_source_exhaust() {
                                                x ^= mask;
                                            } else {
                                                return Err(err)?;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ScopePrior::Prior(priority) => {
                        if len == 1 {
                            self.inputs()[0]
                                .get_state()
                                .for_each_outstanding(|t| receives.push(t.clone()));
                        } else {
                            for input in self.op.inputs() {
                                input.get_state().for_each_outstanding(|t| {
                                    if dedup.insert(t.clone()) {
                                        receives.push(t.clone());
                                    }
                                });
                            }
                        }
                        receives.sort_by(|t1, t2| priority.compare(t1, t2));
                        for tag in receives.drain(..) {
                            self.op.fire_on_receive(&tag)?;
                        }
                    }
                }

                dedup.clear();
                self.dedup = dedup;
                self.receives = receives;
                self.elapse[1] += start.elapsed().as_micros();
            }
            let start = Instant::now();
            self.op.notify()?;
            self.elapse[2] += start.elapsed().as_micros();
        }

        for output in self.op.outputs() {
            output.close_scopes()?;
            output.reset_capacity();
        }

        Ok(self.is_finished())
    }

    pub fn close(&mut self) {
        if !self.is_closed {
            self.is_closed = true;
            self.op.close_outputs();
            if crate::worker_id::is_in_trace() {
                info_worker!(
                    "operator {:?} finished, times st {:?}, total elapse: {:?}",
                    self.meta,
                    self.elapse,
                    self.start.elapsed()
                );
            }
        }
    }
}

impl Deref for OpRuntime {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

impl DerefMut for OpRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}
