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

use crate::api::meta::OperatorMeta;
use crate::api::notify::Notification;
use crate::api::LoopCondition;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputProxy};
use crate::errors::JobExecError;
use crate::operator::iteration::IterationSync;
use crate::operator::{FiredState, OperatorCore};
use crate::{Data, Tag};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct LoopTracker {
    round: u32,
    peers: u32,
    try_end: HashSet<u32>,
}

impl LoopTracker {
    pub fn new(peers: u32) -> Self {
        LoopTracker { round: 0, peers, try_end: HashSet::new() }
    }

    #[inline]
    pub fn vote(&mut self, round: u32, src: u32) {
        if self.round < round {
            self.try_end.clear();
            self.round = round;
        }
        self.try_end.insert(src);
    }

    #[inline]
    pub fn vote_to_halt(&self) -> bool {
        self.try_end.len() as u32 == self.peers
    }
}

pub struct MergeSwitch<D: Data> {
    pub scope_depth: usize,
    pub peers: u32,
    condition: LoopCondition<D>,
    in_loops: HashMap<Tag, LoopTracker>,
    parent_scopes: HashMap<Tag, bool>,
    un_complete: HashSet<Tag>,
    extern_exhaust: bool,
}

impl<D: Data> MergeSwitch<D> {
    pub fn new(meta: &OperatorMeta, condition: LoopCondition<D>) -> Self {
        MergeSwitch {
            scope_depth: meta.scope_depth,
            peers: meta.worker_id.peers,
            condition,
            in_loops: HashMap::new(),
            parent_scopes: HashMap::new(),
            un_complete: HashSet::new(),
            extern_exhaust: false,
        }
    }

    fn check_termination(
        &mut self, tag: &Tag, p: Tag, round: u32, input: &Box<dyn InputProxy>,
        outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut feedback = new_input_session::<u32>(input, tag);
        if !feedback.has_outstanding() {
            return Ok(());
        }

        feedback.for_each_batch(|data_set| {
            for datum in data_set.drain(..) {
                if let Some(tracker) = self.in_loops.get_mut(&p) {
                    tracker.vote(round, datum);
                }
            }
            Ok(())
        })?;

        if !self.in_loops.is_empty() {
            let mut parent_scopes = std::mem::replace(&mut self.parent_scopes, HashMap::new());
            self.in_loops.retain(|k, v| {
                let remove = v.vote_to_halt();
                if remove {
                    trace_worker!("{:?} exit loop;", k);
                    if let Some(is_halt) = parent_scopes.get_mut(k) {
                        *is_halt = true;
                    }
                    outputs[0].drop_retain(k);
                    outputs[1].drop_retain(k);
                }
                !remove
            });
            self.parent_scopes = parent_scopes;
        }

        if self.in_loops.is_empty() && self.extern_exhaust {
            trace_worker!("merge_switch operator finished, dropping loop context...");
            outputs[0].close()?;
            outputs[1].close()?;
        }
        Ok(())
    }
}

impl<D: Data> OperatorCore for MergeSwitch<D> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut output_leave = new_output_session::<D>(&outputs[0], tag);
        let mut output_loop = new_output_session::<D>(&outputs[1], tag);

        let p = tag.to_parent_uncheck();
        let round = tag.current_uncheck();
        let depth = self.scope_depth;
        assert_eq!(tag.len(), depth);
        let mut has_data_into_iter = false;
        if round == 0 && !self.extern_exhaust {
            debug_worker!("{:?} enter iteration;", p);
            if !self.in_loops.contains_key(&p) {
                self.in_loops.insert(p.clone(), LoopTracker::new(self.peers));
                self.parent_scopes.insert(p.clone(), false);
                self.un_complete.insert(tag.clone());
            }

            let condition = &self.condition;
            let mut input = new_input_session::<IterationSync<D>>(&inputs[0], tag);
            if condition.has_until_cond() {
                input.for_each_batch(|data_set| {
                    for data in data_set.drain(..) {
                        match data {
                            IterationSync::Data(mut data) => {
                                for datum in data.drain(..) {
                                    if condition.is_converge(&datum)? {
                                        output_leave.give(datum)?;
                                    } else {
                                        has_data_into_iter |= true;
                                        output_loop.give(datum)?;
                                    }
                                }
                            }
                            _ => (),
                        }
                    }
                    Ok(())
                })?;
            } else {
                has_data_into_iter |= true;
                input.for_each_batch(|data_set| {
                    for data in data_set.drain(..) {
                        match data {
                            IterationSync::Data(mut data) => output_loop.forward(&mut data)?,
                            _ => (),
                        }
                    }
                    Ok(())
                })?;
            }
        }
        self.extern_exhaust |= inputs[0].is_exhaust();

        if round > 0 {
            let mut feedback = new_input_session::<D>(&inputs[1], tag);
            if round >= self.condition.max_iters {
                feedback.for_each_batch(|data_set| {
                    if self.condition.has_until_cond() {
                        for datum in data_set.drain(..) {
                            if self.condition.is_converge(&datum)? {
                                output_leave.give(datum)?;
                            } else {
                                // discard forever;
                            }
                        }
                    } else {
                        output_leave.forward(data_set)?;
                    }
                    Ok(())
                })?;
            } else {
                feedback.for_each_batch(|data_set| {
                    if self.condition.has_until_cond() {
                        for datum in data_set.drain(..) {
                            if self.condition.is_converge(&datum)? {
                                output_leave.give(datum)?;
                            } else {
                                has_data_into_iter |= true;
                                output_loop.give(datum)?;
                            }
                        }
                    } else {
                        has_data_into_iter |= true;
                        output_loop.forward(data_set)?;
                    }
                    Ok(())
                })?;
            }
            std::mem::drop(output_loop);
            std::mem::drop(output_leave);
            // check distribute termination;
            if !has_data_into_iter {
                self.check_termination(tag, p, round, &inputs[2], outputs)?;
            }
        }

        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        if n.port == 0 {
            // because of static enter, the enter operator won't give sub-scope end signal, so notifications
            // from this input port are all from parent scope;
            // assert!(n.tag.len() < self.scope_depth);
            if n.tag.len() < self.scope_depth {
                for output in outputs.iter() {
                    output.retain(&n.tag);
                }

                self.un_complete.retain(|un_cpe| {
                    if n.tag.is_parent_of(un_cpe) {
                        for output in outputs.iter() {
                            output.scope_end(un_cpe.clone());
                        }
                        false
                    } else {
                        true
                    }
                });
            } else {
                self.un_complete.remove(&n.tag);
            }
        } else if n.port == 1 {
            if n.tag.len() == self.scope_depth {
                // End notifications of each iteration;
                let p = n.tag.to_parent_uncheck();
                let round = n.tag.current_uncheck();
                if round == self.condition.max_iters {
                    if self.in_loops.remove(&p).is_some() {
                        outputs[0].drop_retain(&p);
                        outputs[1].drop_retain(&p);
                        if self.in_loops.is_empty() && self.extern_exhaust {
                            outputs[0].close()?;
                            outputs[1].close()?;
                        }
                    }
                } else if round > self.condition.max_iters {
                    outputs[0].ignore(&n.tag);
                    outputs[1].ignore(&n.tag);
                } else {
                    if let Some(is_halt) = self.parent_scopes.get(&p) {
                        if *is_halt {
                            outputs[0].ignore(&n.tag);
                            outputs[1].ignore(&n.tag);
                        }
                    } else {
                        unreachable!("{:?} not found in parent_scopes; ", n.tag);
                    }
                }
            } else if n.tag.len() == self.scope_depth - 1 {
                outputs[0].ignore(&n.tag);
                outputs[1].ignore(&n.tag);
            }
        } else if n.port == 2 {
            outputs[0].ignore(&n.tag);
            outputs[1].ignore(&n.tag);
        } else {
            Err(format!("invalid notification from input port {}", n.port))?
        }

        Ok(())
    }
}
