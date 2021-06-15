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

use crate::communication::output::{OutputDelta, OutputProxy};
use crate::Tag;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct CancelSignal {
    pub port: usize,
    pub ch_index: u32,
    pub(crate) tag: Tag,
}

impl CancelSignal {
    #[allow(dead_code)]
    pub fn take(self) -> Tag {
        self.tag
    }
}

pub trait CancelGuard: Send + 'static {
    fn cancel(&mut self, signal: CancelSignal, outputs: &[Box<dyn OutputProxy>]) -> &mut Vec<Tag>;
}

pub struct DefaultCancelGuard {
    skips: HashMap<Tag, HashSet<usize>>,
    guards: usize,
    pop: Vec<Tag>,
}

impl DefaultCancelGuard {
    pub fn new(guards: usize) -> Self {
        DefaultCancelGuard { skips: HashMap::new(), guards, pop: Vec::new() }
    }
}

impl CancelGuard for DefaultCancelGuard {
    fn cancel(&mut self, signal: CancelSignal, outputs: &[Box<dyn OutputProxy>]) -> &mut Vec<Tag> {
        if signal.port < outputs.len() {
            if outputs[signal.port].skip(signal.ch_index, &signal.tag) {
                let cancel = match outputs[signal.port].output_delta() {
                    OutputDelta::None => Some(signal.tag.clone()),
                    OutputDelta::Advance => Some(signal.tag.retreat()),
                    OutputDelta::ToChild => None,
                    OutputDelta::ToParent(_) => Some(signal.tag.clone()),
                };

                if let Some(cancel) = cancel {
                    if self.guards == 1 {
                        self.pop.push(cancel);
                    } else {
                        if let Some(mut canceled) = self.skips.remove(&cancel) {
                            canceled.insert(signal.port);
                            if canceled.len() == self.guards {
                                self.pop.push(cancel);
                            } else {
                                self.skips.insert(cancel.clone(), canceled);
                            }
                        } else {
                            let mut canceled = HashSet::new();
                            canceled.insert(signal.port);
                            self.skips.insert(cancel, canceled);
                        }
                    }
                }
            }
        }
        &mut self.pop
    }
}
