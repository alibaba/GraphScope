//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use maxgraph_common::proto::query_flow::OperatorBase;

#[derive(Debug)]
pub struct RangeManager {
    start: usize,
    end: usize,
    curr: usize,
    debug_log: bool,
}

impl RangeManager {
    pub fn new(start: usize, end: usize, debug_log: bool) -> Self {
        RangeManager { start, end, curr:0, debug_log }
    }

    pub fn range_filter(&mut self) -> bool {
        self.curr += 1;
        if self.debug_log {
            info!("range filter curr {:?} start {:?} end {:?}", &self.curr, &self.start, &self.end);
        }
        if self.start >= self.end {
            self.curr = self.start + 1;
            return true;
        }

        if self.curr > self.start && self.curr <= self.end {
            return false;
        } else {
            return true;
        }
    }

    pub fn range_filter_with_bulk(&mut self, bulk: usize) -> bool {
        self.curr += bulk;
        if self.debug_log {
            info!("range filter curr {:?} start {:?} end {:?}", &self.curr, &self.start, &self.end);
        }
        if self.start >= self.end {
            info!("start >= end: filter curr {:?} start {:?} end {:?}", &self.curr, &self.start, &self.end);
            return true;
        }
        if self.curr > self.start {
            return false;
        } else {
            return true;
        }
    }

    // return the number of messages in the bulk that should be added into result_list
    pub fn check_range_with_bulk(&mut self, bulk: usize) -> usize {
        if self.start >= self.end || self.curr <= self.start {
            return 0;
        }
        if self.curr <= self.end {
            if self.curr - self.start < bulk {
                return self.curr - self.start;
            } else {
                return bulk;
            };
        } else {
            if self.end - self.start < bulk {
                return self.end - self.start;
            } else if self.curr - self.end < bulk {
                return bulk - (self.curr - self.end);
            } else {
                return 0;
            }
        }
    }

    pub fn range_finish(&self) -> bool {
        self.curr >= self.end
    }
}

pub fn parse_range_manager(base: &OperatorBase) -> Option<RangeManager> {
    if base.has_range_limit() {
        let range_limit = base.get_range_limit();
        Some(RangeManager::new(range_limit.get_range_start() as usize, range_limit.get_range_end() as usize, false))
    } else {
        None
    }
}
