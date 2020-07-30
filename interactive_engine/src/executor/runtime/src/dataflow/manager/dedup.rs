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

use std::cell::RefCell;
use std::collections::HashSet;
use maxgraph_common::proto::query_flow::OperatorBase;

pub struct DedupManager {
    value_list: RefCell<HashSet<i64>>,
    debug_log: bool,
}
unsafe impl Sync for DedupManager {}

impl DedupManager {
    pub fn new(debug_log: bool) -> Self {
        DedupManager { value_list: RefCell::new(HashSet::new()), debug_log }
    }

    pub fn check_dedup(&self, id: i64) -> bool {
        if self.value_list.borrow().contains(&id) {
            if self.debug_log {
                info!("id {:?} exist in list {:?}", &id, self.value_list.borrow());
            }
            return false;
        }

        if self.debug_log {
            info!("id {:?} not exist in list {:?}", &id, self.value_list.borrow());
        }
        self.value_list.borrow_mut().insert(id);
        return true;
    }
}

pub fn parse_dedup_manager(base: &OperatorBase, debug_flag: bool) -> Option<DedupManager> {
    let argument = base.get_argument();
    if argument.get_dedup_local_flag() && !argument.get_subquery_flag() {
        Some(DedupManager::new(debug_flag))
    } else {
        None
    }
}
