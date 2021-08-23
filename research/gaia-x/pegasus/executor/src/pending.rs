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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;

pub const PENDING_BOUND_SIZE: &'static str = "PEGASUS_EXECUTOR_SELECT_BOUND";
lazy_static! {
    pub static ref BOUND_SIZE: usize = ::std::env::var(PENDING_BOUND_SIZE)
        .map(|s| s.parse::<usize>().unwrap_or(256))
        .unwrap_or(256);
}

pub struct PendingPool<T> {
    pool: Vec<Option<T>>,
    inflows: Vec<Arc<SegQueue<T>>>,
    mirror_permits: Arc<CachePadded<AtomicUsize>>,
}

impl<T: Send> PendingPool<T> {
    pub fn new(inflows: Vec<Arc<SegQueue<T>>>, permits: usize) -> Self {
        PendingPool {
            pool: Vec::with_capacity(*BOUND_SIZE),
            inflows,
            mirror_permits: Arc::new(CachePadded::new(AtomicUsize::new(permits - 1))),
        }
    }

    pub fn with(
        pool: Vec<Option<T>>, inflows: Vec<Arc<SegQueue<T>>>, permits: Arc<CachePadded<AtomicUsize>>,
    ) -> Self {
        PendingPool { pool, inflows, mirror_permits: permits }
    }

    pub fn fill<F: FnMut(PendingPool<T>)>(&mut self, mut consume: F) {
        for inflow in self.inflows.iter() {
            if !inflow.is_empty() {
                while let Ok(item) = inflow.pop() {
                    self.pool.push(Some(item));
                }
            }
        }

        while self.pool.len() > *BOUND_SIZE && self.can_split() {
            let at = self.pool.len() - (*BOUND_SIZE >> 1);
            let split = self.pool.split_off(at);
            let p = PendingPool::with(split, self.inflows.clone(), self.mirror_permits.clone());
            (consume)(p)
        }
    }

    pub fn destroy(&self) {
        self.mirror_permits
            .fetch_add(1, Ordering::SeqCst);
    }

    // if can split, fetch a split permit;
    fn can_split(&self) -> bool {
        let mut current = self.mirror_permits.load(Ordering::SeqCst);
        while current > 0 {
            let swap = self
                .mirror_permits
                .swap(current - 1, Ordering::SeqCst);
            if swap == current {
                return true;
            } else {
                current = swap;
            }
        }
        false
    }
}

impl<T: Send> ::std::ops::Deref for PendingPool<T> {
    type Target = Vec<Option<T>>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<T: Send> ::std::ops::DerefMut for PendingPool<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pool
    }
}
