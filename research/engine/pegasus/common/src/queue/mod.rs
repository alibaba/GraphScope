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

mod steal;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::{PopError, PushError, SegQueue};
pub use steal::{WorkStealFactory, WorkStealQueue};

pub struct BoundLinkQueue<T> {
    queue: SegQueue<T>,
    len: AtomicUsize,
    cap: usize,
}

impl<T> BoundLinkQueue<T> {
    pub fn new(cap: usize) -> Self {
        BoundLinkQueue { queue: SegQueue::new(), len: AtomicUsize::new(0), cap }
    }

    pub fn push(&self, item: T) -> Result<(), PushError<T>> {
        if self.len.load(Ordering::SeqCst) == self.cap {
            Err(PushError(item))
        } else {
            self.queue.push(item);
            self.len.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    pub fn pop(&self) -> Result<T, PopError> {
        let result = self.queue.pop();
        if !result.is_err() {
            self.len.fetch_sub(1, Ordering::SeqCst);
        }
        result
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
