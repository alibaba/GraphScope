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
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

/// Thread-local queue.
/// # Example
///
/// ```rust
/// use maxgraph_runtime::server::queue::ThreadQueue;
///
/// let (push, pull) = ThreadQueue::new();
/// assert!(pull.pull().is_ok());
/// assert_eq!(pull.pull().unwrap(), None);
///
/// push.push(3);
/// push.push(4);
/// assert_eq!(pull.pull().unwrap(), Some(3));
///
/// drop(push);
/// assert!(pull.pull().is_err());
///
/// ```
pub struct ThreadQueue;

impl ThreadQueue {
    pub fn new<T>() -> (Push<T>, Pull<T>) {
        let queue = Rc::new(RefCell::new(VecDeque::new()));
        (
            Push { queue: queue.clone() },
            Pull { queue: Rc::downgrade(&queue) }
        )
    }
}

pub struct Push<T> {
    queue: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Push<T> {
    /// Enqueue some data.
    pub fn push(&self, data: T) {
        let mut borrowed = self.queue.borrow_mut();
        borrowed.push_back(data);
    }
}

pub struct Pull<T> {
    queue: Weak<RefCell<VecDeque<T>>>,
}

impl<T> Pull<T> {
    /// Pull data from queue.
    /// Return error if no `Push` exists even though there's something in queue.
    /// Return Ok(None(_)) if no data in queue.
    pub fn pull(&self) -> Result<Option<T>, ()> {
        match self.queue.upgrade() {
            Some(queue) => {
                let mut borrowed = queue.borrow_mut();
                Ok(borrowed.pop_front())
            }
            None => Err(()),
        }
    }
}
