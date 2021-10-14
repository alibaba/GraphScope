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

use super::*;
use std::sync::Arc;
use crossbeam_queue::SegQueue;

pub struct Thread;

impl Thread {
    #[inline]
    pub fn pipeline<T>() -> (ThreadPush<T>, ThreadPull<T>) {
        let queue = Arc::new(SegQueue::new());
        let push = ThreadPush::new(queue.clone());
        let pull = ThreadPull::new(queue);
        (push, pull)
    }
}

pub struct ThreadPush<T> {
    queue: Arc<SegQueue<T>>
}

impl<T> ThreadPush<T> {
    pub fn new(queue: Arc<SegQueue<T>>) -> Self {
        ThreadPush { queue }
    }
}

impl<T: Send> Push<T> for ThreadPush<T> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        self.queue.push(msg);
        Ok(())
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        Ok(())
    }
}

pub struct ThreadPull<T> {
    queue: Arc<SegQueue<T>>,
}

impl<T> ThreadPull<T> {
    pub fn new(queue: Arc<SegQueue
    <T>>) -> Self {
        ThreadPull { queue }
    }
}

impl<T: Send> Pull<T> for ThreadPull<T> {
    #[inline]
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        match self.queue.pop() {
            Ok(d) => Ok(Some(d)),
            Err(_) => Ok(None)
        }
    }
}
