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

use crossbeam_deque::{Steal, Stealer, Worker};

pub struct WorkStealFactory<T: Send> {
    pub size: usize,
    steals: Arc<Vec<Stealer<T>>>,
    workers: Vec<Worker<T>>,
    activate: Arc<AtomicUsize>,
}

pub struct WorkStealQueue<T: Send> {
    pub seq: usize,
    steals: Arc<Vec<Stealer<T>>>,
    worker: Worker<T>,
    activate: Arc<AtomicUsize>,
}

impl<T: Send> WorkStealQueue<T> {
    pub fn new(
        seq: usize, steals: &Arc<Vec<Stealer<T>>>, worker: Worker<T>, activate: &Arc<AtomicUsize>,
    ) -> Self {
        WorkStealQueue { seq, steals: steals.clone(), worker, activate: activate.clone() }
    }

    pub fn push(&self, entry: T) {
        self.worker.push(entry);
    }

    pub fn pop(&self) -> Option<T> {
        if let Some(task) = self.worker.pop() {
            Some(task)
        } else {
            let len = self.activate.load(Ordering::SeqCst);
            for i in 1..len {
                let idx = (self.seq + i) % len;
                if let Some(task) = self.steal(&self.steals[idx]) {
                    return Some(task);
                }
            }
            None
        }
    }

    #[inline]
    fn steal(&self, stealer: &Stealer<T>) -> Option<T> {
        if !stealer.is_empty() {
            let mut count = 0;
            while count < 2 {
                match stealer.steal() {
                    Steal::Success(task) => return Some(task),
                    Steal::Empty => return None,
                    _ => {}
                }
                count += 1;
            }
        }
        None
    }
}

impl<T: Send> WorkStealFactory<T> {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        let mut steals = Vec::with_capacity(size);
        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            let worker = Worker::<T>::new_fifo();
            steals.push(worker.stealer());
            workers.push(worker);
        }
        steals.reverse();
        WorkStealFactory {
            size,
            steals: Arc::new(steals),
            workers,
            activate: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn get_queue(&mut self) -> Option<WorkStealQueue<T>> {
        let active = &self.activate;
        self.workers.pop().map(|w| {
            let seq = active.fetch_add(1, Ordering::SeqCst);
            WorkStealQueue::new(seq, &self.steals, w, &self.activate)
        })
    }
}

#[cfg(test)]
mod test {
    use std::option::Option::Some;

    use crate::queue::WorkStealFactory;

    #[test]
    fn test_steal_queue() {
        let mut factory = WorkStealFactory::<usize>::new(4);
        let queue1 = factory.get_queue().unwrap();
        for j in 0..1000 {
            queue1.push(j);
        }

        let queue2 = factory.get_queue().unwrap();
        let mut count = 0;
        while let Some(_i) = queue2.pop() {
            //::std::thread::sleep(Duration::from_millis(1));
            count += 1;
        }

        println!("steal {} tasks", count);
        assert_eq!(count, 1000);
    }
}
