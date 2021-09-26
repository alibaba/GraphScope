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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pegasus_common::rc::UnsafeRcPtr;

use crate::channel_id::ChannelId;
use crate::data_plane::{Pull, Push};
use crate::errors::{IOError, IOErrorKind};

pub struct ThreadPush<T> {
    pub id: ChannelId,
    ptr: UnsafeRcPtr<RefCell<VecDeque<T>>>,
    exhaust: Arc<AtomicBool>,
    exhaust_local: bool,
    failed: Option<T>,
}

impl<T> ThreadPush<T> {
    fn new(id: ChannelId, ptr: UnsafeRcPtr<RefCell<VecDeque<T>>>, exhaust: &Arc<AtomicBool>) -> Self {
        ThreadPush { id, ptr, exhaust: exhaust.clone(), exhaust_local: false, failed: None }
    }
}

impl<T> Clone for ThreadPush<T> {
    fn clone(&self) -> Self {
        ThreadPush {
            id: self.id,
            ptr: self.ptr.clone(),
            exhaust: self.exhaust.clone(),
            exhaust_local: false,
            failed: None,
        }
    }
}

impl<T: Send> Push<T> for ThreadPush<T> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        if !self.exhaust_local {
            self.ptr.borrow_mut().push_back(msg);
            Ok(())
        } else {
            error_worker!("ThreadPush#push after close;");
            self.failed = Some(msg);
            let err = throw_io_error!(io::ErrorKind::NotConnected, self.id);
            Err(err)
        }
    }

    fn check_failed(&mut self) -> Option<T> {
        self.failed.take()
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        self.exhaust_local = true;
        if Arc::strong_count(&self.exhaust) == 2 {
            self.exhaust.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl<T> Drop for ThreadPush<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.exhaust) == 2 && !self.exhaust.load(Ordering::SeqCst) {
            warn_worker!("{:?}: drop 'ThreadPush' without close;", self.id);
            // if cfg!(debug_assertions) {
            //     let bt = backtrace::Backtrace::new();
            //     error_worker!("caused by:\n{:?}", bt);
            // }
        }
    }
}

pub struct ThreadPull<T> {
    pub id: ChannelId,
    ptr: UnsafeRcPtr<RefCell<VecDeque<T>>>,
    exhaust: Arc<AtomicBool>,
    exhaust_local: bool,
}

impl<T> ThreadPull<T> {
    fn new(id: ChannelId, ptr: UnsafeRcPtr<RefCell<VecDeque<T>>>, exhaust: Arc<AtomicBool>) -> Self {
        ThreadPull { id, ptr, exhaust, exhaust_local: false }
    }
}

impl<T: Send> Pull<T> for ThreadPull<T> {
    fn next(&mut self) -> Result<Option<T>, IOError> {
        if self.exhaust_local {
            //debug_worker!("channel {:?} was exhausted;", self.id);
            let err = throw_io_error!(IOErrorKind::SourceExhaust, self.id);
            Err(err)
        } else {
            match self.ptr.borrow_mut().pop_front() {
                Some(t) => Ok(Some(t)),
                None => {
                    if self.exhaust.load(Ordering::SeqCst) {
                        self.exhaust_local = true;
                        Ok(None)
                    } else if Arc::strong_count(&self.exhaust) == 1 {
                        error_worker!("ThreadPull#pull: fail to pull data as the push disconnected;");
                        let err = throw_io_error!(io::ErrorKind::BrokenPipe, self.id);
                        Err(err)
                    } else {
                        Ok(None)
                    }
                }
            }
        }
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        Ok(!self.ptr.borrow().is_empty())
    }
}

pub fn pipeline<T>(id: ChannelId) -> (ThreadPush<T>, ThreadPull<T>) {
    let queue = UnsafeRcPtr::new(RefCell::new(VecDeque::new()));
    let exhaust = Arc::new(AtomicBool::new(false));
    (ThreadPush::new(id, queue.clone(), &exhaust), ThreadPull::new(id, queue, exhaust))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn thread_push_pull() {
        let (mut tx, mut rx) = pipeline::<u64>(ChannelId::new(0, 0));
        for i in 0..65535 {
            tx.push(i).unwrap();
        }

        let mut j = 0;
        while let Some(i) = rx.next().unwrap() {
            assert_eq!(i, j);
            j += 1;
        }
        assert_eq!(j, 65535);
        tx.close().unwrap();
        assert_eq!(rx.next().unwrap(), None);
        let result = rx.next();
        match result {
            Err(err) => {
                assert!(err.is_source_exhaust(), "unexpected error {:?}", err);
            }
            Ok(_) => {
                panic!("undetected error");
            }
        }
    }
}
