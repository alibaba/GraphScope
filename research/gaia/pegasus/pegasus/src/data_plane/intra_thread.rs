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

use crate::channel_id::SubChannelId;
use crate::data_plane::{Pull, Push};
use crate::errors::{IOError, IOErrorKind};
use crossbeam_utils::CachePadded;
use std::collections::VecDeque;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct ThreadPush<T> {
    pub id: SubChannelId,
    ptr: NonNull<VecDeque<T>>,
    exhaust: Arc<CachePadded<AtomicBool>>,
    disconnected: Arc<CachePadded<AtomicBool>>,
}

impl<T> ThreadPush<T> {
    fn new(
        id: SubChannelId, ptr: NonNull<VecDeque<T>>, exhaust: &Arc<CachePadded<AtomicBool>>,
        disconnected: &Arc<CachePadded<AtomicBool>>,
    ) -> Self {
        ThreadPush { id, ptr, exhaust: exhaust.clone(), disconnected: disconnected.clone() }
    }
}

unsafe impl<T: Send> Send for ThreadPush<T> {}

impl<T: Send> Push<T> for ThreadPush<T> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        if !self.disconnected.load(Ordering::SeqCst) {
            unsafe {
                (*self.ptr.as_ptr()).push_back(msg);
            }
            Ok(())
        } else {
            error_worker!("ThreadPush#push failure, as the receiver is disconnected;");
            let err = throw_io_error!(io::ErrorKind::NotConnected, self.id);
            Err(err)
        }
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        self.exhaust.store(true, Ordering::SeqCst);
        Ok(())
    }
}

pub struct ThreadPull<T> {
    pub id: SubChannelId,
    ptr: NonNull<VecDeque<T>>,
    exhaust: Arc<CachePadded<AtomicBool>>,
    exhaust_local: bool,
    closed: Arc<CachePadded<AtomicBool>>,
}

impl<T> ThreadPull<T> {
    fn new(
        id: SubChannelId, ptr: NonNull<VecDeque<T>>, exhaust: Arc<CachePadded<AtomicBool>>,
        closed: Arc<CachePadded<AtomicBool>>,
    ) -> Self {
        ThreadPull { id, ptr, exhaust, exhaust_local: false, closed }
    }
}

impl<T: Send> Pull<T> for ThreadPull<T> {
    #[inline]
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        if self.exhaust_local {
            error_worker!("channel {:?} broken pipe as it was exhausted;", self.id);
            let err = throw_io_error!(IOErrorKind::SourceExhaust, self.id);
            Err(err)
        } else {
            let r = unsafe {
                let result = (*self.ptr.as_ptr()).pop_front();
                if result.is_none() {
                    if self.exhaust.load(Ordering::SeqCst) {
                        self.exhaust_local = true;
                        let err = throw_io_error!(IOErrorKind::SourceExhaust, self.id);
                        return Err(err);
                    } else if Arc::strong_count(&self.exhaust) == 1 {
                        error_worker!(
                            "ThreadPull#pull: fail to pull data as the sender disconnected;"
                        );
                        let err = throw_io_error!(io::ErrorKind::UnexpectedEof, self.id);
                        return Err(err);
                    }
                }
                result
            };
            Ok(r)
        }
    }
}

impl<T> Drop for ThreadPull<T> {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_ptr());
        }
    }
}

unsafe impl<T: Send> Send for ThreadPull<T> {}

pub fn pipeline<T>(id: SubChannelId) -> (ThreadPush<T>, ThreadPull<T>) {
    let queue = Box::new(VecDeque::new());
    let ptr =
        NonNull::new(Box::into_raw(queue)).expect("inter thread communication_old init failure;");
    let exhaust = Arc::new(CachePadded::new(AtomicBool::new(false)));
    let closed = Arc::new(CachePadded::new(AtomicBool::new(false)));
    (ThreadPush::new(id, ptr, &exhaust, &closed), ThreadPull::new(id, ptr, exhaust, closed))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn thread_push_pull() {
        let id: SubChannelId = [0, 0, 0].into();
        let (mut tx, mut rx) = pipeline::<u64>(id);
        for i in 0..65535 {
            tx.push(i).unwrap();
        }

        let mut j = 0;
        while let Some(i) = rx.pull().unwrap() {
            assert_eq!(i, j);
            j += 1;
        }
        assert_eq!(j, 65535);
        tx.close().unwrap();
        let result = rx.pull();
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
