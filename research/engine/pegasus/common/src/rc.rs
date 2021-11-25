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

use std::ops::Deref;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// This is a highly unsafe reference-count pointer;
///
/// # Safety
///
/// This is highly unsafe:
///
/// * It is much like the standard rc pointer [`Rc`], with a extra feature that it can be send through
/// threads, this may brings huge security problems, because this pointer doesn't provide any [`Sync`]
/// promise;
///
/// * It can be only used in the situation where you are very confident that this pointer and all its
/// copies will be owned by only one thread at a time, otherwise use [`Arc`] instead;
///
///
/// If you have a structure that is not thread-safe, it won't be read or write by multi-threads
/// at same time under any circumstances, and you need a reference count pointer to it to make it
/// accessible at many places.
///
/// At last, this structure may be owned by different threads at different time;
///
/// The [`Rc`] can't be [`Send`], and the [`Arc`] pointer need the structure it points to be [`Sync`],
/// although a mutex lock can be used to make the structure [`Sync`], but it may incurs performance
/// degradation, and a lock is not suitable for this situation indeed. With that in mind, this pointer
/// may be a good choice;
///
/// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`Sync`]: https://doc.rust-lang.org/std/marker/trait.Sync.html
/// [`Arc`]: https://doc.rust-lang.org/std/sync/struct.Arc.html
///
pub struct RcPointer<T: ?Sized> {
    ptr: NonNull<T>,
    count: Arc<AtomicUsize>,
}

unsafe impl<T: ?Sized + Send> Send for RcPointer<T> {}

impl<T> RcPointer<T> {
    #[cfg(not(nightly))]
    pub fn new(item: T) -> Self {
        let ptr = NonNull::new(Box::into_raw(Box::new(item))).unwrap();
        RcPointer { ptr, count: Arc::new(AtomicUsize::new(1)) }
    }

    #[cfg(nightly)]
    pub fn new(item: T) -> Self {
        let ptr = Box::into_raw_non_null(box item);
        RcPointer { ptr, count: Arc::new(AtomicUsize::new(1)) }
    }
}

impl<T: ?Sized> Clone for RcPointer<T> {
    fn clone(&self) -> Self {
        if self.count.fetch_add(1, Ordering::SeqCst) >= 1 {
            RcPointer { ptr: self.ptr, count: self.count.clone() }
        } else {
            unreachable!("already dropped;")
        }
    }
}

impl<T: ?Sized> Deref for RcPointer<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.ptr.as_ptr()) }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RcPointer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (&(*self.ptr.as_ptr())).fmt(f) }
    }
}

impl<T: ?Sized> Drop for RcPointer<T> {
    fn drop(&mut self) {
        if self.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            unsafe { std::ptr::drop_in_place(self.ptr.as_ptr()) }
        }
    }
}

pub struct UnsafeRcPtr<T: ?Sized> {
    ptr: Rc<T>,
}

unsafe impl<T: ?Sized + Send> Send for UnsafeRcPtr<T> {}

impl<T: ?Sized> Clone for UnsafeRcPtr<T> {
    fn clone(&self) -> Self {
        UnsafeRcPtr { ptr: self.ptr.clone() }
    }
}

impl<T: ?Sized> Deref for UnsafeRcPtr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.ptr
    }
}

impl<T: Sized> UnsafeRcPtr<T> {
    pub fn new(entry: T) -> Self {
        UnsafeRcPtr { ptr: Rc::new(entry) }
    }

    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        Rc::try_unwrap(this.ptr).map_err(|ptr| UnsafeRcPtr { ptr })
    }
}
