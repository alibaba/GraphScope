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

use std::iter::Iterator;
use std::fmt;
use std::sync::Arc;
use std::any::Any;
use std::ops::{Deref, DerefMut};
use std::cell::RefCell;
use std::io;

pub trait Paging: Send {
    type Item;

    fn next_page(&mut self) -> Option<Vec<Self::Item>>;
}

impl<D, T: Iterator<Item=Vec<D>> + Send> Paging for T {
    type Item = D;

    fn next_page(&mut self) -> Option<Vec<Self::Item>> {
        self.next()
    }
}

#[derive(Debug, Default)]
pub struct OnePage<D> {
    inner: Option<Vec<D>>
}

impl<D: Send> OnePage<D> {
    #[inline]
    pub fn take(&mut self) -> Option<Vec<D>> {
        self.inner.take()
    }
}

impl<D> ::std::convert::From<Vec<D>> for OnePage<D> {
    #[inline]
    fn from(src: Vec<D>) -> Self {
        OnePage {
            inner: Some(src)
        }
    }
}

impl<D: Send> Paging for OnePage<D> {
    type Item = D;

    #[inline]
    fn next_page(&mut self) -> Option<Vec<Self::Item>> {
        self.take()
    }
}

/// Modify the origin vector, retain the elements whose `preicate` is true,
/// return a new vector consist of elements whose `preicate` is false;
/// This function is O(n);
/// # Note:
/// This function will change the order of elements in the origin vec;
/// # Examples
/// ```
///  use pegasus::common::retain;
///  // create a new numeric list;
///  let mut origin = (0..64).collect::<Vec<_>>();
///  // remove elements which is less than 32;
///  let removed = retain(&mut origin, |item| *item > 31);
///
///  assert_eq!(32, origin.len());
///  assert_eq!(32, removed.len());
///  assert!(origin.iter().all(|item| *item > 31));
///  assert!(removed.iter().all(|item| *item <= 31));
/// ```
#[inline]
pub fn retain<T, F: Fn(&T) -> bool>(origin: &mut Vec<T>, predicate: F) -> Vec<T> {
    let mut target = Vec::new();
    let mut index = origin.len();
    while index > 0 {
        if !predicate(&origin[index - 1]) {
            let item = if index == origin.len() {
                origin.remove(index - 1)
            } else {
                origin.swap_remove(index - 1)
            };
            target.push(item);
        }
        index -= 1;
    }
    target
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct Port {
    pub index: usize,
    pub port: usize,
}

impl Port {
    #[inline(always)]
    pub fn first(index: usize) -> Self {
        Port {index, port: 0}
    }

    #[inline(always)]
    pub fn second(index: usize) -> Self {
        Port {index, port: 1}
    }

    #[inline(always)]
    pub fn new(index: usize, port: usize) -> Self {
        Port {index, port}
    }
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}.{})", self.index, self.port)
    }
}

#[derive(Copy,Clone)]
struct Slice(pub *mut u8, pub usize);

impl Slice {
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            ::std::slice::from_raw_parts(self.0, self.1)
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            ::std::slice::from_raw_parts_mut(self.0, self.1)
        }
    }
}

pub struct Bytes {
    slice: Slice,
    backend: Arc<Box<dyn Any>>
}

impl Bytes {
    pub fn from<B>(bytes: B) -> Self where B: DerefMut<Target=[u8]> + 'static {
        let mut boxed = Box::new(bytes) as Box<dyn Any>;

        let ptr = boxed.downcast_mut::<B>().unwrap().as_mut_ptr();
        let len = boxed.downcast_ref::<B>().unwrap().len();
        let backend = Arc::new(boxed);
        let slice = Slice(ptr, len);
        Bytes {
            slice,
            backend
        }
    }

    pub fn extract_to(&mut self, index: usize) -> Bytes {
        assert!(index <= self.slice.1);

        let result = Bytes {
            slice: Slice(self.slice.0, index),
            backend: self.backend.clone(),
        };

        unsafe { self.slice.0 = self.slice.0.offset(index as isize); }
        self.slice.1 -= index;

        result
    }

    pub fn try_recover<B>(self) -> Result<B, Bytes> where B: DerefMut<Target=[u8]>+'static {
        match Arc::try_unwrap(self.backend) {
            Ok(bytes) => Ok(*bytes.downcast::<B>().unwrap()),
            Err(arc) => Err(Bytes {
                slice: self.slice,
                backend: arc,
            }),
        }
    }

    pub fn try_regenerate<B>(&mut self) -> bool where B: DerefMut<Target=[u8]>+'static {
        if let Some(boxed) = Arc::get_mut(&mut self.backend) {
            let downcast = boxed.downcast_mut::<B>().expect("Downcast failed");
            self.slice = Slice(downcast.as_mut_ptr(), downcast.len());
            true
        }
        else {
            false
        }
    }
}

impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.slice.as_slice()
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.slice.as_mut_slice()
    }
}

impl AsRef<[u8]> for Bytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.slice.as_slice()
    }
}

/// It is carefully managed to make sure that no overlap of byte range between threads;
unsafe impl Send for Bytes { }

/// All Bytes are only be transformed between threads through mpmc channel, no sync;
// unsafe impl Sync for Bytes { }

/// A large binary allocation for writing and sharing.
///
/// A bytes slab wraps a `Bytes` and maintains a valid (written) length, and supports writing after
/// this valid length, and extracting `Bytes` up to this valid length. Extracted bytes are enqueued
/// and checked for uniqueness in order to recycle them (once all shared references are dropped).
pub struct BytesSlab {
    buffer:         Bytes,                      // current working buffer.
    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Bytes>,                 // reclaimed and resuable buffers.
    shift:          usize,                      // current buffer allocation size.
    valid:          usize,                      // buffer[..valid] are valid bytes.
}

impl BytesSlab {
    /// Allocates a new `BytesSlab` with an initial size determined by a shift.
    pub fn new(shift: usize) -> Self {
        BytesSlab {
            buffer: Bytes::from(vec![0u8; 1 << shift].into_boxed_slice()),
            in_progress: Vec::new(),
            stash: Vec::new(),
            shift,
            valid: 0,
        }
    }
    /// The empty region of the slab.
    #[inline]
    pub fn empty(&mut self) -> &mut [u8] {
        &mut self.buffer[self.valid..]
    }
    /// The valid region of the slab.
    #[inline]
    pub fn valid(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.valid]
    }

    #[inline]
    pub fn valid_len(&self) -> usize {
        self.valid
    }

    /// Marks the next `bytes` bytes as valid.
    pub fn make_valid(&mut self, bytes: usize) {
        self.valid += bytes;
    }
    /// Extracts the first `bytes` valid bytes.
    pub fn extract(&mut self, bytes: usize) -> Bytes {
        debug_assert!(bytes <= self.valid);
        self.valid -= bytes;
        self.buffer.extract_to(bytes)
    }

    pub fn extract_valid(&mut self) -> Option<Bytes> {
        if self.valid > 0 {
            Some(self.extract(self.valid))
        } else {
            None
        }
    }

    /// Ensures that `self.empty().len()` is at least `capacity`.
    ///
    /// This method may retire the current buffer if it does not have enough space, in which case
    /// it will copy any remaining contents into a new buffer. If this would not create enough free
    /// space, the shift is increased until it is sufficient.
    pub fn ensure_capacity(&mut self, capacity: usize) {

        if self.empty().len() < capacity {

            let mut increased_shift = false;

            // Increase allocation if copy would be insufficient.
            while self.valid + capacity > (1 << self.shift) {
                self.shift += 1;
                self.stash.clear();         // clear wrongly sized buffers.
                self.in_progress.clear();   // clear wrongly sized buffers.
                increased_shift = true;
            }

            // Attempt to reclaim shared slices.
            if self.stash.is_empty() {
                for shared in self.in_progress.iter_mut() {
                    if let Some(mut bytes) = shared.take() {
                        if bytes.try_regenerate::<Box<[u8]>>() {
                            // NOTE: Test should be redundant, but better safe...
                            if bytes.len() == (1 << self.shift) {
                                self.stash.push(bytes);
                            }
                        }
                        else {
                            *shared = Some(bytes);
                        }
                    }
                }
                self.in_progress.retain(|x| x.is_some());
            }

            let new_buffer = self.stash.pop().unwrap_or_else(|| Bytes::from(vec![0; 1 << self.shift].into_boxed_slice()));
            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

            self.buffer[.. self.valid].copy_from_slice(&old_buffer[.. self.valid]);
            if !increased_shift {
                self.in_progress.push(Some(old_buffer));
            }
        }
    }
}


macro_rules! write_number {
    ($ty:ty, $n:expr, $slab:expr) => ({
        let size = ::std::mem::size_of::<$ty>();
        $slab.ensure_capacity(size);
        let dst = $slab.empty();
        if dst.len() < size {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        } else {
            unsafe {
                let src = *(&$n.to_le() as *const _ as *const [u8;::std::mem::size_of::<$ty>()]);
                ::std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size);
            }
            $slab.make_valid(size);
        }
    })
}

impl BytesSlab {
    pub fn write_u64(&mut self, v: u64) -> Result<(), io::Error> {
        write_number!(u64, v, self);
        Ok(())
    }

    pub fn write_u32(&mut self, v: u32) -> Result<(), io::Error> {
        write_number!(u32, v, self);
        Ok(())
    }

    pub fn write_u16(&mut self, v: u16) -> Result<(), io::Error> {
        write_number!(u16, v, self);
        Ok(())
    }

    pub fn try_read(&mut self, length: usize) -> Option<Bytes> {
        if self.valid < length {
            None
        } else {
            Some(self.extract(length))
        }
    }
}


impl io::Write for BytesSlab {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let length = buf.len();
        self.ensure_capacity(length);
        let empty = self.empty();
        if empty.len() < length {
            Ok(0)
        } else {
            unsafe {
                ::std::ptr::copy_nonoverlapping(buf.as_ptr(), empty.as_mut_ptr(), length);
            }
            self.make_valid(length);
            Ok(length)
        }
    }

    #[inline(always)]
    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

macro_rules! read_number {
    ($ty:ty, $src:expr) => ({
        let size = ::std::mem::size_of::<$ty>();
        if $src.len() < size {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let r = $src.extract_to(size);
        let mut tmp: $ty = 0;
        unsafe {
            ::std::ptr::copy_nonoverlapping(
                r.as_ptr(),
                &mut tmp as *mut $ty as *mut u8,
                size
            )
        }
        tmp.to_le()
    })
}

impl Bytes {
    pub fn read_u64(&mut self) -> Result<u64, io::Error> {
        let r = read_number!(u64, self);
        Ok(r)
    }

    pub fn read_u32(&mut self) -> Result<u32, io::Error> {
        let r =  read_number!(u32, self);
        Ok(r)
    }

    pub fn read_u16(&mut self) -> Result<u16, io::Error> {
        let r = read_number!(u16, self);
        Ok(r)
    }
}

thread_local! {
    pub static SLAB: RefCell<BytesSlab> = RefCell::new(BytesSlab::new(20));
}

#[cfg(test)]
mod test {
    use crate::common::BytesSlab;

    #[test]
    fn test_write_read_number() {
        let mut slab = BytesSlab::new(10);
        slab.write_u64(0).unwrap();
        slab.write_u64(1).unwrap();
        slab.write_u32(2).unwrap();
        slab.write_u32(3).unwrap();
        assert_eq!(slab.valid_len(), 24);
        let mut valid = slab.extract_valid().expect("unreachable");
        assert_eq!(valid.read_u64().unwrap(), 0);
        assert_eq!(valid.read_u64().unwrap(), 1);
        assert_eq!(valid.read_u32().unwrap(), 2);
        assert_eq!(valid.read_u32().unwrap(), 3);
    }



}
