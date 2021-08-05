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

use std::io;
use std::ops::{Deref, DerefMut};

pub use bytes::Bytes;
use bytes::{BufMut, BytesMut};

pub struct BytesSlab {
    bytes: BytesMut,
}

impl BytesSlab {
    /// Allocates a new `BytesSlab` with an initial size;
    pub fn new(size: usize) -> Self {
        BytesSlab { bytes: BytesMut::with_capacity(size) }
    }

    pub fn extract(&mut self) -> Bytes {
        let split = self.bytes.split();
        split.freeze()
    }

    pub fn extract_to(&mut self, len: usize) -> Option<Bytes> {
        if len > self.bytes.capacity() {
            None
        } else {
            Some(self.bytes.split_to(len).freeze())
        }
    }

    pub fn ensure_capacity(&mut self, capacity: usize) {
        self.bytes.reserve(capacity)
    }
}

impl std::io::Write for BytesSlab {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = std::cmp::min(self.bytes.remaining_mut(), buf.len());
        self.bytes.put(&buf[0..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.ensure_capacity(buf.len());
        self.bytes.put_slice(buf);
        Ok(())
    }
}

impl Deref for BytesSlab {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for BytesSlab {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

use std::cell::RefCell;
thread_local! {
    pub static SLAB: RefCell<BytesSlab> = RefCell::new(BytesSlab::new(1 << 20));
}
