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

use std::io;
use crate::common::{BytesSlab, Bytes, SLAB};

/// These traits define how to encode message into binary and decoding it;
/// Different with the `Serialize` & `Deserialize` in the `serde` lib, the message implement these traits
/// can be transformed across different platforms through network and other ways;
/// It is usually used in the `Source` and `Sink` operators, while the former receive messsages
/// from an external system, and the latter send messages to other external system;

/// The encode part;
pub trait CPSerialize {
    /// inspect the length of bytes after serialize;
    fn serialize_len(&self) -> usize;
    /// serialize current message and write it into a binary target;
    /// the wrote length must be equals to the `serialize_len` method's return value;
    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error>;
}

/// The decode part;
pub trait CPDeserialize: Sized {
    fn read_from(bytes: Bytes) -> Result<Self, io::Error>;
}

pub fn write_binary<B: CPSerialize>(response: &B) -> Result<Bytes, ::std::io::Error> {
    SLAB.with(|slab| {
        let mut slab = slab.borrow_mut();
        let length = response.serialize_len();
        response.write_to(&mut slab).map(|_| slab.extract(length))
    })
}
