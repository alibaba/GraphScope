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

use std::fmt;
use std::hash::Hash;
use std::ops::AddAssign;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

/// Trait for the unsigned integer type used for node and edge indices.
///
/// Marked `unsafe` because: the trait must faithfully preserve
/// and convert index values.
pub unsafe trait IndexType:
    Copy + Default + Hash + Ord + fmt::Debug + 'static + AddAssign + Send + Sync
{
    fn new(x: usize) -> Self;
    fn index(&self) -> usize;
    fn max() -> Self;

    fn add_assign(&mut self, other: Self);

    fn read<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>;
    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()>;
}

unsafe impl IndexType for usize {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x
    }
    #[inline(always)]
    fn index(&self) -> Self {
        *self
    }
    #[inline(always)]
    fn max() -> Self {
        ::std::usize::MAX
    }

    #[inline(always)]
    fn add_assign(&mut self, other: Self) {
        *self += other;
    }

    fn read<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let ret = reader.read_u64::<LittleEndian>()? as usize;
        Ok(ret)
    }

    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(*self as u64)
    }
}

unsafe impl IndexType for u32 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u32
    }
    #[inline(always)]
    fn index(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max() -> Self {
        ::std::u32::MAX
    }

    #[inline(always)]
    fn add_assign(&mut self, other: Self) {
        *self += other;
    }

    fn read<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let ret = reader.read_u32::<LittleEndian>()?;
        Ok(ret)
    }

    fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<LittleEndian>(*self)
    }
}

// Index into the NodeIndex and EdgeIndex arrays
/// Edge direction.
#[derive(Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(usize)]
pub enum Direction {
    /// An `Outgoing` edge is an outward edge *from* the current node.
    Outgoing = 0,
    /// An `Incoming` edge is an inbound edge *to* the current node.
    Incoming = 1,
}

impl Direction {
    /// Return the opposite `Direction`.
    #[inline]
    pub fn opposite(self) -> Direction {
        match self {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
        }
    }

    /// Return `0` for `Outgoing` and `1` for `Incoming`.
    #[inline]
    pub fn index(self) -> usize {
        (self as usize) & 0x1
    }
}

impl Clone for Direction {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
