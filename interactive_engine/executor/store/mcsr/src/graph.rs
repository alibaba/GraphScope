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

use pegasus_common::codec::ReadExt;
use pegasus_common::io::WriteExt;

/// Trait for the unsigned integer type used for node and edge indices.
///
/// Marked `unsafe` because: the trait must faithfully preserve
/// and convert index values.
pub unsafe trait IndexType: Copy + Default + Hash + Ord + fmt::Debug + 'static + AddAssign {
    fn new(x: usize) -> Self;
    fn index(&self) -> usize;
    fn max() -> Self;

    fn add_assign(&mut self, other: Self);

    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()>;
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self>;

    fn hi(&self) -> Self;
    fn lo(&self) -> Self;

    fn bw_or(&self, other: Self) -> Self;

    fn bw_and(&self, other: Self) -> Self;
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
        // 1_usize << 48
    }

    #[inline(always)]
    fn add_assign(&mut self, other: Self) {
        *self += other;
    }

    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(*self as u64)
    }

    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        Ok(reader.read_u64()? as Self)
    }

    #[inline(always)]
    fn hi(&self) -> Self {
        self >> 32
    }

    #[inline(always)]
    fn lo(&self) -> Self {
        self & 0b1111_1111_1111_1111_1111_1111_1111_1111
    }

    #[inline(always)]
    fn bw_or(&self, other: Self) -> Self {
        self | other
    }

    #[inline(always)]
    fn bw_and(&self, other: Self) -> Self {
        self & other
    }
}

// unsafe impl IndexType for u32 {
//     #[inline(always)]
//     fn new(x: usize) -> Self {
//         x as u32
//     }
//     #[inline(always)]
//     fn index(&self) -> usize {
//         *self as usize
//     }
//     #[inline(always)]
//     fn max() -> Self {
//         ::std::u32::MAX
//     }
//     #[inline(always)]
//     fn add_assign(&mut self, other: Self) {
//         *self += other;
//     }
//
//     fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
//         writer.write_u32(*self)
//     }
//
//     fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
//         Ok(reader.read_u32()?)
//     }
// }

// unsafe impl IndexType for u16 {
//     #[inline(always)]
//     fn new(x: usize) -> Self {
//         x as u16
//     }
//     #[inline(always)]
//     fn index(&self) -> usize {
//         *self as usize
//     }
//     #[inline(always)]
//     fn max() -> Self {
//         ::std::u16::MAX
//     }
//     #[inline(always)]
//     fn add_assign(&mut self, other: Self) {
//         *self += other;
//     }
// }
//
// unsafe impl IndexType for u8 {
//     #[inline(always)]
//     fn new(x: usize) -> Self {
//         x as u8
//     }
//     #[inline(always)]
//     fn index(&self) -> usize {
//         *self as usize
//     }
//     #[inline(always)]
//     fn max() -> Self {
//         ::std::u8::MAX
//     }
//     #[inline(always)]
//     fn add_assign(&mut self, other: Self) {
//         *self += other;
//     }
// }

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
