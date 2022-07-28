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

use std::fmt::Debug;

use pegasus_common::buffer::{Buffer, ReadBuffer};
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};

use crate::progress::EndOfScope;
use crate::tag::Tag;

pub trait Data: Clone + Send + Sync + Debug + Encode + Decode + 'static {}
impl<T: Clone + Send + Sync + Debug + Encode + Decode + 'static> Data for T {}

pub struct MicroBatch<T> {
    /// the tag of scope this data set belongs to;
    pub tag: Tag,
    /// the index of worker who created this dataset;
    pub src: u32,
    /// sequence of the data batch;
    seq: u64,
    /// if this is the last batch of a scope;
    end: Option<EndOfScope>,
    /// read only data details;
    data: ReadBuffer<T>,

    is_discarded: bool,
}

#[allow(dead_code)]
impl<D> MicroBatch<D> {
    #[inline]
    pub fn empty() -> Self {
        MicroBatch {
            tag: Tag::Root,
            seq: 0,
            src: 0,
            end: None,
            data: ReadBuffer::new(),
            is_discarded: false,
        }
    }

    pub fn new(tag: Tag, src: u32, data: ReadBuffer<D>) -> Self {
        MicroBatch { tag, src, seq: 0, end: None, data, is_discarded: false }
    }

    pub fn last(src: u32, end: EndOfScope) -> Self {
        MicroBatch {
            tag: end.tag.clone(),
            src,
            seq: 0,
            end: Some(end),
            data: ReadBuffer::new(),
            is_discarded: false,
        }
    }

    pub fn set_end(&mut self, end: EndOfScope) {
        self.end = Some(end);
    }

    pub fn set_tag(&mut self, tag: Tag) {
        if let Some(end) = self.end.as_mut() {
            end.tag = tag.clone();
        }
        self.tag = tag;
    }

    pub fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    pub fn get_seq(&self) -> u64 {
        self.seq
    }

    pub fn is_last(&self) -> bool {
        self.end.is_some()
    }

    pub fn get_end(&self) -> Option<&EndOfScope> {
        self.end.as_ref()
    }

    pub fn get_end_mut(&mut self) -> Option<&mut EndOfScope> {
        self.end.as_mut()
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn take_end(&mut self) -> Option<EndOfScope> {
        self.end.take()
    }

    pub fn take_data(&mut self) -> ReadBuffer<D> {
        std::mem::replace(&mut self.data, ReadBuffer::new())
    }

    pub fn clear(&mut self) {
        self.take_data();
    }

    pub fn share(&mut self) -> Self {
        let shared = self.data.make_share();
        MicroBatch {
            tag: self.tag.clone(),
            src: self.src,
            seq: self.seq,
            end: self.end.clone(),
            data: shared,
            is_discarded: false,
        }
    }

    #[inline]
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    pub fn discard(&mut self) {
        self.is_discarded = true;
    }

    pub(crate) fn is_discarded(&self) -> bool {
        self.is_discarded
    }
}

impl<D: Clone> MicroBatch<D> {
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = D> + '_ {
        &mut self.data
    }
}

impl<D> Debug for MicroBatch<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "batch[{:?} len={}]", self.tag, self.data.len())
    }
}

impl<D> std::ops::Deref for MicroBatch<D> {
    type Target = ReadBuffer<D>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<D> std::ops::DerefMut for MicroBatch<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<D: Data> Clone for MicroBatch<D> {
    fn clone(&self) -> Self {
        MicroBatch {
            tag: self.tag.clone(),
            seq: self.seq,
            src: self.src,
            end: self.end.clone(),
            data: self.data.clone(),
            is_discarded: false,
        }
    }
}

impl<D: Data> Encode for MicroBatch<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u64(self.seq)?;
        writer.write_u32(self.src)?;
        let len = self.data.len() as u64;
        writer.write_u64(len)?;
        for data in self.data.iter() {
            data.write_to(writer)?;
        }
        self.end.write_to(writer)?;
        Ok(())
    }
}

impl<D: Data> Decode for MicroBatch<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let seq = reader.read_u64()?;
        let src = reader.read_u32()?;
        let len = reader.read_u64()? as usize;
        let mut buf = Buffer::with_capacity(len);
        for _ in 0..len {
            buf.push(D::read_from(reader)?);
        }
        let data = buf.into_read_only();
        let end = Option::<EndOfScope>::read_from(reader)?;
        Ok(MicroBatch { tag, src, seq, end, data, is_discarded: false })
    }
}
