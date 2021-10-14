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

use std::fmt::{Debug, Formatter};

use pegasus_common::buffer::ReadBuffer;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};

use crate::progress::Weight;
use crate::tag::Tag;

pub trait Data: Clone + Send + Sync + Debug + Encode + Decode + 'static {}
impl<T: Clone + Send + Sync + Debug + Encode + Decode + 'static> Data for T {}

#[derive(Clone)]
pub struct EndByScope {
    pub(crate) tag: Tag,
    pub(crate) source: Weight,
    pub(crate) count: u64,
}

impl EndByScope {
    pub(crate) fn new(tag: Tag, source: Weight, count: u64) -> Self {
        EndByScope { tag, source, count }
    }

    pub(crate) fn merge(&mut self, other: EndByScope) {
        assert_eq!(self.tag, other.tag);
        self.source.merge(other.source);
        self.count += other.count;
    }

    pub(crate) fn contains_source(&self, src: u32) -> bool {
        self.count != 0 || self.source.contains_source(src)
    }
}

impl Debug for EndByScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "end({:?}_{})", self.tag, self.count)
    }
}

impl Encode for EndByScope {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl Decode for EndByScope {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

pub enum MarkedData<D> {
    Data(D),
    Marked(Option<D>, EndByScope),
}

pub struct MicroBatch<T> {
    /// the tag of scope this data set belongs to;
    pub tag: Tag,
    /// the index of worker who created this dataset;
    pub src: u32,
    /// sequence of the data batch;
    seq: u64,
    /// if this is the last batch of a scope;
    end: Option<EndByScope>,
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

    pub fn last(src: u32, end: EndByScope) -> Self {
        MicroBatch {
            tag: end.tag.clone(),
            src,
            seq: 0,
            end: Some(end),
            data: ReadBuffer::new(),
            is_discarded: false,
        }
    }

    pub fn set_end(&mut self, end: EndByScope) {
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

    pub fn get_end(&self) -> Option<&EndByScope> {
        self.end.as_ref()
    }

    pub fn get_end_mut(&mut self) -> Option<&mut EndByScope> {
        self.end.as_mut()
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn take_end(&mut self) -> Option<EndByScope> {
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

    pub fn is_discarded(&self) -> bool {
        self.is_discarded
    }
}

impl<D: Clone> MicroBatch<D> {
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = D> + '_ {
        &mut self.data
    }

    #[inline]
    pub fn drain_to_end(&mut self) -> impl Iterator<Item = MarkedData<D>> + '_ {
        let len = self.data.len();
        DrainEndIter { len, data: &mut self.data, end: &mut self.end, cur: 0 }
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
        self.end.write_to(writer)?;
        let len = self.data.len() as u64;
        writer.write_u64(len)?;
        for data in self.data.iter() {
            data.write_to(writer)?;
        }
        Ok(())
    }
}

impl<D: Data> Decode for MicroBatch<D> {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        // let tag = Tag::read_from(reader)?;
        // let seq = reader.read_u64()?;
        // let src = reader.read_u32()?;
        // let end = Option::<EndSignal>::read_from(reader)?;
        // let len = reader.read_u64()?;
        todo!("buffer reuse")
    }
}

struct DrainEndIter<'a, D: Clone> {
    len: usize,
    data: &'a mut ReadBuffer<D>,
    end: &'a mut Option<EndByScope>,
    cur: usize,
}

impl<'a, D: Clone> Iterator for DrainEndIter<'a, D> {
    type Item = MarkedData<D>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            if let Some(end) = self.end.take() {
                Some(MarkedData::Marked(None, end))
            } else {
                None
            }
        } else {
            if let Some(data) = self.data.next() {
                self.cur += 1;
                if self.cur == self.len {
                    // this maybe the last;
                    if let Some(end) = self.end.take() {
                        Some(MarkedData::Marked(Some(data), end))
                    } else {
                        Some(MarkedData::Data(data))
                    }
                } else {
                    Some(MarkedData::Data(data))
                }
            } else {
                None
            }
        }
    }
}
