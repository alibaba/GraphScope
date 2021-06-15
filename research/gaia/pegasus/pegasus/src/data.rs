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

use crate::tag::Tag;
use crossbeam_channel::Sender;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use std::fmt::Debug;

pub trait Data: Clone + Send + Debug + Encode + Decode + 'static {}
impl<T: Clone + Send + Debug + Encode + Decode + 'static> Data for T {}

pub struct DataSet<T> {
    pub tag: Tag,
    data: Vec<T>,
    recycle_hook: Option<Sender<Vec<T>>>,
}

impl<D> DataSet<D> {
    #[inline]
    pub fn empty() -> Self {
        DataSet { tag: Tag::Root, data: Vec::new(), recycle_hook: None }
    }

    pub fn new<T: Into<Tag>>(tag: T, data: Vec<D>) -> Self {
        let tag: Tag = tag.into();
        DataSet { tag, data, recycle_hook: None }
    }

    pub fn with_hook<T: Into<Tag>>(tag: T, data: Vec<D>, creator_hook: &Sender<Vec<D>>) -> Self {
        let tag: Tag = tag.into();
        DataSet { tag, data, recycle_hook: Some(creator_hook.clone()) }
    }

    #[inline]
    pub fn tag(&self) -> Tag {
        self.tag.clone()
    }

    #[inline]
    pub fn data(&mut self) -> &mut Vec<D> {
        &mut self.data
    }

    #[inline]
    pub fn drain_into(&mut self) -> DataSetIter<D> {
        DataSetIter {
            data: std::mem::replace(&mut self.data, vec![]),
            hook: self.recycle_hook.take(),
            cursor: 0,
        }
    }
}

impl<D> Debug for DataSet<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dataset[{:?}, len={}]", self.tag, self.data.len())
    }
}

impl<D> std::ops::Deref for DataSet<D> {
    type Target = Vec<D>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<D> std::ops::DerefMut for DataSet<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> Drop for DataSet<T> {
    fn drop(&mut self) {
        if let Some(ref hook) = self.recycle_hook {
            if self.data.capacity() > 0 {
                self.data.clear();
                hook.try_send(std::mem::replace(&mut self.data, vec![])).ok();
            }
        }
    }
}

impl<T> AsRef<Tag> for DataSet<T> {
    fn as_ref(&self) -> &Tag {
        &self.tag
    }
}

impl<D: Data> Encode for DataSet<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u32(self.data.len() as u32)?;
        for item in self.data.iter() {
            item.write_to(writer)?;
        }
        Ok(())
    }
}

impl<D: Data> Decode for DataSet<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> ::std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let len = reader.read_u32()? as usize;
        let mut data = Vec::with_capacity(len);
        for _ in 0..len {
            let item = D::read_from(reader)?;
            data.push(item);
        }
        Ok(DataSet::new(tag, data))
    }
}

impl<D: Data> Clone for DataSet<D> {
    fn clone(&self) -> Self {
        DataSet {
            tag: self.tag.clone(),
            data: self.data.clone(),
            recycle_hook: self.recycle_hook.clone(),
        }
    }
}

pub struct DataSetIter<D> {
    data: Vec<D>,
    hook: Option<Sender<Vec<D>>>,
    cursor: usize,
}

impl<D> DataSetIter<D> {
    fn take_next(&mut self) -> Option<D> {
        let len = self.data.len();
        if len == 0 {
            None
        } else if self.cursor < len {
            let datum = self.data.swap_remove(self.cursor);
            self.cursor += 1;
            Some(datum)
        } else {
            self.data.pop()
        }
    }
}

impl<D> Iterator for DataSetIter<D> {
    type Item = D;

    fn next(&mut self) -> Option<Self::Item> {
        self.take_next()
    }
}

impl<D> Drop for DataSetIter<D> {
    fn drop(&mut self) {
        if self.data.capacity() > 0 {
            let buffer = std::mem::replace(&mut self.data, vec![]);
            if let Some(ref hook) = self.hook {
                hook.try_send(buffer).ok();
            }
        }
    }
}
