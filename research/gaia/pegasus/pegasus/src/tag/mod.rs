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

//! Tags implementation.

use pegasus_common::codec::*;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io;
/// Hierarchical tag which identify the data in stream;
///
/// The hierarchy of tag is like a tree, each tag instance has a parent(except the ROOT),
/// and many children. The root of the tag tree is the `ROOT` tag;
///
/// Each tag consists of it's parent and a integer number except the `ROOT` tag;
///
/// In the implementation, we use an array of 32-bits integer to represent tag. The `ROOT` tag is an empty array,
/// all others will copy it's parent, and extend with a new unique integer;
///
#[derive(Clone)]
pub enum Tag {
    Inline { length: u8, data: [u32; TAG_INLINE_LEN] },
    Spilled(Box<[u32]>),
}

#[derive(Debug)]
pub enum TagError {
    RootAdvance,
    AdvanceOverflow,
    RetreatOverflow,
}

impl ::std::fmt::Display for TagError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ::std::error::Error for TagError {}

pub type Result = ::std::result::Result<(), TagError>;

pub const TAG_INLINE_LEN: usize = 3;
const TAG_INLINE_LEN_U8: u8 = 3;

lazy_static! {
    pub static ref ROOT: Tag = Tag::Inline { length: 0, data: [0, 0, 0] };
}

impl Tag {
    pub fn root() -> Self {
        ROOT.clone()
    }

    pub(crate) fn inherit(parent: &Tag, current: u32) -> Self {
        let len = parent.len();
        if len < TAG_INLINE_LEN {
            let mut data = [0; TAG_INLINE_LEN];
            if len > 0 {
                data[..len].clone_from_slice(parent.as_slice());
            }
            data[len] = current;
            Tag::Inline { length: len as u8 + 1, data }
        } else {
            let mut data = Vec::with_capacity(len + 1);
            data.extend_from_slice(parent.as_slice());
            data.push(current);
            Tag::Spilled(data.into_boxed_slice())
        }
    }

    #[inline]
    pub fn from_vec(vec: Vec<u32>) -> Tag {
        let length = vec.len();
        if length == 0 {
            ROOT.clone()
        } else if length <= TAG_INLINE_LEN {
            let mut data = [0; TAG_INLINE_LEN];
            data[..length].copy_from_slice(vec.as_slice());
            Tag::Inline { length: length as u8, data }
        } else {
            Tag::Spilled(vec.into_boxed_slice())
        }
    }

    #[inline]
    pub fn new(cur: u32) -> Self {
        Tag::Inline { length: 1, data: [cur, 0, 0] }
    }

    /// Get the current integer identification. Return `None` if the tag is `ROOT`;
    #[inline]
    pub fn current(&self) -> Option<u32> {
        match self {
            Tag::Inline { length, data } => {
                if *length == 0 {
                    None
                } else {
                    Some(data[*length as usize - 1])
                }
            }
            Tag::Spilled(vec) => Some(vec[vec.len() - 1]),
        }
    }

    #[inline]
    pub fn current_uncheck(&self) -> u32 {
        self.current().expect("can't current on root tag")
    }

    #[inline]
    pub fn parent(&self) -> Option<&[u32]> {
        if self.len() == 0 {
            None
        } else {
            Some(&self.as_slice()[0..self.len() - 1])
        }
    }

    #[inline]
    pub fn to_parent(&self) -> Option<Tag> {
        if self.len() == 0 {
            None
        } else {
            match self {
                Tag::Inline { length, data } => {
                    let mut new_data = *data;
                    new_data[*length as usize - 1] = 0;
                    Some(Tag::Inline { length: length - 1, data: new_data })
                }
                Tag::Spilled(vec) => {
                    let len = vec.len();
                    if len > TAG_INLINE_LEN {
                        if len - 1 == TAG_INLINE_LEN {
                            let mut data = [0u32; TAG_INLINE_LEN];
                            data[..TAG_INLINE_LEN].copy_from_slice(&vec[0..TAG_INLINE_LEN]);
                            Some(Tag::Inline { length: TAG_INLINE_LEN_U8, data })
                        } else {
                            let mut new_vec = vec.to_vec();
                            new_vec.pop().unwrap();
                            Some(Tag::Spilled(new_vec.into_boxed_slice()))
                        }
                    } else {
                        let mut data = [0u32; TAG_INLINE_LEN];
                        let length = len - 1;
                        let (front, _) = data.split_at_mut(length);
                        front.copy_from_slice(&vec[0..length]);
                        Some(Tag::Inline { length: length as u8, data })
                    }
                }
            }
        }
    }

    pub fn split(&self) -> Option<(Tag, u32)> {
        if self.len() == 0 {
            None
        } else if self.len() == 1 {
            Some((ROOT.clone(), self.current_uncheck()))
        } else {
            match self {
                Tag::Inline { length, data } => {
                    let new_data = *data;
                    let cur = new_data[*length as usize - 1];
                    Some((Tag::Inline { length: length - 1, data: new_data }, cur))
                }
                Tag::Spilled(vec) => {
                    let len = vec.len();
                    if len > TAG_INLINE_LEN {
                        if len - 1 == TAG_INLINE_LEN {
                            let mut data = [0u32; TAG_INLINE_LEN];
                            data[..TAG_INLINE_LEN].copy_from_slice(&vec[0..TAG_INLINE_LEN]);
                            let cur = vec[len - 1];
                            Some((Tag::Inline { length: TAG_INLINE_LEN_U8, data }, cur))
                        } else {
                            let mut new_vec = vec.to_vec();
                            let cur = new_vec.pop().unwrap();
                            Some((Tag::Spilled(new_vec.into_boxed_slice()), cur))
                        }
                    } else {
                        let cur = vec[len - 1];
                        let mut data = [0u32; TAG_INLINE_LEN];
                        let length = len - 1;
                        let (front, _) = data.split_at_mut(length);
                        front.copy_from_slice(&vec[0..length]);
                        Some((Tag::Inline { length: length as u8, data }, cur))
                    }
                }
            }
        }
    }

    #[inline]
    pub fn to_parent_uncheck(&self) -> Tag {
        self.to_parent().expect("To parent failure")
    }

    #[inline]
    pub fn advance(&mut self) -> Result {
        let len = self.len();
        if len > 0 {
            let data = self.as_mut_slice();
            if data[len - 1] == !0u32 {
                Err(TagError::AdvanceOverflow)
            } else {
                data[len - 1] += 1;
                Ok(())
            }
        } else {
            Err(TagError::RootAdvance)
        }
    }

    #[inline]
    pub fn advance_unchecked(&mut self) {
        self.advance().expect("Advance tag failure;")
    }

    #[inline]
    pub fn advance_to(&mut self, cur: u32) -> Result {
        if self.len() > 0 {
            match self {
                Tag::Inline { length, data } => {
                    data[*length as usize - 1] = cur;
                }
                Tag::Spilled(vec) => {
                    let len = vec.len();
                    vec[len - 1] = cur;
                }
            }
            Ok(())
        } else {
            Err(TagError::RootAdvance)
        }
    }

    #[inline]
    pub fn retreat(&mut self) -> Result {
        if self.len() > 0 {
            match self {
                Tag::Inline { length, data } => {
                    if data[*length as usize - 1] == 0 {
                        Err(TagError::RetreatOverflow)
                    } else {
                        data[*length as usize - 1] -= 1;
                        Ok(())
                    }
                }
                Tag::Spilled(vec) => {
                    let len = vec.len();
                    if vec[len - 1] == 0 {
                        Err(TagError::RetreatOverflow)
                    } else {
                        vec[len - 1] -= 1;
                        Ok(())
                    }
                }
            }
        } else {
            Err(TagError::RetreatOverflow)
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        match self {
            Tag::Inline { length, data } => &data[0..(*length as usize)],
            Tag::Spilled(vec) => vec,
        }
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u32] {
        match self {
            Tag::Inline { length, data } => &mut data[0..(*length as usize)],
            Tag::Spilled(vec) => vec,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Tag::Inline { length, data: _ } => *length as usize,
            Tag::Spilled(vec) => vec.len(),
        }
    }

    #[inline]
    pub fn is_parent_of(&self, other: &Tag) -> bool {
        let len = self.len();
        if len == 0 {
            true
        } else {
            if len >= other.len() {
                false
            } else {
                let x1 = self.as_slice();
                let x2 = other.as_slice();
                for i in 1..len + 1 {
                    if x1[len - i] != x2[len - i] {
                        return false;
                    }
                }
                true
            }
        }
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        self.len() == 0
    }
}

impl Hash for Tag {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self.as_slice(), state);
    }
}

impl PartialEq for Tag {
    #[inline]
    fn eq(&self, other: &Tag) -> bool {
        let x1 = self.as_slice();
        let x2 = other.as_slice();
        let len = x1.len();
        if len != x2.len() {
            return false;
        } else {
            let up = len + 1;
            for i in 1..up {
                let offset = len - i;
                if x1[offset] != x2[offset] {
                    return false;
                }
            }
            true
        }
    }
}

impl Eq for Tag {}

impl Default for Tag {
    fn default() -> Self {
        ROOT.clone()
    }
}

impl ::std::fmt::Debug for Tag {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        if self.len() == 0 {
            write!(f, "ROOT")
        } else {
            write!(f, "{:?}", self.as_slice())
        }
    }
}

impl ::std::fmt::Display for Tag {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        if self.len() == 0 {
            write!(f, "ROOT")
        } else {
            write!(f, "{:?}", self.as_slice())
        }
    }
}

impl AsRef<Tag> for Tag {
    fn as_ref(&self) -> &Tag {
        self
    }
}

impl From<u32> for Tag {
    #[inline]
    fn from(cur: u32) -> Self {
        Tag::new(cur)
    }
}

impl From<[u32; 2]> for Tag {
    #[inline]
    fn from(array: [u32; 2]) -> Self {
        Tag::Inline { length: 2, data: [array[0], array[1], 0] }
    }
}

impl From<[u32; 3]> for Tag {
    #[inline]
    fn from(data: [u32; 3]) -> Self {
        Tag::Inline { length: 3, data }
    }
}

impl From<Vec<u32>> for Tag {
    #[inline]
    fn from(vec: Vec<u32>) -> Self {
        Tag::from_vec(vec)
    }
}

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.len() == other.len() {
            if self.eq(other) {
                Some(Ordering::Equal)
            } else {
                None
            }
        } else if self.len() < other.len() {
            if self.is_parent_of(other) {
                Some(Ordering::Greater)
            } else {
                None
            }
        } else {
            if other.is_parent_of(self) {
                Some(Ordering::Less)
            } else {
                None
            }
        }
    }
}

impl Encode for Tag {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        let len = self.len();
        writer.write_u8(len as u8)?;
        let size = len * ::std::mem::size_of::<u32>();
        if size > 0 {
            let ptr = self.as_slice().as_ptr() as *const u8;
            let buf = unsafe { std::slice::from_raw_parts(ptr, size) };
            writer.write_all(buf)?;
        }
        Ok(())
    }
}

impl Decode for Tag {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Tag> {
        let len = reader.read_u8()? as usize;
        if len == 0 {
            Ok(Tag::root())
        } else if len == 1 {
            let cur = reader.read_u32()?;
            Ok(Tag::new(cur))
        } else if len <= 3 {
            let mut array = [0u32; 3];
            let length = len * std::mem::size_of::<u32>();
            let bytes =
                unsafe { std::slice::from_raw_parts_mut(array.as_mut_ptr() as *mut u8, length) };
            reader.read_exact(bytes)?;
            Ok(Tag::Inline { length: len as u8, data: array })
        } else {
            let mut array = vec![0u32; len as usize];
            let length = len * std::mem::size_of::<u32>();
            let bytes =
                unsafe { std::slice::from_raw_parts_mut(array.as_mut_ptr() as *mut u8, length) };
            reader.read_exact(bytes)?;
            Ok(Tag::from_vec(array))
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum TagMatcher {
    Equals(Tag),
    Prefix(Tag),
}

#[allow(dead_code)]
impl TagMatcher {
    pub fn matches(&self, other: &Tag) -> bool {
        match self {
            TagMatcher::Equals(this) => this == other,
            TagMatcher::Prefix(this) => this.is_parent_of(other),
        }
    }
}

#[macro_export]
macro_rules! tag {
    ($elem:expr) => ( Tag::new($elem as u32) );
    ($elem1:expr, $elem2:expr) => ( Tag::from([$elem1 as u32, $elem2 as u32]) );
    ($elem1:expr, $elem2:expr, $elem3: expr) => ( Tag::from([$elem1 as u32, $elem2 as u32, $elem3 as u32]) );
    ($elem:expr; $n:expr) => ( Tag::from(std::vec::from_elem($elem as u32, $n)) );
    ($($x:expr),*) => ( Tag::from_vec( <[_]>::into_vec(Box::new([$($x as u32),*]))) );
}

pub mod tools;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn tag_macro_test() {
        let t1 = tag!(1);
        assert_eq!(t1.as_slice(), &[1]);
        let t2 = tag![1, 2];
        assert_eq!(t2.as_slice(), &[1, 2]);
        let t3 = tag![1, 2, 3];
        assert_eq!(t3.as_slice(), &[1, 2, 3]);
        let t4 = tag![1; 4];
        assert_eq!(t4.as_slice(), &[1, 1, 1, 1]);
        let t5 = tag![1, 2, 3, 4];
        assert_eq!(t5.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn tag_test() {
        let mut tag1 = tag![3];
        let mut tag2 = tag![3, 2];
        assert_eq!(tag1.len(), 1);
        assert_eq!(tag2.len(), 2);

        assert_eq!(tag1, 3.into());
        assert_eq!(tag2, Tag::from_vec(vec![3, 2]));
        assert_eq!(tag1.current_uncheck(), 3);
        assert_eq!(tag2.current_uncheck(), 2);
        assert_eq!(tag2.parent().unwrap(), &[3]);
        assert_eq!(tag2.to_parent().unwrap(), tag1);
        assert_eq!(tag1.parent().unwrap().len(), 0);
        assert_eq!(tag1.to_parent().unwrap(), Tag::root());
        assert_eq!(Tag::root().parent(), None);
        assert_eq!(Tag::root().to_parent(), None);

        ///////////// advance /////////////
        tag2.advance_unchecked();
        assert_eq!(tag2, tag![3, 3]);
        assert_eq!(tag2, Tag::inherit(&tag1, 3));

        tag1.advance_unchecked();
        assert_eq!(tag1, Tag::new(4));

        let mut t = tag![1, 3];
        t.advance_unchecked();
        assert_eq!(t, tag![1, 4]);

        let mut t = tag![1, 3, 5];
        t.advance_unchecked();
        assert_eq!(t, tag![1, 3, 6]);

        let mut t = tag![1, 3, 5, 7];
        t.advance_unchecked();
        assert_eq!(t, tag![1, 3, 5, 8]);

        ///////////// retreat /////////////
        let mut t = tag![1, 3];
        t.retreat().unwrap();
        assert_eq!(t, tag![1, 2]);

        let mut t = tag![1, 3, 5];
        t.retreat().unwrap();
        assert_eq!(t, tag![1, 3, 4]);

        let mut t = tag![1, 3, 5, 7];
        t.retreat().unwrap();
        assert_eq!(t, tag![1, 3, 5, 6]);
    }

    #[test]
    fn tag_hash_eq_test() {
        let tag1 = Tag::Inline { length: 2, data: [1, 2, 0] };
        let tag2 = Tag::Spilled(vec![1, 2].into_boxed_slice());
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);

        let tag1 = Tag::Inline { length: 3, data: [1, 2, 3] };
        let tag2 = Tag::Spilled(vec![1, 2, 3].into_boxed_slice());
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);

        let tag1 = tag![0, 188545];
        let tag2 = tag![0, 188546];
        assert_ne!(tag1, tag2);
    }

    #[test]
    fn tag_parent_test() {
        let tag1 = tag!(1);
        let tag12 = Tag::inherit(&tag1, 2);
        let tag123 = Tag::inherit(&tag12, 3);
        assert!(tag1.is_parent_of(&tag12));
        assert!(tag12.is_parent_of(&tag123));
        let tag14 = tag![1, 4];
        assert!(tag1.is_parent_of(&tag14));
    }

    #[ignore]
    #[test]
    fn tag_serialize() {
        // {
        //     let mut bytes = pegasus_common::serialize::encode(&Tag::root()).unwrap();
        //     let decode = Tag::read_from(&mut bytes).unwrap();
        //     assert_eq!(decode, Tag::root());
        // }
        // {
        //     let tag = tag![1];
        //     let mut bytes = pegasus_common::serialize::encode(&tag).unwrap();
        //     let decode = Tag::read_from(&mut bytes).unwrap();
        //     assert_eq!(decode, tag);
        // }
        //
        // {
        //     let tag = tag![1, 2, 3];
        //     let mut bytes = pegasus_common::serialize::encode(&tag).unwrap();
        //     let decode = Tag::read_from(&mut bytes).unwrap();
        //     assert_eq!(decode, tag);
        // }
        //
        // {
        //     let tag: Tag = Tag::from_vec(vec![1, 2, 3, 4]);
        //     let mut bytes = pegasus_common::serialize::encode(&tag).unwrap();
        //     let decode = Tag::read_from(&mut bytes).unwrap();
        //     assert_eq!(decode, tag);
        // }
    }

    #[test]
    fn tag_partial_ord() {
        {
            let root = ROOT.clone();
            assert_eq!(root.partial_cmp(&ROOT), Some(Ordering::Equal));
            let t1 = tag!(1);
            assert_eq!(t1.partial_cmp(&ROOT), Some(Ordering::Less));
            let t12 = tag![1, 2];
            assert_eq!(t12.partial_cmp(&ROOT), Some(Ordering::Less));
        }
        {
            let t1 = tag!(1);
            let t2 = tag!(2);
            assert_eq!(t1.partial_cmp(&t2), None);
            assert_eq!(t2.partial_cmp(&t1), None);
        }
        {
            let t1 = tag!(1);
            let t12 = tag![1, 2];
            let t123 = tag![1, 2, 3];
            assert_eq!(t1.partial_cmp(&t12), Some(Ordering::Greater));
            assert_eq!(t1.partial_cmp(&t123), Some(Ordering::Greater));
            assert_eq!(t12.partial_cmp(&t123), Some(Ordering::Greater));
        }
        {
            let t1 = tag![1, 1];
            let t2 = tag![2, 1];
            assert_eq!(t1.partial_cmp(&t2), None);
            assert_eq!(t2.partial_cmp(&t1), None);
        }
    }
}
