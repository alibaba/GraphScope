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

//! Tags implementation.

use std::fmt::{Debug, Formatter, Display, Error};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

#[derive(Clone, Abomonation, Serialize, Deserialize)]
pub enum Tag {
    Inline { length: u8, data: [u32; TAG_INLINE_LEN] },
    Spilled(Box<Vec<u32>>),
}

pub const TAG_INLINE_LEN: usize = 3;

impl Tag {
    pub(crate) fn from(parent: &Tag, current: u32) -> Self {
        let len = parent.len();
        if len < TAG_INLINE_LEN {
            let mut data = [0; TAG_INLINE_LEN];
            data[..len].clone_from_slice(parent.as_slice());
            data[len] = current;
            Tag::Inline { length: len as u8 + 1, data }
        } else {
            let mut data = Vec::with_capacity(len + 1);
            data.extend_from_slice(parent.as_slice());
            data.push(current);
            Tag::Spilled(Box::new(data))
        }
    }

    #[inline]
    pub fn from_vec(vec: Vec<u32>) -> Tag {
        let length = vec.len();
        if length <= TAG_INLINE_LEN {
            let mut data = [0; TAG_INLINE_LEN];
            data[..length].copy_from_slice(vec.as_slice());
            Tag::Inline { length: length as u8, data }
        } else {
            Tag::Spilled(Box::new(vec))
        }
    }

    #[inline]
    pub fn new(cur: u32) -> Self {
        Tag::Inline { length: 1, data: [cur, 0, 0] }
    }

    #[inline]
    pub fn current(&self) -> u32 {
        match self {
            Tag::Inline { length, data } => data[*length as usize - 1],
            Tag::Spilled(vec) => vec[vec.len() - 1],
        }
    }

    #[inline]
    pub fn parent(&self) -> &[u32] {
        assert!(self.len() > 0);
        &self.as_slice()[0..self.len() - 1]
    }

    #[inline]
    pub fn to_parent(&self) -> Tag {
        assert!(self.len() > 0);
        match self {
            Tag::Inline { length, data } => {
                let mut new_data = *data;
                new_data[*length as usize - 1] = 0;
                Tag::Inline { length: length - 1, data: new_data }
            },
            Tag::Spilled(vec) => {
                let mut new_vec = vec.clone();
                new_vec.pop();
                Tag::Spilled(new_vec)
            },
        }
    }

    #[inline]
    pub fn advance_in_place(&mut self) {
        let len = self.len();
        let data = self.as_mut_slice();
        data[len - 1] += 1;
    }

    #[inline]
    pub fn advance_to(&self, cur: u32) -> Tag {
        match self {
            Tag::Inline { length, data } => {
                let mut new_data = *data;
                new_data[*length as usize - 1] = cur;
                Tag::Inline { length: *length, data: new_data }
            },
            Tag::Spilled(vec) => {
                let mut new_vec = vec.clone();
                let len = new_vec.len();
                new_vec[len - 1] = cur;
                Tag::Spilled(new_vec)
            },
        }
    }

    #[inline]
    pub fn advance(&self) -> Tag {
        let mut tag = self.clone();
        tag.advance_in_place();
        tag
    }

    #[inline]
    pub fn retreat(&self) -> Tag {
        match self {
            Tag::Inline { length, data } => {
                let mut new_data = *data;
                new_data[*length as usize - 1] -= 1;
                Tag::Inline { length: *length, data: new_data }
            },
            Tag::Spilled(vec) => {
                let mut new_vec = vec.clone();
                let len = new_vec.len();
                new_vec[len - 1] -= 1;
                Tag::Spilled(new_vec)
            },
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        match self {
            Tag::Inline { length, data } => &data[0..(*length as usize)],
            Tag::Spilled(vec) => vec.as_slice(),
        }
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u32] {
        match self {
            Tag::Inline { length, data } => &mut data[0..(*length as usize)],
            Tag::Spilled(vec) => vec.as_mut_slice(),
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
        other.as_slice().starts_with(self.as_slice())
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
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Tag {}

impl Default for Tag {
    fn default() -> Self {
        Tag::new(0)
    }
}

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{:?}", self.as_slice())
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{:?}", self.as_slice())
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
    fn partial_cmp(&self, other: &Tag) -> Option<Ordering> {
        assert_eq!(self.len(), other.len());
        self.as_slice().partial_cmp(&other.as_slice())
    }
}

impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_eq!(self.len(), other.len());
        self.as_slice().cmp(other.as_slice())
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum TagMatcher {
    Equals(Tag),
    Prefix(Tag)
}

impl TagMatcher {
    pub fn matches(&self, other: &Tag) -> bool {
        match self {
            TagMatcher::Equals(this) => this == other,
            TagMatcher::Prefix(this) => other.as_slice().starts_with(this.as_slice()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_tag() {
        let mut tag1: Tag = 3.into();
        let mut tag2: Tag = [3, 2].into();

        assert_eq!(tag2, Tag::from_vec(vec![3, 2]));
        assert_eq!(tag1.current(), 3);
        assert_eq!(tag2.current(), 2);
        assert_eq!(tag2.parent(), &[3]);
        assert_eq!(tag2.to_parent(), tag1);
        assert_eq!(tag1.len(), 1);
        assert_eq!(tag2.len(), 2);
        let tag3: Tag = 3.into();
        assert_eq!(tag3, tag1);

        tag2.advance_in_place();
        assert_eq!(tag2, Tag::from(&tag1, 3));

        tag1.advance_in_place();
        assert_eq!(tag1, Tag::new(4));

        ///////////// advance /////////////
        assert_eq!(Tag::from_vec(vec![1, 3]).advance(),
                   Tag::from_vec(vec![1, 4]));

        assert_eq!(Tag::from_vec(vec![1, 3, 5]).advance(),
                   Tag::from_vec(vec![1, 3, 6]));

        assert_eq!(Tag::from_vec(vec![1, 3, 5, 7]).advance(),
                   Tag::from_vec(vec![1, 3, 5, 8]));

        ///////////// retreat /////////////
        assert_eq!(Tag::from_vec(vec![1, 3]).retreat(),
                   Tag::from_vec(vec![1, 2]));

        assert_eq!(Tag::from_vec(vec![1, 3, 5]).retreat(),
                   Tag::from_vec(vec![1, 3, 4]));

        assert_eq!(Tag::from_vec(vec![1, 3, 5, 7]).retreat(),
                   Tag::from_vec(vec![1, 3, 5, 6]));
    }

    #[test]
    fn test_tag_hash_eq() {
        let tag1 = Tag::Inline { length: 2, data: [1, 2, 0] };
        let tag2 = Tag::Spilled(Box::new(vec![1, 2]));
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);

        let tag1 = Tag::Inline { length: 3, data: [1, 2, 3] };
        let tag2 = Tag::Spilled(Box::new(vec![1, 2, 3]));
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_is_parent() {
        let tag1 = Tag::from_vec(vec![1]);
        let tag2 = Tag::from(&tag1, 1);
        let tag3 = Tag::from(&tag2, 1);
        assert!(tag1.is_parent_of(&tag2));
        assert!(tag2.is_parent_of(&tag3));
    }

    #[test]
    fn test_tag_order() {
        let tag1: Tag = [1, 5].into();
        let tag2: Tag = [1, 6].into();
        let tag3: Tag = [2, 4].into();

        assert!(tag1 < tag2);
        assert!(tag1 < tag3);
    }

    #[test]
    #[should_panic]
    fn test_tag_order_panic() {
        let tag1: Tag = [1, 5].into();
        let tag2: Tag = [1, 6, 4].into();
        if tag1 < tag2 {} else {}
    }
}
