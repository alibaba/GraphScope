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

use std::hash::{Hash, Hasher};
use std::io;

use pegasus_common::codec::*;
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
    Root,
    One(u32),
    Two(u32, u32),
    Three(u32, u32, u32),
    Spilled(Vec<u32>),
}

pub const MAX_LENGTH: usize = !0u8 as usize;

pub fn root() -> Tag {
    Tag::Root
}

impl Tag {
    pub fn inherit(parent: &Tag, current: u32) -> Self {
        match parent {
            &Tag::Root => Tag::One(current),
            &Tag::One(v) => Tag::Two(v, current),
            &Tag::Two(a, b) => Tag::Three(a, b, current),
            &Tag::Three(a, b, c) => {
                let mut v = Vec::with_capacity(4);
                v.push(a);
                v.push(b);
                v.push(c);
                Tag::Spilled(v)
            }
            Tag::Spilled(t) => {
                let mut v = Vec::with_capacity(t.len() + 1);
                v.extend_from_slice(&t[..]);
                v.push(current);
                Tag::Spilled(v)
            }
        }
    }

    #[inline]
    pub fn with(cur: u32) -> Self {
        Tag::One(cur)
    }

    /// Get the current integer identification. Return `None` if the tag is `ROOT`;
    #[inline]
    pub fn current(&self) -> Option<u32> {
        match self {
            Tag::Root => None,
            Tag::One(v) => Some(*v),
            Tag::Two(_, v) => Some(*v),
            Tag::Three(_, _, v) => Some(*v),
            Tag::Spilled(v) => {
                if let [.., last] = &v[..] {
                    Some(*last)
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    pub fn current_uncheck(&self) -> u32 {
        self.current()
            .expect("can't current on root tag")
    }

    #[inline]
    pub fn to_parent(&self) -> Option<Tag> {
        match self {
            Tag::Root => None,
            Tag::One(_) => Some(Tag::Root),
            Tag::Two(v, _) => Some(Tag::One(*v)),
            Tag::Three(a, b, _) => Some(Tag::Two(*a, *b)),
            Tag::Spilled(t) => {
                let len = t.len();
                if len > 4 {
                    let mut v = t.clone();
                    v.pop();
                    Some(Tag::Spilled(v))
                } else {
                    Some(if len == 4 {
                        Tag::Three(t[0], t[1], t[2])
                    } else if len == 3 {
                        Tag::Two(t[0], t[1])
                    } else if len == 2 {
                        Tag::One(t[0])
                    } else {
                        Tag::Root
                    })
                }
            }
        }
    }

    #[inline]
    pub fn to_parent_uncheck(&self) -> Tag {
        self.to_parent().expect("To parent failure")
    }

    #[inline]
    pub fn to_parent_silent(&self) -> Tag {
        self.to_parent().unwrap_or(self.clone())
    }

    #[inline]
    pub fn advance(&self) -> Tag {
        match self {
            Tag::Root => Tag::Root,
            Tag::One(v) => Tag::One(*v + 1),
            Tag::Two(a, b) => Tag::Two(*a, *b + 1),
            Tag::Three(a, b, c) => Tag::Three(*a, *b, *c + 1),
            Tag::Spilled(t) => {
                let mut v = t.clone();
                if let [.., last] = &mut v[..] {
                    *last += 1
                }
                Tag::Spilled(v)
            }
        }
    }

    #[inline]
    pub fn advance_to(&self, cur: u32) -> Tag {
        match self {
            Tag::Root => Tag::Root,
            Tag::One(_) => Tag::One(cur),
            Tag::Two(a, _) => Tag::Two(*a, cur),
            Tag::Three(a, b, _) => Tag::Three(*a, *b, cur),
            Tag::Spilled(t) => {
                let mut v = t.clone();
                if let [.., last] = &mut v[..] {
                    *last = cur
                }
                Tag::Spilled(v)
            }
        }
    }

    pub fn retreat(&self) -> Tag {
        if let Some(mut cur) = self.current() {
            if cur > 0 {
                cur -= 1;
            }
            self.advance_to(cur)
        } else {
            Tag::Root
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Tag::Root => 0,
            Tag::One(_) => 1,
            Tag::Two(_, _) => 2,
            Tag::Three(_, _, _) => 3,
            Tag::Spilled(t) => t.len(),
        }
    }

    #[inline]
    pub fn is_parent_of(&self, other: &Tag) -> bool {
        match self {
            Tag::Root => !other.is_root(),
            Tag::One(a) => match other {
                Tag::Root => false,
                Tag::One(_) => false,
                Tag::Two(t, _) => a == t,
                Tag::Three(t, _, _) => a == t,
                Tag::Spilled(t) => {
                    if t.len() > 0 {
                        a == &t[0]
                    } else {
                        false
                    }
                }
            },
            Tag::Two(a, b) => match other {
                Tag::Root => false,
                Tag::One(_) => false,
                Tag::Two(_, _) => false,
                Tag::Three(t_a, t_b, _) => a == t_a && b == t_b,
                Tag::Spilled(t) => {
                    if t.len() > 2 {
                        a == &t[0] && b == &t[1]
                    } else {
                        false
                    }
                }
            },
            Tag::Three(a, b, c) => match other {
                Tag::Spilled(t) if t.len() > 3 => a == &t[0] && b == &t[1] && c == &t[2],
                _ => false,
            },
            Tag::Spilled(v) => {
                let len = v.len();
                match other {
                    Tag::Spilled(o) if o.len() > len => o.starts_with(v),
                    _ => false,
                }
            }
        }
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        match self {
            Tag::Root => true,
            _ => false,
        }
    }
}

impl Hash for Tag {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Tag::Root => {}
            Tag::One(v) => state.write_u32(*v),
            Tag::Two(a, b) => {
                state.write_u32(*a);
                state.write_u32(*b);
            }
            Tag::Three(a, b, c) => {
                state.write_u32(*a);
                state.write_u32(*b);
                state.write_u32(*c);
            }
            Tag::Spilled(t) => Hash::hash_slice(t.as_slice(), state),
        }
    }
}

impl PartialEq for Tag {
    #[inline]
    fn eq(&self, other: &Tag) -> bool {
        match self {
            Tag::Root => other.is_root(),
            Tag::One(v) => match other {
                Tag::One(o) => v == o,
                _ => false,
            },
            Tag::Two(a, b) => match other {
                Tag::Two(t_a, t_b) => a == t_a && b == t_b,
                _ => false,
            },
            Tag::Three(a, b, c) => match other {
                Tag::Three(t_a, t_b, t_c) => a == t_a && b == t_b && c == t_c,
                _ => false,
            },
            Tag::Spilled(v) => {
                assert!(v.len() > 3);
                match other {
                    Tag::Spilled(o) => v.as_slice() == o.as_slice(),
                    _ => false,
                }
            }
        }
    }
}

impl Eq for Tag {}

impl Default for Tag {
    fn default() -> Self {
        Tag::Root
    }
}

impl ::std::fmt::Debug for Tag {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            Tag::Root => write!(f, "[root]"),
            Tag::One(v) => write!(f, "[{}]", v),
            Tag::Two(a, b) => write!(f, "[{},{}]", a, b),
            Tag::Three(a, b, c) => write!(f, "[{},{},{}]", a, b, c),
            Tag::Spilled(v) => write!(f, "{:?}", v),
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
        Tag::One(cur)
    }
}

impl From<[u32; 2]> for Tag {
    #[inline]
    fn from(array: [u32; 2]) -> Self {
        Tag::Two(array[0], array[1])
    }
}

impl From<[u32; 3]> for Tag {
    #[inline]
    fn from(data: [u32; 3]) -> Self {
        Tag::Three(data[0], data[1], data[2])
    }
}

impl From<Vec<u32>> for Tag {
    #[inline]
    fn from(vec: Vec<u32>) -> Self {
        if vec.len() == 0 {
            Tag::Root
        } else if vec.len() == 1 {
            Tag::One(vec[0])
        } else if vec.len() == 2 {
            Tag::Two(vec[0], vec[1])
        } else if vec.len() == 3 {
            Tag::Three(vec[0], vec[1], vec[2])
        } else {
            Tag::Spilled(vec)
        }
    }
}

impl Encode for Tag {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Tag::Root => writer.write_u8(0),
            Tag::One(v) => {
                writer.write_u8(1)?;
                writer.write_u32(*v)
            }
            Tag::Two(a, b) => {
                writer.write_u8(2)?;
                writer.write_u32(*a)?;
                writer.write_u32(*b)
            }
            Tag::Three(a, b, c) => {
                writer.write_u8(3)?;
                writer.write_u32(*a)?;
                writer.write_u32(*b)?;
                writer.write_u32(*c)
            }
            Tag::Spilled(v) => {
                assert!(v.len() < MAX_LENGTH);
                let len = v.len() as u8;
                writer.write_u8(len)?;
                for t in &v[..] {
                    writer.write_u32(*t)?;
                }
                Ok(())
            }
        }
    }
}

impl Decode for Tag {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Tag> {
        let len = reader.read_u8()? as usize;
        if len == 0 {
            Ok(Tag::Root)
        } else if len == 1 {
            let cur = reader.read_u32()?;
            Ok(Tag::One(cur))
        } else if len == 2 {
            let a = reader.read_u32()?;
            let b = reader.read_u32()?;
            Ok(Tag::Two(a, b))
        } else if len == 3 {
            let a = reader.read_u32()?;
            let b = reader.read_u32()?;
            let c = reader.read_u32()?;
            Ok(Tag::Three(a, b, c))
        } else {
            let mut array = Vec::with_capacity(len);
            for _ in 0..len {
                let t = reader.read_u32()?;
                array.push(t);
            }
            Ok(Tag::Spilled(array))
        }
    }
}

pub mod tools;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn tag_test() {
        let tag1 = Tag::One(3);
        assert_eq!(tag1.len(), 1);
        let tag2 = Tag::Two(3, 2);
        assert_eq!(tag2.len(), 2);

        assert_eq!(tag1, 3.into());
        assert_eq!(tag2, vec![3, 2].into());
        assert_eq!(tag1.current_uncheck(), 3);
        assert_eq!(tag2.current_uncheck(), 2);
        assert_eq!(tag2.to_parent().unwrap(), tag1);
        assert_eq!(tag1.to_parent().unwrap(), root());
        assert_eq!(root().to_parent(), None);
    }

    #[test]
    fn tag_hash_eq_test() {
        let tag1 = Tag::Two(1, 2);
        let tag2 = vec![1, 2].into();
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);

        let tag1 = Tag::Three(1, 2, 3);
        let tag2 = vec![1, 2, 3].into();
        assert_eq!(tag1, tag2);
        assert_eq!(tag2, tag1);

        let mut set = HashSet::new();
        set.insert(tag1);
        set.insert(tag2);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn tag_parent_test() {
        let tag1 = Tag::One(1);
        let tag12 = Tag::inherit(&tag1, 2);
        let tag123 = Tag::inherit(&tag12, 3);
        assert!(tag1.is_parent_of(&tag12));
        assert!(tag12.is_parent_of(&tag123));
        let tag14 = Tag::Two(1, 4);
        assert!(tag1.is_parent_of(&tag14));
    }

    #[test]
    fn tag_serialize() {
        let mut bytes = vec![];
        Tag::Root
            .write_to(&mut bytes)
            .expect("write tag root failure;");
        Tag::One(123)
            .write_to(&mut bytes)
            .expect("write tag [123] failure;");
        Tag::One(!0)
            .write_to(&mut bytes)
            .expect("write tag [!0] failure;");
        Tag::Two(128, 255)
            .write_to(&mut bytes)
            .expect("write tag [128, 255] failure;");
        Tag::Three(128, 255, !0)
            .write_to(&mut bytes)
            .expect("write tag [128, 255, !0] failure;");
        Tag::Spilled(vec![128, 255, 512, !0])
            .write_to(&mut bytes)
            .expect("write tag [128, 255, 512, !0] failure;");
        let mut reader = &bytes[..];
        assert_eq!(Tag::read_from(&mut reader).expect("read tag failure"), Tag::Root);
        assert_eq!(Tag::read_from(&mut reader).expect("read tag failure"), Tag::One(123));
        assert_eq!(Tag::read_from(&mut reader).expect("read tag failure"), Tag::One(!0));
        assert_eq!(Tag::read_from(&mut reader).expect("read tag failure"), Tag::Two(128, 255));
        assert_eq!(Tag::read_from(&mut reader).expect("read tag failure"), Tag::Three(128, 255, !0));
        assert_eq!(
            Tag::read_from(&mut reader).expect("read tag failure"),
            Tag::Spilled(vec![128, 255, 512, !0])
        );
    }
}
