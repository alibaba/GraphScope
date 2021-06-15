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
    Root,
    One(u32),
    Two(u32, u32),
    Three(u32, u32, u32),
    Spilled(Vec<u32>),
}

pub const MAX_DEPTH: usize = !0u8 as usize;

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
                v.push(current);
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
        self.current().expect("can't current on root tag")
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
        match self {
            Tag::Root => Tag::Root,
            Tag::One(v) => {
                if *v > 0 {
                    Tag::One(*v - 1)
                } else {
                    Tag::One(*v)
                }
            }
            Tag::Two(a, b) => {
                if *b > 0 {
                    Tag::Two(*a, *b - 1)
                } else {
                    Tag::Two(*a, *b)
                }
            }
            Tag::Three(a, b, c) => {
                if *c > 0 {
                    Tag::Three(*a, *b, *c - 1)
                } else {
                    Tag::Three(*a, *b, *c)
                }
            }
            Tag::Spilled(t) => {
                let mut v = t.clone();
                if let [.., last] = &mut v[..] {
                    if *last > 0 {
                        *last -= 1
                    }
                }
                Tag::Spilled(v)
            }
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

    // pub fn as_slice(&self) -> &[u32] {
    //     match self {
    //         Tag::Root => &[],
    //         Tag::One(t) => &[*t],
    //         Tag::Two(a, b) => &[*a, *b],
    //         Tag::Three(a, b, c) => &[*a, *b, *c],
    //         Tag::Spilled(v) => v.as_slice()
    //     }
    // }
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

impl From<&[u32]> for Tag {
    fn from(vec: &[u32]) -> Self {
        if vec.len() == 0 {
            Tag::Root
        } else if vec.len() == 1 {
            Tag::One(vec[0])
        } else if vec.len() == 2 {
            Tag::Two(vec[0], vec[1])
        } else if vec.len() == 3 {
            Tag::Three(vec[0], vec[1], vec[2])
        } else {
            Tag::Spilled(vec.to_vec())
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
                assert!(v.len() < MAX_DEPTH);
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
            let mut array = vec![0u32; len as usize];
            for i in 0..len {
                let t = reader.read_u32()?;
                array[i] = t;
            }
            Ok(Tag::Spilled(array))
        }
    }
}

#[macro_export]
macro_rules! tag {
    ($elem:expr) => {
        Tag::with($elem as u32)
    };
    ($elem1:expr, $elem2:expr) => {
        Tag::Two($elem1 as u32, $elem2 as u32)
    };
    ($elem1:expr, $elem2:expr, $elem3: expr) => {
        Tag::Three($elem1 as u32, $elem2 as u32, $elem3 as u32)
    };
}

pub mod tools;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    pub const CHECK_TIMES: usize = 10;

    #[test]
    fn tag_equal_test() {
        let mut rng = thread_rng();
        assert_eq!(Tag::Root, Tag::Root);
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            assert_eq!(Tag::One(i), Tag::One(i));
            assert_ne!(Tag::Root, Tag::One(i));
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                assert_eq!(Tag::Two(i, j), Tag::Two(i, j));
                assert_ne!(Tag::Root, Tag::Two(i, j));
                assert_ne!(Tag::One(i), Tag::Two(i, j));
                assert_ne!(Tag::One(j), Tag::Two(i, j));
                if i != j {
                    assert_ne!(Tag::One(i), Tag::One(j));
                    assert_ne!(Tag::Two(i, j), Tag::Two(j, i));
                }
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    assert_eq!(Tag::Three(i, j, k), Tag::Three(i, j, k));
                    assert_ne!(Tag::One(i), Tag::Three(i, j, k));
                    assert_ne!(Tag::One(j), Tag::Three(i, j, k));
                    assert_ne!(Tag::One(k), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(i, i), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(i, j), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(i, k), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(j, i), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(j, j), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(j, k), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(k, i), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(k, j), Tag::Three(i, j, k));
                    assert_ne!(Tag::Two(k, k), Tag::Three(i, j, k));
                    if i != j {
                        assert_ne!(Tag::Three(j, i, k), Tag::Three(i, j, k));
                    }
                    if i != k {
                        assert_ne!(Tag::Three(k, j, i), Tag::Three(i, j, k));
                    }
                    if k != j {
                        assert_ne!(Tag::Two(j, k), Tag::Two(k, j));
                        assert_ne!(Tag::Three(i, k, j), Tag::Three(i, j, k));
                    }
                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        let t = Tag::Spilled(vec![i, j, k, l]);
                        assert_eq!(t, t);
                        assert_eq!(t, Tag::Spilled(vec![i, j, k, l]));
                        assert_ne!(Tag::Root, t);
                        assert_ne!(Tag::One(i), t);
                        assert_ne!(Tag::One(j), t);
                        assert_ne!(Tag::One(k), t);
                        assert_ne!(Tag::One(l), t);

                        assert_ne!(Tag::Two(i, i), t);
                        assert_ne!(Tag::Two(i, j), t);
                        assert_ne!(Tag::Two(i, k), t);
                        assert_ne!(Tag::Two(i, l), t);
                        assert_ne!(Tag::Two(j, i), t);
                        assert_ne!(Tag::Two(j, j), t);
                        assert_ne!(Tag::Two(j, k), t);
                        assert_ne!(Tag::Two(j, l), t);
                        assert_ne!(Tag::Two(k, i), t);
                        assert_ne!(Tag::Two(k, j), t);
                        assert_ne!(Tag::Two(k, k), t);
                        assert_ne!(Tag::Two(k, l), t);
                        assert_ne!(Tag::Two(l, i), t);
                        assert_ne!(Tag::Two(l, j), t);
                        assert_ne!(Tag::Two(l, k), t);
                        assert_ne!(Tag::Two(l, l), t);

                        assert_ne!(Tag::Three(i, i, i), t);
                        assert_ne!(Tag::Three(i, i, j), t);
                        assert_ne!(Tag::Three(i, i, k), t);
                        assert_ne!(Tag::Three(i, i, l), t);
                        assert_ne!(Tag::Three(i, j, i), t);
                        assert_ne!(Tag::Three(i, j, j), t);
                        assert_ne!(Tag::Three(i, j, k), t);
                        assert_ne!(Tag::Three(i, j, l), t);
                        assert_ne!(Tag::Three(i, k, i), t);
                        assert_ne!(Tag::Three(i, k, j), t);
                        assert_ne!(Tag::Three(i, k, k), t);
                        assert_ne!(Tag::Three(i, k, l), t);
                        assert_ne!(Tag::Three(i, l, i), t);
                        assert_ne!(Tag::Three(i, l, j), t);
                        assert_ne!(Tag::Three(i, l, k), t);
                        assert_ne!(Tag::Three(i, l, l), t);

                        assert_ne!(Tag::Three(j, i, i), t);
                        assert_ne!(Tag::Three(j, i, j), t);
                        assert_ne!(Tag::Three(j, i, k), t);
                        assert_ne!(Tag::Three(j, i, l), t);
                        assert_ne!(Tag::Three(j, j, i), t);
                        assert_ne!(Tag::Three(j, j, j), t);
                        assert_ne!(Tag::Three(j, j, k), t);
                        assert_ne!(Tag::Three(j, j, l), t);
                        assert_ne!(Tag::Three(j, k, i), t);
                        assert_ne!(Tag::Three(j, k, j), t);
                        assert_ne!(Tag::Three(j, k, k), t);
                        assert_ne!(Tag::Three(j, k, l), t);
                        assert_ne!(Tag::Three(j, l, i), t);
                        assert_ne!(Tag::Three(j, l, j), t);
                        assert_ne!(Tag::Three(j, l, k), t);
                        assert_ne!(Tag::Three(j, l, l), t);

                        assert_ne!(Tag::Three(k, i, i), t);
                        assert_ne!(Tag::Three(k, i, j), t);
                        assert_ne!(Tag::Three(k, i, k), t);
                        assert_ne!(Tag::Three(k, i, l), t);
                        assert_ne!(Tag::Three(k, j, i), t);
                        assert_ne!(Tag::Three(k, j, j), t);
                        assert_ne!(Tag::Three(k, j, k), t);
                        assert_ne!(Tag::Three(k, j, l), t);
                        assert_ne!(Tag::Three(k, k, i), t);
                        assert_ne!(Tag::Three(k, k, j), t);
                        assert_ne!(Tag::Three(k, k, k), t);
                        assert_ne!(Tag::Three(k, k, l), t);
                        assert_ne!(Tag::Three(k, l, i), t);
                        assert_ne!(Tag::Three(k, l, j), t);
                        assert_ne!(Tag::Three(k, l, k), t);
                        assert_ne!(Tag::Three(k, l, l), t);

                        assert_ne!(Tag::Three(l, i, i), t);
                        assert_ne!(Tag::Three(l, i, j), t);
                        assert_ne!(Tag::Three(l, i, k), t);
                        assert_ne!(Tag::Three(l, i, l), t);
                        assert_ne!(Tag::Three(l, j, i), t);
                        assert_ne!(Tag::Three(l, j, j), t);
                        assert_ne!(Tag::Three(l, j, k), t);
                        assert_ne!(Tag::Three(l, j, l), t);
                        assert_ne!(Tag::Three(l, k, i), t);
                        assert_ne!(Tag::Three(l, k, j), t);
                        assert_ne!(Tag::Three(l, k, k), t);
                        assert_ne!(Tag::Three(l, k, l), t);
                        assert_ne!(Tag::Three(l, l, i), t);
                        assert_ne!(Tag::Three(l, l, j), t);
                        assert_ne!(Tag::Three(l, l, k), t);
                        assert_ne!(Tag::Three(l, l, l), t);

                        if i != j {
                            assert_ne!(Tag::Spilled(vec![j, i, k, l]), t);
                        }

                        if i != k {
                            assert_ne!(Tag::Spilled(vec![k, j, i, l]), t);
                        }

                        if i != l {
                            assert_ne!(Tag::Spilled(vec![l, j, k, l]), t);
                        }

                        if j != k {
                            assert_ne!(Tag::Spilled(vec![i, k, j, l]), t);
                        }

                        if j != l {
                            assert_ne!(Tag::Spilled(vec![i, k, l, j]), t);
                        }

                        if k != l {
                            assert_ne!(Tag::Three(i, j, k), Tag::Three(i, j, l));
                            assert_ne!(Tag::Spilled(vec![i, j, l, k]), t);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn tag_macro_test() {
        let t1 = tag!(1);
        assert_eq!(t1, Tag::One(1));
        let t2 = tag![1, 2];
        assert_eq!(t2, Tag::Two(1, 2));
        let t3 = tag![1, 2, 3];
        assert_eq!(t3, Tag::Three(1, 2, 3));
    }

    #[test]
    fn tag_inherit_test() {
        let mut rng = thread_rng();
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            let t_1 = Tag::One(i);
            assert_eq!(Tag::inherit(&Tag::Root, i), t_1);
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                let t_2 = Tag::Two(i, j);
                assert_eq!(Tag::inherit(&t_1, j), t_2);
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    let t_3 = Tag::Three(i, j, k);
                    assert_eq!(Tag::inherit(&t_2, k), t_3);
                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        let t_4 = Tag::Spilled(vec![i, j, k, l]);
                        assert_eq!(Tag::inherit(&t_3, l), t_4);
                    }
                }
            }
        }
    }

    #[test]
    fn tag_current_test() {
        let mut rng = thread_rng();
        assert_eq!(Tag::Root.current(), None);
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            assert_eq!(Tag::One(i).current(), Some(i));
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                assert_eq!(Tag::Two(i, j).current(), Some(j));
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    assert_eq!(Tag::Three(i, j, k).current(), Some(k));
                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        assert_eq!(Tag::Spilled(vec![i, j, k, l]).current(), Some(l));
                    }
                }
            }
        }
    }

    #[test]
    fn tag_to_parent_test() {
        let mut rng = thread_rng();
        assert_eq!(Tag::Root.to_parent(), None);
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            assert_eq!(Tag::One(i).to_parent(), Some(Tag::Root));
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                assert_eq!(Tag::Two(i, j).to_parent(), Some(Tag::One(i)));
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    assert_eq!(Tag::Three(i, j, k).to_parent(), Some(Tag::Two(i, j)));
                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        assert_eq!(
                            Tag::Spilled(vec![i, j, k, l]).to_parent(),
                            Some(Tag::Three(i, j, k))
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn tag_advance_test() {
        let mut rng = thread_rng();
        assert_eq!(Tag::Root.advance(), Tag::Root);
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            assert_eq!(Tag::One(i).advance(), Tag::One(i + 1));
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                assert_eq!(Tag::One(i).advance_to(j), Tag::One(j));
                assert_eq!(Tag::Two(i, j).advance(), Tag::Two(i, j + 1));
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    assert_eq!(Tag::Two(i, j).advance_to(k), Tag::Two(i, k));
                    assert_eq!(Tag::Three(i, j, k).advance(), Tag::Three(i, j, k + 1));
                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        assert_eq!(Tag::Three(i, j, k).advance_to(l), Tag::Three(i, j, l));
                        assert_eq!(
                            Tag::Spilled(vec![i, j, k, l]).advance(),
                            Tag::Spilled(vec![i, j, k, l + 1])
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn tag_len_test() {
        assert_eq!(Tag::Root.len(), 0);
        assert_eq!(Tag::One(0).len(), 1);
        assert_eq!(Tag::Two(0, 0).len(), 2);
        assert_eq!(Tag::Three(0, 0, 0).len(), 3);
        for i in 4..MAX_DEPTH {
            assert_eq!(Tag::Spilled(vec![0; i]).len(), i);
        }
    }

    #[test]
    fn tag_is_parent_test() {
        let mut rng = thread_rng();
        assert!(!Tag::Root.is_parent_of(&Tag::Root));
        for _ in 0..CHECK_TIMES {
            let i: u32 = rng.gen();
            let t_1 = Tag::One(i);
            assert!(Tag::Root.is_parent_of(&t_1));
            assert!(!t_1.is_parent_of(&t_1));
            assert!(!t_1.is_parent_of(&Tag::Root));
            for _ in 0..CHECK_TIMES {
                let j: u32 = rng.gen();
                let t_2 = Tag::Two(i, j);
                assert!(Tag::Root.is_parent_of(&t_2));
                assert!(t_1.is_parent_of(&t_2));

                assert!(!t_2.is_parent_of(&Tag::Root));
                assert!(!t_2.is_parent_of(&t_1));
                if j != i {
                    assert!(!Tag::One(j).is_parent_of(&t_2));
                }
                assert!(!t_2.is_parent_of(&t_2));
                for _ in 0..CHECK_TIMES {
                    let k: u32 = rng.gen();
                    let t_3 = Tag::Three(i, j, k);

                    assert!(Tag::Root.is_parent_of(&t_3));
                    assert!(t_1.is_parent_of(&t_3));
                    assert!(t_2.is_parent_of(&t_3));

                    if j != i {
                        assert!(!Tag::One(j).is_parent_of(&t_3));
                        assert!(!Tag::Two(j, i).is_parent_of(&t_3));
                    }

                    if k != j {
                        assert!(!Tag::Two(i, k).is_parent_of(&t_3));
                    }

                    assert!(!t_3.is_parent_of(&Tag::Root));
                    assert!(!t_3.is_parent_of(&t_1));
                    assert!(!t_3.is_parent_of(&t_2));
                    assert!(!t_3.is_parent_of(&t_3));

                    for _ in 0..CHECK_TIMES {
                        let l: u32 = rng.gen();
                        let t_4 = Tag::Spilled(vec![i, j, k, l]);
                        assert!(Tag::Root.is_parent_of(&t_4));
                        assert!(t_1.is_parent_of(&t_4));
                        assert!(t_2.is_parent_of(&t_4));
                        assert!(t_3.is_parent_of(&t_4));

                        if i != l {
                            assert!(!Tag::One(l).is_parent_of(&t_4));
                            assert!(!Tag::Two(l, j).is_parent_of(&t_4));
                            assert!(!Tag::Three(l, j, k).is_parent_of(&t_4));
                        }

                        if j != l {
                            assert!(!Tag::Two(i, l).is_parent_of(&t_4));
                            assert!(!Tag::Three(i, l, k).is_parent_of(&t_4));
                        }

                        if k != l {
                            assert!(!Tag::Three(i, j, l).is_parent_of(&t_4));
                        }
                    }
                }
            }
        }
    }

    fn check_serde(tag: Tag) {
        let mut bytes = vec![];
        tag.write_to(&mut bytes).unwrap();
        assert_eq!(Tag::read_from(&mut bytes.as_slice()).unwrap(), tag);
    }

    #[test]
    fn tag_serde_test() {
        check_serde(Tag::Root);
        let mut rng = thread_rng();
        for _ in 0..100 {
            let i: u32 = rng.gen();
            check_serde(Tag::One(i));
        }

        for _ in 0..100 {
            let i: u32 = rng.gen();
            let j: u32 = rng.gen();
            check_serde(Tag::Two(i, j));
        }

        for _ in 0..100 {
            let i: u32 = rng.gen();
            let j: u32 = rng.gen();
            let k: u32 = rng.gen();
            check_serde(Tag::Three(i, j, k));
        }

        for _ in 0..100 {
            let i: u32 = rng.gen();
            let j: u32 = rng.gen();
            let k: u32 = rng.gen();
            let l: u32 = rng.gen();
            check_serde(Tag::Spilled(vec![i, j, k, l]));
        }
    }
}
