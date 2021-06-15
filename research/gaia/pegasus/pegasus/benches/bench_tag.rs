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

#![feature(test)]
extern crate test;

use pegasus::serde::*;
use pegasus::tag;
use pegasus::Tag as NewTag;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Display, Error, Formatter};
use std::hash::Hash;

/// cargo +nightly bench --bench bench_tag;

/// The original Tag implementation comes here.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Tag {
    inner: Vec<u32>,
}

impl Tag {
    pub fn from(parent: &Tag, current: u32) -> Self {
        let mut inner = parent.inner.clone();
        inner.push(current);
        Tag { inner }
    }

    #[inline]
    pub fn from_vec(inner: Vec<u32>) -> Tag {
        Tag { inner }
    }

    #[inline]
    pub fn new(cur: u32) -> Self {
        Tag { inner: vec![cur] }
    }

    #[inline]
    pub fn current(&self) -> u32 {
        let len = self.inner.len();
        self.inner[len - 1]
    }

    #[inline]
    pub fn parent(&self) -> &[u32] {
        assert!(self.len() > 0);
        &self.as_slice()[0..self.len() - 1]
    }

    #[inline]
    pub fn to_parent(&self) -> Tag {
        assert!(self.len() > 0);
        let mut inner = self.inner.clone();
        inner.pop();
        Tag { inner }
    }

    #[inline]
    pub fn advance_in_place(&mut self) {
        let len = self.inner.len();
        self.inner[len - 1] += 1;
    }

    #[inline]
    pub fn advance_to(&self, cur: u32) -> Tag {
        let mut inner = self.inner.clone();
        let len = inner.len();
        inner[len - 1] += cur;
        Tag { inner }
    }

    #[inline]
    pub fn advance(&self) -> Tag {
        let mut inner = self.inner.clone();
        let len = inner.len();
        inner[len - 1] += 1;
        Tag { inner }
    }

    #[inline]
    pub fn retreat(&self) -> Tag {
        let mut inner = self.inner.clone();
        let len = inner.len();
        inner[len - 1] -= 1;
        Tag { inner }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        self.inner.as_slice()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Default for Tag {
    fn default() -> Self {
        Tag::new(0)
    }
}

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "<{:?}>", self.inner)
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "<{:?}>", self.inner)
    }
}

impl From<u32> for Tag {
    fn from(cur: u32) -> Self {
        Tag::new(cur)
    }
}

impl From<[u32; 2]> for Tag {
    fn from(array: [u32; 2]) -> Self {
        Tag::from_vec(array.to_vec())
    }
}

impl From<[u32; 3]> for Tag {
    fn from(array: [u32; 3]) -> Self {
        Tag::from_vec(array.to_vec())
    }
}

impl From<Vec<u32>> for Tag {
    fn from(vec: Vec<u32>) -> Self {
        Tag::from_vec(vec)
    }
}

////////////with inline optimized ////////////
#[bench]
fn bench_new_inline_tag_clone(b: &mut test::Bencher) {
    let tag: NewTag = [1, 2, 3].into();
    b.iter(|| tag.clone());
}

#[bench]
fn bench_new_inline_tag_to_parent(b: &mut test::Bencher) {
    let tag: NewTag = [1, 2, 3].into();
    b.iter(|| tag.to_parent());
}

#[bench]
fn bench_new_inline_tag_advance(b: &mut test::Bencher) {
    let mut tag: NewTag = [1, 2, 3].into();
    b.iter(|| tag.advance());
}

#[bench]
fn bench_new_inline_tag_hash(b: &mut test::Bencher) {
    let tag: NewTag = [1, 2, 3].into();
    let mut state = DefaultHasher::new();
    b.iter(|| tag.hash(&mut state));
}

#[bench]
fn bench_new_inline_tag_eq(b: &mut test::Bencher) {
    let tag1: NewTag = [1, 2].into();
    let tag2: NewTag = [1, 2].into();
    b.iter(|| tag1 == tag2);
}

//////////// without inline optimized ////////////

#[bench]
fn bench_old_tag_clone(b: &mut test::Bencher) {
    let tag: Tag = [1, 2, 3].into();
    b.iter(|| tag.clone());
}

#[bench]
fn bench_old_tag_to_parent(b: &mut test::Bencher) {
    let tag: Tag = [1, 2, 3].into();
    b.iter(|| tag.to_parent());
}

#[bench]
fn bench_old_tag_advance(b: &mut test::Bencher) {
    let tag: Tag = [1, 2, 3].into();
    b.iter(|| tag.advance());
}

#[bench]
fn bench_old_tag_advance_in_place(b: &mut test::Bencher) {
    let mut tag: Tag = [1, 2, 3].into();
    b.iter(|| tag.advance_in_place());
}

#[bench]
fn bench_old_tag_hash(b: &mut test::Bencher) {
    let tag: Tag = [1, 2, 3].into();
    let mut state = DefaultHasher::new();
    b.iter(|| tag.hash(&mut state));
}

#[bench]
fn bench_array_hash(b: &mut test::Bencher) {
    let mut state = DefaultHasher::new();
    b.iter(|| [1, 2, 3].hash(&mut state));
}

//////////// serialize & deserialize ////////////
#[bench]
fn bench_ser_deserialize(b: &mut test::Bencher) {
    let t: NewTag = [1, 2, 3].into();
    let _ = pegasus::serde::encode(&t).unwrap();
    b.iter(|| {
        let mut bytes = pegasus::serde::encode(&t).unwrap();
        let decode = NewTag::read_from(&mut bytes).unwrap();
        assert_eq!(decode, t);
    })
}
