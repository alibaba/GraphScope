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

use crate::Tag;

/// Describing how data's scope will be changed when being send from an output port.
#[derive(Debug, Copy, Clone)]
pub enum ScopeDelta {
    /// The scope won't be changed.
    None,
    /// Go to a next adjacent sibling scope;
    ToSibling(u32),
    /// Go to child scope;
    ToChild(u32),
    /// Go to parent scope;
    ToParent(u32),
}

impl Default for ScopeDelta {
    fn default() -> Self {
        ScopeDelta::None
    }
}

impl ScopeDelta {
    pub fn level_delta(&self) -> i32 {
        match self {
            ScopeDelta::None => 0,
            ScopeDelta::ToSibling(_) => 0,
            ScopeDelta::ToChild(a) => *a as i32,
            ScopeDelta::ToParent(a) => 0 - *a as i32,
        }
    }

    pub fn try_merge(&mut self, other: ScopeDelta) -> Option<ScopeDelta> {
        match (*self, other) {
            (ScopeDelta::None, x) => {
                *self = x;
                None
            }
            (_, ScopeDelta::None) => None,
            (ScopeDelta::ToSibling(a), ScopeDelta::ToSibling(b)) => {
                *self = ScopeDelta::ToSibling(a + b);
                None
            }
            (ScopeDelta::ToSibling(_), other) => Some(other),
            (ScopeDelta::ToChild(a), ScopeDelta::ToChild(b)) => {
                *self = ScopeDelta::ToChild(a + b);
                None
            }
            (ScopeDelta::ToChild(_), other) => Some(other),
            (ScopeDelta::ToParent(a), ScopeDelta::ToParent(b)) => {
                *self = ScopeDelta::ToParent(a + b);
                None
            }
            (ScopeDelta::ToParent(_), other) => Some(other),
        }
    }

    pub fn evolve(&self, tag: &Tag) -> Tag {
        match self {
            ScopeDelta::None => tag.clone(),
            ScopeDelta::ToSibling(d) => {
                assert!(*d > 0);
                let mut e = tag.advance();
                if *d > 1 {
                    for _ in 1..*d {
                        e = e.advance();
                    }
                }
                e
            }
            ScopeDelta::ToChild(d) => {
                assert!(*d > 0);
                let mut e = Tag::inherit(tag, 0);
                if *d > 1 {
                    for _ in 1..*d {
                        e = Tag::inherit(&e, 0);
                    }
                }
                e
            }
            ScopeDelta::ToParent(d) => {
                assert!(*d > 0);
                let mut e = tag.to_parent_silent();
                if *d > 1 {
                    for _ in 1..*d {
                        e = e.to_parent_silent();
                    }
                }
                e
            }
        }
    }

    pub fn evolve_back(&self, tag: &Tag) -> Tag {
        match self {
            ScopeDelta::None => tag.clone(),
            ScopeDelta::ToSibling(d) => {
                assert!(*d > 0);
                let mut e = tag.retreat();
                if *d > 1 {
                    for _ in 1..*d {
                        e = e.retreat();
                    }
                }
                e
            }
            ScopeDelta::ToChild(d) => {
                assert!(*d > 0);
                let mut e = tag.to_parent_silent();
                if *d > 1 {
                    for _ in 1..*d {
                        e = e.to_parent_silent();
                    }
                }
                e
            }
            ScopeDelta::ToParent(d) => {
                assert!(*d > 0);
                let mut e = Tag::inherit(tag, 0);
                if *d > 1 {
                    for _ in 1..*d {
                        e = Tag::inherit(&e, 0);
                    }
                }
                e
            }
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct MergedScopeDelta {
    pub origin_scope_level: usize,
    deltas: ScopeDelta,
}

impl MergedScopeDelta {
    pub fn new(origin_scope_level: usize) -> Self {
        MergedScopeDelta { origin_scope_level, deltas: ScopeDelta::None }
    }

    pub fn output_scope_level(&self) -> usize {
        let x = self.origin_scope_level as i32 + self.deltas.level_delta();
        assert!(x >= 0);
        x as usize
    }

    pub fn scope_level_delta(&self) -> i32 {
        self.deltas.level_delta()
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.deltas.try_merge(delta)
    }

    #[inline]
    pub fn evolve(&self, tag: &Tag) -> Tag {
        self.deltas.evolve(tag)
    }

    #[inline]
    pub fn evolve_back(&self, tag: &Tag) -> Tag {
        self.deltas.evolve_back(tag)
    }
}
