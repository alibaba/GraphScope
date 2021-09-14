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

impl ScopeDelta {
    pub fn try_merge(&mut self, other: ScopeDelta) -> Option<ScopeDelta> {
        match *self {
            ScopeDelta::None => {
                *self = other;
                None
            }
            ScopeDelta::ToSibling(a) => match other {
                ScopeDelta::None => None,
                ScopeDelta::ToSibling(b) => {
                    *self = ScopeDelta::ToSibling(a + b);
                    None
                }
                ScopeDelta::ToChild(_) => Some(other),
                ScopeDelta::ToParent(d) => {
                    *self = ScopeDelta::ToParent(d);
                    None
                }
            },
            ScopeDelta::ToChild(a) => match other {
                ScopeDelta::None => None,
                ScopeDelta::ToSibling(_) => Some(other),
                ScopeDelta::ToChild(b) => {
                    *self = ScopeDelta::ToChild(a + b);
                    None
                }
                ScopeDelta::ToParent(b) => {
                    if a > b {
                        *self = ScopeDelta::ToChild(a - b);
                    } else if a == b {
                        *self = ScopeDelta::None;
                    } else {
                        *self = ScopeDelta::ToParent(b - a);
                    }
                    None
                }
            },
            ScopeDelta::ToParent(a) => match other {
                ScopeDelta::None => None,
                ScopeDelta::ToSibling(_) => Some(other),
                ScopeDelta::ToChild(b) => {
                    if a > b {
                        *self = ScopeDelta::ToParent(a - b);
                    } else if a == b {
                        *self = ScopeDelta::None;
                    } else {
                        *self = ScopeDelta::ToChild(b - a);
                    }
                    None
                }
                ScopeDelta::ToParent(b) => {
                    *self = ScopeDelta::ToParent(a + b);
                    None
                }
            },
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

#[derive(Default, Clone)]
pub struct MergedScopeDelta {
    pub origin_scope_level: usize,
    pub scope_level_delta: i32,
    deltas: Vec<ScopeDelta>,
}

impl MergedScopeDelta {
    pub fn new(origin_scope_level: usize) -> Self {
        MergedScopeDelta { origin_scope_level, scope_level_delta: 0, deltas: vec![] }
    }

    pub fn output_scope_level(&self) -> usize {
        let x = self.origin_scope_level as i32 + self.scope_level_delta;
        assert!(x >= 0);
        x as usize
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) {
        match delta {
            ScopeDelta::None => (),
            ScopeDelta::ToSibling(t) => {
                if self.deltas.is_empty() {
                    self.deltas.push(ScopeDelta::ToSibling(t));
                } else {
                    let len = self.deltas.len();
                    if let Some(d) = self.deltas[len - 1].try_merge(ScopeDelta::ToSibling(t)) {
                        self.deltas.push(d);
                    }
                }
            }
            ScopeDelta::ToChild(t) => {
                let d = t as i32;
                self.scope_level_delta += d;
                if self.deltas.is_empty() {
                    self.deltas.push(ScopeDelta::ToChild(t));
                } else {
                    let len = self.deltas.len();
                    if let Some(d) = self.deltas[len - 1].try_merge(ScopeDelta::ToChild(t)) {
                        self.deltas.push(d);
                    }
                }
            }
            ScopeDelta::ToParent(t) => {
                let d = t as i32;
                self.scope_level_delta -= d;
                if self.deltas.is_empty() {
                    self.deltas.push(ScopeDelta::ToParent(t));
                } else {
                    let len = self.deltas.len();
                    if let Some(d) = self.deltas[len - 1].try_merge(ScopeDelta::ToParent(t)) {
                        self.deltas.push(d);
                    }
                }
            }
        }
    }

    #[inline]
    pub fn evolve(&self, tag: &Tag) -> Tag {
        if self.deltas.is_empty() {
            tag.clone()
        } else {
            let mut e = self.deltas[0].evolve(tag);
            if self.deltas.len() > 1 {
                for d in &self.deltas[1..] {
                    e = d.evolve(&e);
                }
            }
            e
        }
    }

    #[inline]
    pub fn evolve_back(&self, tag: &Tag) -> Tag {
        if self.deltas.is_empty() {
            tag.clone()
        } else {
            let len = self.deltas.len();
            let mut e = self.deltas[len - 1].evolve_back(tag);
            if len > 1 {
                for i in (0..len - 1).rev() {
                    e = self.deltas[i].evolve_back(&e);
                }
            }
            e
        }
    }
}
