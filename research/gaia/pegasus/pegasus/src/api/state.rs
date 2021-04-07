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

use super::meta::OperatorMeta;
use crate::Tag;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub trait State: Send + Default + 'static {}

impl<T: ?Sized + Send + Default + 'static> State for T {}

pub struct OperatorState<S: State> {
    inner: S,
    reach_final: bool,
}

impl<S: State> std::ops::Deref for OperatorState<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: State> std::ops::DerefMut for OperatorState<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<S: State> OperatorState<S> {
    #[inline(always)]
    pub fn set_final(&mut self) {
        self.reach_final = true;
    }

    #[inline(always)]
    pub fn is_reach_final(&self) -> bool {
        self.reach_final
    }

    #[inline]
    pub fn detach(self) -> S {
        self.inner
    }
}

impl<S: State> Default for OperatorState<S> {
    fn default() -> Self {
        OperatorState { inner: S::default(), reach_final: false }
    }
}

pub struct StateMap<V> {
    scope_depth: usize,
    map: HashMap<Tag, Option<V>>,
    notified: Vec<(Tag, V)>,
}

impl<V> Default for StateMap<V> {
    fn default() -> Self {
        StateMap { scope_depth: usize::default(), map: HashMap::new(), notified: Vec::new() }
    }
}

impl<V> StateMap<V> {
    pub fn new(meta: &OperatorMeta) -> Self {
        StateMap { scope_depth: meta.scope_depth, map: HashMap::new(), notified: Vec::new() }
    }

    pub fn insert<T: AsRef<Tag>>(&mut self, key: T, state: V) {
        let key = key.as_ref().clone();
        self.map.insert(key, Some(state));
    }

    pub fn get<T: AsRef<Tag>>(&self, key: T) -> Option<&V> {
        self.map.get(key.as_ref()).map(|v| v.as_ref()).unwrap_or(None)
    }

    pub fn contains<T: AsRef<Tag>>(&self, key: T) -> bool {
        self.map.contains_key(key.as_ref())
    }

    pub fn get_mut<T: AsRef<Tag>>(&mut self, key: T) -> Option<&mut V> {
        self.map.get_mut(key.as_ref()).map(|v| v.as_mut()).unwrap_or(None)
    }

    pub fn entry<T: AsRef<Tag>>(&mut self, key: T) -> WrapEntry<'_, V> {
        let entry = self.map.entry(key.as_ref().clone());
        WrapEntry::new(entry)
    }

    pub fn remove<T: AsRef<Tag>>(&mut self, key: T) -> Option<V> {
        self.map.remove(key.as_ref()).unwrap_or(None)
    }

    pub fn notify<T: AsRef<Tag>>(&mut self, notification: T) {
        let notification = notification.as_ref();
        if notification.len() == self.scope_depth {
            if let Some(Some(state)) = self.map.remove(notification) {
                self.notified.push((notification.clone(), state));
            }
        } else if notification.len() < self.scope_depth {
            let mut notified = std::mem::replace(&mut self.notified, vec![]);
            if notification.is_root() {
                for (tag, mut state) in self.map.drain() {
                    if let Some(state) = state.take() {
                        notified.push((tag.clone(), state));
                    }
                }
            } else {
                self.map.retain(|k, s| {
                    let remove = notification.is_parent_of(k);
                    if remove {
                        if let Some(state) = s.take() {
                            notified.push((k.clone(), state));
                        }
                    }
                    !remove
                });
            }
            self.notified = notified;
        }
    }

    #[inline]
    pub fn extract_notified(&mut self) -> &mut Vec<(Tag, V)> {
        &mut self.notified
    }
}

pub struct WrapEntry<'a, V> {
    inner: Entry<'a, Tag, Option<V>>,
}

impl<'a, V> WrapEntry<'a, V> {
    pub fn new(entry: Entry<'a, Tag, Option<V>>) -> Self {
        WrapEntry { inner: entry }
    }

    pub fn or_insert(self, value: V) -> &'a mut V {
        self.inner.or_insert(Some(value)).as_mut().unwrap()
    }

    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        self.inner.or_insert_with(|| Some(default())).as_mut().unwrap()
    }
}
