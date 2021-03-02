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

use crate::structure::{Element, GraphElement, Tag};
use crate::Object;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::downcast::*;
use pegasus_common::io::{ReadExt, WriteExt};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Deref;

#[derive(Clone)]
pub enum PathItem {
    OnGraph(GraphElement),
    Detached(Object),
}

impl Debug for PathItem {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PathItem::OnGraph(e) => write!(f, "{:?}", e),
            PathItem::Detached(e) => write!(f, "{:?}", e),
        }
    }
}

impl PathItem {
    #[inline]
    pub fn as_element(&self) -> Option<&GraphElement> {
        match self {
            PathItem::OnGraph(e) => Some(e),
            PathItem::Detached(_) => None,
        }
    }

    #[inline]
    pub fn as_mut_element(&mut self) -> Option<&mut GraphElement> {
        match self {
            PathItem::OnGraph(e) => Some(e),
            PathItem::Detached(_) => None,
        }
    }

    #[inline]
    pub fn as_detached(&self) -> Option<&Object> {
        match self {
            PathItem::OnGraph(_) => None,
            PathItem::Detached(e) => Some(e),
        }
    }
}

#[derive(Clone)]
pub struct Path {
    history: Vec<PathItem>,
    head: usize,
    tags: RefCell<HashMap<Tag, Vec<usize>>>,
}

impl Path {
    pub fn new<T: Into<GraphElement>>(first: T) -> Self {
        let first = PathItem::OnGraph(first.into());
        Path { history: vec![first], head: 0, tags: RefCell::new(HashMap::new()) }
    }

    pub fn size(&self) -> usize {
        self.history.len()
    }

    pub fn head(&self) -> &PathItem {
        assert!(self.head < self.history.len());
        &self.history[self.head]
    }

    pub fn head_mut(&mut self) -> &mut PathItem {
        assert!(self.head < self.history.len());
        &mut self.history[self.head]
    }

    pub fn extend_with<T: Into<GraphElement>>(&mut self, element: T, labels: &HashSet<String>) {
        self.history.push(PathItem::OnGraph(element.into()));
        self.head = self.history.len() - 1;
        let head = self.head;
        let mut label = self.tags.borrow_mut();
        for s in labels.iter() {
            label.entry(s.clone()).or_insert_with(Vec::new).push(head);
        }
    }

    pub fn extend(&self, labels: &HashSet<String>) {
        let head = self.head;
        let mut label = self.tags.borrow_mut();
        for s in labels.iter() {
            label.entry(s.clone()).or_insert_with(Vec::new).push(head);
        }
    }

    pub fn add_detached<T: Into<Object>>(&mut self, value: T, labels: &HashSet<String>) {
        self.history.push(PathItem::Detached(value.into()));
        self.head = self.history.len() - 1;
        let head = self.head;
        let mut labeled = self.tags.borrow_mut();
        for s in labels.iter() {
            labeled.entry(s.clone()).or_insert_with(Vec::new).push(head);
        }
    }

    pub fn modify_head_with<T: Into<GraphElement>>(
        &mut self, element: T, labels: &HashSet<String>,
    ) {
        assert!(self.head < self.history.len());
        let head = self.head;
        self.history[head] = PathItem::OnGraph(element.into());
        let mut label = self.tags.borrow_mut();
        for s in labels.iter() {
            label.entry(s.clone()).or_insert_with(Vec::new).push(head);
        }
    }

    pub fn get(&self, index: usize) -> Option<&PathItem> {
        if index >= self.history.len() {
            None
        } else {
            Some(&self.history[index])
        }
    }

    pub fn has_tag(&self, label: &str) -> bool {
        self.tags.borrow().contains_key(label)
    }

    pub fn objects(&self) -> &[PathItem] {
        self.history.as_slice()
    }

    pub fn tags(&self) -> &[String] {
        unimplemented!("Path#labels")
    }

    pub fn is_simple(&self) -> bool {
        let mut set = HashSet::new();
        for e in self.history.iter() {
            if let PathItem::OnGraph(e) = e {
                if !set.insert(e.id()) {
                    return false;
                }
            }
        }
        true
    }

    pub fn select_first(&self, label: &Tag) -> Option<&PathItem> {
        let tags = self.tags.borrow();
        if let Some(idx) = tags.get(label) {
            if let Some(&first) = idx.first() {
                return self.history.get(first);
            }
        }
        None
    }

    pub fn select_last(&self, label: &Tag) -> Option<&PathItem> {
        let tags = self.tags.borrow();
        if let Some(idx) = tags.get(label) {
            if let Some(&last) = idx.last() {
                return self.history.get(last);
            }
        }
        None
    }

    pub fn sub_path(&self, _from_label: &str, _to_label: &str) -> Self {
        unimplemented!()
    }

    pub fn finalize(self) -> ResultPath {
        ResultPath::new(self.history)
    }

    pub fn length(&self) -> usize {
        self.history.len()
    }
}

impl Debug for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "path[{:?}]", self.history)
    }
}

#[derive(Debug, Clone)]
pub struct ResultPath {
    elements: Vec<PathItem>,
}

impl ResultPath {
    pub fn new(elements: Vec<PathItem>) -> Self {
        ResultPath { elements }
    }
}

impl Deref for ResultPath {
    type Target = Vec<PathItem>;

    fn deref(&self) -> &Self::Target {
        &self.elements
    }
}

impl_as_any!(ResultPath);

impl Encode for ResultPath {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl Decode for ResultPath {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        unimplemented!()
    }
}
