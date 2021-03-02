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

use crate::object::BorrowObject;
use crate::structure::element::Label;
use crate::{Object, ID};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum Token {
    Id,
    Label,
    Property(String),
}

pub trait Details: Send + Sync {
    fn get_property(&self, key: &str) -> Option<BorrowObject>;

    fn get_id(&self) -> ID;

    fn get_label(&self) -> &Label;
}

#[derive(Clone)]
pub struct DynDetails {
    inner: Arc<dyn Details>,
}

impl DynDetails {
    pub fn new<P: Details + 'static>(p: P) -> Self {
        DynDetails { inner: Arc::new(p) }
    }
}

impl Details for DynDetails {
    fn get_property(&self, key: &str) -> Option<BorrowObject> {
        self.inner.get_property(key)
    }

    fn get_id(&self) -> ID {
        self.inner.get_id()
    }

    fn get_label(&self) -> &Label {
        self.inner.get_label()
    }
}

#[allow(dead_code)]
pub struct DefaultDetails {
    id: ID,
    label: Label,
    inner: HashMap<String, Object>,
}

#[allow(dead_code)]
impl DefaultDetails {
    pub fn new(id: ID, label: Label) -> Self {
        DefaultDetails { id, label, inner: HashMap::new() }
    }

    pub fn new_with_prop(id: ID, label: Label, properties: HashMap<String, Object>) -> Self {
        DefaultDetails { id, label, inner: properties }
    }
}

impl Deref for DefaultDetails {
    type Target = HashMap<String, Object>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DefaultDetails {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Details for DefaultDetails {
    fn get_property(&self, key: &str) -> Option<BorrowObject> {
        self.inner.get(key).map(|o| o.as_borrow())
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_label(&self) -> &Label {
        &self.label
    }
}
