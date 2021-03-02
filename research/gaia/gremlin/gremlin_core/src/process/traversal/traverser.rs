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

use crate::process::traversal::path::{Path, PathItem, ResultPath};
use crate::process::traversal::pop::Pop;
use crate::structure::{GraphElement, Tag};
use crate::{DynIter, Object};
use pegasus::codec::*;
use pegasus::Data;
use pegasus_server::AnyData;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

bitflags! {
    #[derive(Default)]
    pub struct Requirement: u64 {
        const BULK          = 0b000000001;
        const LABELEDPATH   = 0b000000010;
        const NESTEDLOOP    = 0b000000100;
        const OBJECT        = 0b000001000;
        const ONEBULK       = 0b000010000;
        const PATH          = 0b000100000;
        const SACK          = 0b001000000;
        const SIDEEFFECT    = 0b010000000;
        const SINGLELOOP    = 0b100000000;
    }
}

#[derive(Clone, Debug)]
pub enum Traverser {
    Path(Path),
    NoPath(GraphElement),
    Unknown(Object),
}

impl Traverser {
    pub fn new<E: Into<GraphElement>>(e: E) -> Self {
        Traverser::NoPath(e.into())
    }

    pub fn with_path<E: Into<GraphElement>>(e: E, labels: &HashSet<String>) -> Self {
        let path = Path::new(e.into());
        path.extend(labels);
        Traverser::Path(path)
    }

    pub fn get_element(&self) -> Option<&GraphElement> {
        match self {
            Traverser::Path(p) => p.head().as_element(),
            Traverser::NoPath(e) => Some(e),
            Traverser::Unknown(_) => None,
        }
    }

    pub fn get_element_mut(&mut self) -> Option<&mut GraphElement> {
        match self {
            Traverser::Path(p) => p.head_mut().as_mut_element(),
            Traverser::NoPath(e) => Some(e),
            Traverser::Unknown(_) => None,
        }
    }

    pub fn get_object(&self) -> Option<&Object> {
        match self {
            Traverser::Path(p) => p.head().as_detached(),
            Traverser::NoPath(_) => None,
            Traverser::Unknown(o) => Some(o),
        }
    }

    pub fn split<E: Into<GraphElement>>(&self, e: E, labels: &HashSet<String>) -> Traverser {
        match self {
            Traverser::Path(p) => {
                let mut path = p.clone();
                path.extend_with(e, labels);
                Traverser::Path(path)
            }
            Traverser::NoPath(_) => Traverser::NoPath(e.into()),
            Traverser::Unknown(_) => Traverser::NoPath(e.into()),
        }
    }

    pub fn split_with_value<T: Into<Object>>(&self, o: T, labels: &HashSet<String>) -> Traverser {
        match self {
            Traverser::Path(p) => {
                let mut path = p.clone();
                path.add_detached(o, labels);
                Traverser::Path(path)
            }
            Traverser::NoPath(e) => {
                let mut e = e.clone();
                e.attach(o);
                Traverser::NoPath(e)
            }
            Traverser::Unknown(_) => Traverser::Unknown(o.into()),
        }
    }

    pub fn modify_head<E: Into<GraphElement>>(&self, e: E, labels: &HashSet<String>) -> Traverser {
        match self {
            Traverser::Path(p) => {
                let mut path = p.clone();
                path.modify_head_with(e, labels);
                Traverser::Path(path)
            }
            _ => Traverser::NoPath(e.into()),
        }
    }

    pub fn add_labels(&self, labels: &HashSet<String>) {
        match self {
            Traverser::Path(p) => p.extend(labels),
            _ => (),
        }
    }

    pub fn is_simple(&self) -> bool {
        match self {
            Traverser::Path(p) => p.is_simple(),
            _ => true,
        }
    }

    pub fn select(&self, label: &Tag) -> Option<&PathItem> {
        match self {
            Traverser::Path(p) => p.select_first(label),
            _ => None,
        }
    }

    pub fn select_as_element(&self, label: &Tag) -> Option<&GraphElement> {
        self.select_pop_as_element(Pop::Last, label)
    }

    pub fn select_as_value(&self, label: &Tag) -> Option<&Object> {
        self.select_pop_as_value(Pop::Last, label)
    }

    // TODO: select_pop
    pub fn select_pop(&self, pop: Pop, label: &Tag) -> Option<&PathItem> {
        match self {
            Traverser::Path(p) => match pop {
                Pop::First => p.select_first(label),
                Pop::Last => p.select_last(label),
                _ => unimplemented!(),
            },
            _ => None,
        }
    }

    pub fn select_pop_as_element(&self, pop: Pop, label: &Tag) -> Option<&GraphElement> {
        let path_item = self.select_pop(pop, label);
        if let Some(path_item) = path_item {
            path_item.as_element()
        } else {
            None
        }
    }

    pub fn select_pop_as_value(&self, pop: Pop, label: &Tag) -> Option<&Object> {
        let path_item = self.select_pop(pop, label);
        if let Some(path_item) = path_item {
            path_item.as_detached()
        } else {
            None
        }
    }

    pub fn has_cyclic_path(&self) -> bool {
        match self {
            Traverser::Path(p) => !p.is_simple(),
            _ => false,
        }
    }

    pub fn take_path(self) -> ResultPath {
        match self {
            Traverser::Path(p) => p.finalize(),
            Traverser::NoPath(e) => ResultPath::new(vec![PathItem::OnGraph(e)]),
            Traverser::Unknown(e) => ResultPath::new(vec![PathItem::Detached(e)]),
        }
    }

    pub fn get_path_len(&self) -> usize {
        match self {
            Traverser::Path(p) => p.length(),
            _ => 0,
        }
    }
}

impl Encode for Traverser {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl Decode for Traverser {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        unimplemented!()
    }
}

impl PartialEq for Traverser {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Traverser::Path(p) => match p.head() {
                PathItem::OnGraph(e) => match other {
                    Traverser::Path(other_p) => match other_p.head() {
                        PathItem::OnGraph(o) => e == o,
                        PathItem::Detached(_) => false,
                    },
                    Traverser::NoPath(o) => e == o,
                    Traverser::Unknown(_) => false,
                },
                PathItem::Detached(obj) => match other {
                    Traverser::Path(other_p) => match other_p.head() {
                        PathItem::OnGraph(_) => false,
                        PathItem::Detached(other_obj) => obj == other_obj,
                    },
                    Traverser::NoPath(_) => false,
                    Traverser::Unknown(other_obj) => obj == other_obj,
                },
            },
            Traverser::NoPath(e) => match other {
                Traverser::Path(p) => match p.head() {
                    PathItem::OnGraph(o) => e == o,
                    PathItem::Detached(_) => false,
                },
                Traverser::NoPath(o) => e == o,
                Traverser::Unknown(_) => false,
            },
            Traverser::Unknown(obj) => match other {
                Traverser::Path(p) => match p.head() {
                    PathItem::OnGraph(_) => false,
                    PathItem::Detached(other_obj) => obj == other_obj,
                },
                Traverser::NoPath(_) => false,
                Traverser::Unknown(other_obj) => obj == other_obj,
            },
        }
    }
}

/// not sure if is correct or safe to impl Eq for Traverser, as 'Object' only impl PartialEq;
impl Eq for Traverser {}

/// mock sync of type T , be sure that it is really safe;
/// It is used in traverser, the traverser is not sync,
/// so the traverser won't be shared between threads.
/// Because this type is only owned by traverser, so it also won't be shared between threads,
/// so it is safe to implement Sync,
pub struct ShadeSync<T: Send> {
    pub inner: T,
}

unsafe impl<T: Send> Sync for ShadeSync<T> {}

impl<T: Clone + Send> Clone for ShadeSync<T> {
    fn clone(&self) -> Self {
        ShadeSync { inner: self.inner.clone() }
    }
}

impl<T: Debug + Send> Debug for ShadeSync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl AnyData for Traverser {
    fn with<T: Data + Eq>(raw: T) -> Self {
        let v = ShadeSync { inner: raw };
        Traverser::Unknown(Object::UnknownOwned(Box::new(v)))
    }
}

pub struct TraverserSplitIter<E> {
    labels: Arc<HashSet<String>>,
    origin: Traverser,
    children: DynIter<E>,
}

impl<E> TraverserSplitIter<E> {
    pub fn new(origin: Traverser, labels: &Arc<HashSet<String>>, children: DynIter<E>) -> Self {
        TraverserSplitIter { labels: labels.clone(), origin, children }
    }
}

impl<E: Into<GraphElement>> Iterator for TraverserSplitIter<E> {
    type Item = Result<Traverser, Box<dyn std::error::Error + Send>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.children.next() {
            Some(Ok(elem)) => Some(Ok(self.origin.split(elem, &self.labels))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
