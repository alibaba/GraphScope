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

use crate::generated::gremlin as pb;
use crate::generated::gremlin::TraverserRequirement;
use crate::process::traversal::path::{Path, PathItem, ResultPath};
use crate::process::traversal::pop::Pop;
use crate::structure::codec::ParseError;
use crate::structure::{GraphElement, Tag};
use crate::{DynIter, Element, FromPb, Object};
use bit_set::BitSet;
use pegasus::api::function::{FnResult, Partition};
use pegasus::codec::*;
use pegasus::Data;
use pegasus_server::AnyData;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io;
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

impl FromPb<Vec<pb::TraverserRequirement>> for Requirement {
    fn from_pb(requirements_pb: Vec<pb::TraverserRequirement>) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        let mut requirements: Requirement = Default::default();
        for requirement_pb in requirements_pb {
            match requirement_pb {
                TraverserRequirement::Bulk => requirements.insert(Requirement::BULK),
                TraverserRequirement::LabeledPath => requirements.insert(Requirement::LABELEDPATH),
                TraverserRequirement::NestedLoop => requirements.insert(Requirement::NESTEDLOOP),
                TraverserRequirement::Object => requirements.insert(Requirement::OBJECT),
                TraverserRequirement::OneBulk => requirements.insert(Requirement::ONEBULK),
                TraverserRequirement::Path => requirements.insert(Requirement::PATH),
                TraverserRequirement::Sack => requirements.insert(Requirement::SACK),
                TraverserRequirement::SideEffects => requirements.insert(Requirement::SIDEEFFECT),
                TraverserRequirement::SingleLoop => requirements.insert(Requirement::SINGLELOOP),
            }
        }
        Ok(requirements)
    }
}

#[derive(Clone, Debug)]
pub enum Traverser {
    Path(Path),
    LabelPath(Path),
    NoPath(GraphElement),
    Unknown(Object),
}

impl Traverser {
    pub fn new<E: Into<GraphElement>>(e: E) -> Self {
        Traverser::NoPath(e.into())
    }

    pub fn with_path<E: Into<GraphElement>>(
        e: E, labels: &BitSet, requirement: Requirement,
    ) -> Self {
        if requirement.contains(Requirement::PATH) {
            debug!("start a path traverser");
            let mut path = Path::new(e.into(), false);
            path.extend(labels);
            Traverser::Path(path)
        } else {
            debug!("start a label path traverser");
            let mut path = Path::new(e.into(), true);
            path.extend(labels);
            Traverser::LabelPath(path)
        }
    }

    pub fn get_element(&self) -> Option<&GraphElement> {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => p.head().as_element(),
            Traverser::NoPath(e) => Some(e),
            Traverser::Unknown(_) => None,
        }
    }

    pub fn get_element_mut(&mut self) -> Option<&mut GraphElement> {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => p.head_mut().as_mut_element(),
            Traverser::NoPath(e) => Some(e),
            Traverser::Unknown(_) => None,
        }
    }

    pub fn get_object(&self) -> Option<&Object> {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => p.head().as_detached(),
            Traverser::NoPath(_) => None,
            Traverser::Unknown(o) => Some(o),
        }
    }

    pub fn get_object_mut(&mut self) -> Option<&mut Object> {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => p.head_mut().as_mut_detached(),
            Traverser::NoPath(_) => None,
            Traverser::Unknown(o) => Some(o),
        }
    }

    pub fn get_element_attached(&self) -> Option<&Object> {
        if let Some(element) = self.get_element() {
            element.get_attached()
        } else {
            None
        }
    }

    pub fn split<E: Into<GraphElement>>(&mut self, e: E, labels: &BitSet) {
        match self {
            Traverser::Path(p) => {
                p.extend_with(e.into(), labels, false);
            }
            Traverser::LabelPath(p) => {
                p.extend_with(e.into(), labels, true);
            }
            Traverser::NoPath(ori) => *ori = e.into(),
            Traverser::Unknown(_) => unimplemented!(),
        }
    }

    pub fn split_with_value<T: Into<Object>>(&mut self, o: T, labels: &BitSet) {
        match self {
            Traverser::Path(p) => {
                p.extend_with(o.into(), labels, false);
            }
            Traverser::LabelPath(p) => {
                p.extend_with(o.into(), labels, true);
            }
            Traverser::NoPath(_) => *self = Traverser::Unknown(o.into()),
            Traverser::Unknown(ori) => {
                *ori = o.into();
            }
        }
    }

    pub fn modify_head<E: Into<GraphElement>>(&mut self, e: E, labels: &BitSet) {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => {
                p.modify_head_with(e, labels);
            }
            Traverser::NoPath(ori) => *ori = e.into(),
            Traverser::Unknown(_) => unimplemented!(),
        }
    }

    pub fn remove_labels(&mut self, labels: &BitSet) {
        match self {
            Traverser::Path(p) => {
                debug!("Remove tags {:?} in Path {:?}, but why?", labels, p);
                p.remove_tag(labels, false)
            }
            Traverser::LabelPath(p) => p.remove_tag(labels, true),
            Traverser::NoPath(e) => {
                debug!("Try remove tags {:?} in NoPath {:?}, but will not", labels, e)
            }
            Traverser::Unknown(o) => {
                debug!("Try remove tags {:?} in Unknown {:?}, but will not", labels, o)
            }
        }
    }

    pub fn add_labels(&mut self, labels: &BitSet) {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => p.extend(labels),
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
            Traverser::Path(p) | Traverser::LabelPath(p) => p.select(label),
            _ => None,
        }
    }

    pub fn select_as_element(&self, label: Option<&Tag>) -> Option<&GraphElement> {
        if let Some(label) = label {
            self.select_pop_as_element(Pop::Last, label)
        } else {
            self.get_element()
        }
    }

    pub fn select_as_value(&self, label: &Tag) -> Option<&Object> {
        self.select_pop_as_value(Pop::Last, label)
    }

    // TODO: select_pop
    pub fn select_pop(&self, pop: Pop, label: &Tag) -> Option<&PathItem> {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => match pop {
                _ => p.select(label),
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
            Traverser::Path(p) | Traverser::LabelPath(p) => p.finalize(),
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

    pub fn transform(self, requirement: Requirement) -> Traverser {
        match self {
            Traverser::Path(p) => {
                if requirement.contains(Requirement::PATH) {
                    Traverser::Path(p)
                } else if requirement.contains(Requirement::LABELEDPATH) {
                    Traverser::LabelPath(p)
                } else {
                    // Assume it's object for now
                    match p.head() {
                        PathItem::OnGraph(e) => Traverser::NoPath(e.clone()),
                        PathItem::Detached(o) => Traverser::Unknown(o.clone()),
                        PathItem::Empty => unimplemented!(),
                    }
                }
            }
            Traverser::LabelPath(_) => unimplemented!(),
            Traverser::NoPath(_) => unimplemented!(),
            Traverser::Unknown(_) => unimplemented!(),
        }
    }
}

impl Encode for Traverser {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Traverser::Path(p) => {
                writer.write_u8(0)?;
                p.write_to(writer)?;
            }
            Traverser::NoPath(element) => {
                writer.write_u8(1)?;
                element.write_to(writer)?;
            }
            Traverser::Unknown(object) => {
                writer.write_u8(2)?;
                object.write_to(writer)?;
            }
            Traverser::LabelPath(p) => {
                writer.write_u8(3)?;
                p.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Traverser {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let p = <Path>::read_from(reader)?;
                Ok(Traverser::Path(p))
            }
            1 => {
                let element = <GraphElement>::read_from(reader)?;
                Ok(Traverser::NoPath(element))
            }
            2 => {
                let object = <Object>::read_from(reader)?;
                Ok(Traverser::Unknown(object))
            }
            3 => {
                let p = <Path>::read_from(reader)?;
                Ok(Traverser::LabelPath(p))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl PartialEq for Traverser {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => match p.head() {
                PathItem::OnGraph(e) => match other {
                    Traverser::Path(other_p) | Traverser::LabelPath(other_p) => {
                        match other_p.head() {
                            PathItem::OnGraph(o) => e == o,
                            PathItem::Detached(_) => false,
                            PathItem::Empty => false,
                        }
                    }
                    Traverser::NoPath(o) => e == o,
                    Traverser::Unknown(_) => false,
                },
                PathItem::Detached(obj) => match other {
                    Traverser::Path(other_p) | Traverser::LabelPath(other_p) => {
                        match other_p.head() {
                            PathItem::OnGraph(_) => false,
                            PathItem::Detached(other_obj) => obj == other_obj,
                            PathItem::Empty => false,
                        }
                    }
                    Traverser::NoPath(_) => false,
                    Traverser::Unknown(other_obj) => obj == other_obj,
                },
                PathItem::Empty => match other {
                    Traverser::Path(other_p) => match other_p.head() {
                        PathItem::Empty => true,
                        _ => false,
                    },
                    _ => false,
                },
            },
            Traverser::NoPath(e) => match other {
                Traverser::Path(p) | Traverser::LabelPath(p) => match p.head() {
                    PathItem::OnGraph(o) => e == o,
                    PathItem::Detached(_) => false,
                    PathItem::Empty => false,
                },
                Traverser::NoPath(o) => e == o,
                Traverser::Unknown(_) => false,
            },
            Traverser::Unknown(obj) => match other {
                Traverser::Path(p) | Traverser::LabelPath(p) => match p.head() {
                    PathItem::OnGraph(_) => false,
                    PathItem::Detached(other_obj) => obj == other_obj,
                    PathItem::Empty => false,
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

impl<T: Send + Encode> Encode for ShadeSync<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.inner.write_to(writer)?;
        Ok(())
    }
}

impl<T: Send + Decode> Decode for ShadeSync<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let inner = T::read_from(reader)?;
        Ok(ShadeSync { inner })
    }
}

impl<T: Debug + Send> Debug for ShadeSync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<T: Send> ShadeSync<T> {
    pub fn get(&self) -> &T {
        &self.inner
    }
}

pub struct TraverserSplitIter<E> {
    labels: Arc<BitSet>,
    origin: Traverser,
    children: DynIter<E>,
}

impl<E> TraverserSplitIter<E> {
    pub fn new(origin: Traverser, labels: &Arc<BitSet>, children: DynIter<E>) -> Self {
        TraverserSplitIter { labels: labels.clone(), origin, children }
    }
}

impl<E: Into<GraphElement>> Iterator for TraverserSplitIter<E> {
    type Item = Result<Traverser, Box<dyn std::error::Error + Send>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut traverser = self.origin.clone();
        match self.children.next() {
            Some(Ok(elem)) => {
                traverser.split(elem, &self.labels);
                Some(Ok(traverser))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl Hash for Traverser {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        match self {
            Traverser::Path(p) | Traverser::LabelPath(p) => {
                let head = p.head();
                match head {
                    PathItem::OnGraph(e) => e.id().hash(&mut state),
                    PathItem::Detached(o) => o.hash(&mut state),
                    PathItem::Empty => {}
                }
            }
            Traverser::NoPath(e) => e.id().hash(&mut state),
            Traverser::Unknown(o) => o.hash(&mut state),
        }
    }
}

impl Partition for Traverser {
    fn get_partition(&self) -> FnResult<u64> {
        let mut state = DefaultHasher::new();
        self.hash(&mut state);
        Ok(state.finish())
    }
}

impl AnyData for Traverser {}
impl Traverser {
    pub fn with<T: Data + Eq>(raw: T) -> Self {
        let v = ShadeSync { inner: raw };
        Traverser::Unknown(Object::UnknownOwned(Box::new(v)))
    }
}
