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
use crate::{DynIter, Element, FromPb};
use bit_set::BitSet;
use dyn_type::Object;

use pegasus::codec::*;
use pegasus::Data;
use pegasus_server::AnyData;

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::Arc;

bitflags! {
    #[derive(Default)]
    pub struct Requirement: u64 {
        const BULK          = 0b000000001;
        const LABELED_PATH    = 0b000000010;
        const NESTED_LOOP    = 0b000000100;
        const OBJECT        = 0b000001000;
        const ONE_BULK       = 0b000010000;
        const PATH          = 0b000100000;
        const SACK          = 0b001000000;
        const SIDE_EFFECT    = 0b010000000;
        const SINGLE_LOOP    = 0b100000000;
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
                TraverserRequirement::LabeledPath => requirements.insert(Requirement::LABELED_PATH),
                TraverserRequirement::NestedLoop => requirements.insert(Requirement::NESTED_LOOP),
                TraverserRequirement::Object => requirements.insert(Requirement::OBJECT),
                TraverserRequirement::OneBulk => requirements.insert(Requirement::ONE_BULK),
                TraverserRequirement::Path => requirements.insert(Requirement::PATH),
                TraverserRequirement::Sack => requirements.insert(Requirement::SACK),
                TraverserRequirement::SideEffects => requirements.insert(Requirement::SIDE_EFFECT),
                TraverserRequirement::SingleLoop => requirements.insert(Requirement::SINGLE_LOOP),
            }
        }
        Ok(requirements)
    }
}

#[derive(Clone, Debug)]
pub enum Traverser {
    Path(Path),
    LabeledPath(Path),
    NoPath(GraphElement),
    Object(Object),
}

impl Traverser {
    pub fn new<E: Into<GraphElement>>(e: E) -> Self {
        Traverser::NoPath(e.into())
    }

    pub fn with_path<E: Into<GraphElement>>(e: E, tags: &BitSet, requirement: Requirement) -> Self {
        if requirement.contains(Requirement::PATH) {
            debug!("start a path traverser");
            let mut path = Path::new(e.into(), false);
            path.extend(tags);
            Traverser::Path(path)
        } else {
            debug!("start a label path traverser");
            let mut path = Path::new(e.into(), true);
            path.extend(tags);
            Traverser::LabeledPath(path)
        }
    }

    pub fn get_element(&self) -> Option<&GraphElement> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.head().and_then(|x| x.as_element()),
            Traverser::NoPath(e) => Some(e),
            Traverser::Object(_) => None,
        }
    }

    pub fn get_element_mut(&mut self) -> Option<&mut GraphElement> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.head_mut().as_mut_element(),
            Traverser::NoPath(e) => Some(e),
            Traverser::Object(_) => None,
        }
    }

    pub fn get_object(&self) -> Option<&Object> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => {
                p.head().and_then(|x| x.as_detached())
            }
            Traverser::NoPath(_) => None,
            Traverser::Object(o) => Some(o),
        }
    }

    pub fn get_object_mut(&mut self) -> Option<&mut Object> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.head_mut().as_mut_detached(),
            Traverser::NoPath(_) => None,
            Traverser::Object(o) => Some(o),
        }
    }

    pub fn get_element_attached(&self) -> Option<&Object> {
        if let Some(element) = self.get_element() {
            element.get_attached()
        } else {
            None
        }
    }

    pub fn split<E: Into<GraphElement>>(&mut self, e: E, tags: &BitSet) {
        match self {
            Traverser::Path(p) => {
                p.extend_with(e.into(), tags, false);
            }
            Traverser::LabeledPath(p) => {
                p.extend_with(e.into(), tags, true);
            }
            Traverser::NoPath(ori) => *ori = e.into(),
            Traverser::Object(_) => unimplemented!(),
        }
    }

    pub fn split_with_value<T: Into<Object>>(&mut self, o: T, tags: &BitSet) {
        match self {
            Traverser::Path(p) => {
                p.extend_with(o.into(), tags, false);
            }
            Traverser::LabeledPath(p) => {
                p.extend_with(o.into(), tags, true);
            }
            Traverser::NoPath(_) => *self = Traverser::Object(o.into()),
            Traverser::Object(ori) => {
                *ori = o.into();
            }
        }
    }

    pub fn remove_tags(&mut self, tags: &BitSet) {
        match self {
            Traverser::Path(p) => {
                debug!("Remove tags {:?} in Path {:?}, but why?", tags, p);
                p.remove_tag(tags)
            }
            Traverser::LabeledPath(p) => p.remove_tag(tags),
            Traverser::NoPath(e) => {
                debug!("Try remove tags {:?} in NoPath {:?}, but will not", tags, e)
            }
            Traverser::Object(o) => {
                debug!("Try remove tags {:?} in Unknown {:?}, but will not", tags, o)
            }
        }
    }

    pub fn add_tags(&mut self, tags: &BitSet) {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.extend(tags),
            _ => (),
        }
    }

    pub fn is_simple(&self) -> bool {
        match self {
            Traverser::Path(p) => p.is_simple(),
            _ => true,
        }
    }

    pub fn select(&self, tag: &Tag) -> Option<&PathItem> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.select(tag),
            _ => None,
        }
    }

    pub fn select_as_element(&self, tag: Option<&Tag>) -> Option<&GraphElement> {
        if let Some(tag) = tag {
            self.select_pop_as_element(Pop::Last, tag)
        } else {
            self.get_element()
        }
    }

    pub fn select_as_value(&self, tag: &Tag) -> Option<&Object> {
        self.select_pop_as_value(Pop::Last, tag)
    }

    pub fn select_pop(&self, pop: Pop, tag: &Tag) -> Option<&PathItem> {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => match pop {
                _ => p.select(tag),
            },
            _ => None,
        }
    }

    pub fn select_pop_as_element(&self, pop: Pop, tag: &Tag) -> Option<&GraphElement> {
        let path_item = self.select_pop(pop, tag);
        if let Some(path_item) = path_item {
            path_item.as_element()
        } else {
            None
        }
    }

    pub fn select_pop_as_value(&self, pop: Pop, tag: &Tag) -> Option<&Object> {
        let path_item = self.select_pop(pop, tag);
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
            Traverser::Path(p) | Traverser::LabeledPath(p) => p.finalize(),
            Traverser::NoPath(e) => ResultPath::new(vec![PathItem::OnGraph(e)]),
            Traverser::Object(e) => ResultPath::new(vec![PathItem::Detached(e)]),
        }
    }

    pub fn get_path_len(&self) -> usize {
        match self {
            Traverser::Path(p) => p.length(),
            Traverser::LabeledPath(p) => {
                debug!("May not be right, since this is label path length rather than path");
                p.length()
            }
            _ => 0,
        }
    }

    pub fn transform(self, requirement: Requirement) -> Traverser {
        match self {
            Traverser::Path(p) => {
                if requirement.contains(Requirement::PATH) {
                    Traverser::Path(p)
                } else if requirement.contains(Requirement::LABELED_PATH) {
                    Traverser::LabeledPath(p)
                } else {
                    // Assume it's object for now
                    match p.head() {
                        Some(PathItem::OnGraph(e)) => Traverser::NoPath(e.clone()),
                        Some(PathItem::Detached(o)) => Traverser::Object(o.clone()),
                        Some(PathItem::Empty) => unreachable!(),
                        None => unreachable!(),
                    }
                }
            }
            Traverser::LabeledPath(p) => {
                if requirement.contains(Requirement::PATH) {
                    debug!("Current is LabeledPath traverser, transform to Path should not happen");
                    Traverser::Path(p)
                } else if requirement.contains(Requirement::LABELED_PATH) {
                    Traverser::LabeledPath(p)
                } else {
                    match p.head() {
                        Some(PathItem::OnGraph(e)) => Traverser::NoPath(e.clone()),
                        Some(PathItem::Detached(o)) => Traverser::Object(o.clone()),
                        Some(PathItem::Empty) => unreachable!(),
                        None => unreachable!(),
                    }
                }
            }
            Traverser::NoPath(e) => {
                debug!("Current is NoPath traverser, transform will do nothing");
                Traverser::NoPath(e)
            }
            Traverser::Object(o) => {
                debug!(
                    "Current is object traverser, transform will do nothing. It may happen when object is ResultPath"
                );
                Traverser::Object(o)
            }
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
            Traverser::Object(object) => {
                writer.write_u8(2)?;
                object.write_to(writer)?;
            }
            Traverser::LabeledPath(p) => {
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
                Ok(Traverser::Object(object))
            }
            3 => {
                let p = <Path>::read_from(reader)?;
                Ok(Traverser::LabeledPath(p))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

/// To compare the `Traverser` for `groupby` or `dedup`.
///
/// It should require only compare on the `head` of the traverser.
/// In addition, if two traversers have different types, for example, one is a `Path` or `LabeledPath`
/// and the other is a `NoPath` or `Object`, the comparison will be made while detaching the
/// graph element or object from the head of the path.
impl PartialEq for Traverser {
    fn eq(&self, other: &Self) -> bool {
        let _is_path_eq_elem = |path: &Path, e: &GraphElement| -> bool {
            if let Some(head) = path.head() {
                if let Some(e1) = head.as_element() {
                    e1 == e
                } else {
                    false
                }
            } else {
                false
            }
        };

        let _is_path_eq_obj = |path: &Path, o: &Object| -> bool {
            if let Some(head) = path.head() {
                if let Some(o1) = head.as_detached() {
                    o1 == o
                } else {
                    false
                }
            } else {
                false
            }
        };

        match (self, other) {
            // Path compare with Path
            (Traverser::Path(p1), Traverser::Path(p2))
            | (Traverser::Path(p1), Traverser::LabeledPath(p2))
            | (Traverser::LabeledPath(p1), Traverser::Path(p2))
            | (Traverser::LabeledPath(p1), Traverser::LabeledPath(p2)) => p1.is_head_eq(p2),
            // Path compare with NoPath, namely GraphElement
            (Traverser::Path(p), Traverser::NoPath(e))
            | (Traverser::LabeledPath(p), Traverser::NoPath(e))
            | (Traverser::NoPath(e), Traverser::Path(p))
            | (Traverser::NoPath(e), Traverser::LabeledPath(p)) => _is_path_eq_elem(p, e),
            // Path compare with Object
            (Traverser::Path(p), Traverser::Object(o))
            | (Traverser::LabeledPath(p), Traverser::Object(o))
            | (Traverser::Object(o), Traverser::Path(p))
            | (Traverser::Object(o), Traverser::LabeledPath(p)) => _is_path_eq_obj(p, o),
            // GraphElement compare with GraphElement
            (Traverser::NoPath(e1), Traverser::NoPath(e2)) => e1 == e2,
            // Object compare with Object
            (Traverser::Object(o1), Traverser::Object(o2)) => o1 == o2,
            // `false` for all other cases
            (_, _) => false,
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
    tags: Arc<BitSet>,
    origin: Traverser,
    children: DynIter<E>,
}

impl<E> TraverserSplitIter<E> {
    pub fn new(origin: Traverser, tags: &Arc<BitSet>, children: DynIter<E>) -> Self {
        TraverserSplitIter { tags: tags.clone(), origin, children }
    }
}

impl<E: Into<GraphElement>> Iterator for TraverserSplitIter<E> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        let mut traverser = self.origin.clone();
        match self.children.next() {
            Some(elem) => {
                traverser.split(elem, &self.tags);
                Some(traverser)
            }
            None => None,
        }
    }
}

impl Hash for Traverser {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        match self {
            Traverser::Path(p) | Traverser::LabeledPath(p) => {
                let head = p.head();
                match head {
                    Some(PathItem::OnGraph(e)) => e.id().hash(&mut state),
                    Some(PathItem::Detached(o)) => o.hash(&mut state),
                    // "Special token "" to hash an empty `PathItem`
                    Some(PathItem::Empty) => "".hash(&mut state),
                    // "Special token "~NONE" to hash an none `PathItem`
                    None => "~NONE".hash(&mut state),
                }
            }
            Traverser::NoPath(e) => e.id().hash(&mut state),
            Traverser::Object(o) => o.hash(&mut state),
        }
    }
}

impl AnyData for Traverser {}

/// not sure if is correct to unsafe impl Sync for Traverser
unsafe impl Sync for Traverser {}

impl Traverser {
    pub fn with<T: Data + Eq>(raw: T) -> Self {
        let v = ShadeSync { inner: raw };
        Traverser::Object(Object::DynOwned(Box::new(v)))
    }
}
