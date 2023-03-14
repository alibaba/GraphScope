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

use std::any::Any;
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};
use std::io;

use ahash::HashMap;
use dyn_type::{BorrowObject, Object};
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::{LabelId, NameOrId};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::apis::{
    get_graph, read_id, write_id, Details, DynDetails, Element, GraphElement, PropertyValue, QueryParams,
    ID,
};
use crate::utils::expr::eval::Context;

#[derive(Clone, Debug, Default)]
pub struct Vertex {
    id: ID,
    label: Option<LabelId>,
    details: DynDetails,
}

impl Vertex {
    pub fn new(id: ID, label: Option<LabelId>, details: DynDetails) -> Self {
        Vertex { id, label, details }
    }

    pub fn get_details_mut(&mut self) -> &mut DynDetails {
        &mut self.details
    }
}

impl Element for Vertex {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }
    fn len(&self) -> usize {
        1
    }
    fn as_borrow_object(&self) -> BorrowObject {
        self.id().into()
    }
}

impl GraphElement for Vertex {
    fn id(&self) -> ID {
        self.id
    }
    fn label(&self) -> Option<LabelId> {
        self.label
    }

    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        match self.details {
            DynDetails::Empty => {
                let mut prop = None;
                if let Some(graph) = get_graph() {
                    if let Ok(mut iter) = graph.get_vertex(&[self.id], &QueryParams::default()) {
                        if let Some(v) = iter.next() {
                            prop = v
                                .get_property(key)
                                .map(|prop| PropertyValue::Owned(prop.try_to_owned().unwrap()));
                        }
                    }
                }
                prop
            }
            DynDetails::Default(_) | DynDetails::Lazy(_) => self.details.get_property(key),
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        match self.details {
            DynDetails::Empty => {
                let mut prop = None;
                if let Some(graph) = get_graph() {
                    if let Ok(mut iter) = graph.get_vertex(&[self.id], &QueryParams::default()) {
                        if let Some(v) = iter.next() {
                            prop = v.get_all_properties();
                        }
                    }
                }
                prop
            }
            DynDetails::Default(_) | DynDetails::Lazy(_) => self.details.get_all_properties(),
        }
    }
}

impl_as_any!(Vertex);

impl Encode for Vertex {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        write_id(writer, self.id)?;
        self.label.write_to(writer)?;
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Vertex {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let id = read_id(reader)?;
        let label = <Option<LabelId>>::read_from(reader)?;
        let details = <DynDetails>::read_from(reader)?;
        Ok(Vertex { id, label, details })
    }
}

impl Context<Vertex> for Vertex {
    fn get(&self, _tag: Option<&NameOrId>) -> Option<&Vertex> {
        Some(&self)
    }
}

impl Hash for Vertex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for Vertex {
    // TODO: not sure if it is reasonable. Vertex may be not comparable.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Eq for Vertex {}

impl Ord for Vertex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl From<ID> for Vertex {
    fn from(id: ID) -> Self {
        Vertex::new(id, None, DynDetails::default())
    }
}

impl TryFrom<result_pb::Vertex> for Vertex {
    type Error = ParsePbError;
    fn try_from(v: result_pb::Vertex) -> Result<Self, Self::Error> {
        let vertex = Vertex::new(
            v.id as ID,
            v.label
                .map(|label| label.try_into())
                .transpose()?,
            DynDetails::default(),
        );
        Ok(vertex)
    }
}
