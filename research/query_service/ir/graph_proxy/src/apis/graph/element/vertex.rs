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

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};
use std::io;

use dyn_type::BorrowObject;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::apis::{read_id, write_id, DefaultDetails, DynDetails, Element, GraphElement, ID};
use crate::utils::expr::eval::Context;

#[derive(Clone, Debug)]
pub struct Vertex {
    id: ID,
    label: Option<NameOrId>,
    details: DynDetails,
}

impl Vertex {
    pub fn new(id: ID, label: Option<NameOrId>, details: DynDetails) -> Self {
        Vertex { id, label, details }
    }

    pub fn get_details_mut(&mut self) -> &mut DynDetails {
        &mut self.details
    }
}

impl Element for Vertex {
    fn details(&self) -> Option<&DynDetails> {
        Some(&self.details)
    }

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

    fn label(&self) -> Option<&NameOrId> {
        self.label.as_ref()
    }
}

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
        let label = <Option<NameOrId>>::read_from(reader)?;
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
        self.id() == other.id()
    }
}

impl PartialOrd for Vertex {
    // TODO: not sure if it is reasonable. Vertex may be not comparable.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_borrow_object()
            .partial_cmp(&other.as_borrow_object())
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
            DynDetails::new(DefaultDetails::default()),
        );
        Ok(vertex)
    }
}
