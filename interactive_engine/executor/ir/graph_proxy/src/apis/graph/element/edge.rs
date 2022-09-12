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
use ir_common::{LabelId, NameOrId};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::apis::{read_id, write_id, DynDetails, Element, GraphElement, ID};
use crate::utils::expr::eval::Context;

#[derive(Clone, Debug)]
pub struct Edge {
    id: ID,
    label: Option<LabelId>,
    pub src_id: ID,
    pub dst_id: ID,
    pub src_label: Option<LabelId>,
    pub dst_label: Option<LabelId>,
    /// An indicator for whether this edge is obtained from the source or destination vertex
    from_src: bool,
    details: DynDetails,
}

impl Element for Edge {
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

impl GraphElement for Edge {
    fn id(&self) -> ID {
        self.id
    }
    fn label(&self) -> Option<LabelId> {
        self.label
    }
    fn details(&self) -> Option<&DynDetails> {
        Some(&self.details)
    }
}

impl Edge {
    pub fn new(id: ID, label: Option<LabelId>, src: ID, dst: ID, details: DynDetails) -> Self {
        Edge {
            id,
            label,
            src_id: src,
            dst_id: dst,
            src_label: None,
            dst_label: None,
            from_src: true,
            details,
        }
    }

    pub fn with_from_src(
        id: ID, label: Option<LabelId>, src: ID, dst: ID, from_src: bool, details: DynDetails,
    ) -> Self {
        Edge { id, label, src_id: src, dst_id: dst, src_label: None, dst_label: None, from_src, details }
    }

    pub fn set_src_label(&mut self, label: LabelId) {
        self.src_label = Some(label);
    }

    pub fn set_dst_label(&mut self, label: LabelId) {
        self.dst_label = Some(label);
    }

    pub fn get_src_label(&self) -> Option<&LabelId> {
        self.src_label.as_ref()
    }

    pub fn get_dst_label(&self) -> Option<&LabelId> {
        self.dst_label.as_ref()
    }

    pub fn get_other_id(&self) -> ID {
        if self.from_src {
            self.dst_id
        } else {
            self.src_id
        }
    }

    pub fn get_other_label(&self) -> Option<&LabelId> {
        if self.from_src {
            self.get_dst_label()
        } else {
            self.get_src_label()
        }
    }

    pub fn get_details_mut(&mut self) -> &mut DynDetails {
        &mut self.details
    }
}

impl Encode for Edge {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        write_id(writer, self.id)?;
        self.label.write_to(writer)?;
        write_id(writer, self.src_id)?;
        write_id(writer, self.dst_id)?;
        self.src_label.write_to(writer)?;
        self.dst_label.write_to(writer)?;
        self.from_src.write_to(writer)?;
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Edge {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let id = read_id(reader)?;
        let label = <Option<LabelId>>::read_from(reader)?;
        let src_id = read_id(reader)?;
        let dst_id = read_id(reader)?;
        let src_label = <Option<LabelId>>::read_from(reader)?;
        let dst_label = <Option<LabelId>>::read_from(reader)?;
        let from_src = <bool>::read_from(reader)?;
        let details = <DynDetails>::read_from(reader)?;
        Ok(Edge { id, label, src_id, dst_id, src_label, dst_label, from_src, details })
    }
}

impl Context<Edge> for Edge {
    fn get(&self, _tag: Option<&NameOrId>) -> Option<&Edge> {
        Some(&self)
    }
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl PartialOrd for Edge {
    // TODO: not sure if it is reasonable. Edge may be not comparable.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id().partial_cmp(&other.id())
    }
}

impl TryFrom<result_pb::Edge> for Edge {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Edge) -> Result<Self, Self::Error> {
        let mut edge = Edge::new(
            e.id as ID,
            e.label
                .map(|label| label.try_into())
                .transpose()?,
            e.src_id as ID,
            e.dst_id as ID,
            DynDetails::default(),
        );
        if let Some(src_label) = e.src_label {
            edge.set_src_label(src_label.try_into()?);
        }
        if let Some(dst_label) = e.dst_label {
            edge.set_dst_label(dst_label.try_into()?);
        }
        Ok(edge)
    }
}
