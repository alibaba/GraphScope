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

use crate::expr::eval::Context;
use crate::graph::element::{Element, GraphElement};
use crate::graph::property::{Details, DynDetails};
use crate::graph::ID;
use dyn_type::BorrowObject;
use ir_common::NameOrId;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::io;

#[derive(Clone, Debug)]
pub struct Edge {
    pub src_id: ID,
    pub dst_id: ID,
    src_label: Option<NameOrId>,
    dst_label: Option<NameOrId>,
    /// An indicator for whether this edge is obtained from the source or destination vertex
    from_src: bool,
    details: DynDetails,
}

impl Element for Edge {
    fn details(&self) -> Option<&DynDetails> {
        Some(&self.details)
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.id().into()
    }
}

impl GraphElement for Edge {
    fn id(&self) -> ID {
        self.details.get_id()
    }

    fn label(&self) -> Option<&NameOrId> {
        self.details.get_label()
    }
}

impl Edge {
    pub fn new(src: ID, dst: ID, details: DynDetails) -> Self {
        Edge { src_id: src, dst_id: dst, src_label: None, dst_label: None, from_src: true, details }
    }

    pub fn with_from_src(src: ID, dst: ID, from_src: bool, details: DynDetails) -> Self {
        Edge { src_id: src, dst_id: dst, src_label: None, dst_label: None, from_src, details }
    }

    pub fn set_src_label(&mut self, label: NameOrId) {
        self.src_label = Some(label);
    }

    pub fn set_dst_label(&mut self, label: NameOrId) {
        self.dst_label = Some(label);
    }

    pub fn get_src_label(&self) -> Option<&NameOrId> {
        self.src_label.as_ref()
    }

    pub fn get_dst_label(&self) -> Option<&NameOrId> {
        self.dst_label.as_ref()
    }

    pub fn get_other_id(&self) -> ID {
        if self.from_src {
            self.dst_id
        } else {
            self.src_id
        }
    }

    pub fn get_other_label(&self) -> Option<&NameOrId> {
        if self.from_src {
            self.get_dst_label()
        } else {
            self.get_src_label()
        }
    }
}

impl Encode for Edge {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u128(self.src_id)?;
        writer.write_u128(self.dst_id)?;
        self.src_label.write_to(writer)?;
        self.dst_label.write_to(writer)?;
        self.from_src.write_to(writer)?;
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Edge {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let src_id = reader.read_u128()?;
        let dst_id = reader.read_u128()?;
        let src_label = <Option<NameOrId>>::read_from(reader)?;
        let dst_label = <Option<NameOrId>>::read_from(reader)?;
        let from_src = <bool>::read_from(reader)?;
        let details = <DynDetails>::read_from(reader)?;
        Ok(Edge { src_id, dst_id, src_label, dst_label, from_src, details })
    }
}

impl Context<Edge> for Edge {
    fn get(&self, _tag: Option<&NameOrId>) -> Option<&Edge> {
        Some(&self)
    }
}
