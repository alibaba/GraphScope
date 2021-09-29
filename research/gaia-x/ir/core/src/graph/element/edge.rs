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

use crate::graph::element::Element;
use crate::graph::property::{Details, DynDetails, Label, ID};
use dyn_type::BorrowObject;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::io;

#[derive(Clone)]
pub struct Edge {
    pub src_id: ID,
    pub dst_id: ID,
    src_label: Option<Label>,
    dst_label: Option<Label>,
    details: DynDetails,
}

impl Element for Edge {
    fn id(&self) -> Option<ID> {
        Some(self.details.get_id())
    }

    fn label(&self) -> Option<&Label> {
        Some(self.details.get_label())
    }

    fn details(&self) -> Option<&DynDetails> {
        Some(&self.details)
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.id().unwrap().into()
    }
}

impl Edge {
    pub fn new(src: ID, dst: ID, details: DynDetails) -> Self {
        Edge {
            src_id: src,
            dst_id: dst,
            src_label: None,
            dst_label: None,
            details,
        }
    }

    pub fn set_src_label(&mut self, label: Label) {
        self.src_label = Some(label);
    }

    pub fn set_dst_label(&mut self, label: Label) {
        self.dst_label = Some(label);
    }
}

impl Encode for Edge {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u128(self.src_id)?;
        writer.write_u128(self.dst_id)?;
        self.src_label.write_to(writer)?;
        self.dst_label.write_to(writer)?;
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Edge {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let src_id = reader.read_u128()?;
        let dst_id = reader.read_u128()?;
        let src_label = <Option<Label>>::read_from(reader)?;
        let dst_label = <Option<Label>>::read_from(reader)?;
        let details = <DynDetails>::read_from(reader)?;
        Ok(Edge {
            src_id,
            dst_id,
            src_label,
            dst_label,
            details,
        })
    }
}
