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

use std::io;

use dyn_type::BorrowObject;
use ir_common::NameOrId;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::expr::eval::Context;
use crate::graph::element::{Element, GraphElement};
use crate::graph::property::{Details, DynDetails};
use crate::graph::ID;

#[derive(Clone, Debug)]
pub struct Vertex {
    details: DynDetails,
}

impl Vertex {
    pub fn new(details: DynDetails) -> Self {
        Vertex { details }
    }
}

impl Element for Vertex {
    fn details(&self) -> Option<&DynDetails> {
        Some(&self.details)
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.id().into()
    }
}

impl GraphElement for Vertex {
    fn id(&self) -> ID {
        self.details.get_id()
    }

    fn label(&self) -> Option<&NameOrId> {
        self.details.get_label()
    }
}

impl Encode for Vertex {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Vertex {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let details = <DynDetails>::read_from(reader)?;
        Ok(Vertex { details })
    }
}

impl Context<Vertex> for Vertex {
    fn get(&self, _tag: Option<&NameOrId>) -> Option<&Vertex> {
        Some(&self)
    }
}
