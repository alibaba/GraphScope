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

use crate::structure::element::{read_id, write_id, Element, Label, ID};
use crate::structure::property::DynDetails;
use crate::structure::Details;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::io;

#[derive(Clone)]
pub struct Vertex {
    pub id: ID,
    pub label: Option<Label>,
    details: DynDetails,
}

impl Vertex {
    pub fn new<D: Details + 'static>(id: ID, label: Option<Label>, details: D) -> Self {
        Vertex { id, label, details: DynDetails::new(details) }
    }
}

impl Element for Vertex {
    fn id(&self) -> ID {
        self.id
    }

    fn label(&self) -> &Label {
        if let Some(ref l) = self.label {
            l
        } else {
            self.details.get_label()
        }
    }

    fn details(&self) -> &DynDetails {
        &self.details
    }
}

impl Encode for Vertex {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        write_id(self.id, writer)?;
        self.label.write_to(writer)?;
        self.details.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Vertex {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let id = read_id(reader)?;
        let label = <Option<Label>>::read_from(reader)?;
        let details = <DynDetails>::read_from(reader)?;
        Ok(Vertex { id, label, details })
    }
}
