//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use dyn_type::BorrowObject;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::graph::element::{Element, GraphElement, VertexOrEdge};
use crate::graph::property::DynDetails;
use crate::graph::ID;

#[derive(Clone, Debug, Hash, PartialEq, PartialOrd)]
pub enum PathStruct {
    // save the whole path
    WHOLE(Vec<VertexOrEdge>),
    // save the path_end
    END(VertexOrEdge),
}

#[derive(Clone, Debug, Hash)]
pub struct GraphPath {
    path: PathStruct,
    // TODO(bingqing): may not be a usize, can be a WeightFunc that can define a user-given weight calculation
    weight: usize,
}

impl GraphPath {
    // is_whole_path: Whether to preserve the whole path or only the path_end
    pub fn new<E: Into<VertexOrEdge>>(entry: E, is_whole_path: bool) -> Self {
        if is_whole_path {
            let mut path = Vec::new();
            path.push(entry.into());
            GraphPath { path: PathStruct::WHOLE(path), weight: 1 }
        } else {
            GraphPath { path: PathStruct::END(entry.into()), weight: 1 }
        }
    }

    pub fn append<E: Into<VertexOrEdge>>(&mut self, entry: E) {
        match self.path {
            PathStruct::WHOLE(ref mut w) => w.push(entry.into()),
            PathStruct::END(ref mut e) => *e = entry.into(),
        }
        self.weight += 1;
    }
}

impl Element for GraphPath {
    fn details(&self) -> Option<&DynDetails> {
        None
    }

    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl GraphElement for GraphPath {
    fn id(&self) -> ID {
        todo!()
    }

    fn label(&self) -> Option<&NameOrId> {
        None
    }

    fn len(&self) -> usize {
        self.weight
    }
}

impl PartialEq for GraphPath {
    fn eq(&self, other: &Self) -> bool {
        // We define eq by structure, ignoring path weight
        self.path.eq(&other.path)
    }
}
impl PartialOrd for GraphPath {
    // We define partial_cmp by structure, ignoring path weight
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

impl Encode for PathStruct {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            PathStruct::WHOLE(path) => {
                writer.write_u8(0)?;
                writer.write_u64(path.len() as u64)?;
                for vertex_or_edge in path {
                    vertex_or_edge.write_to(writer)?;
                }
            }
            PathStruct::END(path_end) => {
                writer.write_u8(1)?;
                path_end.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for PathStruct {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let length = <u64>::read_from(reader)?;
                let mut path = Vec::with_capacity(length as usize);
                for _i in 0..length {
                    let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                    path.push(vertex_or_edge)
                }
                Ok(PathStruct::WHOLE(path))
            }
            1 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                Ok(PathStruct::END(vertex_or_edge))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for GraphPath {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.path.write_to(writer)?;
        writer.write_u64(self.weight as u64)?;
        Ok(())
    }
}

impl Decode for GraphPath {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let path = <PathStruct>::read_from(reader)?;
        let weight = <u64>::read_from(reader)? as usize;
        Ok(GraphPath { path, weight })
    }
}
