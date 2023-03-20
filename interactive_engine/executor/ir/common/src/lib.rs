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

use std::convert::TryFrom;
use std::io;

use dyn_type::{BorrowObject, Object};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use prost::Message;

use crate::error::{ParsePbError, ParsePbResult};
use crate::generated::common as common_pb;
use crate::generated::results as result_pb;

pub mod error;
pub mod expr_parse;
pub mod utils;

pub use utils::*;

#[macro_use]
extern crate serde;

#[rustfmt::skip]
#[cfg(feature = "proto_inplace")]
pub mod generated {
    #[path = "algebra.rs"]
    pub mod algebra;
    #[path = "common.rs"]
    pub mod common;
    #[path = "results.rs"]
    pub mod results;
    #[path = "schema.rs"]
    pub mod schema;
    #[path = "physical.rs"]
    pub mod physical;
}

#[cfg(not(feature = "proto_inplace"))]
pub mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }
    pub mod algebra {
        tonic::include_proto!("algebra");
    }
    pub mod results {
        tonic::include_proto!("results");
    }
    pub mod schema {
        tonic::include_proto!("schema");
    }
    pub mod physical {
        tonic::include_proto!("physical");
    }
}

pub type KeyId = i32;
pub type LabelId = i32;

/// Refer to a key of a relation or a graph element, by either a string-type name or an identifier
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub enum NameOrId {
    Str(String),
    Id(KeyId),
}

impl Default for NameOrId {
    fn default() -> Self {
        Self::Str("".to_string())
    }
}

impl NameOrId {
    pub fn as_object(&self) -> Object {
        match self {
            NameOrId::Str(s) => s.to_string().into(),
            NameOrId::Id(id) => (*id as i32).into(),
        }
    }

    pub fn as_borrow_object(&self) -> BorrowObject {
        match self {
            NameOrId::Str(s) => BorrowObject::String(s.as_str()),
            NameOrId::Id(id) => (*id as i32).into(),
        }
    }
}

impl From<KeyId> for NameOrId {
    fn from(id: KeyId) -> Self {
        Self::Id(id)
    }
}

impl From<u32> for NameOrId {
    fn from(id: u32) -> Self {
        Self::Id(id as KeyId)
    }
}

impl From<String> for NameOrId {
    fn from(str: String) -> Self {
        Self::Str(str)
    }
}

impl From<&str> for NameOrId {
    fn from(str: &str) -> Self {
        Self::Str(str.to_string())
    }
}

impl Encode for NameOrId {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            NameOrId::Id(id) => {
                writer.write_u8(0)?;
                writer.write_i32(*id)?;
            }
            NameOrId::Str(str) => {
                writer.write_u8(1)?;
                str.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for NameOrId {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let id = reader.read_i32()?;
                Ok(NameOrId::Id(id))
            }
            1 => {
                let str = <String>::read_from(reader)?;
                Ok(NameOrId::Str(str))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl TryFrom<common_pb::NameOrId> for NameOrId {
    type Error = ParsePbError;

    fn try_from(t: common_pb::NameOrId) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use common_pb::name_or_id::Item;

        if let Some(item) = t.item {
            match item {
                Item::Name(name) => Ok(NameOrId::Str(name)),
                Item::Id(id) => {
                    if id < 0 {
                        Err(ParsePbError::from("key id must be positive number"))
                    } else {
                        Ok(NameOrId::Id(id as KeyId))
                    }
                }
            }
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}

impl TryFrom<common_pb::NameOrId> for KeyId {
    type Error = ParsePbError;

    fn try_from(t: common_pb::NameOrId) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use common_pb::name_or_id::Item;

        if let Some(item) = t.item {
            match item {
                Item::Name(_) => Err(ParsePbError::from("key must be a number")),
                Item::Id(id) => {
                    if id < 0 {
                        Err(ParsePbError::from("key id must be positive number"))
                    } else {
                        Ok(id as KeyId)
                    }
                }
            }
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}

impl From<NameOrId> for common_pb::NameOrId {
    fn from(tag: NameOrId) -> Self {
        let name_or_id = match tag {
            NameOrId::Str(name) => common_pb::name_or_id::Item::Name(name),
            NameOrId::Id(id) => common_pb::name_or_id::Item::Id(id),
        };
        common_pb::NameOrId { item: Some(name_or_id) }
    }
}

impl From<NameOrId> for Object {
    fn from(tag: NameOrId) -> Self {
        match tag {
            NameOrId::Str(name) => Object::from(name),
            NameOrId::Id(id) => Object::from(id),
        }
    }
}

impl Encode for result_pb::Results {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        let mut bytes = vec![];
        self.encode_raw(&mut bytes);
        writer.write_u32(bytes.len() as u32)?;
        writer.write_all(bytes.as_slice())?;
        Ok(())
    }
}

impl Decode for result_pb::Results {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut buffer = Vec::with_capacity(len);
        reader.read_exact(&mut buffer)?;
        result_pb::Results::decode(buffer.as_slice())
            .map_err(|_e| std::io::Error::new(std::io::ErrorKind::Other, "decoding result_pb failed!"))
    }
}
