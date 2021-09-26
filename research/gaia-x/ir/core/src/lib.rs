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

#[macro_use]
extern crate pegasus_common;
pub use crate::plan::ffi::*;

use crate::error::{ParsePbError, ParsePbResult};
use crate::generated::common as pb;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::io;

#[cfg(feature = "proto_inplace")]
mod generated {
    #[path = "algebra.rs"]
    pub mod algebra;
    #[path = "common.rs"]
    pub mod common;
}

#[cfg(not(feature = "proto_inplace"))]
mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }

    pub mod algebra {
        tonic::include_proto!("algebra");
    }
}

pub mod error;
pub mod expr;
pub mod graph;
pub mod plan;

pub type KeyId = i32;

/// Refer to a key of a relation or a graph element, by either a string-type name or an identifier
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum NameOrId {
    Str(String),
    Id(KeyId),
}

impl From<KeyId> for NameOrId {
    fn from(id: KeyId) -> Self {
        Self::Id(id)
    }
}

impl From<String> for NameOrId {
    fn from(str: String) -> Self {
        Self::Str(str)
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

impl FromPb<pb::NameOrId> for NameOrId {
    fn from_pb(t: pb::NameOrId) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use pb::name_or_id::Item;

        if let Some(item) = t.item {
            match item {
                Item::Name(name) => Ok(NameOrId::Str(name)),
                Item::NameId(id) => {
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

/// While it is frequently needed to transfer a proto-buf structure into a Rust structure,
/// we use this `trait` to support the transformation while capture any possible error.
pub trait FromPb<T> {
    /// A function to transfer a proto-buf structure into a Rust structure
    fn from_pb(pb: T) -> ParsePbResult<Self>
    where
        Self: Sized;
}
