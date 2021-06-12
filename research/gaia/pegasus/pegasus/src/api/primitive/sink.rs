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

use crate::api::meta::OperatorMeta;
use crate::codec::{Decode, Encode, ReadExt, WriteExt};
use crate::errors::BuildJobError;
use crate::{Data, Tag};
use std::io;

pub enum ResultSet<D> {
    Data(Vec<D>),
    End,
}

pub trait Sink<D: Data> {
    fn sink_by<B, F>(&self, construct: B) -> Result<(), BuildJobError>
    where
        B: FnOnce(&OperatorMeta) -> F,
        F: FnMut(&Tag, ResultSet<D>) + Send + 'static;
}

impl<D: Encode> Encode for ResultSet<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            ResultSet::Data(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            ResultSet::End => {
                writer.write_u8(1)?;
            }
        }
        Ok(())
    }
}

impl<D: Decode> Decode for ResultSet<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = <u8>::read_from(reader)?;
        match e {
            0 => {
                let v = <Vec<D>>::read_from(reader)?;
                Ok(ResultSet::Data(v))
            }
            1 => Ok(ResultSet::End),
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}
