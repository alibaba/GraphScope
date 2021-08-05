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

use crate::{de_dyn_obj, Object, Primitives};
use core::any::TypeId;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::io;

impl Encode for Primitives {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Primitives::Byte(b) => {
                writer.write_u8(0)?;
                b.write_to(writer)?;
            }
            Primitives::Integer(i) => {
                writer.write_u8(1)?;
                i.write_to(writer)?;
            }
            Primitives::Long(l) => {
                writer.write_u8(2)?;
                l.write_to(writer)?;
            }
            Primitives::Float(f) => {
                writer.write_u8(3)?;
                f.write_to(writer)?;
            }
            Primitives::ULLong(ull) => {
                writer.write_u8(4)?;
                ull.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Primitives {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let b = <i8>::read_from(reader)?;
                Ok(Primitives::Byte(b))
            }
            1 => {
                let i = <i32>::read_from(reader)?;
                Ok(Primitives::Integer(i))
            }
            2 => {
                let l = <i64>::read_from(reader)?;
                Ok(Primitives::Long(l))
            }
            3 => {
                let f = <f64>::read_from(reader)?;
                Ok(Primitives::Float(f))
            }
            4 => {
                let lll = <u128>::read_from(reader)?;
                Ok(Primitives::ULLong(lll))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for Object {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Object::Primitive(p) => {
                writer.write_u8(0)?;
                p.write_to(writer)?;
                Ok(())
            }
            Object::String(str) => {
                writer.write_u8(1)?;
                str.write_to(writer)?;
                Ok(())
            }
            Object::Blob(b) => {
                writer.write_u8(2)?;
                let len = b.len();
                writer.write_u64(len as u64)?;
                writer.write_all(&(**b))?;
                Ok(())
            }
            Object::DynOwned(dyn_type) => {
                writer.write_u8(3)?;
                let bytes = (**dyn_type).to_bytes()?;
                bytes.write_to(writer)?;
                Ok(())
            }
        }
    }
}

impl Decode for Object {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let p = <Primitives>::read_from(reader)?;
                Ok(Object::Primitive(p))
            }
            1 => {
                let str = <String>::read_from(reader)?;
                Ok(Object::String(str))
            }
            2 => {
                let len = <u64>::read_from(reader)?;
                let mut b = vec![];
                for _i in 0..len {
                    let ele = <u8>::read_from(reader)?;
                    b.push(ele);
                }
                Ok(Object::Blob(b.into_boxed_slice()))
            }
            3 => {
                let bytes = <Vec<u8>>::read_from(reader)?;
                let mut bytes_reader = &bytes[0..];
                let number = <u64>::read_from(&mut bytes_reader)?;
                let t: TypeId = unsafe { std::mem::transmute(number) };
                let obj = de_dyn_obj(&t, &mut bytes_reader)?;
                Ok(Object::DynOwned(obj))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "not supported")),
        }
    }
}
