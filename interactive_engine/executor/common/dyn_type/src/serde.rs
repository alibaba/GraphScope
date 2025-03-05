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

use std::any::TypeId;
use std::collections::BTreeMap;
use std::io;

use chrono::{Datelike, Timelike};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::{de_dyn_obj, DateTimeFormats, Object, Primitives};

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
            Primitives::UInteger(i) => {
                writer.write_u8(5)?;
                i.write_to(writer)?;
            }
            Primitives::ULong(l) => {
                writer.write_u8(6)?;
                l.write_to(writer)?;
            }
            Primitives::Double(d) => {
                writer.write_u8(7)?;
                d.write_to(writer)?;
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
                let f = <f32>::read_from(reader)?;
                Ok(Primitives::Float(f))
            }
            4 => {
                let lll = <u128>::read_from(reader)?;
                Ok(Primitives::ULLong(lll))
            }
            5 => {
                let i = <u32>::read_from(reader)?;
                Ok(Primitives::UInteger(i))
            }
            6 => {
                let l = <u64>::read_from(reader)?;
                Ok(Primitives::ULong(l))
            }
            7 => {
                let d = <f64>::read_from(reader)?;
                Ok(Primitives::Double(d))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

#[rustversion::before(1.72.0)]
fn type_id_from_bytes<R: ReadExt>(reader: &mut R) -> io::Result<TypeId> {
    let number = <u64>::read_from(reader)?;
    Ok(unsafe { std::mem::transmute(number) })
}

#[rustversion::since(1.72.0)]
fn type_id_from_bytes<R: ReadExt>(reader: &mut R) -> io::Result<TypeId> {
    let number = <u128>::read_from(reader)?;
    Ok(unsafe { std::mem::transmute(number) })
}

impl Encode for DateTimeFormats {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            DateTimeFormats::Date(d) => {
                writer.write_u8(0)?;
                writer.write_i16(d.year() as i16)?;
                writer.write_u8(d.month() as u8)?;
                writer.write_u8(d.day() as u8)?;
            }
            DateTimeFormats::Time(t) => {
                writer.write_u8(1)?;
                writer.write_u8(t.hour() as u8)?;
                writer.write_u8(t.minute() as u8)?;
                writer.write_u8(t.second() as u8)?;
                writer.write_u32(t.nanosecond() as u32)?;
            }
            DateTimeFormats::DateTime(datetime) => {
                writer.write_u8(2)?;
                writer.write_i64(datetime.timestamp_millis())?;
            }
            DateTimeFormats::DateTimeWithTz(datetime_with_tz) => {
                writer.write_u8(3)?;
                writer.write_i64(
                    datetime_with_tz
                        .naive_local()
                        .timestamp_millis(),
                )?;
                writer.write_i32(datetime_with_tz.offset().local_minus_utc())?;
            }
        }
        Ok(())
    }
}

impl Decode for DateTimeFormats {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let year = <i16>::read_from(reader)?;
                let month = <u8>::read_from(reader)?;
                let day = <u8>::read_from(reader)?;
                let date = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("invalid date {:?}-{:?}-{:?}", year, month, day),
                        )
                    })?;
                Ok(DateTimeFormats::Date(date))
            }
            1 => {
                let hour = <u8>::read_from(reader)?;
                let minute = <u8>::read_from(reader)?;
                let second = <u8>::read_from(reader)?;
                let nano = <u32>::read_from(reader)?;
                let time =
                    chrono::NaiveTime::from_hms_nano_opt(hour as u32, minute as u32, second as u32, nano)
                        .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "invalid time {:?}:{:?}:{:?}.{:?}",
                                hour,
                                minute,
                                second,
                                nano / 1000_000
                            ),
                        )
                    })?;
                Ok(DateTimeFormats::Time(time))
            }
            2 => {
                let timestamp_millis = <i64>::read_from(reader)?;
                let date_time =
                    chrono::NaiveDateTime::from_timestamp_millis(timestamp_millis).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("invalid datetime {:?}", timestamp_millis),
                        )
                    })?;
                Ok(DateTimeFormats::DateTime(date_time))
            }
            3 => {
                let native_local_timestamp_millis = <i64>::read_from(reader)?;
                let offset = <i32>::read_from(reader)?;
                let tz = chrono::FixedOffset::east_opt(offset).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, format!("invalid offset {:?}", offset))
                })?;
                let date_time = chrono::NaiveDateTime::from_timestamp_millis(native_local_timestamp_millis)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("invalid datetime {:?}", native_local_timestamp_millis),
                        )
                    })?
                    .and_local_timezone(tz)
                    .single()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "invalid datetime with timezone {:?} {:?}",
                                native_local_timestamp_millis, tz
                            ),
                        )
                    })?;

                Ok(DateTimeFormats::DateTimeWithTz(date_time))
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
            Object::Vector(vec) => {
                writer.write_u8(2)?;
                vec.write_to(writer)?;
                Ok(())
            }
            Object::KV(kv) => {
                writer.write_u8(3)?;
                writer.write_u64(kv.len() as u64)?;
                for (key, val) in kv {
                    key.write_to(writer)?;
                    val.write_to(writer)?;
                }
                Ok(())
            }
            Object::Blob(b) => {
                writer.write_u8(4)?;
                let len = b.len();
                writer.write_u64(len as u64)?;
                writer.write_all(&(**b))?;
                Ok(())
            }
            Object::DynOwned(dyn_type) => {
                writer.write_u8(5)?;
                let bytes = (**dyn_type).to_bytes()?;
                bytes.write_to(writer)?;
                Ok(())
            }
            Object::None => writer.write_u8(6),
            Object::DateFormat(date) => {
                writer.write_u8(7)?;
                date.write_to(writer)?;
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
                let vec = <Vec<Object>>::read_from(reader)?;
                Ok(Object::Vector(vec))
            }
            3 => {
                let len = <u64>::read_from(reader)?;
                let mut map = BTreeMap::new();
                for _ in 0..len {
                    let key = <Object>::read_from(reader)?;
                    let value = <Object>::read_from(reader)?;
                    map.insert(key, value);
                }
                Ok(Object::KV(map))
            }
            4 => {
                let len = <u64>::read_from(reader)?;
                let mut b = vec![];
                for _i in 0..len {
                    let ele = <u8>::read_from(reader)?;
                    b.push(ele);
                }
                Ok(Object::Blob(b.into_boxed_slice()))
            }
            5 => {
                let bytes = <Vec<u8>>::read_from(reader)?;
                let mut bytes_reader = &bytes[0..];
                let t: TypeId = type_id_from_bytes(&mut bytes_reader)?;
                let obj = de_dyn_obj(&t, &mut bytes_reader)?;
                Ok(Object::DynOwned(obj))
            }
            6 => Ok(Object::None),
            7 => {
                let date = <DateTimeFormats>::read_from(reader)?;
                Ok(Object::DateFormat(date))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "not supported")),
        }
    }
}
