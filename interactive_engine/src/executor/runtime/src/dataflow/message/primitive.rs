//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use maxgraph_common::proto::message;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::io::Cursor;
use protobuf::well_known_types::BoolValue;
use protobuf::{parse_from_bytes, RepeatedField};
use protobuf::Message;

pub trait Write {
    fn into_bytes(self) -> Vec<u8>;
}

pub trait Read {
    fn parse_bytes(bytes: &[u8]) -> Self;
}

impl Write for i32 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_i32::<BigEndian>(self).unwrap();
        wtr
    }
}

impl Read for i32 {
    fn parse_bytes(bytes: &[u8]) -> i32 {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_i32::<BigEndian>();
        r.ok().unwrap_or(::std::i32::MAX)
    }
}

impl Write for i64 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_i64::<BigEndian>(self).unwrap();
        wtr
    }
}

impl Read for i64 {
    fn parse_bytes(bytes: &[u8]) -> i64 {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_i64::<BigEndian>();
        r.ok().unwrap_or(::std::i64::MAX)
    }
}

impl Write for f32 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_f32::<BigEndian>(self).unwrap();
        wtr
    }
}

impl Read for f32 {
    fn parse_bytes(bytes: &[u8]) -> f32 {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_f32::<BigEndian>();
        r.ok().unwrap_or(::std::f32::MAX)
    }
}

impl Write for f64 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_f64::<BigEndian>(self).unwrap();
        wtr
    }
}

impl Read for f64 {
    fn parse_bytes(bytes: &[u8]) -> f64 {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_f64::<BigEndian>();
        r.ok().unwrap_or(::std::f64::MAX)
    }
}

impl Write for bool {
    fn into_bytes(self) -> Vec<u8> {
        let mut bool_value = BoolValue::new();
        bool_value.set_value(self);
        bool_value.write_to_bytes().unwrap()
    }
}

impl Read for bool {
    fn parse_bytes(bytes: &[u8]) -> Self {
        let bool_value = parse_from_bytes::<BoolValue>(bytes).expect("parse bool value");
        bool_value.get_value()
    }
}

impl Write for i16 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_i16::<BigEndian>(self).unwrap();
        wtr
    }
}

impl Read for i16 {
    fn parse_bytes(bytes: &[u8]) -> Self {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_i16::<BigEndian>();
        r.ok().unwrap_or(::std::i16::MAX)
    }
}

impl Write for u8 {
    fn into_bytes(self) -> Vec<u8> {
        let mut wtr = vec![];
        wtr.write_u8(self).unwrap();
        wtr
    }
}

impl Read for u8 {
    fn parse_bytes(bytes: &[u8]) -> u8 {
        let mut rdr = Cursor::new(bytes);
        let r = rdr.read_u8();
        r.ok().unwrap_or(::std::u8::MAX)
    }
}

impl Write for String {
    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }
}

impl Read for String {
    fn parse_bytes(bytes: &[u8]) -> String {
        let r = String::from_utf8(bytes.to_vec());
        r.ok().unwrap_or(String::from("ERROR_PARSE"))
    }
}

impl Write for Vec<i32> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListInt::new();
        list.set_value(self);
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<i32> {
    fn parse_bytes(bytes: &[u8]) -> Vec<i32> {
        let mut list = parse_from_bytes::<message::ListInt>(bytes).expect("parse list int");
        list.take_value()
    }
}

impl Write for Vec<String> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListString::new();
        list.set_value(protobuf::RepeatedField::from_vec(self));
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<String> {
    fn parse_bytes(bytes: &[u8]) -> Vec<String> {
        let mut list = parse_from_bytes::<message::ListString>(bytes).expect("parse list int");
        list.take_value().to_vec()
    }
}

impl Write for Vec<i64> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListLong::new();
        list.set_value(self);
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<i64> {
    fn parse_bytes(bytes: &[u8]) -> Vec<i64> {
        let mut list = parse_from_bytes::<message::ListLong>(bytes).expect("parse list int");
        list.take_value()
    }
}

impl Write for Vec<f32> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListFloat::new();
        list.set_value(self);
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<f32> {
    fn parse_bytes(bytes: &[u8]) -> Vec<f32> {
        let mut list = parse_from_bytes::<message::ListFloat>(bytes).expect("parse list int");
        list.take_value()
    }
}

impl Write for Vec<f64> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListDouble::new();
        list.set_value(self);
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<f64> {
    fn parse_bytes(bytes: &[u8]) -> Vec<f64> {
        let mut list = parse_from_bytes::<message::ListDouble>(bytes).expect("parse list int");
        list.take_value()
    }
}

impl Write for Vec<Vec<u8>> {
    fn into_bytes(self) -> Vec<u8> {
        let mut list = message::ListBinary::new();
        list.set_value(RepeatedField::from_vec(self));
        list.write_to_bytes().unwrap()
    }
}

impl Read for Vec<Vec<u8>> {
    fn parse_bytes(bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut list = parse_from_bytes::<message::ListBinary>(bytes).expect("parse list binary");
        list.take_value().to_vec()
    }
}
