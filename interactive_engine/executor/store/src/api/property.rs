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

#![allow(dead_code)]
use ::byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::io::Cursor;
use std::io::Read;
use std::io::Write;

use crate::schema::prelude::*;
use crate::error::*;

#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub enum Property {
    Bool(bool),
    Char(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Date(String),
    ListInt(Vec<i32>),
    ListLong(Vec<i64>),
    ListFloat(Vec<f32>),
    ListDouble(Vec<f64>),
    ListString(Vec<String>),
    ListBytes(Vec<Vec<u8>>),
    Null,
    Unknown,
}

impl Property {
    /// this method is only for `GremlinService` and `DebugService`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = vec![];
        match self {
            Property::Bool(ref v) => {
                data.write_u8(*v as u8).unwrap();
            }
            Property::Char(ref v) => {
                data.write_u8(*v as u8).unwrap();
            }
            Property::Short(ref v) => {
                data.write_i16::<BigEndian>(*v).unwrap();
            }
            Property::Int(ref v) => {
                data.write_i32::<BigEndian>(*v).unwrap();
            }
            Property::Long(ref v) => {
                data.write_i64::<BigEndian>(*v).unwrap();
            }
            Property::Float(ref v) => {
                data.write_f32::<BigEndian>(*v).unwrap();
            }
            Property::Double(ref v) => {
                data.write_f64::<BigEndian>(*v).unwrap();
            }
            Property::Bytes(ref v) => {
                let mut copy = vec![0; v.len()];
                copy.clone_from_slice(v);
                data.write_i32::<BigEndian>(copy.len() as i32).unwrap();
                data.extend(copy.iter());
            }
            Property::String(ref v) => {
                let bytes = v.as_bytes();
                let mut copy = vec![0; bytes.len()];
                copy.clone_from_slice(bytes);
                data.write_i32::<BigEndian>(copy.len() as i32).unwrap();
                data.extend(copy.iter());
            }
            Property::Date(ref v) => {
                let bytes = v.as_bytes();
                let mut copy = vec![0; bytes.len()];
                copy.clone_from_slice(bytes);
                data.write_i32::<BigEndian>(copy.len() as i32).unwrap();
                data.extend(copy.iter());
            }
            Property::ListInt(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v.iter() {
                    data.write_i32::<BigEndian>(*x).unwrap();
                }
            }
            Property::ListLong(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v {
                    data.write_i64::<BigEndian>(*x).unwrap();
                }
            }
            Property::ListFloat(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v {
                    data.write_f32::<BigEndian>(*x).unwrap();
                }
            }
            Property::ListDouble(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v {
                    data.write_f64::<BigEndian>(*x).unwrap();
                }
            }
            Property::ListString(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v {
                    data.write_i32::<BigEndian>(x.len() as i32).unwrap();
                    data.write(x.as_bytes()).unwrap();
                }
            }
            Property::ListBytes(ref v) => {
                data.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for x in v {
                    data.write_i32::<BigEndian>(x.len() as i32).unwrap();
                    data.write(x.as_slice()).unwrap();
                }
            }
            Property::Null => {
                panic!("property is null");
            }
            _ => unimplemented!()
        };
        data
    }

    /// this method is used to create data for realtime insert. The result can be past by function
    /// `parse_vec_to_property`
    pub fn to_vec(&self) -> Vec<u8> {
        let mut ret = Vec::new();
        match *self {
            Property::Bool(ref v) => {
                if *v { vec![1] } else { vec![0] }
            }
            Property::Char(ref v) => vec![*v as u8],
            Property::Short(ref v) => {
                ret.write_i16::<BigEndian>(*v).unwrap();
                ret
            }
            Property::Int(ref v) => {
                ret.write_i32::<BigEndian>(*v).unwrap();
                ret
            }
            Property::Long(ref v) => {
                ret.write_i64::<BigEndian>(*v).unwrap();
                ret
            }
            Property::Float(ref v) => {
                ret.write_f32::<BigEndian>(*v).unwrap();
                ret
            }
            Property::Double(ref v) => {
                ret.write_f64::<BigEndian>(*v).unwrap();
                ret
            }
            Property::String(ref v) => v.as_bytes().to_vec(),
            Property::Date(ref v) => v.as_bytes().to_vec(),
            Property::Bytes(ref v) => v.clone(),
            Property::ListInt(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for i in 0..v.len() {
                    ret.write_i32::<BigEndian>(v[i]).unwrap();
                }
                ret
            }
            Property::ListLong(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for i in 0..v.len() {
                    ret.write_i64::<BigEndian>(v[i]).unwrap();
                }
                ret
            }
            Property::ListFloat(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for i in 0..v.len() {
                    ret.write_f32::<BigEndian>(v[i]).unwrap();
                }
                ret
            }
            Property::ListDouble(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                for i in 0..v.len() {
                    ret.write_f64::<BigEndian>(v[i]).unwrap();
                }
                ret
            }
            Property::ListString(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                let mut end_off = 0;
                for i in 0..v.len() {
                    end_off += v[i].len();
                    ret.write_i32::<BigEndian>(end_off as i32).unwrap();
                }
                for i in 0..v.len() {
                    ret.write(v[i].as_bytes()).unwrap();
                }
                ret
            }
            Property::ListBytes(ref v) => {
                ret.write_i32::<BigEndian>(v.len() as i32).unwrap();
                let mut end_off = 0;
                for i in 0..v.len() {
                    end_off += v[i].len();
                    ret.write_i32::<BigEndian>(end_off as i32).unwrap();
                }
                for i in 0..v.len() {
                    ret.write(v[i].as_slice()).unwrap();
                }
                ret
            }
            Property::Null => {
                panic!("property is null");
            }
            _ => unreachable!()
        }
    }

    pub fn is_null(&self) -> bool {
        *self == Property::Null
    }

    pub fn transform(&self, data_type: &DataType) -> GraphTraceResult<Vec<u8>> {
        if self.is_data_type(data_type) {
            return Ok(self.to_vec());
        }
        match *self {
            Property::Bool(v) => {
                let x = if v { 1 } else { 0 };
                long_to_data_type(x, data_type)
            }
            Property::Char(v) => {
                let x = v as i64;
                long_to_data_type(x, data_type)
            }
            Property::Short(v) => {
                let x = v as i64;
                long_to_data_type(x, data_type)
            }
            Property::Int(v) => {
                let x = v as i64;
                long_to_data_type(x, data_type)
            }
            Property::Long(v) => long_to_data_type(v, data_type),
            Property::Float(v) => {
                let x = v as f64;
                double_to_data_type(x, data_type)
            }
            Property::Double(v) => double_to_data_type(v, data_type),
            _ => {
                let msg = format!("{:?} cannot transform to {:?}", self, data_type);
                let err = graph_err!(GraphErrorCode::DataError, msg, transform, data_type);
                Err(err)
            },
        }
    }

    fn is_data_type(&self, data_type: &DataType) -> bool {
        match *self {
            Property::Bool(_) => *data_type == DataType::Bool,
            Property::Char(_) => *data_type == DataType::Char,
            Property::Short(_) => *data_type == DataType::Short,
            Property::Int(_) => *data_type == DataType::Int,
            Property::Long(_) => *data_type == DataType::Long,
            Property::Float(_) => *data_type == DataType::Float,
            Property::Double(_) => *data_type == DataType::Double,
            Property::String(_) => *data_type == DataType::String,
            _ => unimplemented!()
        }
    }
}

fn long_to_data_type(x: i64, data_type: &DataType) -> GraphTraceResult<Vec<u8>> {
    match *data_type {
        DataType::Bool => Ok(Property::Bool(x != 0).to_vec()),
        DataType::Char => {
            if x > u8::max_value() as i64 || x < 0 {
                let msg = format!("{} cannot be transformed to char", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, long_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Char(x as u8).to_vec())
            }
        }
        DataType::Short => {
            if x > i16::max_value() as i64 || x < i16::min_value() as i64 {
                let msg = format!("{} cannot be transformed to short", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, long_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Short(x as i16).to_vec())
            }
        }
        DataType::Int => {
            if x > i32::max_value() as i64 || x < i32::min_value() as i64 {
                let msg = format!("{} cannot be transformed to int", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, long_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Int(x as i32).to_vec())
            }
        },
        DataType::Long => Ok(Property::Long(x).to_vec()),
        DataType::Float => Ok(Property::Float(x as f32).to_vec()),
        DataType::Double => Ok(Property::Double(x as f64).to_vec()),
        _ => {
            let msg = format!("{} cannot be transformed to {:?}", x, data_type);
            let err = graph_err!(GraphErrorCode::DataError, msg, long_to_data_type, x, data_type);
            Err(err)
        }
    }
}

fn double_to_data_type(x: f64, data_type: &DataType) -> GraphTraceResult<Vec<u8>> {
    match *data_type {
        DataType::Bool => Ok(Property::Bool(x != 0.0).to_vec()),
        DataType::Char => {
            if x > u8::max_value() as f64 || x < u8::min_value() as f64 {
                let msg = format!("{} cannot be transformed to char", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, double_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Char(x as u8).to_vec())
            }
        }
        DataType::Short => {
            if x > i16::max_value() as f64 || x < i16::min_value() as f64 {
                let msg = format!("{} cannot be transformed to short", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, double_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Short(x as i16).to_vec())
            }
        }
        DataType::Int => {
            if x > i32::max_value() as f64 || x < i32::min_value() as f64 {
                let msg = format!("{} cannot be transformed to int", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, double_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Int(x as i32).to_vec())
            }
        },
        DataType::Long => {
            if x > i64::max_value() as f64 || x < i64::min_value() as f64 {
                let msg = format!("{} cannot be transformed to long", x);
                let err = graph_err!(GraphErrorCode::DataError, msg, double_to_data_type, x, data_type);
                Err(err)
            } else {
                Ok(Property::Long(x as i64).to_vec())
            }
        }
        DataType::Float => Ok(Property::Float(x as f32).to_vec()),
        DataType::Double => Ok(Property::Double(x).to_vec()),
        _ => {
            let msg = format!("{} cannot be transformed to {:?}", x, data_type);
            let err = graph_err!(GraphErrorCode::DataError, msg, double_to_data_type, x, data_type);
            Err(err)
        }
    }
}

pub fn parse_property(data: &str, data_type: DataType) -> Property {
    match data_type {
        DataType::Bool => {
            match data {
                "true" => Property::Bool(true),
                "false" => Property::Bool(false),
                _ => Property::Unknown,
            }
        }
        DataType::Char => {
            match data.len() {
                1 => Property::Char(data.as_bytes()[0]),
                _ => Property::Unknown,
            }
        }
        DataType::Short => {
            match data.parse::<i16>() {
                Ok(x) => Property::Short(x),
                _ => Property::Unknown,
            }
        }
        DataType::Int => {
            match data.parse::<i32>() {
                Ok(x) => Property::Int(x),
                _ => Property::Unknown,
            }
        }
        DataType::Long => {
            match data.parse::<i64>() {
                Ok(x) => Property::Long(x),
                _ => Property::Unknown,
            }
        }
        DataType::Float => {
            match data.parse::<f32>() {
                Ok(x) => Property::Float(x),
                _ => Property::Unknown,
            }
        }
        DataType::Double => {
            match data.parse::<f64>() {
                Ok(x) => Property::Double(x),
                _ => Property::Unknown,
            }
        }
        DataType::String => { Property::String(data.to_owned()) }
        DataType::Bytes => { Property::Bytes(Vec::from(data.to_owned().as_bytes())) }
        DataType::Date => { Property::Date(data.to_owned()) }
        DataType::ListInt => {
            if data.len() == 0 {
                Property::ListInt(vec![])
            } else {
                let items: Vec<&str> = data.split(",").collect();
                Property::ListInt(items.iter().map(|x| x.parse().unwrap()).collect())
            }
        }
        DataType::ListLong => {
            if data.len() == 0 {
                Property::ListLong(vec![])
            } else {
                let items: Vec<&str> = data.split(",").collect();
                Property::ListLong(items.iter().map(|x| x.parse().unwrap()).collect())
            }
        }
        DataType::ListFloat => {
            if data.len() == 0 {
                Property::ListFloat(vec![])
            } else {
                let items: Vec<&str> = data.split(",").collect();
                Property::ListFloat(items.iter().map(|x| x.parse().unwrap()).collect())
            }
        }
        DataType::ListDouble => {
            if data.len() == 0 {
                Property::ListDouble(vec![])
            } else {
                let items: Vec<&str> = data.split(",").collect();
                Property::ListDouble(items.iter().map(|x| x.parse().unwrap()).collect())
            }
        }
        DataType::ListString => {
            if data.len() == 0 {
                Property::ListString(vec![])
            } else {
                let items: Vec<&str> = data.split(",").collect();
                Property::ListString(items.iter().map(|x| x.to_string()).collect())
            }
        }
        DataType::Unknown => Property::Unknown,
        _ => Property::Unknown,
    }
}

impl Property {
    /// get boolean value
    pub fn get_bool(&self) -> Result<bool, String> {
        match self {
            &Property::Bool(b) => { Ok(b) }
            _ => { Err(format!("get bool value fail from property=>{:?}", self)) }
        }
    }

    /// get int value
    pub fn get_int(&self) -> Result<i32, String> {
        match self {
            &Property::Int(i) => { Ok(i) }
            _ => { Err(format!("get int value fail from property=>{:?}", self)) }
        }
    }

    /// get long value
    pub fn get_long(&self) -> Result<i64, String> {
        match self {
            &Property::Int(l) => { Ok(l as i64) }
            &Property::Long(l) => { Ok(l) }
            _ => { Err(format!("get long value fail from property=>{:?}", self)) }
        }
    }

    /// get float value
    pub fn get_float(&self) -> Result<f32, String> {
        match self {
            &Property::Int(i) => { Ok(i as f32) }
            &Property::Float(f) => { Ok(f) }
            _ => { Err(format!("get float value fail from property=>{:?}", self)) }
        }
    }

    /// get double value
    pub fn get_double(&self) -> Result<f64, String> {
        match self {
            &Property::Int(d) => { Ok(d as f64) }
            &Property::Long(d) => { Ok(d as f64) }
            &Property::Float(d) => { Ok(d as f64) }
            &Property::Double(d) => { Ok(d) }
            _ => { Err(format!("get double value fail from property=>{:?}", self)) }
        }
    }

    /// get string ref value
    pub fn get_string(&self) -> Result<&String, String> {
        match self {
            &Property::String(ref s) => { Ok(s) }
            _ => { Err(format!("get string ref value fail from property=>{:?}", self)) }
        }
    }

    /// get bytes
    pub fn get_bytes(&self) -> Result<&Vec<u8>, String> {
        match self {
            &Property::Bytes(ref bytes) => { Ok(bytes) }
            _ => { Err(format!("get bytes fail from property=>{:?}", self)) }
        }
    }

    /// get int list
    pub fn get_list(&self) -> Result<&Vec<i32>, String> {
        match self {
            &Property::ListInt(ref lists) => { Ok(lists) }
            _ => { Err(format!("get list fail from property=>{:?}", self)) }
        }
    }

    /// get long list
    pub fn get_long_list(&self) -> Result<&Vec<i64>, String> {
        match self {
            &Property::ListLong(ref list) => Ok(list),
            _ => { Err(format!("get long list fail from property=>{:?}", self)) }
        }
    }

    /// get float list
    pub fn get_float_list(&self) -> Result<&Vec<f32>, String> {
        match self {
            &Property::ListFloat(ref list) => Ok(list),
            _ => { Err(format!("get float list fail from property=>{:?}", self)) }
        }
    }

    pub fn take_double_list(&mut self) -> Result<Vec<f64>, String> {
        match self {
            Property::ListInt(l) => {
                let mut list: Vec<f64> = Vec::with_capacity(l.len());
                for i in l.iter() {
                    list.push((i.clone()) as f64);
                }
                Ok(list)
            }
            Property::ListLong(l) => {
                let mut list: Vec<f64> = Vec::with_capacity(l.len());
                for i in l.iter() {
                    list.push((i.clone()) as f64);
                }
                Ok(list)
            }
            Property::ListFloat(l) => {
                let mut list: Vec<f64> = Vec::with_capacity(l.len());
                for i in l.iter() {
                    list.push((i.clone()) as f64);
                }
                Ok(list)
            }
            Property::ListDouble(list) => Ok(list.to_vec()),
            _ => { Err(format!("get double list fail from property=>{:?}", self)) }
        }
    }

    /// get double list
    pub fn get_double_list(&self) -> Result<&Vec<f64>, String> {
        match self {
            &Property::ListDouble(ref list) => Ok(list),
            _ => { Err(format!("get double list fail from property=>{:?}", self)) }
        }
    }

    /// get string list
    pub fn get_string_list(&self) -> Result<&Vec<String>, String> {
        match self {
            &Property::ListString(ref list) => Ok(list),
            _ => { Err(format!("get string list fail from property=>{:?}", self)) }
        }
    }

    pub fn get_bytes_list(&self) -> Result<&Vec<Vec<u8>>, String> {
        match self {
            &Property::ListBytes(ref list) => Ok(list),
            _ => { Err(format!("get bytes list fail from property=>{:?}", self)) }
        }
    }
}

pub fn parse_proerty_as_string(data: Vec<u8>, data_type: &DataType) -> Option<String> {
    let mut rdr = Cursor::new(data);
    match *data_type {
        DataType::String | DataType::Date => {
            let len = rdr.read_i32::<BigEndian>().unwrap();
            let mut ret = String::new();
            rdr.read_to_string(&mut ret).unwrap();
            assert_eq!(len as usize, ret.len());
            Some(format!("\"{}\"", ret))
        }
        DataType::Double => {
            let ret = rdr.read_f64::<BigEndian>().unwrap();
            Some(ret.to_string())
        }
        DataType::Float => {
            let ret = rdr.read_f32::<BigEndian>().unwrap();
            Some(ret.to_string())
        }
        DataType::Long => {
            let ret = rdr.read_i64::<BigEndian>().unwrap();
            Some(ret.to_string())
        }
        DataType::Int => {
            let ret = rdr.read_i32::<BigEndian>().unwrap();
            Some(ret.to_string())
        }
        DataType::Short => {
            let ret = rdr.read_i16::<BigEndian>().unwrap();
            Some(ret.to_string())
        }
        DataType::Char => {
            let ret = rdr.read_u8().unwrap();
            Some(format!("'{}'", ret as char))
        }
        DataType::Bool => {
            let ret = rdr.read_u8().unwrap();
            match ret {
                0 => Some("False".to_owned()),
                _ => Some("True".to_owned()),
            }
        }
        DataType::ListInt => {
            let len = rdr.read_i32::<BigEndian>().unwrap();
            let mut data = Vec::new();
            for _ in 0..len {
                let tmp = rdr.read_i32::<BigEndian>().unwrap();
                data.push(tmp);
            }
            Some(format!("{:?}", data))
        }
        _ => { unimplemented!() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_proerty_as_string() {
        let p = Property::String("aaabbb".to_owned());
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::String).unwrap();
        assert_eq!(res, "\"aaabbb\"");
        let p = Property::Date("2000-01-01".to_owned());
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::String).unwrap();
        assert_eq!(res, "\"2000-01-01\"");
        let p = Property::Double(3.1415926);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Double).unwrap();
        assert_eq!(res, "3.1415926");
        let p = Property::Float(3.14);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Float).unwrap();
        assert_eq!(res, "3.14");
        let p = Property::Long(123456);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Long).unwrap();
        assert_eq!(res, "123456");
        let p = Property::Int(123456);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Int).unwrap();
        assert_eq!(res, "123456");
        let p = Property::Short(123);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Short).unwrap();
        assert_eq!(res, "123");
        let p = Property::Char('x' as u8);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Char).unwrap();
        assert_eq!(res, "'x'");
        let p = Property::Bool(false);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Bool).unwrap();
        assert_eq!(res, "False");
        let p = Property::Bool(true);
        let res = parse_proerty_as_string(p.to_bytes(), &DataType::Bool).unwrap();
        assert_eq!(res, "True");
    }

    #[test]
    fn test_property_transform() {
        let val = 123;
        let _properties = vec![Property::Int(val), Property::Double(val as f64), Property::Float(val as f32),
                              Property::Short(val as i16), Property::Long(val as i64), Property::Char(val as u8)];
        let _target = vec![(DataType::Float, Property::Float(val as f32)),
                          (DataType::Double, Property::Double(val as f64)), (DataType::Int, Property::Int(val)),
                          (DataType::Long, Property::Long(val as i64)), (DataType::Short, Property::Short(val as i16)),
                          (DataType::Char, Property::Char(val as u8)), (DataType::Bool, Property::Bool(val != 0))];

        let p = Property::String("aaaa".to_owned());
        let t = DataType::Int;
        assert!(p.transform(&t).is_err());
    }
}
