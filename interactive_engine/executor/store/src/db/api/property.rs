#![allow(dead_code)]
use std::collections::HashMap;
use std::marker::PhantomData;

use std::cmp::Ordering;
use crate::db::common::bytes::transform;
use crate::db::common::bytes::util::{UnsafeBytesWriter, UnsafeBytesReader};
use crate::db::common::numeric::*;
use crate::db::api::PropertyId;
use super::GraphResult;
use super::error::*;
use protobuf::ProtobufEnum;
use crate::db::proto::model::PropertyValuePb;
use crate::db::proto::common::DataTypePb;

pub trait PropertyMap {
    fn get(&self, prop_id: PropertyId) -> Option<ValueRef>;
    fn as_map(&self) -> HashMap<PropertyId, ValueRef>;
}

impl dyn PropertyMap {
    pub fn from_proto(pb: &HashMap<PropertyId, PropertyValuePb>) -> HashMap<PropertyId, ValueRef> {
        let mut m = HashMap::new();
        for (id, val_pb) in pb {
            let val_type = ValueType::from_i32(val_pb.get_dataType().value()).unwrap();
            m.insert(*id, ValueRef::new(val_type, val_pb.get_val()));
        }
        m
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ValueType {
    Bool = 1,
    Char = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Bytes = 9,
    IntList = 10,
    LongList = 11,
    FloatList = 12,
    DoubleList = 13,
    StringList = 14,
}

impl ValueType {
    #[cfg(test)]
    pub fn all_value_types() -> Vec<ValueType> {
        vec![ValueType::Bool, ValueType::Char, ValueType::Short,
             ValueType::Int, ValueType::Long, ValueType::Float, ValueType::Double,
             ValueType::String, ValueType::Bytes, ValueType::IntList, ValueType::LongList,
             ValueType::FloatList, ValueType::DoubleList, ValueType::StringList]
    }

    #[cfg(test)]
    pub fn count() -> usize {
        14
    }

    pub fn from_i32(x: i32) -> GraphResult<Self> {
        match x {
            x if x == ValueType::Bool as i32 => Ok(ValueType::Bool),
            x if x == ValueType::Char as i32 => Ok(ValueType::Char),
            x if x == ValueType::Short as i32 => Ok(ValueType::Short),
            x if x == ValueType::Int as i32 => Ok(ValueType::Int),
            x if x == ValueType::Long as i32 => Ok(ValueType::Long),
            x if x == ValueType::Float as i32 => Ok(ValueType::Float),
            x if x == ValueType::Double as i32 => Ok(ValueType::Double),
            x if x == ValueType::String as i32 => Ok(ValueType::String),
            x if x == ValueType::Bytes as i32 => Ok(ValueType::Bytes),
            x if x == ValueType::IntList as i32 => Ok(ValueType::IntList),
            x if x == ValueType::LongList as i32 => Ok(ValueType::LongList),
            x if x == ValueType::FloatList as i32 => Ok(ValueType::FloatList),
            x if x == ValueType::DoubleList as i32 => Ok(ValueType::DoubleList),
            x if x == ValueType::StringList as i32 => Ok(ValueType::StringList),
            _ => {
                let msg = format!("invalid input");
                let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, from_i32, x);
                Err(err)
            }
        }
    }

    pub fn to_proto(&self) -> GraphResult<DataTypePb> {
        let v = *self as i32;
        match DataTypePb::from_i32(v) {
            None => {
                let msg = format!("invalid input");
                let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, to_proto, v);
                Err(err)
            },
            Some(pb) => {
                Ok(pb)
            },
        }
    }

    pub fn has_fixed_length(&self) -> bool {
        match *self {
            ValueType::Bool | ValueType::Char | ValueType::Short | ValueType::Int |
            ValueType::Long | ValueType::Float | ValueType::Double => true,
            _ => false,
        }
    }

    /// this method is only valid for fix length type
    pub fn len(&self) -> usize {
        match *self {
            ValueType::Bool | ValueType::Char => 1,
            ValueType::Short => 2,
            ValueType::Int | ValueType::Float => 4,
            ValueType::Long | ValueType::Double => 8,
            _ => panic!("{:?} doesn't has fixed len", self),
        }
    }
}

#[derive(Copy, Clone)]
pub struct ValueRef<'a> {
    r#type: ValueType,
    data: &'a [u8],
}

impl<'a> ValueRef<'a> {
    pub(crate) fn new(r#type: ValueType, data: &'a [u8]) -> Self {
        ValueRef {
            r#type,
            data,
        }
    }

    pub fn get_type(&self) -> &ValueType {
        &self.r#type
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        self.data
    }

    pub fn get_bool(&self) -> GraphResult<bool> {
        let res = self.check_type_match(ValueType::Bool);
        res_unwrap!(res, get_bool)?;
        Ok(get_bool(self.data))

    }

    pub fn get_char(&self) -> GraphResult<u8> {
        let res = self.check_type_match(ValueType::Char);
        res_unwrap!(res, get_char)?;
        Ok(get_char(self.data))
    }

    pub fn get_short(&self) -> GraphResult<i16> {
        let res = self.check_type_match(ValueType::Short);
        res_unwrap!(res, get_short)?;
        Ok(get_short(self.data))
    }

    pub fn get_int(&self) -> GraphResult<i32> {
        let res = self.check_type_match(ValueType::Int);
        res_unwrap!(res, get_int)?;
        Ok(get_int(self.data))
    }

    pub fn get_long(&self) -> GraphResult<i64> {
        let res = self.check_type_match(ValueType::Long);
        res_unwrap!(res, get_long)?;
        Ok(get_long(self.data))
    }

    pub fn get_float(&self) -> GraphResult<f32> {
        let res = self.check_type_match(ValueType::Float);
        res_unwrap!(res, get_float)?;
        Ok(get_float(self.data))
    }

    pub fn get_double(&self) -> GraphResult<f64> {
        let res = self.check_type_match(ValueType::Double);
        res_unwrap!(res, get_double)?;
        Ok(get_double(self.data))
    }

    pub fn get_str(&self) -> GraphResult<&str> {
        let res = self.check_type_match(ValueType::String);
        res_unwrap!(res, get_str)?;
        ::std::str::from_utf8(self.data).map_err(|e| {
            gen_graph_err!(GraphErrorCode::Utf8Error, e.to_string(), get_str)
        })
    }

    pub fn get_bytes(&self) -> GraphResult<&[u8]> {
        let res = self.check_type_match(ValueType::Bytes);
        res_unwrap!(res, get_str)?;
        Ok(self.data)
    }

    pub fn get_int_list(&self) -> GraphResult<NumericArray<i32>> {
        let res = self.check_type_match(ValueType::IntList)
            .and_then(|_| self.check_numeric_array(ValueType::IntList));
        let reader = res_unwrap!(res, get_int_list)?;
        Ok(NumericArray::new(reader))
    }

    pub fn get_long_list(&self) -> GraphResult<NumericArray<i64>> {
        let res = self.check_type_match(ValueType::LongList)
            .and_then(|_| self.check_numeric_array(ValueType::LongList));
        let reader = res_unwrap!(res, get_long_list)?;
        Ok(NumericArray::new(reader))
    }

    pub fn get_float_list(&self) -> GraphResult<NumericArray<f32>> {
        let res = self.check_type_match(ValueType::FloatList)
            .and_then(|_| self.check_numeric_array(ValueType::FloatList));
        let reader = res_unwrap!(res, get_float_list)?;
        Ok(NumericArray::new(reader))
    }

    pub fn get_double_list(&self) -> GraphResult<NumericArray<f64>> {
        let res = self.check_type_match(ValueType::DoubleList)
            .and_then(|_| self.check_numeric_array(ValueType::DoubleList));
        let reader = res_unwrap!(res, get_double_list)?;
        Ok(NumericArray::new(reader))
    }

    pub fn get_str_list(&self) -> GraphResult<StrArray> {
        let res = self.check_type_match(ValueType::StringList)
            .and_then(|_| self.weak_check_str_list());
        let reader = res_unwrap!(res, get_str_list)?;
        Ok(StrArray::new(reader))
    }

    /// transform value to long if it is a number, else return None
    pub fn to_long(&self) -> Option<i64> {
        match self.r#type {
            ValueType::Bool => Some(get_bool(self.data) as i64),
            ValueType::Char => Some(get_char(self.data) as i64),
            ValueType::Short => Some(get_short(self.data) as i64),
            ValueType::Int => Some(get_int(self.data) as i64),
            ValueType::Long => Some(get_long(self.data)),
            ValueType::Float => Some(get_float(self.data) as i64),
            ValueType::Double => Some(get_double(self.data) as i64),
            _ => None,
        }
    }

    /// transform value to double if it is a number, else return None
    pub fn to_double(&self) -> Option<f64> {
        match self.r#type {
            ValueType::Bool => {
                if get_bool(self.data) {
                    Some(1.0)
                } else {
                    Some(0.0)
                }
            },
            ValueType::Char => Some(get_char(self.data) as f64),
            ValueType::Short => Some(get_short(self.data) as f64),
            ValueType::Int => Some(get_int(self.data) as f64),
            ValueType::Long => Some(get_long(self.data) as f64),
            ValueType::Float => Some(get_float(self.data) as f64),
            ValueType::Double => Some(get_double(self.data)),
            _ => None,
        }
    }

    pub fn check_type_match(&self, value_type: ValueType) -> GraphResult<()> {
        if self.r#type != value_type {
            let msg = format!("cannot transform {:?} to {:?}", self.r#type, value_type);
            let err = gen_graph_err!(GraphErrorCode::ValueTypeMismatch, msg, check_type_match, value_type);
            return Err(err);
        }
        Ok(())
    }

    fn check_numeric_array(&self, value_type: ValueType) -> GraphResult<UnsafeBytesReader> {
        let reader = UnsafeBytesReader::new(self.data);
        let len = match value_type {
            ValueType::IntList | ValueType::FloatList => 4,
            ValueType::LongList | ValueType::DoubleList => 8,
            _ => unreachable!(),
        };
        if reader.read_i32(0).to_be() * len + 4 == self.data.len() as i32 {
            return Ok(reader);
        }
        let msg = format!("invalid {:?} bytes, data len is {}", value_type, self.data.len());
        let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, check_numeric_array);
        Err(err)
    }

    /// it's called weak because str content won't be check, so it map happens that the content is
    /// in valid utf8 and when user extract the str, the process will be panic
    fn weak_check_str_list(&self) -> GraphResult<UnsafeBytesReader> {
        let reader = UnsafeBytesReader::new(self.data);
        let len = reader.read_i32(0).to_be() as usize;
        let total_len = reader.read_i32(4 * len).to_be() as usize;
        if total_len == self.data.len() - (4 + 4 * len) {
            return Ok(reader);
        }
        let msg = format!("invalid str array bytes");
        let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, weak_check_str_list);
        Err(err)
    }
}

impl std::fmt::Debug for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self.r#type {
            ValueType::Bool => write!(f, "Bool({})", get_bool(self.data)),
            ValueType::Char => write!(f, "Char({})", get_char(self.data)),
            ValueType::Short => write!(f, "Short({})", get_short(self.data)),
            ValueType::Int => write!(f, "Int({})", get_int(self.data)),
            ValueType::Long => write!(f, "Long({})", get_long(self.data)),
            ValueType::Float => write!(f, "Float({})", get_float(self.data)),
            ValueType::Double => write!(f, "Double({})", get_double(self.data)),
            ValueType::String => {
                if let Ok(s) = self.get_str() {
                    write!(f, "String({})", s)
                } else {
                    Err(std::fmt::Error::default())
                }
            },
            ValueType::Bytes => write!(f, "Bytes({:?})", self.get_bytes().unwrap()),
            ValueType::IntList => write!(f, "IntArray({:?})", self.get_int_list().unwrap()),
            ValueType::LongList => write!(f, "LongArray({:?})", self.get_long_list().unwrap()),
            ValueType::FloatList => write!(f, "FloatArray({:?})", self.get_float_list().unwrap()),
            ValueType::DoubleList => write!(f, "DoubleArray({:?})", self.get_double_list().unwrap()),
            ValueType::StringList => write!(f, "StringArray({:?})", self.get_str_list().unwrap()),
        }
    }
}

impl PartialEq for ValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match self.r#type {
            ValueType::Bool | ValueType::Char | ValueType::Short |
            ValueType::Int | ValueType::Long => {
                match other.r#type {
                    ValueType::Bool | ValueType::Char | ValueType::Short |
                    ValueType::Int | ValueType::Long => self.to_long().unwrap() == other.to_long().unwrap(),
                    ValueType::Float | ValueType::Double => self.to_double().unwrap() == other.to_double().unwrap(),
                    _ => false,
                }
            },
            ValueType::Float | ValueType::Double => {
                match other.r#type {
                    ValueType::Bool | ValueType::Char | ValueType::Short | ValueType::Int |
                    ValueType::Long | ValueType::Float | ValueType::Double => self.to_double().unwrap() == other.to_double().unwrap(),
                    _ => false,
                }
            },
            _ => {
                self.r#type == other.r#type && self.data == other.data
            },
        }
    }
}

impl PartialOrd for ValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.r#type {
            ValueType::Bool | ValueType::Char | ValueType::Short |
            ValueType::Int | ValueType::Long => {
                match other.r#type {
                    ValueType::Bool | ValueType::Char | ValueType::Short |
                    ValueType::Int | ValueType::Long => Some(self.to_long().unwrap().cmp(&other.to_long().unwrap())),
                    ValueType::Float | ValueType::Double => self.to_double().unwrap().partial_cmp(&other.to_double().unwrap()),
                    _ => None,
                }
            },
            ValueType::Float | ValueType::Double => {
                match other.r#type {
                    ValueType::Bool | ValueType::Char | ValueType::Short | ValueType::Int |
                    ValueType::Long | ValueType::Float | ValueType::Double => self.to_double().unwrap().partial_cmp(&other.to_double().unwrap()),
                    _ => None,
                }
            },
            _ => {
                if self.r#type == other.r#type {
                    match self.r#type {
                        ValueType::String => {
                            let s1 = self.get_str().ok()?;
                            let s2 = other.get_str().ok()?;
                            s1.partial_cmp(s2)
                        }
                        ValueType::Bytes => {
                            let b1 = self.get_bytes().ok()?;
                            let b2 = other.get_bytes().ok()?;
                            b1.partial_cmp(b2)
                        }
                        ValueType::IntList => {
                            let arr1 = self.get_int_list().ok()?;
                            let arr2 = other.get_int_list().ok()?;
                            arr1.partial_cmp(&arr2)
                        }
                        ValueType::LongList => {
                            let arr1 = self.get_long_list().ok()?;
                            let arr2 = other.get_long_list().ok()?;
                            arr1.partial_cmp(&arr2)
                        }
                        ValueType::FloatList => {
                            let arr1 = self.get_float_list().ok()?;
                            let arr2 = other.get_float_list().ok()?;
                            arr1.partial_cmp(&arr2)
                        }
                        ValueType::DoubleList => {
                            let arr1 = self.get_double_list().ok()?;
                            let arr2 = other.get_double_list().ok()?;
                            arr1.partial_cmp(&arr2)
                        }
                        ValueType::StringList => {
                            let arr1 = self.get_str_list().ok()?;
                            let arr2 = self.get_str_list().ok()?;
                            arr1.partial_cmp(&arr2)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    None
                }
            }
        }
    }
}

fn get_bool(data: &[u8]) -> bool {
    debug_assert_eq!(data.len(), 1);
    data[0] != 0
}

fn get_char(data: &[u8]) -> u8 {
    debug_assert_eq!(data.len(), 1);
    data[0]
}

fn get_short(data: &[u8]) -> i16 {
    debug_assert_eq!(data.len(), 2);
    let reader = UnsafeBytesReader::new(data);
    // don't forget to_be here
    reader.read_i16(0).to_be()
}

fn get_int(data: &[u8]) -> i32 {
    debug_assert_eq!(data.len(), 4);
    let reader = UnsafeBytesReader::new(data);
    // don't forget to_be here
    reader.read_i32(0).to_be()
}

fn get_long(data: &[u8]) -> i64 {
    debug_assert_eq!(data.len(), 8);
    let reader = UnsafeBytesReader::new(data);
    // don't forget to_be here
    reader.read_i64(0).to_be()
}

fn get_float(data: &[u8]) -> f32 {
    debug_assert_eq!(data.len(), 4);
    let reader = UnsafeBytesReader::new(data);
    // don't forget to_be here
    f32::from_bits(reader.read_u32(0).to_be())
}

fn get_double(data: &[u8]) -> f64 {
    debug_assert_eq!(data.len(), 8);
    let reader = UnsafeBytesReader::new(data);
    // don't forget to_be here
    f64::from_bits(reader.read_u64(0).to_be())
}

/// value encoding(all value is format in big endian):
/// bool: true -> [1], false [0], 1B
/// char: x -> [x], 1B
/// short: x -> [a, b], where x==(a<<8|b), 2B
/// int: x -> [x1, x2, x3, x4] where x==(x1<<24|(x2<<16)|(x3<<8)|x4), 4B
/// long: x -> [x1, x2, x3, x4, x5, x6, x7, x8] where x==((x1<<56)|(x2<<48)|(x3<<40)|(x4<<32)|(x5<<24)|(x6<<16)|(x7<<8)|x8), 8B
/// float: y = x.to_bits() -> [y1, y2, y3, y4] where y=(y1<<24|(y2<<16)|(y3<<8)|y4), 4B
/// double: y = x.to_bits() -> [y1, y2, y3, y4, y5, y6, y7, y8] where x==((y1<<56)|(y2<<48)|(y3<<40)|(y4<<32)|(y5<<24)|(y6<<16)|(y7<<8)|y8), 8B
/// string: x -> x.as_bytes()
/// bytes: x -> x.as_bytes()
/// int array:
///     +-----+----+----+-----+----+
///     | len | x1 | x2 | ... | xn |
///     +-----+----+----+-----+----+
///     | 4B  | 4B | 4B | ... | 4B |
///     +-----+----+----+-----+----+ len and every xi is in int format above
/// long array:
///     +-----+----+----+-----+----+
///     | len | x1 | x2 | ... | xn |
///     +-----+----+----+-----+----+
///     | 4B  | 8B | 8B | ... | 8B |
///     +-----+----+----+-----+----+ len is in int format above and every xi is in long format above
/// float array:
///     +-----+----+----+-----+----+
///     | len | x1 | x2 | ... | xn |
///     +-----+----+----+-----+----+
///     | 4B  | 4B | 4B | ... | 4B |
///     +-----+----+----+-----+----+ len is in int format above and every xi is in float format above
/// double array:
///     +-----+----+----+-----+----+
///     | len | x1 | x2 | ... | xn |
///     +-----+----+----+-----+----+
///     | 4B  | 8B | 8B | ... | 8B |
///     +-----+----+----+-----+----+ len is in int format above and every xi is in double format above
/// string array:
///     +-----+------+------+-----+------+------+------+-----+------+
///     | len | off1 | off2 | ... | offn | str1 | str2 | ... | strn |
///     +-----+------+------+-----+------+------+------+-----+------+
///     | 4B  |  4B  |  4B  | ... |  4B  | x1 B | x2 B | ... | xn B |
///     +-----+------+------+-----+------+------+------+-----+------+
///     len and offi is in int format above, stri is in string format above
///     off1 == x1 means it's str1's end offset
///     off2 == x1 + x2 means it's str2's end offset
///     ...
///     offn = x1 + x2 + ... + xn
#[derive(Clone)]
pub struct Value {
    r#type: ValueType,
    data: Vec<u8>,
}

macro_rules! gen_array {
    ($arr:ident, $ty:ty, $func:tt) => {
        {
            let size = ::std::mem::size_of::<$ty>();
            let total_len = $arr.len() * size + 4;
            let mut data = Vec::with_capacity(total_len);
            unsafe { data.set_len(total_len); }
            let mut writer = UnsafeBytesWriter::new(&mut data);
            writer.write_i32(0, ($arr.len() as i32).to_be());
            for i in 0..$arr.len() {
                writer.$func(4 + size * i, $arr[i].to_big_endian());
            }
            data
        }
    };
}

impl Value {
    pub(crate) fn new(r#type: ValueType, data: Vec<u8>) -> Self {
        Value {
            r#type,
            data,
        }
    }

    pub fn from_proto(pb: &PropertyValuePb) -> GraphResult<Self> {
        let val_type = ValueType::from_i32(pb.get_dataType().value())?;
        Ok(Value::new(val_type, Vec::from(pb.get_val())))
    }

    pub fn from_value_ref(value_ref: &ValueRef) -> Self {
        Value::new(value_ref.r#type, value_ref.data.to_vec())
    }

    pub fn to_proto(&self) -> GraphResult<PropertyValuePb> {
        let mut pb = PropertyValuePb::new();
        pb.set_dataType(self.r#type.to_proto()?);
        pb.set_val(self.data.clone());
        Ok(pb)
    }

    pub fn bool(v: bool) -> Self {
        Value::new(ValueType::Bool, vec![v as u8])
    }

    pub fn char(v: u8) -> Self {
        Value::new(ValueType::Char, vec![v])
    }

    pub fn short(v: i16) -> Self {
        let data = transform::i16_to_vec(v.to_be());
        Value::new(ValueType::Short, data)
    }

    pub fn int(v: i32) -> Self {
        let data = transform::i32_to_vec(v.to_be());
        Value::new(ValueType::Int, data)
    }

    pub fn long(v: i64) -> Self {
        let data = transform::i64_to_vec(v.to_be());
        Value::new(ValueType::Long, data)
    }

    pub fn float(v: f32) -> Self {
        let data = transform::u32_to_vec(v.to_bits().to_be());
        Value::new(ValueType::Float, data)
    }

    pub fn double(v: f64) -> Self {
        let data = transform::u64_to_vec(v.to_bits().to_be());
        Value::new(ValueType::Double, data)
    }

    pub fn string(v: &str) -> Self {
        let data = v.as_bytes().to_vec();
        Value::new(ValueType::String, data)
    }

    pub fn bytes(v: &[u8]) -> Self {
        let data = v.to_vec();
        Value::new(ValueType::Bytes, data)
    }

    pub fn int_list(v: &[i32]) -> Self {
        let data = gen_array!(v, i32, write_i32);
        Value::new(ValueType::IntList, data)
    }

    pub fn long_list(v: &[i64]) -> Self {
        let data = gen_array!(v, i64, write_i64);
        Value::new(ValueType::LongList, data)
    }

    pub fn float_list(v: &[f32]) -> Self {
        let data = gen_array!(v, f32, write_f32);
        Value::new(ValueType::FloatList, data)
    }

    pub fn double_list(v: &[f64]) -> Self {
        let data = gen_array!(v, f64, write_f64);
        Value::new(ValueType::DoubleList, data)
    }

    pub fn string_list(v: &[String]) -> Self {
        let mut size = 4 + 4 * v.len();
        for s in v {
            size += s.len();
        }
        let mut data = Vec::with_capacity(size);
        unsafe { data.set_len(size); }
        let mut writer = UnsafeBytesWriter::new(&mut data);
        writer.write_i32(0, (v.len() as i32).to_be());
        let mut off = 0;
        let mut pos = 4;
        for s in v {
            off += s.len() as i32;
            writer.write_i32(pos, off.to_be());
            pos += 4;
        }
        for s in v {
            writer.write_bytes(pos, s.as_bytes());
            pos += s.len();
        }
        Value::new(ValueType::StringList, data)
    }

    pub fn as_ref(&self) -> ValueRef {
        ValueRef::new(self.r#type, &self.data)
    }

    pub fn get_bool(&self) -> GraphResult<bool> {
        res_unwrap!(self.as_ref().get_bool(), get_bool)
    }

    pub fn get_char(&self) -> GraphResult<u8> {
        res_unwrap!(self.as_ref().get_char(), get_char)
    }

    pub fn get_short(&self) -> GraphResult<i16> {
        res_unwrap!(self.as_ref().get_short(), get_short)
    }

    pub fn get_int(&self) -> GraphResult<i32> {
        res_unwrap!(self.as_ref().get_int(), get_int)
    }

    pub fn get_long(&self) -> GraphResult<i64> {
        res_unwrap!(self.as_ref().get_long(), get_long)
    }

    pub fn get_float(&self) -> GraphResult<f32> {
        res_unwrap!(self.as_ref().get_float(), get_float)
    }

    pub fn get_double(&self) -> GraphResult<f64> {
        res_unwrap!(self.as_ref().get_double(), get_double)
    }

    pub fn get_str(&self) -> GraphResult<&str> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_str(), get_str)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_bytes(&self) -> GraphResult<&[u8]> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_bytes(), get_bytes)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_int_list(&self) -> GraphResult<NumericArray<i32>> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_int_list(), get_int_list)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_long_list(&self) -> GraphResult<NumericArray<i64>> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_long_list(), get_long_list)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_float_list(&self) -> GraphResult<NumericArray<f32>> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_float_list(), get_float_list)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_double_list(&self) -> GraphResult<NumericArray<f64>> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_double_list(), get_double_list)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn get_str_list(&self) -> GraphResult<StrArray> {
        let r = self.as_ref();
        let ret = res_unwrap!(r.get_str_list(), get_str_list)?;
        unsafe { Ok(::std::mem::transmute(ret)) }
    }

    pub fn is_type(&self, r#type: ValueType) -> bool {
        self.r#type == r#type
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl From<String> for Value {
    fn from(data: String) -> Self {
        Value::string(&data)
    }
}

impl From<&str> for Value {
    fn from(data: &str) -> Self {
        Value::string(data)
    }
}

impl From<&[u8]> for Value {
    fn from(data: &[u8]) -> Self {
        Value::bytes(data)
    }
}

impl From<bool> for Value {
    fn from(data: bool) -> Self {
        Value::bool(data)
    }
}

impl From<u8> for Value {
    fn from(data: u8) -> Self {
        Value::char(data)
    }
}

impl From<i16> for Value {
    fn from(data: i16) -> Self {
        Value::short(data)
    }
}

impl From<i32> for Value {
    fn from(data: i32) -> Self {
        Value::int(data)
    }
}

impl From<i64> for Value {
    fn from(data: i64) -> Self {
        Value::long(data)
    }
}

impl From<f32> for Value {
    fn from(data: f32) -> Self {
        Value::float(data)
    }
}

impl From<f64> for Value {
    fn from(data: f64) -> Self {
        Value::double(data)
    }
}

impl From<Vec<i32>> for Value {
    fn from(data: Vec<i32>) -> Self {
        Value::int_list(&data)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(&other.as_ref())
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().partial_cmp(&other.as_ref())
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.as_ref())
    }
}

pub struct NumericArray<'a, T> {
    reader: UnsafeBytesReader<'a>,
    len: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T> NumericArray<'a, T> {
    pub fn len(&self) -> usize {
        self.len
    }
}

impl<'a, T: ToBigEndian> NumericArray<'a, T> {
    fn new(reader: UnsafeBytesReader<'a>) -> Self {
        let len = reader.read_i32(0).to_be() as usize;
        NumericArray {
            reader,
            len,
            _phantom: Default::default(),
        }
    }

    pub fn get(&self, idx: usize) -> Option<T> {
        if idx < self.len {
            let offset = 4 + ::std::mem::size_of::<T>() * idx;
            let tmp = *self.reader.read_ref::<T>(offset);
            return Some(tmp.to_big_endian());
        }
        None
    }

    pub fn iter(&self) -> NumericArrayIter<T> {
        NumericArrayIter::new(&self)
    }
}

impl<'a, T: ToBigEndian + std::fmt::Debug> std::fmt::Debug for NumericArray<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if self.len == 0 {
            write!(f, "[]")
        } else if self.len < 10 {
            write!(f, "[{:?}", self.get(0).unwrap())?;
            for i in 1..self.len {
                write!(f, ", {:?}", self.get(i).unwrap())?;
            }
            write!(f, "]")
        } else {
            write!(f, "[{:?}, {:?}, {:?}, ..., {:?}]", self.get(0).unwrap(), self.get(1).unwrap(),
            self.get(2).unwrap(), self.get(self.len - 1).unwrap())
        }
    }
}

pub struct NumericArrayIter<'a, T> {
    array: &'a NumericArray<'a, T>,
    cur: usize,
}

impl<'a, T> NumericArrayIter<'a, T> {
    fn new(array: &'a NumericArray<'a, T>) -> Self {
        NumericArrayIter {
            array,
            cur: 0,
        }
    }
}

impl<'a, T: ToBigEndian> Iterator for NumericArrayIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.array.get(self.cur);
        self.cur += 1;
        ret
    }
}

impl<'a, T: PartialEq + ToBigEndian> PartialEq for NumericArray<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() == other.len() {
            for i in 0..self.len() {
                if self.get(i).unwrap() != other.get(i).unwrap() {
                    return false;
                }
            }
            return true;
        }
        false
    }
}

impl<'a, T: PartialOrd + ToBigEndian> PartialOrd for NumericArray<'a, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let size = self.len();
        for i in 0..size {
            let x = self.get(i).unwrap();
            if let Some(y) = other.get(i) {
                let o = x.partial_cmp(&y)?;
                if o != Ordering::Equal {
                    return Some(o);
                }
            } else {
                return Some(Ordering::Greater);
            }
        }
        if other.len() > size {
            return Some(Ordering::Less);
        }
        Some(Ordering::Equal)
    }
}

pub struct StrArray<'a> {
    reader: UnsafeBytesReader<'a>,
    len: usize,
}

impl<'a> StrArray<'a> {
    fn new(reader: UnsafeBytesReader<'a>) -> Self {
        let len = reader.read_i32(0).to_be() as usize;
        StrArray {
            reader,
            len,
        }
    }

    pub fn get(&self, idx: usize) -> Option<&str> {
        if idx < self.len {
            let str_start_off = 4 + 4 * self.len;
            let start_off = if idx == 0 {
                0
            } else {
                self.reader.read_i32(4 + (idx - 1) * 4).to_be() as usize
            };
            let end_off = self.reader.read_i32(4 + idx * 4).to_be() as usize;
            let len = end_off - start_off;
            let offset = str_start_off + start_off;
            let data = self.reader.read_bytes(offset, len);
            let ret = ::std::str::from_utf8(data).expect("data in str array is in valid utf8");
            return Some(ret);
        }
        None
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> StrArrayIter {
        StrArrayIter::new(&self)
    }
}

pub struct StrArrayIter<'a> {
    array: &'a StrArray<'a>,
    cur: usize,
}

impl<'a> StrArrayIter<'a> {
    pub fn new(array: &'a StrArray<'a>) -> Self {
        StrArrayIter {
            array,
            cur: 0,
        }
    }
}

impl<'a> Iterator for StrArrayIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.array.get(self.cur);
        self.cur += 1;
        return ret;
    }
}

impl<'a> PartialEq for StrArray<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() == other.len() {
            for i in 0..self.len() {
                if self.get(i).unwrap() != other.get(i).unwrap() {
                    return false;
                }
            }
            return true;
        }
        false
    }
}

impl<'a> PartialOrd for StrArray<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let size = self.len();
        for i in 0..size {
            let s1 = self.get(i).unwrap();
            if let Some(s2) = other.get(i) {
                let o = s1.partial_cmp(s2)?;
                if o != Ordering::Equal {
                    return Some(o);
                }
            } else {
                return Some(Ordering::Greater);
            }
        }
        if other.len() > size {
            return Some(Ordering::Less);
        }
        Some(Ordering::Equal)
    }
}

impl<'a> std::fmt::Debug for StrArray<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if self.len == 0 {
            write!(f, "[]")
        } else if self.len < 10 {
            write!(f, "[{:?}", self.get(0).unwrap())?;
            for i in 1..self.len {
                write!(f, ", {:?}", self.get(i).unwrap())?;
            }
            write!(f, "]")
        } else {
            write!(f, "[{:?}, {:?}, {:?}, ..., {:?}]", self.get(0).unwrap(), self.get(1).unwrap(), self.get(2).unwrap(), self.get(self.len - 1).unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;


    macro_rules! normal_test {
        ($input:expr, $data_ty:tt, $func:tt) => {
            {
                for x in $input {
                    let v = Value::$data_ty(x);
                    assert_eq!(v.$func().unwrap(), x);
                    assert_eq!(v.as_ref().$func().unwrap(), x);
                    assert!(v.get_int_list().is_err());
                    assert!(v.get_long_list().is_err());
                    assert!(v.get_float_list().is_err());
                    assert!(v.get_double_list().is_err());
                    assert!(v.get_str_list().is_err());
                }
            }
        };
    }

    #[test]
    fn value_normal_test() {
        let input = vec![true, false];
        normal_test!(input, bool, get_bool);

        let input = vec!['a' as u8, 'k' as u8, 'z' as u8];
        normal_test!(input, char, get_char);

        let input = vec![1i16, 2, 123, 24323, -123, -23422];
        normal_test!(input, short, get_short);

        let input = vec![122, 123122211, 123334123, -2342423, -232];
        normal_test!(input, int, get_int);

        let input = vec![1i64, 1231312121222121, -12312312312, 121, -12];
        normal_test!(input, long, get_long);

        let input = vec![1.1f32, 2.2, 23242.2323, -3.23342];
        normal_test!(input, float, get_float);

        let input = vec![1.1, 2.2, 2324123123122.2323, -123123.23342];
        normal_test!(input, double, get_double);

        let input = vec!["aaa", "bbb", "ccc", ""];
        normal_test!(input, string, get_str);
    }

    macro_rules! array_test {
        ($input:expr, $arr_ty:tt, $func:tt) => {
            {
                for x in $input {
                    let v = Value::$arr_ty(&x);
                    let array = v.$func().unwrap();
                    check_numeric_array(array, &x);
                    let tmp = v.as_ref();
                    let array = tmp.$func().unwrap();
                    check_numeric_array(array, &x);
                    assert!(v.get_bool().is_err());
                    assert!(v.get_char().is_err());
                    assert!(v.get_short().is_err());
                    assert!(v.get_int().is_err());
                    assert!(v.get_long().is_err());
                    assert!(v.get_float().is_err());
                    assert!(v.get_double().is_err());
                    assert!(v.get_str().is_err());
                    assert!(v.get_str_list().is_err());
                }
            }
        };
    }

    #[test]
    fn value_array_test() {
        let input = vec![vec![1, 2, 3], vec![-1, 323, 2321], vec![1], vec![-12]];
        array_test!(input, int_list, get_int_list);

        let input = vec![vec![1, 222222, 322], vec![-11231, 32123121313, 2321], vec![13211], vec![-1131232]];
        array_test!(input, long_list, get_long_list);

        let input = vec![vec![1.12, 2.223, 3.453], vec![-1.23, 323.3343, 2321.343], vec![1.0], vec![-12.3333]];
        array_test!(input, float_list, get_float_list);

        let input = vec![vec![1.12111121321, 2.2123213123, 3.4513213],
                         vec![-1.212313, 323.33412313, 2321.3412313], vec![131231.0], vec![-112312312.3333]];
        array_test!(input, double_list, get_double_list);

        let input = vec![vec!["aaa".to_owned(), "bbb".to_owned(), "ccc".to_owned()], vec!["fadas".to_owned(), "dds.,,".to_owned()]];
        for x in input {
            let v = Value::string_list(&x);
            let array = v.get_str_list().unwrap();
            check_str_list(array, &x);
            let tmp = v.as_ref();
            let array = tmp.get_str_list().unwrap();
            check_str_list(array, &x);
        }
    }

    #[test]
    fn value_partial_cmp_test() {
        let small = vec![Value::bool(false), Value::char(0), Value::short(0), Value::int(0), Value::long(0), Value::float(0.0), Value::double(0.0)];
        let big = vec![Value::bool(true), Value::char(10), Value::short(12), Value::int(123), Value::long(1234), Value::float(12345.1), Value::double(123456.1)];
        for i in 0..small.len() {
            for j in i+1..small.len() {
                assert_eq!(small[i], small[j]);
            }
            for j in 0..big.len() {
                assert!(small[i] < big[j]);
            }
            for j in i+1..big.len() {
                assert!(big[i] < big[j]);
            }
        }

        let data = vec!["aaa", "bbb", "ccc", "ddd", "esdada", "fff"];
        for i in 0..data.len() {
            assert_eq!(Value::string(data[i]), Value::string(data[i]));
            for j in i+1..data.len() {
                assert!(Value::string(data[i]) < Value::string(data[j]));
            }
        }

        let data = vec![b"aaa", b"bbb", b"ccc", b"ddd", b"eee", b"fff"];
        for i in 0..data.len() {
            assert_eq!(Value::bytes(data[i]), Value::bytes(data[i]));
            for j in i+1..data.len() {
                assert!(Value::bytes(data[i]) < Value::bytes(data[j]));
            }
        }

        assert_eq!(Value::int_list(&[1, 2, 3]), Value::int_list(&[1, 2, 3]));
        assert!(Value::int_list(&[1, 2, 3, 4]) > Value::int_list(&[1, 2, 3]));
        assert!(Value::int_list(&[1, 2, 3]) < Value::int_list(&[1, 2, 3, 4]));
        assert!(Value::int_list(&[1, 3, 2]) > Value::int_list(&[1, 2, 3, 4]));
    }

    fn check_numeric_array<T: ToBigEndian + PartialEq + Debug>(array: NumericArray<T>, ans: &[T]) {
        assert_eq!(array.len(), ans.len());
        for i in 0..ans.len() {
            assert_eq!(array.get(i).unwrap(), ans[i]);
        }
        let mut idx = 0;
        for x in array.iter() {
            assert_eq!(x, ans[idx]);
            idx += 1;
        }
    }

    fn check_str_list(array: StrArray, ans: &[String]) {
        assert_eq!(array.len(), ans.len());
        for i in 0..ans.len() {
            assert_eq!(array.get(i).unwrap(), ans[i].as_str());
        }
        let mut idx = 0;
        for x in array.iter() {
            assert_eq!(x, ans[idx].as_str());
            idx += 1;
        }
    }

}
