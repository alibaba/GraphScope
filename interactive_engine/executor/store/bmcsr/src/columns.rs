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

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use dyn_type::object::RawType;
use dyn_type::CastError;
#[cfg(feature = "hugepage_table")]
use huge_container::HugeVec;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use serde::{Deserialize, Serialize};

use crate::date::Date;
use crate::date_time::DateTime;
use crate::types::DefaultId;

#[cfg(feature = "hugepage_table")]
pub type ColumnContainer<T> = HugeVec<T>;

#[cfg(not(feature = "hugepage_table"))]
pub type ColumnContainer<T> = Vec<T>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    Int32 = 1,
    UInt32 = 2,
    Int64 = 3,
    UInt64 = 4,
    Double = 5,
    String = 6,
    Date = 7,
    DateTime = 8,
    LCString = 9,
    ID = 10,
    NULL = 0,
}

impl Encode for DataType {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match *self {
            DataType::NULL => writer.write_u8(0),
            DataType::Int32 => writer.write_u8(1),
            DataType::UInt32 => writer.write_u8(2),
            DataType::Int64 => writer.write_u8(3),
            DataType::UInt64 => writer.write_u8(4),
            DataType::Double => writer.write_u8(5),
            DataType::String => writer.write_u8(6),
            DataType::Date => writer.write_u8(7),
            DataType::DateTime => writer.write_u8(8),
            DataType::LCString => writer.write_u8(9),
            DataType::ID => writer.write_u8(10),
        };
        Ok(())
    }
}

impl Decode for DataType {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data_type = match reader.read_u8()? {
            0 => DataType::NULL,
            1 => DataType::Int32,
            2 => DataType::UInt32,
            3 => DataType::Int64,
            4 => DataType::UInt64,
            5 => DataType::Double,
            6 => DataType::String,
            7 => DataType::Date,
            8 => DataType::DateTime,
            9 => DataType::LCString,
            10 => DataType::ID,
            _ => panic!("Unknown data type"),
        };
        Ok(data_type)
    }
}

impl DataType {
    pub fn from_i32(n: i32) -> Option<Self> {
        match n {
            0 => Some(Self::NULL),
            1 => Some(Self::Int32),
            2 => Some(Self::UInt32),
            3 => Some(Self::Int64),
            4 => Some(Self::UInt64),
            5 => Some(Self::Double),
            6 => Some(Self::String),
            7 => Some(Self::Date),
            8 => Some(Self::DateTime),
            9 => Some(Self::LCString),
            10 => Some(Self::ID),
            _ => None,
        }
    }

    pub fn to_i32(&self) -> i32 {
        match self {
            Self::NULL => 0,
            Self::Int32 => 1,
            Self::UInt32 => 2,
            Self::Int64 => 3,
            Self::UInt64 => 4,
            Self::Double => 5,
            Self::String => 6,
            Self::Date => 7,
            Self::DateTime => 8,
            Self::LCString => 9,
            Self::ID => 10,
        }
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(_token: &'a str) -> Self {
        info!("token = {}", _token);
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "INT32" {
            DataType::Int32
        } else if token == "UINT32" {
            DataType::UInt32
        } else if token == "INT64" {
            DataType::Int64
        } else if token == "UINT64" {
            DataType::UInt64
        } else if token == "DOUBLE" {
            DataType::Double
        } else if token == "STRING" {
            DataType::String
        } else if token == "DATE" {
            DataType::Date
        } else if token == "DATETIME" {
            DataType::DateTime
        } else if token == "ID" {
            DataType::ID
        } else if token == "LCString" {
            DataType::LCString
        } else {
            error!("Unsupported type {:?}", token);
            DataType::NULL
        }
    }
}

#[derive(Clone)]
pub enum Item {
    Boolean(bool),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Date(Date),
    DateTime(DateTime),
    VertexId(usize),
    EdgeId((u64, u64)),
    Null,
}

#[derive(Clone)]
pub enum RefItem<'a> {
    Boolean(&'a bool),
    Int32(&'a i32),
    UInt32(&'a u32),
    Int64(&'a i64),
    UInt64(&'a u64),
    Float(&'a f32),
    Double(&'a f64),
    Date(&'a Date),
    DateTime(&'a DateTime),
    VertexId(&'a usize),
    EdgeId((&'a u64, &'a u64)),
    String(&'a String),
    Null,
}

impl<'a> RefItem<'a> {
    pub fn to_owned(self) -> Item {
        match self {
            RefItem::Boolean(v) => Item::Boolean(*v),
            RefItem::Int32(v) => Item::Int32(*v),
            RefItem::UInt32(v) => Item::UInt32(*v),
            RefItem::Int64(v) => Item::Int64(*v),
            RefItem::UInt64(v) => Item::UInt64(*v),
            RefItem::Float(v) => Item::Float(*v),
            RefItem::Double(v) => Item::Double(*v),
            RefItem::Date(v) => Item::Date(*v),
            RefItem::DateTime(v) => Item::DateTime(*v),
            RefItem::VertexId(v) => Item::VertexId(*v),
            RefItem::EdgeId((src, dst)) => Item::EdgeId((*src, *dst)),
            RefItem::String(v) => Item::String(v.clone()),
            RefItem::Null => Item::Null,
        }
    }
}

pub trait ConvertItem {
    fn to_ref_item(&self) -> RefItem;
    fn from_item(v: Item) -> Self;
}

impl ConvertItem for i32 {
    fn to_ref_item(&self) -> RefItem {
        RefItem::Int32(self)
    }

    fn from_item(v: Item) -> Self {
        match v {
            Item::Int32(v) => v,
            _ => 0,
        }
    }
}

impl ConvertItem for DateTime {
    fn to_ref_item(&self) -> RefItem {
        RefItem::DateTime(self)
    }

    fn from_item(v: Item) -> Self {
        match v {
            Item::DateTime(v) => v,
            _ => DateTime::empty(),
        }
    }
}

impl ConvertItem for () {
    fn to_ref_item(&self) -> RefItem {
        RefItem::Null
    }

    fn from_item(_v: Item) -> Self {
        ()
    }
}

impl Debug for Item {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Item::Int32(v) => {
                write!(f, "int32[{}]", v)
            }
            Item::UInt32(v) => {
                write!(f, "uint32[{}]", v)
            }
            Item::Int64(v) => {
                write!(f, "int64[{}]", v)
            }
            Item::UInt64(v) => {
                write!(f, "uint64[{}]", v)
            }
            Item::Double(v) => {
                write!(f, "double[{}]", v)
            }
            Item::Date(v) => {
                write!(f, "date[{}]", v.to_string())
            }
            Item::DateTime(v) => {
                write!(f, "datetime[{}]", v.to_string())
            }
            Item::VertexId(v) => {
                write!(f, "id[{}]", v)
            }
            Item::String(v) => {
                write!(f, "string[{}]", v)
            }
            _ => {
                write!(f, "")
            }
        }
    }
}

impl ToString for Item {
    fn to_string(&self) -> String {
        match self {
            Item::Int32(v) => v.to_string(),
            Item::UInt32(v) => v.to_string(),
            Item::Int64(v) => v.to_string(),
            Item::UInt64(v) => v.to_string(),
            Item::Double(v) => v.to_string(),
            Item::Date(v) => v.to_string(),
            Item::DateTime(v) => v.to_string(),
            Item::VertexId(v) => v.to_string(),
            Item::String(v) => v.to_string(),
            _ => "".to_string(),
        }
    }
}

impl<'a> Debug for RefItem<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RefItem::Int32(v) => {
                write!(f, "int32[{}]", v)
            }
            RefItem::UInt32(v) => {
                write!(f, "uint32[{}]", v)
            }
            RefItem::Int64(v) => {
                write!(f, "int64[{}]", v)
            }
            RefItem::UInt64(v) => {
                write!(f, "uint64[{}]", v)
            }
            RefItem::Double(v) => {
                write!(f, "double[{}]", v)
            }
            RefItem::Date(v) => {
                write!(f, "date[{}]", v.to_string())
            }
            RefItem::DateTime(v) => {
                write!(f, "datetime[{}]", v.to_string())
            }
            RefItem::VertexId(v) => {
                write!(f, "id[{}]", v)
            }
            RefItem::String(v) => {
                write!(f, "string[{}]", v)
            }
            _ => {
                write!(f, "")
            }
        }
    }
}

impl<'a> ToString for RefItem<'a> {
    fn to_string(&self) -> String {
        match self {
            RefItem::Int32(v) => v.to_string(),
            RefItem::UInt32(v) => v.to_string(),
            RefItem::Int64(v) => v.to_string(),
            RefItem::UInt64(v) => v.to_string(),
            RefItem::Double(v) => v.to_string(),
            RefItem::Date(v) => v.to_string(),
            RefItem::DateTime(v) => v.to_string(),
            RefItem::VertexId(v) => v.to_string(),
            RefItem::String(v) => v.to_string(),
            _ => "".to_string(),
        }
    }
}

impl<'a> RefItem<'a> {
    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            RefItem::Int32(v) => Ok(**v as u64),
            RefItem::UInt32(v) => Ok(**v as u64),
            RefItem::Int64(v) => Ok(**v as u64),
            RefItem::UInt64(v) => Ok(**v),
            RefItem::Double(v) => Ok(**v as u64),
            RefItem::Date(_) => Ok(0_u64),
            RefItem::DateTime(v) => Ok(v.to_i64() as u64),
            RefItem::VertexId(v) => Ok(**v as u64),
            RefItem::String(_) => Err(CastError::new::<u64>(RawType::String)),
            _ => Ok(0_u64),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            RefItem::Int32(v) => Ok(**v),
            RefItem::UInt32(v) => Ok(**v as i32),
            RefItem::Int64(v) => Ok(**v as i32),
            RefItem::UInt64(v) => Ok(**v as i32),
            RefItem::Double(v) => Ok(**v as i32),
            RefItem::Date(_) => Ok(0),
            RefItem::DateTime(_) => Ok(0),
            RefItem::VertexId(v) => Ok(**v as i32),
            RefItem::String(_) => Err(CastError::new::<i32>(RawType::String)),
            _ => Ok(0),
        }
    }

    #[inline]
    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            RefItem::String(str) => Ok(Cow::Borrowed(*str)),
            _ => Err(CastError::new::<String>(RawType::Unknown)),
        }
    }

    #[inline]
    pub fn as_datetime(&self) -> Result<DateTime, CastError> {
        match self {
            RefItem::Int32(_) => Err(CastError::new::<u64>(RawType::Integer)),
            RefItem::UInt32(_) => Err(CastError::new::<u64>(RawType::Integer)),
            RefItem::Int64(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::UInt64(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::Double(_) => Err(CastError::new::<u64>(RawType::Float)),
            RefItem::Date(_) => Err(CastError::new::<u64>(RawType::Unknown)),
            RefItem::DateTime(v) => Ok(**v),
            RefItem::VertexId(_) => Err(CastError::new::<u64>(RawType::Long)),
            RefItem::String(_) => Err(CastError::new::<u64>(RawType::String)),
            _ => Err(CastError::new::<u64>(RawType::Unknown)),
        }
    }
}

pub trait Column: Debug {
    fn get_type(&self) -> DataType;
    fn get(&self, index: usize) -> Option<RefItem>;
    fn set(&mut self, index: usize, val: Item);
    fn push(&mut self, val: Item);
    fn len(&self) -> usize;
    fn as_any(&self) -> &dyn Any;

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>);
    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize);
    fn move_elem(&mut self, from: usize, to: usize);
    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize);
    fn resize(&mut self, size: usize);

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()>;
    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()>;
}

pub struct Int32Column {
    pub data: ColumnContainer<i32>,
}

unsafe impl Send for Int32Column {}

unsafe impl Sync for Int32Column {}

impl Int32Column {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k] != other.data[k] {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<i32>) -> Int32Column {
        Int32Column { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<i32>) -> Int32Column {
        Int32Column { data }
    }

    pub fn clone_from(other: &Int32Column) -> Int32Column {
        Int32Column { data: other.data.clone() }
    }
}

impl Debug for Int32Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int32Column: {:?}", self.data)
    }
}

impl Column for Int32Column {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Int32(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<i32>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_i32::<LittleEndian>()?);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_i32::<LittleEndian>(*v)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct UInt32Column {
    pub data: ColumnContainer<u32>,
}

unsafe impl Send for UInt32Column {}

unsafe impl Sync for UInt32Column {}

impl UInt32Column {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k] != other.data[k] {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<u32>) -> UInt32Column {
        UInt32Column { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<u32>) -> UInt32Column {
        UInt32Column { data }
    }

    pub fn clone_from(other: &UInt32Column) -> UInt32Column {
        UInt32Column { data: other.data.clone() }
    }
}

impl Debug for UInt32Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UInt32Column: {:?}", self.data)
    }
}

impl Column for UInt32Column {
    fn get_type(&self) -> DataType {
        DataType::UInt32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::UInt32(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::UInt32(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::UInt32(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<u32>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_u32::<LittleEndian>()?);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_u32::<LittleEndian>(*v)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct Int64Column {
    pub data: ColumnContainer<i64>,
}

unsafe impl Send for Int64Column {}

unsafe impl Sync for Int64Column {}

impl Int64Column {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k] != other.data[k] {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<i64>) -> Int64Column {
        Int64Column { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<i64>) -> Int64Column {
        Int64Column { data }
    }

    pub fn clone_from(other: &Int64Column) -> Int64Column {
        Int64Column { data: other.data.clone() }
    }
}

impl Debug for Int64Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int64Column: {:?}", self.data)
    }
}

impl Column for Int64Column {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Int64(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Int64(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Int64(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<i64>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_i64::<LittleEndian>()?);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_i64::<LittleEndian>(*v)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct UInt64Column {
    pub data: ColumnContainer<u64>,
}

unsafe impl Send for UInt64Column {}

unsafe impl Sync for UInt64Column {}

impl UInt64Column {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k] != other.data[k] {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<u64>) -> UInt64Column {
        UInt64Column { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<u64>) -> UInt64Column {
        UInt64Column { data }
    }

    pub fn clone_from(other: &UInt64Column) -> UInt64Column {
        UInt64Column { data: other.data.clone() }
    }
}

impl Debug for UInt64Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UInt64Column: {:?}", self.data)
    }
}

impl Column for UInt64Column {
    fn get_type(&self) -> DataType {
        DataType::UInt64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::UInt64(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<u64>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_u64::<LittleEndian>()?);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_u64::<LittleEndian>(*v)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct IDColumn {
    pub data: ColumnContainer<DefaultId>,
}

unsafe impl Send for IDColumn {}

unsafe impl Sync for IDColumn {}

impl IDColumn {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<usize>) -> IDColumn {
        IDColumn { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<usize>) -> IDColumn {
        IDColumn { data }
    }

    pub fn clone_from(other: &IDColumn) -> IDColumn {
        IDColumn { data: other.data.clone() }
    }
}

impl Debug for IDColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDColumn: {:?}", self.data)
    }
}

impl Column for IDColumn {
    fn get_type(&self) -> DataType {
        DataType::ID
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data
            .get(index)
            .map(|x| RefItem::VertexId(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::VertexId(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::VertexId(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<DefaultId>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_u64::<LittleEndian>()? as DefaultId);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_u64::<LittleEndian>(*v as u64)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct DoubleColumn {
    pub data: ColumnContainer<f64>,
}

unsafe impl Send for DoubleColumn {}

unsafe impl Sync for DoubleColumn {}

impl DoubleColumn {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<f64>) -> DoubleColumn {
        DoubleColumn { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<f64>) -> DoubleColumn {
        DoubleColumn { data }
    }

    pub fn clone_from(other: &DoubleColumn) -> DoubleColumn {
        DoubleColumn { data: other.data.clone() }
    }
}

impl Debug for DoubleColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DoubleColumn: {:?}", self.data)
    }
}

impl Column for DoubleColumn {
    fn get_type(&self) -> DataType {
        DataType::Double
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Double(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Double(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0_f64;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Double(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0_f64);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<f64>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_f64::<LittleEndian>()?);
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_f64::<LittleEndian>(*v)?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0.0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct StringColumn {
    pub data: Vec<String>,
}

unsafe impl Send for StringColumn {}

unsafe impl Sync for StringColumn {}

impl StringColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<String>) -> StringColumn {
        StringColumn { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<String>) -> StringColumn {
        StringColumn { data }
    }

    pub fn clone_from(other: &StringColumn) -> StringColumn {
        StringColumn { data: other.data.clone() }
    }
}

impl Debug for StringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringColumn: {:?}", self.data)
    }
}

impl Column for StringColumn {
    fn get_type(&self) -> DataType {
        DataType::String
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::String(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::String(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = String::from("");
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::String(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(String::from(""));
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = Vec::<String>::with_capacity(row_num);
        for _ in 0..row_num {
            let length = reader.read_i32::<LittleEndian>()?;
            let mut string_bytes = vec![0u8; length as usize];
            reader.read_exact(&mut string_bytes)?;
            data.push(String::from_utf8(string_bytes).unwrap());
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_i32::<LittleEndian>(v.len() as i32)?;
            writer.write_all(v.as_bytes())?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, String::new());
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index].clone();
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index].clone();
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from].clone();
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        for i in 0..num {
            self.data[self_index + i] = casted_col.data[col_index + i].clone();
        }
    }
}

pub struct LCStringColumn {
    pub data: ColumnContainer<u16>,
    pub table: HashMap<String, u16>,
    pub list: Vec<String>,
}

unsafe impl Send for LCStringColumn {}

unsafe impl Sync for LCStringColumn {}

impl LCStringColumn {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new(), table: HashMap::new(), list: Vec::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        if self.list != other.list {
            return false;
        }
        for k in 0..num {
            if self.data[k] != other.data[k] {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<u16>, table: HashMap<String, u16>, list: Vec<String>) -> LCStringColumn {
        LCStringColumn { data, table, list }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<u16>, table: HashMap<String, u16>, list: Vec<String>) -> LCStringColumn {
        LCStringColumn { data, table, list }
    }

    pub fn clone_from(other: &LCStringColumn) -> LCStringColumn {
        LCStringColumn { data: other.data.clone(), table: other.table.clone(), list: other.list.clone() }
    }
}

impl Debug for LCStringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LCStringColumn: {:?},{:?},{:?}", self.data, self.table, self.list)
    }
}

impl Column for LCStringColumn {
    fn get_type(&self) -> DataType {
        DataType::LCString
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data
            .get(index)
            .map(|x| RefItem::String(&self.list[*x as usize]))
    }

    fn set(&mut self, index: usize, val: Item) {
        let value = match val {
            Item::String(v) => v,
            _ => "".to_string(),
        };
        if let Some(v) = self.table.get(&value) {
            self.data[index] = *v;
        } else {
            assert!(self.list.len() < 65535);
            let cur = self.list.len() as u16;
            self.list.push(value.clone());
            self.table.insert(value, cur);
            self.data[index] = cur;
        }
    }

    fn push(&mut self, val: Item) {
        let value = match val {
            Item::String(v) => v,
            _ => "".to_string(),
        };
        if let Some(v) = self.table.get(&value) {
            self.data.push(*v);
        } else {
            assert!(self.list.len() < 65535);
            let cur = self.list.len() as u16;
            self.list.push(value.clone());
            self.table.insert(value, cur);
            self.data.push(cur);
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<u16>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(reader.read_u16::<LittleEndian>()?);
        }

        let list_size = reader.read_u16::<LittleEndian>()? as usize;
        let mut list = Vec::<String>::with_capacity(list_size);
        let mut table = HashMap::new();
        for i in 0..list_size {
            let length = reader.read_i32::<LittleEndian>()?;
            let mut string_bytes = vec![0u8; length as usize];
            reader.read_exact(&mut string_bytes)?;
            let parsed_string = String::from_utf8(string_bytes).unwrap();
            list.push(parsed_string.clone());
            table.insert(parsed_string, i as u16);
        }

        self.data = data;
        self.table = table;
        self.list = list;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_u16::<LittleEndian>(*v)?;
        }

        writer.write_u16::<LittleEndian>(self.list.len() as u16)?;
        for s in self.list.iter() {
            let length = s.len() as i32;
            writer.write_i32::<LittleEndian>(length)?;
            writer.write_all(s.as_bytes())?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                let val = casted_col.list[casted_col.data[index] as usize].clone();
                if let Some(idx) = self.table.get(&val) {
                    self.data[*i] = *idx;
                } else {
                    let idx = self.table.len() as u16;
                    self.list.push(val.clone());
                    self.table.insert(val, idx);
                    self.data[*i] = idx;
                }
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        let val = casted_col.list[casted_col.data[col_index] as usize].clone();
        if let Some(idx) = self.table.get(&val) {
            self.data[self_index] = *idx;
        } else {
            let idx = self.table.len() as u16;
            self.list.push(val.clone());
            self.table.insert(val, idx);
            self.data[self_index] = idx;
        }
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        for i in 0..num {
            let val = casted_col.list[casted_col.data[col_index + i] as usize].clone();
            if let Some(idx) = self.table.get(&val) {
                self.data[self_index + i] = *idx;
            } else {
                let idx = self.table.len() as u16;
                self.list.push(val.clone());
                self.table.insert(val, idx);
                self.data[self_index + i] = idx;
            }
        }
    }
}

pub struct DateColumn {
    pub data: ColumnContainer<Date>,
}

unsafe impl Send for DateColumn {}

unsafe impl Sync for DateColumn {}

impl DateColumn {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k].to_i32() != other.data[k].to_i32() {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<Date>) -> DateColumn {
        DateColumn { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<Date>) -> DateColumn {
        DateColumn { data }
    }

    pub fn clone_from(other: &DateColumn) -> DateColumn {
        DateColumn { data: other.data.clone() }
    }
}

impl Debug for DateColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DateColumn: {:?}", self.data)
    }
}

impl Column for DateColumn {
    fn get_type(&self) -> DataType {
        DataType::Date
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Date(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::Date(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = Date::empty();
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Date(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(Date::empty());
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<Date>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(Date::from_i32(reader.read_i32::<LittleEndian>()?));
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_i32::<LittleEndian>(v.to_i32())?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, Date::empty());
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}

pub struct DateTimeColumn {
    pub data: ColumnContainer<DateTime>,
}

unsafe impl Send for DateTimeColumn {}

unsafe impl Sync for DateTimeColumn {}

impl DateTimeColumn {
    pub fn new() -> Self {
        Self { data: ColumnContainer::new() }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        let num = self.data.len();
        for k in 0..num {
            if self.data[k].to_i64() != other.data[k].to_i64() {
                return false;
            }
        }
        return true;
    }

    #[cfg(feature = "hugepage_table")]
    pub fn from(data: HugeVec<DateTime>) -> DateTimeColumn {
        DateTimeColumn { data }
    }

    #[cfg(not(feature = "hugepage_table"))]
    pub fn from(data: Vec<DateTime>) -> DateTimeColumn {
        DateTimeColumn { data }
    }

    pub fn clone_from(other: &DateTimeColumn) -> DateTimeColumn {
        DateTimeColumn { data: other.data.clone() }
    }
}

impl Debug for DateTimeColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DateTimeColumn: {:?}", self.data)
    }
}

impl Column for DateTimeColumn {
    fn get_type(&self) -> DataType {
        DataType::DateTime
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data
            .get(index)
            .map(|x| RefItem::DateTime(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::DateTime(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = DateTime::empty();
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::DateTime(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(DateTime::empty());
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn deserialize(&mut self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let row_num = reader.read_u64::<LittleEndian>()? as usize;
        let mut data = ColumnContainer::<DateTime>::with_capacity(row_num);
        for _ in 0..row_num {
            data.push(DateTime::new(reader.read_i64::<LittleEndian>()?));
        }
        self.data = data;
        Ok(())
    }

    fn serialize(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_u64::<LittleEndian>(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_i64::<LittleEndian>(v.to_i64())?;
        }

        Ok(())
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, DateTime::empty());
    }

    fn set_column_batch(&mut self, index: &Vec<usize>, col: &Box<dyn Column>) {
        if col.as_any().is::<Self>() {
            let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
            for (index, i) in index.iter().enumerate() {
                self.data[*i] = casted_col.data[index];
            }
        }
    }

    fn set_column_elem(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index] = casted_col.data[col_index];
    }

    fn move_elem(&mut self, from: usize, to: usize) {
        self.data[to] = self.data[from];
    }

    fn copy_range(&mut self, self_index: usize, col: &Box<dyn Column>, col_index: usize, num: usize) {
        let casted_col = col.as_any().downcast_ref::<Self>().unwrap();
        self.data[self_index..self_index + num]
            .copy_from_slice(&casted_col.data[col_index..col_index + num]);
    }
}
