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

use core::slice;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Write};
use std::mem;

use dyn_type::object::RawType;
use dyn_type::CastError;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use serde::{Deserialize, Serialize};

use crate::date::Date;
use crate::date_time::DateTime;
use crate::types::DefaultId;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    Int32,
    UInt32,
    Int64,
    UInt64,
    Double,
    String,
    Date,
    DateTime,
    LCString,
    ID,
    NULL,
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
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Double(f64),
    String(String),
    Date(Date),
    DateTime(DateTime),
    ID(DefaultId),
    Null,
}

#[derive(Clone)]
pub enum RefItem<'a> {
    Int32(&'a i32),
    UInt32(&'a u32),
    Int64(&'a i64),
    UInt64(&'a u64),
    Double(&'a f64),
    Date(&'a Date),
    DateTime(&'a DateTime),
    ID(&'a DefaultId),
    String(&'a String),
    Null,
}

impl<'a> RefItem<'a> {
    pub fn to_owned(self) -> Item {
        match self {
            RefItem::Int32(v) => Item::Int32(*v),
            RefItem::UInt32(v) => Item::UInt32(*v),
            RefItem::Int64(v) => Item::Int64(*v),
            RefItem::UInt64(v) => Item::UInt64(*v),
            RefItem::Double(v) => Item::Double(*v),
            RefItem::Date(v) => Item::Date(*v),
            RefItem::DateTime(v) => Item::DateTime(*v),
            RefItem::ID(v) => Item::ID(*v),
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
            Item::ID(v) => {
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
            Item::ID(v) => v.to_string(),
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
            RefItem::ID(v) => {
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
            RefItem::ID(v) => v.to_string(),
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
            RefItem::ID(v) => Ok(**v as u64),
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
            RefItem::ID(v) => Ok(**v as i32),
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
            RefItem::ID(_) => Err(CastError::new::<u64>(RawType::Long)),
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
}

pub struct Int32Column {
    pub data: Vec<i32>,
}

impl Int32Column {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<i32>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_i32);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<i32>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for Int32Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int32Column: {:?}", self.data)
    }
}

impl Encode for Int32Column {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for Int32Column {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<i32>::read_from(reader)?;
        info!("int32 column: {}", vec.capacity() * 4_usize);
        Ok(Self { data: vec })
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
}

pub struct UInt32Column {
    pub data: Vec<u32>,
}

impl UInt32Column {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<u32>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_u32);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<u32>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for UInt32Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UInt32Column: {:?}", self.data)
    }
}

impl Encode for UInt32Column {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for UInt32Column {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<u32>::read_from(reader)?;
        info!("uint32 column: {}", vec.capacity() * 4_usize);
        Ok(Self { data: vec })
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
}

pub struct Int64Column {
    pub data: Vec<i64>,
}

impl Int64Column {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<i64>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_i64);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<i64>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for Int64Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int64Column: {:?}", self.data)
    }
}

impl Encode for Int64Column {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for Int64Column {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<i64>::read_from(reader)?;
        info!("int64 column: {}", vec.capacity() * 8_usize);
        Ok(Self { data: vec })
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
}

pub struct UInt64Column {
    pub data: Vec<u64>,
}

impl UInt64Column {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<u64>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_u64);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<u64>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for UInt64Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UInt64Column: {:?}", self.data)
    }
}

impl Encode for UInt64Column {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for UInt64Column {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<u64>::read_from(reader)?;
        info!("uint64 column: {}", vec.capacity() * 8_usize);
        Ok(Self { data: vec })
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
}

pub struct IDColumn {
    pub data: Vec<DefaultId>,
}

impl IDColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl Debug for IDColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDColumn: {:?}", self.data)
    }
}

impl Encode for IDColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.data.len() as u64)?;
        for v in self.data.iter() {
            writer.write_u64(*v as u64)?;
        }
        Ok(())
    }
}

impl Decode for IDColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u64()? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(reader.read_u64()? as DefaultId);
        }
        info!("id column: {}", vec.capacity() * 8_usize);
        Ok(Self { data: vec })
    }
}

impl Column for IDColumn {
    fn get_type(&self) -> DataType {
        DataType::ID
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::ID(x))
    }

    fn set(&mut self, index: usize, val: Item) {
        match val {
            Item::ID(v) => {
                self.data[index] = v;
            }
            _ => {
                self.data[index] = 0;
            }
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::ID(v) => {
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
}

pub struct DoubleColumn {
    pub data: Vec<f64>,
}

impl DoubleColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<f64>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_f64);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<f64>(),
            );
            f.read_exact(data_slice).unwrap();
        }
    }
}

impl Debug for DoubleColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DoubleColumn: {:?}", self.data)
    }
}

impl Encode for DoubleColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for DoubleColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<f64>::read_from(reader)?;
        info!("double column: {}", vec.capacity() * 8_usize);
        Ok(Self { data: vec })
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
}

pub struct StringColumn {
    pub data: Vec<String>,
}

impl StringColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl Debug for StringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringColumn: {:?}", self.data)
    }
}

impl Encode for StringColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for StringColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<String>::read_from(reader)?;
        let mut ret = vec.capacity() * mem::size_of::<String>();
        for s in vec.iter() {
            ret += s.len();
        }
        info!("string column: {}", ret);
        Ok(Self { data: vec })
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
}

pub struct LCStringColumn {
    pub data: Vec<u16>,
    pub table: HashMap<String, u16>,
    pub list: Vec<String>,
}

impl LCStringColumn {
    pub fn new() -> Self {
        Self { data: Vec::new(), table: HashMap::new(), list: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let mut table_bytes = Vec::new();
        table_bytes
            .write_u64(self.list.len() as u64)
            .unwrap();
        for v in self.list.iter() {
            v.write_to(&mut table_bytes).unwrap();
        }
        f.write_u64(table_bytes.len() as u64).unwrap();
        f.write(&table_bytes).unwrap();

        f.write_u64(self.data.len() as u64).unwrap();
        let data_size = self.data.len() * std::mem::size_of::<u16>();

        unsafe {
            let data_slice = slice::from_raw_parts(self.data.as_ptr() as *const u8, data_size);
            f.write_all(data_slice).unwrap();
        }

        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let table_bytes_len = f.read_u64().unwrap() as usize;
        let mut table_bytes = vec![0_u8; table_bytes_len];
        f.read_exact(&mut table_bytes).unwrap();
        let mut table_bytes_slice = table_bytes.as_slice();
        self.list.clear();
        self.table.clear();
        let list_len = table_bytes_slice.read_u64().unwrap() as usize;
        for i in 0..list_len {
            let str = String::read_from(&mut table_bytes_slice).unwrap();
            self.list.push(str.clone());
            self.table.insert(str, i as u16);
        }

        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, 0_u16);
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<u16>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for LCStringColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LCStringColumn: {:?},{:?},{:?}", self.data, self.table, self.list)
    }
}

impl Encode for LCStringColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)?;
        self.list.write_to(writer)
    }
}

impl Decode for LCStringColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data = Vec::<u16>::read_from(reader)?;
        let list = Vec::<String>::read_from(reader)?;
        let mut table = HashMap::new();
        for (index, key) in list.iter().enumerate() {
            table.insert(key.to_owned(), index as u16);
        }
        Ok(Self { data, table, list })
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
}

pub struct DateColumn {
    pub data: Vec<Date>,
}

impl DateColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<Date>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, Date::empty());
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<Date>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for DateColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DateColumn: {:?}", self.data)
    }
}

impl Encode for DateColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for DateColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<Date>::read_from(reader)?;
        info!("date column: {}", vec.capacity() * mem::size_of::<Date>());
        Ok(Self { data: vec })
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
}

pub struct DateTimeColumn {
    pub data: Vec<DateTime>,
}

impl DateTimeColumn {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn serialize(&self, f: &mut File) {
        let row_num = self.data.len();
        f.write_u64(row_num as u64).unwrap();
        unsafe {
            let data_slice = slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                row_num * std::mem::size_of::<DateTime>(),
            );
            f.write_all(data_slice).unwrap();
        }
        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, f: &mut File) {
        let row_num = f.read_u64().unwrap() as usize;
        self.data.resize(row_num, DateTime::empty());
        unsafe {
            let data_slice = slice::from_raw_parts_mut(
                self.data.as_mut_ptr() as *mut u8,
                row_num * std::mem::size_of::<DateTime>(),
            );
            f.read_exact(data_slice).unwrap();
        }
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
}

impl Debug for DateTimeColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DateTimeColumn: {:?}", self.data)
    }
}

impl Encode for DateTimeColumn {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.data.write_to(writer)
    }
}

impl Decode for DateTimeColumn {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vec = Vec::<DateTime>::read_from(reader)?;
        info!("datetime column: {}", vec.capacity() * mem::size_of::<DateTime>());
        Ok(Self { data: vec })
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
}
