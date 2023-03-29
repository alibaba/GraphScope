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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Write};

use csv::StringRecord;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::columns::*;
use crate::date::parse_date;
use crate::date_time::parse_datetime;
use crate::error::GDBResult;

#[derive(Debug)]
pub struct ColTable {
    columns: Vec<Box<dyn Column>>,
    pub header: HashMap<String, usize>,
    row_num: usize,
}

impl ColTable {
    pub fn new(types: Vec<(DataType, String)>) -> Self {
        let mut columns = Vec::<Box<dyn Column>>::with_capacity(types.len());
        let mut header = HashMap::new();
        for pair in types.into_iter().enumerate() {
            header.insert(pair.1 .1, pair.0);
            match pair.1 .0 {
                DataType::Int32 => {
                    columns.push(Box::new(Int32Column::new()));
                }
                DataType::UInt32 => {
                    columns.push(Box::new(UInt32Column::new()));
                }
                DataType::Int64 => {
                    columns.push(Box::new(Int64Column::new()));
                }
                DataType::UInt64 => {
                    columns.push(Box::new(UInt64Column::new()));
                }
                DataType::String => {
                    columns.push(Box::new(StringColumn::new()));
                }
                DataType::LCString => {
                    columns.push(Box::new(LCStringColumn::new()));
                }
                DataType::Double => {
                    columns.push(Box::new(DoubleColumn::new()));
                }
                DataType::Date => {
                    columns.push(Box::new(DateColumn::new()));
                }
                DataType::DateTime => {
                    columns.push(Box::new(DateTimeColumn::new()));
                }
                DataType::ID => {
                    columns.push(Box::new(IDColumn::new()));
                }
                DataType::NULL => {
                    error!("Unexpected column type");
                }
            }
        }
        Self { columns, header, row_num: 0 }
    }

    pub fn col_num(&self) -> usize {
        self.columns.len()
    }

    pub fn row_num(&self) -> usize {
        self.row_num
    }

    pub fn push(&mut self, row: &Vec<Item>) {
        let col_num = self.columns.len();
        if row.len() < col_num {
            info!("schema not match when push, row_len = {}, col num = {}", row.len(), col_num);
            return;
        }
        for i in 0..col_num {
            self.columns[i].push(row[i].clone());
        }
        self.row_num += 1;
    }

    pub fn insert(&mut self, index: usize, row: &Vec<Item>) {
        let col_num = self.columns.len();
        if self.row_num <= index {
            let null_num = index - self.row_num;
            for i in 0..col_num {
                let col = &mut self.columns[i];
                for _ in 0..null_num {
                    col.push(Item::Null);
                }
            }
            self.row_num = index;
            self.push(row);
        } else {
            for i in 0..col_num {
                self.columns[i].set(index, row[i].clone());
            }
        }
    }

    pub fn get_column_by_index(&self, index: usize) -> &'_ Box<dyn Column> {
        &self.columns[index]
    }

    pub fn get_column_by_name(&self, name: &str) -> &'_ Box<dyn Column> {
        let index = self.header.get(name).unwrap();
        &self.columns[*index]
    }

    pub fn get_item(&self, col_name: &str, row_i: usize) -> Option<RefItem> {
        if let Some(col_i) = self.header.get(col_name) {
            self.columns[*col_i].get(row_i)
        } else {
            None
        }
    }

    pub fn get_item_by_index(&self, col_i: usize, row_i: usize) -> Option<RefItem> {
        if col_i < self.columns.len() {
            self.columns[col_i].get(row_i)
        } else {
            None
        }
    }

    pub fn serialize_table(&self, path: &String) {
        let mut f = File::create(path).unwrap();
        f.write_u64(self.row_num as u64).unwrap();
        let mut header_bytes = Vec::new();
        header_bytes
            .write_u64(self.header.len() as u64)
            .unwrap();
        for pair in self.header.iter() {
            pair.0.write_to(&mut header_bytes).unwrap();
            header_bytes.write_u64(*pair.1 as u64).unwrap();
        }
        f.write_u64(header_bytes.len() as u64).unwrap();
        f.write(&header_bytes).unwrap();
        f.write_u64(self.columns.len() as u64).unwrap();

        for col in self.columns.iter() {
            match col.get_type() {
                DataType::Int32 => {
                    f.write_u8(0).unwrap();
                    col.as_any()
                        .downcast_ref::<Int32Column>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::UInt32 => {
                    f.write_u8(1).unwrap();
                    col.as_any()
                        .downcast_ref::<UInt32Column>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::Int64 => {
                    f.write_u8(2).unwrap();
                    col.as_any()
                        .downcast_ref::<Int64Column>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::UInt64 => {
                    f.write_u8(3).unwrap();
                    col.as_any()
                        .downcast_ref::<UInt64Column>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::Double => {
                    f.write_u8(4).unwrap();
                    col.as_any()
                        .downcast_ref::<DoubleColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::String => {
                    f.write_u8(5).unwrap();
                    let string_array = &col
                        .as_any()
                        .downcast_ref::<StringColumn>()
                        .unwrap()
                        .data;
                    let mut string_column_bytes = Vec::new();
                    string_column_bytes
                        .write_u64(string_array.len() as u64)
                        .unwrap();
                    for str in string_array.iter() {
                        str.write_to(&mut string_column_bytes).unwrap();
                    }
                    f.write_u64(string_column_bytes.len() as u64)
                        .unwrap();
                    f.write(&string_column_bytes).unwrap();
                }
                DataType::Date => {
                    f.write_u8(6).unwrap();
                    col.as_any()
                        .downcast_ref::<DateColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::DateTime => {
                    f.write_u8(7).unwrap();
                    col.as_any()
                        .downcast_ref::<DateTimeColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::LCString => {
                    f.write_u8(8).unwrap();
                    col.as_any()
                        .downcast_ref::<LCStringColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                _ => {
                    info!("unexpected type...");
                }
            }
        }
    }

    pub fn deserialize_table(&mut self, path: &String) {
        let mut f = File::open(path).unwrap();
        self.row_num = f.read_u64().unwrap() as usize;
        let header_bytes_len = f.read_u64().unwrap() as usize;
        let mut header_bytes = vec![0_u8; header_bytes_len];
        f.read_exact(&mut header_bytes).unwrap();
        let mut header_bytes_slice = header_bytes.as_slice();
        self.header = HashMap::new();
        let header_len = header_bytes_slice.read_u64().unwrap();
        for _ in 0..header_len {
            let str = String::read_from(&mut header_bytes_slice).unwrap();
            let index = header_bytes_slice.read_u64().unwrap() as usize;
            self.header.insert(str, index);
        }

        let column_len = f.read_u64().unwrap() as usize;
        for _ in 0..column_len {
            let t = f.read_u8().unwrap();
            if t == 0 {
                let mut col = Int32Column::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 1 {
                let mut col = UInt32Column::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 2 {
                let mut col = Int64Column::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 3 {
                let mut col = UInt64Column::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 4 {
                let mut col = DoubleColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 5 {
                let mut col = StringColumn::new();
                let string_array = &mut col.data;
                let string_column_bytes_len = f.read_u64().unwrap() as usize;
                let mut string_column_bytes = vec![0_u8; string_column_bytes_len];
                f.read_exact(&mut string_column_bytes).unwrap();
                let mut string_column_bytes_slice = string_column_bytes.as_slice();
                let string_array_len = string_column_bytes_slice.read_u64().unwrap() as usize;
                for _ in 0..string_array_len {
                    string_array.push(String::read_from(&mut string_column_bytes_slice).unwrap());
                }
                self.columns.push(Box::new(col));
            } else if t == 6 {
                let mut col = DateColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 7 {
                let mut col = DateTimeColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 8 {
                let mut col = LCStringColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else {
                info!("unexpected type...");
            }
        }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.header != other.header {
            info!("header not same");
            return false;
        }
        if self.columns.len() != other.columns.len() {
            info!("columns num not same");
            return false;
        }
        let col_num = self.columns.len();
        for i in 0..col_num {
            if self.columns[i].get_type() != other.columns[i].get_type() {
                info!("column-{} type not same", i);
                return false;
            }
            match self.columns[i].get_type() {
                DataType::Int32 => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<Int32Column>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<Int32Column>()
                                .unwrap(),
                        )
                    {
                        info!("column-{} data not same", i);
                        return false;
                    }
                }
                DataType::DateTime => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<DateTimeColumn>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<DateTimeColumn>()
                                .unwrap(),
                        )
                    {
                        info!("column-{} data not same", i);
                        return false;
                    }
                }
                DataType::String => {
                    let lhs = &self.columns[i]
                        .as_any()
                        .downcast_ref::<StringColumn>()
                        .unwrap()
                        .data;
                    let rhs = &other.columns[i]
                        .as_any()
                        .downcast_ref::<StringColumn>()
                        .unwrap()
                        .data;
                    if lhs.len() != rhs.len() {
                        info!("column-{} data not same", i);
                        return false;
                    }
                    let num = lhs.len();
                    for i in 0..num {
                        if lhs[i] != rhs[i] {
                            info!("column-{} data not same", i);
                            return false;
                        }
                    }
                }
                DataType::LCString => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<LCStringColumn>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<LCStringColumn>()
                                .unwrap(),
                        )
                    {
                        info!("column-{} data not same", i);
                        return false;
                    }
                }
                DataType::Date => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<DateColumn>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<DateColumn>()
                                .unwrap(),
                        )
                    {
                        info!("column-{} data not same", i);
                        return false;
                    }
                }
                _ => {
                    info!("unexpected type");
                    return false;
                }
            }
        }
        return true;
    }
}

unsafe impl Sync for ColTable {}

pub fn parse_properties(
    record: &StringRecord, header: &[(String, DataType)], selected: &[bool],
) -> GDBResult<Vec<Item>> {
    let mut properties = Vec::new();
    for (index, val) in record.iter().enumerate() {
        if selected[index] {
            match header[index].1 {
                DataType::Int32 => {
                    properties.push(Item::Int32(val.parse::<i32>()?));
                }
                DataType::UInt32 => {
                    properties.push(Item::UInt32(val.parse::<u32>()?));
                }
                DataType::Int64 => {
                    properties.push(Item::Int64(val.parse::<i64>()?));
                }
                DataType::UInt64 => {
                    properties.push(Item::UInt64(val.parse::<u64>()?));
                }
                DataType::String => {
                    properties.push(Item::String(val.to_string()));
                }
                DataType::Date => {
                    properties.push(Item::Date(parse_date(val)?));
                }
                DataType::DateTime => {
                    properties.push(Item::DateTime(parse_datetime(val)));
                }
                DataType::Double => {
                    properties.push(Item::Double(val.parse::<f64>()?));
                }
                DataType::NULL => {
                    error!("Unexpected field type");
                }
                DataType::ID => {}
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }
    Ok(properties)
}
