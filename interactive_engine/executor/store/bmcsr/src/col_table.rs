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
use std::io::{BufReader, BufWriter, Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use csv::StringRecord;

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

    pub fn new_empty(&self) -> Self {
        let mut types = Vec::<(DataType, String)>::new();
        types.resize(self.col_num(), (DataType::NULL, String::new()));
        for col_i in 0..self.col_num() {
            types[col_i].0 = self.columns[col_i].get_type();
        }
        for (name, idx) in self.header.iter() {
            types[*idx].1 = name.clone();
        }
        Self::new(types)
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

    pub fn get_row(&self, row_i: usize) -> Option<Vec<Item>> {
        if row_i < self.row_num {
            let mut row = Vec::new();
            for col in self.columns.iter() {
                row.push(col.get(row_i).unwrap().to_owned());
            }
            Some(row)
        } else {
            None
        }
    }

    pub fn set_table_row(&mut self, self_i: usize, other: &ColTable, other_i: usize) {
        if self.row_num <= self_i {
            self.resize(self_i + 1);
        }
        for col_i in 0..self.col_num() {
            self.columns[col_i].set_column_elem(self_i, &other.columns[col_i], other_i);
        }
    }

    pub fn move_row(&mut self, from: usize, to: usize) {
        for col_i in 0..self.col_num() {
            self.columns[col_i].move_elem(from, to);
        }
    }

    pub fn copy_range(&mut self, self_i: usize, other: &ColTable, other_i: usize, num: usize) {
        if self.row_num < (self_i + num) {
            self.resize(self_i + num);
        }
        for col_i in 0..self.col_num() {
            self.columns[col_i].copy_range(self_i, &other.columns[col_i], other_i, num);
        }
    }

    pub fn resize(&mut self, row_num: usize) {
        for col_i in 0..self.col_num() {
            self.columns[col_i].resize(row_num);
        }
        self.row_num = row_num;
    }

    pub fn serialize_table(&self, path: &String) {
        let f = File::create(path).unwrap();
        let mut writer = BufWriter::new(f);
        writer
            .write_u64::<LittleEndian>(self.row_num as u64)
            .unwrap();
        writer
            .write_u64::<LittleEndian>(self.header.len() as u64)
            .unwrap();
        for pair in self.header.iter() {
            writer
                .write_u64::<LittleEndian>(pair.0.len() as u64)
                .unwrap();
            writer.write_all(pair.0.as_bytes()).unwrap();
            writer
                .write_u64::<LittleEndian>(*pair.1 as u64)
                .unwrap();
        }

        writer
            .write_u64::<LittleEndian>(self.columns.len() as u64)
            .unwrap();
        for col in self.columns.iter() {
            writer
                .write_i32::<LittleEndian>(col.get_type().to_i32())
                .unwrap();
            col.serialize(&mut writer).unwrap();
        }
    }

    pub fn deserialize_table(&mut self, path: &String) {
        let f = File::open(path).unwrap();
        let mut reader = BufReader::new(f);
        self.row_num = reader.read_u64::<LittleEndian>().unwrap() as usize;
        let header_len = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.header.clear();
        for _ in 0..header_len {
            let str_len = reader.read_u64::<LittleEndian>().unwrap() as usize;
            let mut str_bytes = vec![0u8; str_len];
            reader.read_exact(&mut str_bytes).unwrap();
            let s = String::from_utf8(str_bytes).unwrap();
            let ind = reader.read_u64::<LittleEndian>().unwrap() as usize;

            self.header.insert(s, ind);
        }

        let column_len = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.columns.clear();
        for _ in 0..column_len {
            let t = DataType::from_i32(reader.read_i32::<LittleEndian>().unwrap()).unwrap();
            match t {
                DataType::Int32 => {
                    let mut col = Int32Column::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::UInt32 => {
                    let mut col = UInt32Column::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::Int64 => {
                    let mut col = Int64Column::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::UInt64 => {
                    let mut col = UInt64Column::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::Double => {
                    let mut col = DoubleColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::String => {
                    let mut col = StringColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::Date => {
                    let mut col = DateColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::DateTime => {
                    let mut col = DateTimeColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::LCString => {
                    let mut col = LCStringColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::ID => {
                    let mut col = IDColumn::new();
                    col.deserialize(&mut reader).unwrap();
                    self.columns.push(Box::new(col));
                }
                DataType::NULL => {
                    let col = Int32Column::new();
                    self.columns.push(Box::new(col));
                }
            };
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
unsafe impl Send for ColTable {}

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

pub fn parse_properties_by_mappings(
    record: &StringRecord, header: &[(String, DataType)], mappings: &Vec<i32>,
) -> GDBResult<Vec<Item>> {
    let mut properties = vec![Item::Null; mappings.len()];
    for (index, val) in record.iter().enumerate() {
        if mappings[index] >= 0 {
            match header[mappings[index] as usize].1 {
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
