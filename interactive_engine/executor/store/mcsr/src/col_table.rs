use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use csv::StringRecord;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::browser::parse_browser;
use crate::columns::*;
use crate::date::parse_date;
use crate::date_time::parse_datetime;
use crate::error::GDBResult;
use crate::ip_addr::parse_ip_addr;

#[derive(Debug)]
pub struct ColTable {
    columns: Vec<Box<dyn Column>>,
    pub header: HashMap<String, usize>,
    row_num: usize,
}

impl Encode for ColTable {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.row_num as u64)?;
        writer.write_u64(self.header.len() as u64)?;
        for pair in self.header.iter() {
            pair.0.write_to(writer)?;
            writer.write_u64(*pair.1 as u64)?;
        }
        writer.write_u64(self.columns.len() as u64)?;
        for col in self.columns.iter() {
            match col.get_type() {
                DataType::Int32 => {
                    writer.write_u8(0)?;
                    col.as_any()
                        .downcast_ref::<Int32Column>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::UInt32 => {
                    writer.write_u8(1)?;
                    col.as_any()
                        .downcast_ref::<UInt32Column>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::Int64 => {
                    writer.write_u8(2)?;
                    col.as_any()
                        .downcast_ref::<Int64Column>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::UInt64 => {
                    writer.write_u8(3)?;
                    col.as_any()
                        .downcast_ref::<UInt64Column>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::String => {
                    writer.write_u8(4)?;
                    col.as_any()
                        .downcast_ref::<StringColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::LCString => {
                    writer.write_u8(11)?;
                    col.as_any()
                        .downcast_ref::<LCStringColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::Double => {
                    writer.write_u8(5)?;
                    col.as_any()
                        .downcast_ref::<DoubleColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::Date => {
                    writer.write_u8(8)?;
                    col.as_any()
                        .downcast_ref::<DateColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::DateTime => {
                    writer.write_u8(6)?;
                    col.as_any()
                        .downcast_ref::<DateTimeColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::NULL => {
                    error!("Unexpected column type");
                }
                DataType::ID => {
                    writer.write_u8(7)?;
                    col.as_any()
                        .downcast_ref::<IDColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::IpAddr => {
                    writer.write_u8(9)?;
                    col.as_any()
                        .downcast_ref::<IpAddrColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
                DataType::Browser => {
                    writer.write_u8(10)?;
                    col.as_any()
                        .downcast_ref::<BrowserColumn>()
                        .unwrap()
                        .write_to(writer)?;
                }
            };
        }
        Ok(())
    }
}

impl Decode for ColTable {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let row_num = reader.read_u64()? as usize;
        let header_len = reader.read_u64()? as usize;
        let mut header = HashMap::new();
        for _ in 0..header_len {
            let str = String::read_from(reader)?;
            let index = reader.read_u64()? as usize;
            info!("\tcolumn-{}: {}", index, str);
            header.insert(str, index);
        }

        let mut columns: Vec<Box<dyn Column>> = vec![];
        let column_len = reader.read_u64()? as usize;
        for _ in 0..column_len {
            let t = reader.read_u8()?;
            if t == 0 {
                columns.push(Box::new(Int32Column::read_from(reader)?));
            } else if t == 1 {
                columns.push(Box::new(UInt32Column::read_from(reader)?));
            } else if t == 2 {
                columns.push(Box::new(Int64Column::read_from(reader)?));
            } else if t == 3 {
                columns.push(Box::new(UInt64Column::read_from(reader)?));
            } else if t == 4 {
                columns.push(Box::new(StringColumn::read_from(reader)?));
            } else if t == 11 {
                columns.push(Box::new(LCStringColumn::read_from(reader)?));
            } else if t == 5 {
                columns.push(Box::new(DoubleColumn::read_from(reader)?));
            } else if t == 6 {
                columns.push(Box::new(DateTimeColumn::read_from(reader)?));
            } else if t == 7 {
                columns.push(Box::new(IDColumn::read_from(reader)?));
            } else if t == 8 {
                columns.push(Box::new(DateColumn::read_from(reader)?));
            } else if t == 9 {
                columns.push(Box::new(IpAddrColumn::read_from(reader)?));
            } else if t == 10 {
                columns.push(Box::new(BrowserColumn::read_from(reader)?));
            } else {
                println!("Invalid type {}", t);
            }
        }

        Ok(Self { columns, header, row_num })
    }
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
                DataType::IpAddr => {
                    columns.push(Box::new(IpAddrColumn::new()));
                }
                DataType::Browser => {
                    columns.push(Box::new(BrowserColumn::new()));
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
            println!("schema not match when push, row_len = {}, col num = {}", row.len(), col_num);
            return;
        }
        for i in 0..col_num {
            self.columns[i].push(row[i].clone());
        }
        self.row_num += 1;
    }

    pub fn insert(&mut self, index: usize, row: &Vec<Item>) {
        let col_num = self.columns.len();
        if index < self.row_num {
            println!("insert to overwrite a record, index = {}, row_num = {}", index, self.row_num);
        } else if index > self.row_num {
            println!("insert will append nulls, index = {}, row_num = {}", index, self.row_num);
        }
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
            println!("{:?}", self.header);
            println!("col-{} not in table", col_name);
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

    pub fn export<P: AsRef<Path>>(&self, path: P) -> GDBResult<()> {
        crate::io::export(&self, path)?;
        Ok(())
    }

    pub fn import<P: AsRef<Path>>(path: P) -> GDBResult<Self> {
        let table = crate::io::import::<Self, _>(path)?;
        Ok(table)
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
                DataType::DateTime => {
                    f.write_u8(6).unwrap();
                    col.as_any()
                        .downcast_ref::<DateTimeColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::String => {
                    f.write_u8(4).unwrap();
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
                DataType::LCString => {
                    f.write_u8(11).unwrap();
                    col.as_any()
                        .downcast_ref::<LCStringColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::Date => {
                    f.write_u8(8).unwrap();
                    col.as_any()
                        .downcast_ref::<DateColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::IpAddr => {
                    f.write_u8(9).unwrap();
                    col.as_any()
                        .downcast_ref::<IpAddrColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                DataType::Browser => {
                    f.write_u8(10).unwrap();
                    col.as_any()
                        .downcast_ref::<BrowserColumn>()
                        .unwrap()
                        .serialize(&mut f);
                }
                _ => {
                    println!("unexpected type...");
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
            } else if t == 6 {
                let mut col = DateTimeColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 4 {
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
            } else if t == 11 {
                let mut col = LCStringColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 8 {
                let mut col = DateColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 9 {
                let mut col = IpAddrColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else if t == 10 {
                let mut col = BrowserColumn::new();
                col.deserialize(&mut f);
                self.columns.push(Box::new(col));
            } else {
                println!("unexpected type...");
            }
        }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.header != other.header {
            println!("header not same");
            return false;
        }
        if self.columns.len() != other.columns.len() {
            println!("columns num not same");
            return false;
        }
        let col_num = self.columns.len();
        for i in 0..col_num {
            if self.columns[i].get_type() != other.columns[i].get_type() {
                println!("column-{} type not same", i);
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
                        println!("column-{} data not same", i);
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
                        println!("column-{} data not same", i);
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
                        println!("column-{} data not same", i);
                        return false;
                    }
                    let num = lhs.len();
                    for i in 0..num {
                        if lhs[i] != rhs[i] {
                            println!("column-{} data not same", i);
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
                        println!("column-{} data not same", i);
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
                        println!("column-{} data not same", i);
                        return false;
                    }
                }
                DataType::IpAddr => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<IpAddrColumn>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<IpAddrColumn>()
                                .unwrap(),
                        )
                    {
                        println!("column-{} data not same", i);
                        return false;
                    }
                }
                DataType::Browser => {
                    if !self.columns[i]
                        .as_any()
                        .downcast_ref::<BrowserColumn>()
                        .unwrap()
                        .is_same(
                            other.columns[i]
                                .as_any()
                                .downcast_ref::<BrowserColumn>()
                                .unwrap(),
                        )
                    {
                        println!("column-{} data not same", i);
                        return false;
                    }
                }
                _ => {
                    println!("unexpected type");
                    return false;
                }
            }
        }
        return true;
    }
}

impl Serialize for ColTable {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut bytes = Vec::new();
        if self.write_to(&mut bytes).is_ok() {
            bytes.serialize(serializer)
        } else {
            Result::Err(S::Error::custom("Serialize table failed!"))
        }
    }
}

impl<'de> Deserialize<'de> for ColTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        let mut bytes = vec.as_slice();
        Self::read_from(&mut bytes).map_err(|_| D::Error::custom("Deserialize table failed!"))
    }
}

unsafe impl Sync for ColTable {}

pub fn parse_properties_beta(
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
                DataType::IpAddr => {
                    properties.push(Item::IpAddr(parse_ip_addr(val)?));
                }
                DataType::Browser => {
                    properties.push(Item::Browser(parse_browser(val)?));
                }
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }
    Ok(properties)
}

pub fn parse_properties<'a, Iter: Clone + Iterator<Item = &'a str>>(
    mut record_iter: Iter, _header: Option<&[(String, DataType)]>,
) -> GDBResult<Vec<Item>> {
    let mut properties = Vec::new();
    if _header.is_none() {
        return Ok(properties);
    }
    let header = _header.unwrap();
    let mut header_iter = header.iter();

    let header_count = header_iter.clone().count();
    let record_count = record_iter.clone().count();
    let mut skip = false;
    if record_count > header_count {
        skip = true;
    }

    while let Some(val) = record_iter.next() {
        if skip {
            skip = false;
            continue;
        }
        if let Some((_, ty)) = header_iter.next() {
            match ty {
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
                DataType::IpAddr => {
                    properties.push(Item::IpAddr(parse_ip_addr(val)?));
                }
                DataType::Browser => {
                    properties.push(Item::Browser(parse_browser(val)?));
                }
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }

    Ok(properties)
}
