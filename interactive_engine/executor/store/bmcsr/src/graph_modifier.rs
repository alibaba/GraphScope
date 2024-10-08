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
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use csv::ReaderBuilder;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use rayon::prelude::*;
use rust_htslib::bgzf::Reader as GzReader;

use crate::bmscsr::BatchMutableSingleCsr;
use crate::col_table::{parse_properties, parse_properties_by_mappings, ColTable};
use crate::columns::*;
use crate::columns::*;
use crate::csr::CsrTrait;
use crate::date::Date;
use crate::date_time::DateTime;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::graph_loader::{get_files_list, get_files_list_beta};
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, LabelId};

#[derive(Clone, Copy)]
pub enum WriteType {
    Insert,
    Delete,
    Set,
}

#[derive(Clone)]
pub struct ColumnInfo {
    index: i32,
    name: String,
    data_type: DataType,
}

impl ColumnInfo {
    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

#[derive(Clone)]
pub struct ColumnMappings {
    column: ColumnInfo,
    property_name: String,
}

impl ColumnMappings {
    pub fn new(index: i32, name: String, data_type: DataType, property_name: String) -> Self {
        ColumnMappings { column: ColumnInfo { index, name, data_type }, property_name }
    }
}

impl Encode for ColumnMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32(self.column.index);
        self.column.name.write_to(writer)?;
        self.column.data_type.write_to(writer)?;
        self.property_name.write_to(writer)?;
        Ok(())
    }
}

impl Decode for ColumnMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let index = reader.read_i32()?;
        let name = String::read_from(reader)?;
        let data_type = DataType::read_from(reader)?;
        let property_name = String::read_from(reader)?;
        Ok(ColumnMappings { column: ColumnInfo { index, name, data_type }, property_name })
    }
}

impl ColumnMappings {
    pub fn column(&self) -> &ColumnInfo {
        &self.column
    }

    pub fn property_name(&self) -> &String {
        &self.property_name
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum DataSource {
    File,
    Memory,
}

#[derive(Clone)]
pub struct FileInput {
    pub delimiter: String,
    pub header_row: bool,
    pub quoting: bool,
    pub quote_char: String,
    pub double_quote: bool,
    pub escape_char: String,
    pub block_size: String,
    pub location: String,
}

impl Encode for FileInput {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.delimiter.write_to(writer)?;
        self.header_row.write_to(writer)?;
        self.quoting.write_to(writer)?;
        self.quote_char.write_to(writer)?;
        self.double_quote.write_to(writer)?;
        self.escape_char.write_to(writer)?;
        self.block_size.write_to(writer)?;
        self.location.write_to(writer)?;
        Ok(())
    }
}

impl Decode for FileInput {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let delimiter = String::read_from(reader)?;
        let header_row = bool::read_from(reader)?;
        let quoting = bool::read_from(reader)?;
        let quote_char = String::read_from(reader)?;
        let double_quote = bool::read_from(reader)?;
        let escape_char = String::read_from(reader)?;
        let block_size = String::read_from(reader)?;
        let location = String::read_from(reader)?;
        Ok(FileInput {
            delimiter,
            header_row,
            quoting,
            quote_char,
            double_quote,
            escape_char,
            block_size,
            location,
        })
    }
}

impl FileInput {
    pub fn new(delimiter: String, header_row: bool, location: String) -> Self {
        FileInput {
            delimiter,
            header_row,
            quoting: true,
            quote_char: "'".to_string(),
            double_quote: true,
            escape_char: "".to_string(),
            block_size: "4Mb".to_string(),
            location,
        }
    }
}

fn write_column<W: WriteExt>(column: &Box<dyn Column>, writer: &mut W) -> std::io::Result<()> {
    if let Some(int32_column) = column.as_any().downcast_ref::<Int32Column>() {
        writer.write_u8(0);
        writer.write_u64(column.len() as u64);
        for i in int32_column.data.iter() {
            writer.write_i32(*i);
        }
    }
    if let Some(uint32_column) = column.as_any().downcast_ref::<UInt32Column>() {
        writer.write_u8(1);
        writer.write_u64(column.len() as u64);
        for i in uint32_column.data.iter() {
            writer.write_u32(*i);
        }
    }
    if let Some(int64_column) = column.as_any().downcast_ref::<Int64Column>() {
        writer.write_u8(2);
        writer.write_u64(column.len() as u64);
        for i in int64_column.data.iter() {
            writer.write_i64(*i);
        }
    }
    if let Some(uint64_column) = column.as_any().downcast_ref::<UInt64Column>() {
        writer.write_u8(3);
        writer.write_u64(column.len() as u64);
        for i in uint64_column.data.iter() {
            writer.write_u64(*i);
        }
    }
    if let Some(id_column) = column.as_any().downcast_ref::<IDColumn>() {
        writer.write_u8(4);
        writer.write_u64(column.len() as u64);
        for i in id_column.data.iter() {
            writer.write_u64(*i as u64);
        }
    }
    if let Some(double_column) = column.as_any().downcast_ref::<DoubleColumn>() {
        writer.write_u8(5);
        writer.write_u64(column.len() as u64);
        for i in double_column.data.iter() {
            writer.write_f64(*i);
        }
    }
    if let Some(string_column) = column.as_any().downcast_ref::<StringColumn>() {
        writer.write_u8(6);
        writer.write_u64(column.len() as u64);
        for i in string_column.data.iter() {
            i.write_to(writer);
        }
    }
    if let Some(lc_string_column) = column.as_any().downcast_ref::<LCStringColumn>() {
        writer.write_u8(7);
        writer.write_u64(lc_string_column.list.len() as u64);
        for i in lc_string_column.list.iter() {
            i.write_to(writer);
        }
        writer.write_u64(lc_string_column.data.len() as u64);
        for i in lc_string_column.data.iter() {
            writer.write_u16(*i);
        }
    }
    if let Some(date_column) = column.as_any().downcast_ref::<DateColumn>() {
        writer.write_u8(8);
        writer.write_u64(column.len() as u64);
        for i in date_column.data.iter() {
            writer.write_i32(i.to_i32());
        }
    }
    if let Some(datetime_column) = column.as_any().downcast_ref::<DateTimeColumn>() {
        writer.write_u8(9);
        writer.write_u64(column.len() as u64);
        for i in datetime_column.data.iter() {
            writer.write_i64(i.to_i64());
        }
    }
    Ok(())
}

fn read_column<R: ReadExt>(reader: &mut R) -> std::io::Result<Box<dyn Column>> {
    let data: Box<dyn Column> = match reader.read_u8()? {
        0 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<i32>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_i32()?);
            }
            Box::new(Int32Column { data })
        }
        1 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<u32>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u32()?);
            }
            Box::new(UInt32Column { data })
        }
        2 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<i64>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_i64()?);
            }
            Box::new(Int64Column { data })
        }
        3 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<u64>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u64()?);
            }
            Box::new(UInt64Column { data })
        }
        4 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<usize>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u64()? as usize);
            }
            Box::new(IDColumn { data })
        }
        5 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<f64>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_f64()?);
            }
            Box::new(DoubleColumn { data })
        }
        6 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<String>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(String::read_from(reader)?);
            }
            Box::new(StringColumn { data })
        }
        7 => {
            let mut list = Vec::<String>::new();
            let mut table = HashMap::<String, u16>::new();
            let list_len = reader.read_u64()? as usize;
            for i in 0..list_len {
                let name = String::read_from(reader)?;
                list.push(name.clone());
                table.insert(name, i as u16);
            }
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<u16>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(reader.read_u16()?);
            }
            Box::new(LCStringColumn { data, table, list })
        }
        8 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<Date>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(Date::from_i32(reader.read_i32()?));
            }
            Box::new(DateColumn { data })
        }
        9 => {
            let data_len = reader.read_u64()? as usize;
            let mut data = ColumnContainer::<DateTime>::with_capacity(data_len);
            for i in 0..data_len {
                data.push(DateTime::new(reader.read_i64()?));
            }
            Box::new(DateTimeColumn { data })
        }
        _ => panic!("Unknown column type"),
    };
    Ok(data)
}

fn clone_column(input: &Box<dyn Column>) -> Box<dyn Column> {
    if let Some(int32_column) = input.as_any().downcast_ref::<Int32Column>() {
        Box::new(Int32Column::clone_from(int32_column))
    } else if let Some(uint32_column) = input.as_any().downcast_ref::<UInt32Column>() {
        Box::new(UInt32Column::clone_from(uint32_column))
    } else if let Some(int64_column) = input.as_any().downcast_ref::<Int64Column>() {
        Box::new(Int64Column::clone_from(int64_column))
    } else if let Some(uint64_column) = input.as_any().downcast_ref::<UInt64Column>() {
        Box::new(UInt64Column::clone_from(uint64_column))
    } else if let Some(id_column) = input.as_any().downcast_ref::<IDColumn>() {
        Box::new(IDColumn::clone_from(id_column))
    } else if let Some(doule_column) = input.as_any().downcast_ref::<DoubleColumn>() {
        Box::new(DoubleColumn::clone_from(doule_column))
    } else if let Some(string_column) = input.as_any().downcast_ref::<StringColumn>() {
        Box::new(StringColumn::clone_from(string_column))
    } else if let Some(lc_string_column) = input.as_any().downcast_ref::<LCStringColumn>() {
        Box::new(LCStringColumn::clone_from(lc_string_column))
    } else if let Some(date_column) = input.as_any().downcast_ref::<DateColumn>() {
        Box::new(DateColumn::clone_from(date_column))
    } else if let Some(datetime_column) = input.as_any().downcast_ref::<DateTimeColumn>() {
        Box::new(DateTimeColumn::clone_from(datetime_column))
    } else {
        panic!("Unknown column type")
    }
}

pub struct ColumnMetadata {
    data: Box<dyn Column>,
    column_name: String,
    data_type: DataType,
}

impl Encode for ColumnMetadata {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        write_column(&self.data, writer)?;
        self.column_name.write_to(writer)?;
        self.data_type.write_to(writer)?;
        Ok(())
    }
}

impl Decode for ColumnMetadata {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data: Box<dyn Column> = read_column(reader)?;
        let column_name = String::read_from(reader)?;
        let data_type = DataType::read_from(reader)?;
        Ok(ColumnMetadata { data, column_name, data_type })
    }
}

impl Clone for ColumnMetadata {
    fn clone(&self) -> Self {
        let data = clone_column(&self.data);
        ColumnMetadata { data, column_name: self.column_name.clone(), data_type: self.data_type.clone() }
    }
}

impl ColumnMetadata {
    pub fn new(data: Box<dyn Column>, column_name: String, data_type: DataType) -> Self {
        ColumnMetadata { data, column_name, data_type }
    }

    pub fn data(&self) -> &Box<dyn Column> {
        &self.data
    }

    pub fn take_data(&mut self) -> Box<dyn Column> {
        std::mem::replace(&mut self.data, Box::new(Int32Column::new()))
    }

    pub fn column_name(&self) -> String {
        self.column_name.clone()
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

#[derive(Clone)]
pub struct DataFrame {
    columns: Vec<ColumnMetadata>,
}

impl Encode for DataFrame {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.columns.write_to(writer)?;
        Ok(())
    }
}

impl Decode for DataFrame {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let columns = Vec::<ColumnMetadata>::read_from(reader)?;
        Ok(DataFrame { columns })
    }
}

impl DataFrame {
    pub fn new_vertices_ids(data: Vec<u64>) -> Self {
        let columns =
            vec![ColumnMetadata::new(Box::new(UInt64Column { data }), "id".to_string(), DataType::ID)];
        DataFrame { columns }
    }

    pub fn new_edges_ids(data: Vec<usize>) -> Self {
        let columns =
            vec![ColumnMetadata::new(Box::new(IDColumn { data }), "id".to_string(), DataType::ID)];
        DataFrame { columns }
    }

    pub fn add_column(&mut self, column: ColumnMetadata) {
        self.columns.push(column);
    }

    pub fn columns(&self) -> &Vec<ColumnMetadata> {
        &self.columns
    }

    pub fn take_columns(&mut self) -> Vec<ColumnMetadata> {
        std::mem::replace(&mut self.columns, Vec::new())
    }
}

#[derive(Clone)]
pub struct Input {
    data_source: DataSource,
    file_input: Option<FileInput>,
    memory_data: Option<DataFrame>,
}

impl Encode for Input {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self.data_source {
            DataSource::File => writer.write_u8(0),
            DataSource::Memory => writer.write_u8(1),
        };
        self.file_input.write_to(writer)?;
        self.memory_data.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Input {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let data_source = match reader.read_u8()? {
            0 => DataSource::File,
            1 => DataSource::Memory,
            _ => panic!("Unknown DataSource type"),
        };
        let file_input = Option::<FileInput>::read_from(reader)?;
        let memory_data = Option::<DataFrame>::read_from(reader)?;
        Ok(Input { data_source, file_input, memory_data })
    }
}

impl Input {
    pub fn data_source(&self) -> DataSource {
        self.data_source
    }

    pub fn file_input(&self) -> Option<&FileInput> {
        self.file_input.as_ref()
    }

    pub fn memory_data(&self) -> Option<&DataFrame> {
        self.memory_data.as_ref()
    }

    pub fn take_memory_data(&mut self) -> Option<DataFrame> {
        self.memory_data.take()
    }

    pub fn file(file: FileInput) -> Self {
        Input { data_source: DataSource::File, file_input: Some(file), memory_data: None }
    }

    pub fn memory(memory_data: DataFrame) -> Self {
        Input { data_source: DataSource::Memory, file_input: None, memory_data: Some(memory_data) }
    }
}

#[derive(Clone)]
pub struct VertexMappings {
    label_id: LabelId,
    inputs: Vec<Input>,
    column_mappings: Vec<ColumnMappings>,
}

impl Encode for VertexMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.label_id);
        self.inputs.write_to(writer)?;
        self.column_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for VertexMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let label_id = reader.read_u8()?;
        let inputs = Vec::<Input>::read_from(reader)?;
        let column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        Ok(VertexMappings { label_id, inputs, column_mappings })
    }
}

impl VertexMappings {
    pub fn new(label_id: LabelId, inputs: Vec<Input>, column_mappings: Vec<ColumnMappings>) -> Self {
        VertexMappings { label_id, inputs, column_mappings }
    }

    pub fn vertex_label(&self) -> LabelId {
        self.label_id
    }

    pub fn inputs(&self) -> &Vec<Input> {
        &self.inputs
    }

    pub fn take_inputs(&mut self) -> Vec<Input> {
        std::mem::replace(&mut self.inputs, Vec::new())
    }

    pub fn column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.column_mappings
    }
}

#[derive(Clone)]
pub struct EdgeMappings {
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,
    inputs: Vec<Input>,
    src_column_mappings: Vec<ColumnMappings>,
    dst_column_mappings: Vec<ColumnMappings>,
    column_mappings: Vec<ColumnMappings>,
}

impl Encode for EdgeMappings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.src_label);
        writer.write_u8(self.edge_label);
        writer.write_u8(self.dst_label);
        self.inputs.write_to(writer)?;
        self.src_column_mappings.write_to(writer)?;
        self.dst_column_mappings.write_to(writer)?;
        self.column_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for EdgeMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let src_label = reader.read_u8()?;
        let edge_label = reader.read_u8()?;
        let dst_label = reader.read_u8()?;
        let inputs = Vec::<Input>::read_from(reader)?;
        let src_column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        let dst_column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        let column_mappings = Vec::<ColumnMappings>::read_from(reader)?;
        Ok(EdgeMappings {
            src_label,
            edge_label,
            dst_label,
            inputs,
            src_column_mappings,
            dst_column_mappings,
            column_mappings,
        })
    }
}

impl EdgeMappings {
    pub fn new(
        src_label: LabelId, edge_label: LabelId, dst_label: LabelId, inputs: Vec<Input>,
        src_column_mappings: Vec<ColumnMappings>, dst_column_mappings: Vec<ColumnMappings>,
        column_mappings: Vec<ColumnMappings>,
    ) -> Self {
        EdgeMappings {
            src_label,
            edge_label,
            dst_label,
            inputs,
            src_column_mappings,
            dst_column_mappings,
            column_mappings,
        }
    }

    pub fn src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn edge_label(&self) -> LabelId {
        self.edge_label
    }

    pub fn dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn inputs(&self) -> &Vec<Input> {
        &self.inputs
    }

    pub fn take_inputs(&mut self) -> Vec<Input> {
        std::mem::replace(&mut self.inputs, Vec::new())
    }

    pub fn src_column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.src_column_mappings
    }

    pub fn dst_column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.dst_column_mappings
    }

    pub fn column_mappings(&self) -> &Vec<ColumnMappings> {
        &self.column_mappings
    }
}

#[derive(Clone)]
pub struct WriteOperation {
    write_type: WriteType,
    vertex_mappings: Option<VertexMappings>,
    edge_mappings: Option<EdgeMappings>,
}

impl Debug for WriteOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "This is a write operation")
    }
}

impl Encode for WriteOperation {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self.write_type {
            WriteType::Insert => writer.write_u8(0),
            WriteType::Delete => writer.write_u8(1),
            WriteType::Set => writer.write_u8(2),
        };
        self.vertex_mappings.write_to(writer)?;
        self.edge_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for WriteOperation {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let write_type = match reader.read_u8()? {
            0 => WriteType::Insert,
            1 => WriteType::Delete,
            2 => WriteType::Set,
            _ => panic!("Unknown write type"),
        };
        let vertex_mappings = Option::<VertexMappings>::read_from(reader)?;
        let edge_mappings = Option::<EdgeMappings>::read_from(reader)?;
        Ok(WriteOperation { write_type, vertex_mappings, edge_mappings })
    }
}

unsafe impl Send for WriteOperation {}

unsafe impl Sync for WriteOperation {}

impl WriteOperation {
    pub fn insert_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Insert,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn insert_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Insert,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn delete_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Delete,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn delete_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Delete,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn set_vertices(vertex_mappings: VertexMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Set,
            vertex_mappings: Some(vertex_mappings),
            edge_mappings: None,
        }
    }

    pub fn set_edges(edge_mappings: EdgeMappings) -> Self {
        WriteOperation {
            write_type: WriteType::Set,
            vertex_mappings: None,
            edge_mappings: Some(edge_mappings),
        }
    }

    pub fn write_type(&self) -> WriteType {
        self.write_type
    }

    pub fn has_vertex_mappings(&self) -> bool {
        self.vertex_mappings.is_some()
    }

    pub fn vertex_mappings(&self) -> Option<&VertexMappings> {
        self.vertex_mappings.as_ref()
    }

    pub fn take_vertex_mappings(&mut self) -> Option<VertexMappings> {
        self.vertex_mappings.take()
    }

    pub fn has_edge_mappings(&self) -> bool {
        self.edge_mappings.is_some()
    }

    pub fn edge_mappings(&self) -> Option<&EdgeMappings> {
        self.edge_mappings.as_ref()
    }

    pub fn take_edge_mappings(&mut self) -> Option<EdgeMappings> {
        self.edge_mappings.take()
    }
}

pub struct AliasData {
    pub alias_index: i32,
    pub column_data: Box<dyn Column>,
}

impl Debug for AliasData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Alias index: {}, data: {:?}", self.alias_index, self.column_data)
    }
}

impl Encode for AliasData {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32(self.alias_index)?;
        write_column(&self.column_data, writer)?;
        Ok(())
    }
}

impl Decode for AliasData {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let alias_index = reader.read_i32()?;
        let column_data = read_column(reader)?;
        Ok(AliasData { alias_index, column_data })
    }
}

impl Clone for AliasData {
    fn clone(&self) -> Self {
        let column_data = clone_column(&self.column_data);
        AliasData { alias_index: self.alias_index, column_data }
    }
}

unsafe impl Send for AliasData {}

unsafe impl Sync for AliasData {}

pub fn apply_write_operations(
    graph: &mut GraphDB<usize, usize>, mut write_operations: Vec<WriteOperation>, parallel: u32,
) {
    let mut merged_delete_vertices_data: HashMap<LabelId, Vec<u64>> = HashMap::new();
    for mut write_op in write_operations.drain(..) {
        match write_op.write_type() {
            WriteType::Insert => {
                if let Some(mut vertex_mappings) = write_op.take_vertex_mappings() {
                    let vertex_label = vertex_mappings.vertex_label();
                    let inputs = vertex_mappings.inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    for input in inputs.iter() {
                        insert_vertices(graph, vertex_label, input, column_mappings, parallel);
                    }
                }
                if let Some(edge_mappings) = write_op.take_edge_mappings() {
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let inputs = edge_mappings.inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    for input in inputs.iter() {
                        insert_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            parallel,
                        );
                    }
                }
            }
            WriteType::Delete => {
                if let Some(mut vertex_mappings) = write_op.take_vertex_mappings() {
                    let vertex_label = vertex_mappings.vertex_label();
                    let inputs = vertex_mappings.take_inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    for mut input in inputs.into_iter() {
                        match input.data_source() {
                            DataSource::Memory => {
                                let mut id_col = -1;
                                for column_mapping in column_mappings {
                                    let column = column_mapping.column();
                                    let column_index = column.index();
                                    let property_name = column_mapping.property_name();
                                    if property_name == "id" {
                                        id_col = column_index;
                                        break;
                                    }
                                }
                                if input.data_source() == DataSource::Memory {
                                    let mut memory_data = input.take_memory_data().unwrap();
                                    let mut data = memory_data.take_columns();
                                    let mut vertex_id_column = data
                                        .get_mut(id_col as usize)
                                        .expect("Failed to get id column");
                                    let mut data = vertex_id_column.take_data();
                                    if let Some(uint64_column) =
                                        data.as_any().downcast_ref::<UInt64Column>()
                                    {
                                        if let Some(mut combined_data) =
                                            merged_delete_vertices_data.get_mut(&vertex_label)
                                        {
                                            combined_data.append(&mut uint64_column.data.clone())
                                        } else {
                                            merged_delete_vertices_data
                                                .insert(vertex_label, uint64_column.data.clone());
                                        }
                                    } else {
                                        panic!("Unknown data type");
                                    }
                                }
                                continue;
                            }
                            _ => {}
                        }
                        delete_vertices(graph, vertex_label, &input, column_mappings, parallel);
                    }
                }
                if let Some(edge_mappings) = write_op.take_edge_mappings() {
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let inputs = edge_mappings.inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    for input in inputs.iter() {
                        delete_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            parallel,
                        );
                    }
                }
            }
            WriteType::Set => {
                if let Some(mut vertex_mappings) = write_op.take_vertex_mappings() {
                    let vertex_label = vertex_mappings.vertex_label();
                    let mut inputs = vertex_mappings.take_inputs();
                    let column_mappings = vertex_mappings.column_mappings();
                    for mut input in inputs.drain(..) {
                        set_vertices(graph, vertex_label, input, column_mappings, parallel);
                    }
                }
                if let Some(mut edge_mappings) = write_op.take_edge_mappings() {
                    let src_label = edge_mappings.src_label();
                    let edge_label = edge_mappings.edge_label();
                    let dst_label = edge_mappings.dst_label();
                    let mut inputs = edge_mappings.take_inputs();
                    let src_column_mappings = edge_mappings.src_column_mappings();
                    let dst_column_mappings = edge_mappings.dst_column_mappings();
                    let column_mappings = edge_mappings.column_mappings();
                    for mut input in inputs.drain(..) {
                        set_edges(
                            graph,
                            src_label,
                            edge_label,
                            dst_label,
                            input,
                            src_column_mappings,
                            dst_column_mappings,
                            column_mappings,
                            parallel,
                        );
                    }
                }
            }
        };
    }
    for (vertex_label, vertex_ids) in merged_delete_vertices_data.into_iter() {
        let column_mappings =
            vec![ColumnMappings::new(0, "id".to_string(), DataType::ID, "id".to_string())];
        let input = Input::memory(DataFrame::new_vertices_ids(vertex_ids));
        delete_vertices(graph, vertex_label, &input, &column_mappings, parallel);
    }
}

fn insert_vertices<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, input: &Input, column_mappings: &Vec<ColumnMappings>,
    parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(vertex_header) = graph
                    .graph_schema
                    .get_vertex_header(vertex_label)
                {
                    for (i, (property_name, data_type)) in vertex_header.iter().enumerate() {
                        if let Some((column_index, column_data_type)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("vertex label {} not found", vertex_label)
                }
                modifier
                    .apply_vertices_insert_with_filename(graph, vertex_label, &filenames, id_col, &mappings)
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn insert_edges<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input: &Input,
    src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(edge_header) = graph
                    .graph_schema
                    .get_edge_header(src_label, edge_label, dst_label)
                {
                    for (i, (property_name, _)) in edge_header.iter().enumerate() {
                        if let Some((column_index, _)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("edge label {}_{}_{} not found", src_label, edge_label, dst_label)
                }
                modifier
                    .apply_edges_insert_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                        &mappings,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn delete_vertices(
    graph: &mut GraphDB<usize, usize>, vertex_label: LabelId, input: &Input,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                modifier
                    .apply_vertices_delete_with_filename(graph, vertex_label, &filenames, id_col)
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                let data = memory_data.columns();
                let vertex_id_column = data
                    .get(id_col as usize)
                    .expect("Failed to get id column");
                if let Some(uint64_column) = vertex_id_column
                    .data()
                    .as_any()
                    .downcast_ref::<UInt64Column>()
                {
                    let data = uint64_column
                        .data
                        .iter()
                        .map(|&x| x as usize)
                        .collect();
                    delete_vertices_by_ids(graph, vertex_label, &data, parallel);
                }
            }
        }
    }
}

pub fn delete_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    input: &Input, src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);

                modifier
                    .apply_edges_delete_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn delete_vertices_by_ids<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, global_ids: &Vec<G>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = HashSet::new();
    for v in global_ids.iter() {
        if v.index() as u64 == u64::MAX {
            continue;
        }
        if let Some(internal_id) = graph.vertex_map.get_internal_id(*v) {
            lids.insert(internal_id.1);
        }
    }
    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for e_label_i in 0..edge_label_num {
        for src_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(src_label_i as LabelId, e_label_i as LabelId, vertex_label as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                src_label_i as LabelId,
                vertex_label as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut ie_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(ie_list) = ie_csr.get_edges(*v) {
                    for e in ie_list {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
            ie_csr.delete_vertices(&lids);
            if let Some(table) = oe_prop.as_mut() {
                oe_csr.parallel_delete_edges_with_props(&ie_to_delete, false, table, parallel);
            } else {
                oe_csr.parallel_delete_edges(&ie_to_delete, false, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
        for dst_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(vertex_label as LabelId, e_label_i as LabelId, dst_label_i as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                vertex_label as LabelId,
                dst_label_i as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut oe_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(oe_list) = oe_csr.get_edges(*v) {
                    for e in oe_list {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
            oe_csr.delete_vertices(&lids);
            if let Some(table) = ie_prop.as_mut() {
                ie_csr.parallel_delete_edges_with_props(&oe_to_delete, true, table, parallel);
            } else {
                ie_csr.parallel_delete_edges(&oe_to_delete, true, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    // delete vertices
    for v in lids.iter() {
        graph.vertex_map.remove_vertex(vertex_label, v);
    }
}

pub fn set_vertices(
    graph: &mut GraphDB<usize, usize>, vertex_label: LabelId, mut input: Input,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                let id_column = column_data
                    .get_mut(id_col as usize)
                    .expect("Failed to find id column");
                let mut data = id_column.take_data();
                let global_ids = {
                    if let Some(id_column) = data.as_any().downcast_ref::<IDColumn>() {
                        id_column.data.clone()
                    } else if let Some(uint64_column) = data.as_any().downcast_ref::<UInt64Column>() {
                        let mut lid = vec![];
                        for i in uint64_column.data.iter() {
                            lid.push(graph.get_internal_id(*i as usize));
                        }
                        lid
                    } else {
                        panic!("DataType of id col is not VertexId")
                    }
                };
                for (k, v) in column_map.iter() {
                    if k == "id" {
                        continue;
                    }
                    let column_index = v.0;
                    let column_data_type = v.1;
                    graph.init_vertex_index_prop(k.clone(), vertex_label, column_data_type);
                    let column = column_data
                        .get_mut(column_index as usize)
                        .expect("Failed to find column");
                    graph.set_vertex_index_prop(k.clone(), vertex_label, &global_ids, column.take_data());
                }
            }
        }
    }
}

pub fn set_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    mut input: Input, src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                if !src_vertex_mappings.is_empty() {
                    let offset_col_id = src_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let mut data = offset_column.take_data();
                    let offsets = {
                        if let Some(id_column) = data.as_any().downcast_ref::<IDColumn>() {
                            id_column.data.clone()
                        } else {
                            panic!("DataType of id col is not VertexId")
                        }
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        graph.init_edge_index_prop(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            column_data_type,
                        );
                        let mut column = column_data
                            .get_mut(column_index as usize)
                            .expect("Failed to find column");
                        graph.set_edge_index_prop(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            None,
                            None,
                            Some(&offsets),
                            Some(column.take_data()),
                        );
                    }
                }
                if !dst_vertex_mappings.is_empty() {
                    let offset_col_id = dst_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let mut data = offset_column.take_data();
                    let offsets = {
                        if let Some(id_column) = data.as_any().downcast_ref::<IDColumn>() {
                            id_column.data.clone()
                        } else {
                            panic!("DataType of id col is not VertexId")
                        }
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        graph.init_edge_index_prop(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            column_data_type,
                        );
                        let mut column = column_data
                            .get_mut(column_index as usize)
                            .expect("Failed to find column");
                        graph.set_edge_index_prop(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            Some(&offsets),
                            Some(column.take_data()),
                            None,
                            None,
                        );
                    }
                }
            }
        }
    }
}

fn process_csv_rows<F>(path: &PathBuf, mut process_row: F, skip_header: bool, delim: u8)
where
    F: FnMut(&csv::StringRecord),
{
    if let Some(path_str) = path.clone().to_str() {
        if path_str.ends_with(".csv.gz") {
            if let Ok(gz_reader) = GzReader::from_path(&path) {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(gz_reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        } else if path_str.ends_with(".csv") {
            if let Ok(file) = File::open(&path) {
                let reader = BufReader::new(file);
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        }
    }
}

pub struct DeleteGenerator<G: FromStr + Send + Sync + IndexType + std::fmt::Display = DefaultId> {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,

    persons: Vec<(String, G)>,
    comments: Vec<(String, G)>,
    posts: Vec<(String, G)>,
    forums: Vec<(String, G)>,

    person_set: HashSet<G>,
    comment_set: HashSet<G>,
    post_set: HashSet<G>,
    forum_set: HashSet<G>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq + std::fmt::Display> DeleteGenerator<G> {
    pub fn new(input_dir: &PathBuf) -> DeleteGenerator<G> {
        Self {
            input_dir: input_dir.clone(),
            delim: b'|',
            skip_header: false,

            persons: vec![],
            comments: vec![],
            posts: vec![],
            forums: vec![],

            person_set: HashSet::new(),
            comment_set: HashSet::new(),
            post_set: HashSet::new(),
            forum_set: HashSet::new(),
        }
    }

    fn load_vertices(&self, input_prefix: PathBuf, label: LabelId) -> Vec<(String, G)> {
        let mut ret = vec![];

        let suffixes = vec!["*.csv.gz".to_string(), "*.csv".to_string()];
        let files = get_files_list(&input_prefix, &suffixes);
        if files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &input_prefix,
                &suffixes,
                files.err().unwrap()
            );
            return ret;
        }
        let files = files.unwrap();
        if files.is_empty() {
            return ret;
        }
        let parser = LDBCVertexParser::<G>::new(label, 1);
        for file in files {
            process_csv_rows(
                &file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    ret.push((
                        record
                            .get(0)
                            .unwrap()
                            .parse::<String>()
                            .unwrap(),
                        vertex_meta.global_id,
                    ));
                },
                self.skip_header,
                self.delim,
            );
        }

        ret
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn iterate_persons<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();

        let hasCreator_label = graph
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let hasModerator_label = graph
            .graph_schema
            .get_edge_label_id("HASMODERATOR")
            .unwrap();

        let comment_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, comment_label, Direction::Incoming);
        let post_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, post_label, Direction::Incoming);
        let forum_hasModerator_person =
            graph.get_sub_graph(person_label, hasModerator_label, forum_label, Direction::Incoming);

        let forum_title_column = graph.vertex_prop_table[forum_label as usize]
            .get_column_by_name("title")
            .as_any()
            .downcast_ref::<StringColumn>()
            .unwrap();

        for (dt, id) in self.persons.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != person_label {
                    warn!("Vertex {} is not a person", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }
                for e in comment_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }

                for e in post_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }

                for e in forum_hasModerator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let title = forum_title_column.get(e.index()).unwrap();
                    let title_string = title.to_string();
                    if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                        let oid = graph
                            .vertex_map
                            .get_global_id(forum_label, *e)
                            .unwrap();
                        self.forums.push((dt.clone(), oid));
                    }
                }
            } else {
                warn!("Vertex Person - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_forums<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();

        let containerOf_label = graph
            .graph_schema
            .get_edge_label_id("CONTAINEROF")
            .unwrap();

        let forum_containerOf_post =
            graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        for (dt, id) in self.forums.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != forum_label {
                    warn!("Vertex {} is not a forum", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in forum_containerOf_post
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Forum - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_posts<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_post =
            graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Incoming);
        for (dt, id) in self.posts.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != post_label {
                    warn!("Vertex {} is not a post", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in comment_replyOf_post.get_adj_list(lid).unwrap() {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Post - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_comments<I>(&mut self, graph: &GraphDB<G, I>)
    where
        I: Send + Sync + IndexType,
    {
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_comment =
            graph.get_sub_graph(comment_label, replyOf_label, comment_label, Direction::Incoming);
        let mut index = 0;
        while index < self.comments.len() {
            let (dt, id) = self.comments[index].clone();
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(id) {
                if got_label != comment_label {
                    warn!("Vertex {} is not a comment", LDBCVertexParser::<G>::get_original_id(id));
                    index += 1;
                    continue;
                }

                for e in comment_replyOf_comment
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
                index += 1;
            } else {
                warn!("Vertex Comment - {} does not exist", LDBCVertexParser::<G>::get_original_id(id));
                index += 1;
                continue;
            }
        }
    }

    pub fn generate<I>(&mut self, graph: &GraphDB<G, I>, batch_id: &str)
    where
        I: Send + Sync + IndexType,
    {
        let output_dir = self
            .input_dir
            .join("extra_deletes")
            .join("dynamic");
        std::fs::create_dir_all(&output_dir).unwrap();

        let prefix = self.input_dir.join("deletes").join("dynamic");

        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        self.persons = self.load_vertices(
            prefix
                .clone()
                .join("Person")
                .join(format!("batch_id={}", batch_id)),
            person_label,
        );
        self.person_set = self.persons.iter().map(|(_, id)| *id).collect();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        self.comments = self.load_vertices(
            prefix
                .clone()
                .join("Comment")
                .join(format!("batch_id={}", batch_id)),
            comment_label,
        );
        self.comment_set = self
            .comments
            .iter()
            .map(|(_, id)| *id)
            .collect();

        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        self.posts = self.load_vertices(
            prefix
                .clone()
                .join("Post")
                .join(format!("batch_id={}", batch_id)),
            post_label,
        );
        self.post_set = self.posts.iter().map(|(_, id)| *id).collect();

        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        self.forums = self.load_vertices(
            prefix
                .clone()
                .join("Forum")
                .join(format!("batch_id={}", batch_id)),
            forum_label,
        );
        self.forum_set = self.forums.iter().map(|(_, id)| *id).collect();

        self.iterate_persons(graph);
        self.iterate_forums(graph);
        self.iterate_posts(graph);
        self.iterate_comments(graph);

        let batch_dir = format!("batch_id={}", batch_id);

        let person_dir_path = output_dir
            .clone()
            .join("Person")
            .join(&batch_dir);
        std::fs::create_dir_all(&person_dir_path).unwrap();
        let mut person_file = File::create(person_dir_path.join("part-0.csv")).unwrap();
        writeln!(person_file, "deletionDate|id").unwrap();
        for (dt, id) in self.persons.iter() {
            if !self.person_set.contains(id) {
                self.person_set.insert(*id);
                writeln!(person_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let forum_dir_path = output_dir
            .clone()
            .join("Forum")
            .join(&batch_dir);
        std::fs::create_dir_all(&forum_dir_path).unwrap();
        let mut forum_file = File::create(forum_dir_path.join("part-0.csv")).unwrap();
        writeln!(forum_file, "deletionDate|id").unwrap();
        for (dt, id) in self.forums.iter() {
            if !self.forum_set.contains(id) {
                self.forum_set.insert(*id);
                writeln!(forum_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let post_dir_path = output_dir.clone().join("Post").join(&batch_dir);
        std::fs::create_dir_all(&post_dir_path).unwrap();
        let mut post_file = File::create(post_dir_path.join("part-0.csv")).unwrap();
        writeln!(post_file, "deletionDate|id").unwrap();
        for (dt, id) in self.posts.iter() {
            if !self.post_set.contains(id) {
                self.post_set.insert(*id);
                writeln!(post_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let comment_dir_path = output_dir
            .clone()
            .join("Comment")
            .join(&batch_dir);
        std::fs::create_dir_all(&comment_dir_path).unwrap();
        let mut comment_file = File::create(comment_dir_path.join("part-0.csv")).unwrap();
        writeln!(comment_file, "deletionDate|id").unwrap();
        for (dt, id) in self.comments.iter() {
            if !self.comment_set.contains(id) {
                self.comment_set.insert(*id);
                writeln!(comment_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }
    }
}

pub struct GraphModifier {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,
    parallel: u32,
}

struct CsrRep<I> {
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,

    ie_csr: Box<dyn CsrTrait<I>>,
    ie_prop: Option<ColTable>,
    oe_csr: Box<dyn CsrTrait<I>>,
    oe_prop: Option<ColTable>,
}

impl GraphModifier {
    pub fn new<D: AsRef<Path>>(input_dir: D) -> GraphModifier {
        Self { input_dir: input_dir.as_ref().to_path_buf(), delim: b'|', skip_header: false, parallel: 0 }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    pub fn parallel(&mut self, parallel: u32) {
        self.parallel = parallel;
    }

    fn take_csr<G, I>(
        &self, graph: &mut GraphDB<G, I>, src_label_i: LabelId, dst_label_i: LabelId, e_label_i: LabelId,
    ) -> CsrRep<I>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let index = graph.edge_label_to_index(src_label_i, dst_label_i, e_label_i, Direction::Outgoing);

        CsrRep {
            src_label: src_label_i,
            edge_label: e_label_i,
            dst_label: dst_label_i,

            ie_csr: std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new())),
            ie_prop: graph.ie_edge_prop_table.remove(&index),
            oe_csr: std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new())),
            oe_prop: graph.oe_edge_prop_table.remove(&index),
        }
    }

    fn take_csrs_with_label<G, I>(&self, graph: &mut GraphDB<G, I>, label: LabelId) -> Vec<CsrRep<I>>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut results = vec![];
        for e_label_i in 0..edge_label_num {
            for label_i in 0..vertex_label_num {
                if !graph
                    .graph_schema
                    .get_edge_header(label as LabelId, e_label_i as LabelId, label_i as LabelId)
                    .is_none()
                {
                    let index = graph.edge_label_to_index(
                        label as LabelId,
                        label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );
                    results.push(CsrRep {
                        src_label: label as LabelId,
                        edge_label: e_label_i as LabelId,
                        dst_label: label_i as LabelId,
                        ie_csr: std::mem::replace(
                            &mut graph.ie[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        ie_prop: graph.ie_edge_prop_table.remove(&index),
                        oe_csr: std::mem::replace(
                            &mut graph.oe[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        oe_prop: graph.oe_edge_prop_table.remove(&index),
                    });
                }
                if !graph
                    .graph_schema
                    .get_edge_header(label_i as LabelId, e_label_i as LabelId, label as LabelId)
                    .is_none()
                {
                    if label_i as LabelId != label {
                        let index = graph.edge_label_to_index(
                            label_i as LabelId,
                            label as LabelId,
                            e_label_i as LabelId,
                            Direction::Outgoing,
                        );
                        results.push(CsrRep {
                            src_label: label_i as LabelId,
                            edge_label: e_label_i as LabelId,
                            dst_label: label as LabelId,
                            ie_csr: std::mem::replace(
                                &mut graph.ie[index],
                                Box::new(BatchMutableSingleCsr::new()),
                            ),
                            ie_prop: graph.ie_edge_prop_table.remove(&index),
                            oe_csr: std::mem::replace(
                                &mut graph.oe[index],
                                Box::new(BatchMutableSingleCsr::new()),
                            ),
                            oe_prop: graph.oe_edge_prop_table.remove(&index),
                        });
                    }
                }
            }
        }
        results
    }
    fn take_csrs<G, I>(&self, graph: &mut GraphDB<G, I>) -> Vec<CsrRep<I>>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut results = vec![];

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    if graph
                        .graph_schema
                        .get_edge_header(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        )
                        .is_none()
                    {
                        continue;
                    }

                    let index = graph.edge_label_to_index(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );

                    results.push(CsrRep {
                        src_label: src_label_i as LabelId,
                        edge_label: e_label_i as LabelId,
                        dst_label: dst_label_i as LabelId,

                        ie_csr: std::mem::replace(
                            &mut graph.ie[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        ie_prop: graph.ie_edge_prop_table.remove(&index),
                        oe_csr: std::mem::replace(
                            &mut graph.oe[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        oe_prop: graph.oe_edge_prop_table.remove(&index),
                    });
                }
            }
        }

        results
    }

    fn set_csr<G, I>(&self, graph: &mut GraphDB<G, I>, reps: CsrRep<I>)
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let index =
            graph.edge_label_to_index(reps.src_label, reps.dst_label, reps.edge_label, Direction::Outgoing);

        graph.ie[index] = reps.ie_csr;
        if let Some(table) = reps.ie_prop {
            graph.ie_edge_prop_table.insert(index, table);
        }
        graph.oe[index] = reps.oe_csr;
        if let Some(table) = reps.oe_prop {
            graph.oe_edge_prop_table.insert(index, table);
        }
    }

    fn set_csrs<G, I>(&self, graph: &mut GraphDB<G, I>, mut reps: Vec<CsrRep<I>>)
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        for result in reps.drain(..) {
            let index = graph.edge_label_to_index(
                result.src_label,
                result.dst_label,
                result.edge_label,
                Direction::Outgoing,
            );

            graph.ie[index] = result.ie_csr;
            if let Some(table) = result.ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = result.oe_csr;
            if let Some(table) = result.oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    fn parallel_delete_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, edge_file_strings: &Vec<String>,
        input_header: &[(String, DataType)], delete_sets: &Vec<HashSet<I>>, p: u32,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return ();
        }

        let src_delete_set = &delete_sets[src_label as usize];
        let dst_delete_set = &delete_sets[dst_label as usize];
        let mut delete_edge_set = Vec::new();

        let mut src_col_id = 0;
        let mut dst_col_id = 1;

        for (index, (n, _)) in input_header.iter().enumerate() {
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }

        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return ();
        }

        let edge_files = edge_files.unwrap();
        for edge_file in edge_files.iter() {
            process_csv_rows(
                edge_file,
                |record| {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Some((got_src_label, src_lid)) = graph
                        .vertex_map
                        .get_internal_id(edge_meta.src_global_id)
                    {
                        if let Some((got_dst_label, dst_lid)) = graph
                            .vertex_map
                            .get_internal_id(edge_meta.dst_global_id)
                        {
                            if got_src_label != src_label || got_dst_label != dst_label {
                                return;
                            }
                            if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                return;
                            }
                            delete_edge_set.push((src_lid, dst_lid));
                        }
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty() {
            return ();
        }

        let mut oe_to_delete = Vec::new();
        let mut ie_to_delete = Vec::new();

        for v in src_delete_set.iter() {
            if let Some(oe_list) = input.oe_csr.get_edges(*v) {
                for e in oe_list {
                    if !dst_delete_set.contains(e) {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
        }
        for v in dst_delete_set.iter() {
            if let Some(ie_list) = input.ie_csr.get_edges(*v) {
                for e in ie_list {
                    if !src_delete_set.contains(e) {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
        }

        input.oe_csr.delete_vertices(src_delete_set);
        if let Some(table) = input.oe_prop.as_mut() {
            input
                .oe_csr
                .parallel_delete_edges_with_props(&delete_edge_set, false, table, p);
            input
                .oe_csr
                .parallel_delete_edges_with_props(&ie_to_delete, false, table, p);
        } else {
            input
                .oe_csr
                .parallel_delete_edges(&delete_edge_set, false, p);
            input
                .oe_csr
                .parallel_delete_edges(&ie_to_delete, false, p);
        }

        input.ie_csr.delete_vertices(dst_delete_set);
        if let Some(table) = input.ie_prop.as_mut() {
            input
                .ie_csr
                .parallel_delete_edges_with_props(&delete_edge_set, true, table, p);
            input
                .ie_csr
                .parallel_delete_edges_with_props(&oe_to_delete, true, table, p);
        } else {
            input
                .ie_csr
                .parallel_delete_edges(&delete_edge_set, true, p);
            input
                .ie_csr
                .parallel_delete_edges(&oe_to_delete, true, p);
        }
    }

    pub fn apply_vertices_delete_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, label: LabelId, filenames: &Vec<String>, id_col: i32,
    ) -> GDBResult<()>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let mut delete_sets = vec![HashSet::new(); graph.vertex_label_num as usize];
        let mut delete_set = HashSet::new();
        info!("Deleting vertex - {}", graph.graph_schema.vertex_label_names()[label as usize]);
        let vertex_files_prefix = self.input_dir.clone();
        let vertex_files = get_files_list(&vertex_files_prefix, filenames).unwrap();
        if vertex_files.is_empty() {
            return Ok(());
        }

        let parser = LDBCVertexParser::<G>::new(label as LabelId, id_col as usize);
        for vertex_file in vertex_files.iter() {
            process_csv_rows(
                vertex_file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    let (got_label, lid) = graph
                        .vertex_map
                        .get_internal_id(vertex_meta.global_id)
                        .unwrap();
                    if got_label == label as LabelId {
                        delete_set.insert(lid);
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        delete_sets[label as usize] = delete_set;

        let mut input_reps = self.take_csrs_with_label(graph, label);
        input_reps.iter_mut().for_each(|rep| {
            let edge_file_strings = vec![];
            let input_header = graph
                .graph_schema
                .get_edge_header(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap();
            self.parallel_delete_rep(
                rep,
                graph,
                &edge_file_strings,
                &input_header,
                &delete_sets,
                self.parallel,
            );
        });
        self.set_csrs(graph, input_reps);
        let delete_set = &delete_sets[label as usize];
        for v in delete_set.iter() {
            graph.vertex_map.remove_vertex(label, v);
        }

        Ok(())
    }

    pub fn apply_edges_delete_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32,
    ) -> GDBResult<()>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let mut input_resp = self.take_csr(graph, src_label, dst_label, edge_label);
        let mut input_header: Vec<(String, DataType)> = vec![];
        input_header.resize(
            std::cmp::max(src_id_col as usize, dst_id_col as usize) + 1,
            ("".to_string(), DataType::NULL),
        );
        input_header[src_id_col as usize] = ("start_id".to_string(), DataType::ID);
        input_header[dst_id_col as usize] = ("end_id".to_string(), DataType::ID);
        let delete_sets = vec![HashSet::new(); graph.vertex_label_num as usize];
        self.parallel_delete_rep(
            &mut input_resp,
            graph,
            filenames,
            &input_header,
            &delete_sets,
            self.parallel,
        );
        self.set_csr(graph, input_resp);
        Ok(())
    }

    fn apply_deletes<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let vertex_label_num = graph.vertex_label_num;
        let mut delete_sets = vec![];
        for v_label_i in 0..vertex_label_num {
            let mut delete_set = HashSet::new();
            if let Some(vertex_file_strings) = delete_schema.get_vertex_file(v_label_i as LabelId) {
                if !vertex_file_strings.is_empty() {
                    info!(
                        "Deleting vertex - {}",
                        graph.graph_schema.vertex_label_names()[v_label_i as usize]
                    );
                    let vertex_files_prefix = self.input_dir.clone();
                    let vertex_files = get_files_list_beta(&vertex_files_prefix, &vertex_file_strings);
                    if vertex_files.is_empty() {
                        delete_sets.push(delete_set);
                        continue;
                    }
                    let input_header = delete_schema
                        .get_vertex_header(v_label_i as LabelId)
                        .unwrap();
                    let mut id_col = 0;
                    for (index, (n, _)) in input_header.iter().enumerate() {
                        if n == "id" {
                            id_col = index;
                            break;
                        }
                    }
                    let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col);
                    for vertex_file in vertex_files.iter() {
                        process_csv_rows(
                            vertex_file,
                            |record| {
                                let vertex_meta = parser.parse_vertex_meta(&record);
                                let (got_label, lid) = graph
                                    .vertex_map
                                    .get_internal_id(vertex_meta.global_id)
                                    .unwrap();
                                if got_label == v_label_i as LabelId {
                                    delete_set.insert(lid);
                                }
                            },
                            self.skip_header,
                            self.delim,
                        );
                    }
                }
            }
            delete_sets.push(delete_set);
        }

        let mut input_reps = self.take_csrs(graph);
        input_reps.iter_mut().for_each(|rep| {
            let default_vec: Vec<String> = vec![];
            let edge_file_strings = delete_schema
                .get_edge_file(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap_or_else(|| &default_vec);
            let input_header = delete_schema
                .get_edge_header(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap_or_else(|| &[]);

            self.parallel_delete_rep(
                rep,
                graph,
                &edge_file_strings,
                &input_header,
                &delete_sets,
                self.parallel,
            );
        });
        self.set_csrs(graph, input_reps);

        for v_label_i in 0..vertex_label_num {
            let delete_set = &delete_sets[v_label_i as usize];
            if delete_set.is_empty() {
                continue;
            }
            for v in delete_set.iter() {
                graph
                    .vertex_map
                    .remove_vertex(v_label_i as LabelId, v);
            }
        }

        Ok(())
    }

    pub fn apply_vertices_insert_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, label: LabelId, filenames: &Vec<String>, id_col: i32,
        mappings: &Vec<i32>,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let graph_header = graph
            .graph_schema
            .get_vertex_header(label as LabelId)
            .unwrap();
        let header = graph_header.to_vec();

        let parser = LDBCVertexParser::<G>::new(label as LabelId, id_col as usize);
        let vertex_files_prefix = self.input_dir.clone();

        let vertex_files = get_files_list(&vertex_files_prefix, filenames);
        if vertex_files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &vertex_files_prefix,
                filenames,
                vertex_files.err().unwrap()
            );
            return Ok(());
        }
        let vertex_files = vertex_files.unwrap();
        if vertex_files.is_empty() {
            return Ok(());
        }
        for vertex_file in vertex_files.iter() {
            process_csv_rows(
                vertex_file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if let Ok(properties) = parse_properties_by_mappings(&record, &header, mappings) {
                        graph.insert_vertex(vertex_meta.label, vertex_meta.global_id, Some(properties));
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        Ok(())
    }

    fn apply_vertices_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let v_label_num = graph.vertex_label_num;
        for v_label_i in 0..v_label_num {
            if let Some(vertex_file_strings) = input_schema.get_vertex_file(v_label_i as LabelId) {
                if vertex_file_strings.is_empty() {
                    continue;
                }

                let input_header = input_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let graph_header = graph
                    .graph_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let mut keep_set = HashSet::new();
                for pair in graph_header {
                    keep_set.insert(pair.0.clone());
                }
                let mut selected = vec![false; input_header.len()];
                let mut id_col_id = 0;
                for (index, (n, _)) in input_header.iter().enumerate() {
                    if keep_set.contains(n) {
                        selected[index] = true;
                    }
                    if n == "id" {
                        id_col_id = index;
                    }
                }
                let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col_id);
                let vertex_files_prefix = self.input_dir.clone();

                let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings);
                if vertex_files.is_err() {
                    warn!(
                        "Get vertex files {:?}/{:?} failed: {:?}",
                        &vertex_files_prefix,
                        &vertex_file_strings,
                        vertex_files.err().unwrap()
                    );
                    continue;
                }
                let vertex_files = vertex_files.unwrap();
                if vertex_files.is_empty() {
                    continue;
                }
                for vertex_file in vertex_files.iter() {
                    process_csv_rows(
                        vertex_file,
                        |record| {
                            let vertex_meta = parser.parse_vertex_meta(&record);
                            if let Ok(properties) =
                                parse_properties(&record, input_header, selected.as_slice())
                            {
                                graph.insert_vertex(
                                    vertex_meta.label,
                                    vertex_meta.global_id,
                                    Some(properties),
                                );
                            }
                        },
                        self.skip_header,
                        self.delim,
                    );
                }
            }
        }

        Ok(())
    }

    fn load_insert_edges<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        input_header: &[(String, DataType)], graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
            keep_set.insert(pair.0.clone());
        }

        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }

        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        if table_header.is_empty() {
            for file in files.iter() {
                process_csv_rows(
                    file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    },
                    self.skip_header,
                    self.delim,
                );
            }
            Ok((edges, None))
        } else {
            let mut prop_table = ColTable::new(table_header);
            for file in files.iter() {
                process_csv_rows(
                    file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    },
                    self.skip_header,
                    self.delim,
                )
            }
            Ok((edges, Some(prop_table)))
        }
    }

    fn parallel_insert_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, edge_file_strings: &Vec<String>,
        input_header: &[(String, DataType)], p: u32,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let t = Instant::now();
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return;
        }

        if edge_file_strings.is_empty() {
            return;
        }

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return;
        }
        let edge_files = edge_files.unwrap();
        if edge_files.is_empty() {
            return;
        }

        let (edges, table) = self
            .load_insert_edges::<G>(
                src_label,
                edge_label,
                dst_label,
                input_header,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap();

        let parsed_edges: Vec<(I, I)> = edges
            .par_iter()
            .map(|(src, dst)| {
                let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(*src).unwrap();
                let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(*dst).unwrap();
                if got_src_label != src_label || got_dst_label != dst_label {
                    warn!("insert edges with wrong label");
                    (<I as IndexType>::max(), <I as IndexType>::max())
                } else {
                    (src_lid, dst_lid)
                }
            })
            .collect();

        let new_src_num = graph.vertex_map.vertex_num(src_label);
        input.oe_prop = if let Some(old_table) = input.oe_prop.take() {
            Some(input.oe_csr.insert_edges_with_prop(
                new_src_num,
                &parsed_edges,
                table.as_ref().unwrap(),
                false,
                p,
                old_table,
            ))
        } else {
            input
                .oe_csr
                .insert_edges(new_src_num, &parsed_edges, false, p);
            None
        };

        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        input.ie_prop = if let Some(old_table) = input.ie_prop.take() {
            Some(input.ie_csr.insert_edges_with_prop(
                new_dst_num,
                &parsed_edges,
                table.as_ref().unwrap(),
                true,
                p,
                old_table,
            ))
        } else {
            input
                .ie_csr
                .insert_edges(new_dst_num, &parsed_edges, true, p);
            None
        };

        println!(
            "insert edge (parallel{}): {} - {} - {}: {}",
            p,
            graph.graph_schema.vertex_label_names()[src_label as usize],
            graph.graph_schema.edge_label_names()[edge_label as usize],
            graph.graph_schema.vertex_label_names()[dst_label as usize],
            t.elapsed().as_secs_f32(),
        );
    }

    pub fn apply_edges_insert_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32, mappings: &Vec<i32>,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_id_col as usize, dst_id_col as usize);

        let edge_files_prefix = self.input_dir.clone();
        let edge_files = get_files_list(&edge_files_prefix, filenames);
        if edge_files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &edge_files_prefix,
                filenames,
                edge_files.err().unwrap()
            );
            return Ok(());
        }
        let edge_files = edge_files.unwrap();
        let mut input_reps = self.take_csr(graph, src_label, dst_label, edge_label);
        let mut edges = vec![];
        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
        }
        let mut prop_table = ColTable::new(table_header.clone());
        if table_header.is_empty() {
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    },
                    self.skip_header,
                    self.delim,
                );
            }
        } else {
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        if let Ok(properties) =
                            parse_properties_by_mappings(&record, &graph_header, mappings)
                        {
                            prop_table.push(&properties);
                        }
                    },
                    self.skip_header,
                    self.delim,
                )
            }
        }

        let parsed_edges: Vec<(I, I)> = edges
            .par_iter()
            .map(|(src, dst)| {
                let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(*src).unwrap();
                let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(*dst).unwrap();
                if got_src_label != src_label || got_dst_label != dst_label {
                    warn!("insert edges with wrong label");
                    (<I as IndexType>::max(), <I as IndexType>::max())
                } else {
                    (src_lid, dst_lid)
                }
            })
            .collect();
        let new_src_num = graph.vertex_map.vertex_num(src_label);
        input_reps.oe_prop = if let Some(old_table) = input_reps.oe_prop.take() {
            Some(input_reps.oe_csr.insert_edges_with_prop(
                new_src_num,
                &parsed_edges,
                &prop_table,
                false,
                self.parallel,
                old_table,
            ))
        } else {
            input_reps
                .oe_csr
                .insert_edges(new_src_num, &parsed_edges, false, self.parallel);
            None
        };

        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        input_reps.ie_prop = if let Some(old_table) = input_reps.ie_prop.take() {
            Some(input_reps.ie_csr.insert_edges_with_prop(
                new_dst_num,
                &parsed_edges,
                &prop_table,
                true,
                self.parallel,
                old_table,
            ))
        } else {
            input_reps
                .ie_csr
                .insert_edges(new_dst_num, &parsed_edges, true, self.parallel);
            None
        };
        self.set_csr(graph, input_reps);
        Ok(())
    }

    fn apply_edges_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut input_reps = self.take_csrs(graph);
        for ir in input_reps.iter_mut() {
            let edge_files = input_schema.get_edge_file(ir.src_label, ir.edge_label, ir.dst_label);
            if edge_files.is_none() {
                continue;
            }
            let input_header = input_schema
                .get_edge_header(ir.src_label, ir.edge_label, ir.dst_label)
                .unwrap();
            self.parallel_insert_rep(ir, graph, edge_files.unwrap(), input_header, self.parallel);
        }
        self.set_csrs(graph, input_reps);

        Ok(())
    }

    pub fn insert<G, I>(&mut self, graph: &mut GraphDB<G, I>, insert_schema: &InputSchema) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_vertices_inserts(graph, &insert_schema)?;
        self.apply_edges_inserts(graph, &insert_schema)?;
        Ok(())
    }

    pub fn delete<G, I>(&mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_deletes(graph, &delete_schema)?;
        Ok(())
    }
}
