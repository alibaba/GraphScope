use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::read;
use std::io;

use bmcsr::columns::Item as GraphItem;
use dyn_type::CastError;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use serde::{Deserialize, Serialize};

pub type DefaultId = usize;
pub type InternalId = usize;
pub type LabelId = u8;

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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i32(self.column.index);
        self.column.name.write_to(writer)?;
        self.column.data_type.write_to(writer)?;
        self.property_name.write_to(writer)?;
        Ok(())
    }
}

impl Decode for ColumnMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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

#[derive(Clone, Copy)]
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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

#[derive(Clone)]
pub struct Column {
    data: ColumnData,
    column_name: String,
    data_type: DataType,
}

impl Encode for Column {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.data.write_to(writer)?;
        self.column_name.write_to(writer)?;
        self.data_type.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Column {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let data = ColumnData::read_from(reader)?;
        let column_name = String::read_from(reader)?;
        let data_type = DataType::read_from(reader)?;
        Ok(Column { data, column_name, data_type })
    }
}

impl Column {
    pub fn new(data: ColumnData, column_name: String, data_type: DataType) -> Self {
        Column { data, column_name, data_type }
    }

    pub fn data(&self) -> &ColumnData {
        &self.data
    }

    pub fn take_data(&mut self) -> ColumnData {
        std::mem::replace(&mut self.data, ColumnData::NullArray)
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
    columns: Vec<Column>,
}

impl Encode for DataFrame {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.columns.write_to(writer)?;
        Ok(())
    }
}

impl Decode for DataFrame {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let columns = Vec::<Column>::read_from(reader)?;
        Ok(DataFrame { columns })
    }
}

impl DataFrame {
    pub fn new_vertices_ids(ids: Vec<u64>) -> Self {
        let columns = vec![Column::new(ColumnData::UInt64Array(ids), "id".to_string(), DataType::VertexId)];
        DataFrame { columns }
    }

    pub fn new_edges_ids(ids: Vec<usize>) -> Self {
        let columns = vec![Column::new(ColumnData::VertexIdArray(ids), "id".to_string(), DataType::VertexId)];
        DataFrame { columns }
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn take_columns(&mut self) -> Vec<Column> {
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self.label_id);
        self.inputs.write_to(writer)?;
        self.column_mappings.write_to(writer)?;
        Ok(())
    }
}

impl Decode for VertexMappings {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    VertexId,
    EdgeId,
    String,
    Date,
    Timestamp,
    NULL,
}

impl Encode for DataType {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match *self {
            DataType::Boolean => writer.write_u8(0),
            DataType::Int32 => writer.write_u8(1),
            DataType::UInt32 => writer.write_u8(2),
            DataType::Int64 => writer.write_u8(3),
            DataType::UInt64 => writer.write_u8(4),
            DataType::Float32 => writer.write_u8(5),
            DataType::Float64 => writer.write_u8(6),
            DataType::VertexId => writer.write_u8(7),
            DataType::EdgeId => writer.write_u8(8),
            DataType::String => writer.write_u8(9),
            DataType::Date => writer.write_u8(10),
            DataType::Timestamp => writer.write_u8(11),
            DataType::NULL => writer.write_u8(12),
        };
        Ok(())
    }
}

impl Decode for DataType {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let data_type = match reader.read_u8()? {
            0 => DataType::Boolean,
            1 => DataType::Int32,
            2 => DataType::UInt32,
            3 => DataType::Int64,
            4 => DataType::UInt64,
            5 => DataType::Float32,
            6 => DataType::Float64,
            7 => DataType::VertexId,
            8 => DataType::EdgeId,
            9 => DataType::String,
            10 => DataType::Date,
            11 => DataType::Timestamp,
            _ => DataType::NULL,
        };
        Ok(data_type)
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(_token: &'a str) -> Self {
        println!("token = {}", _token);
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "LONG" {
            DataType::Int64
        } else if token == "INT32" {
            DataType::Int32
        } else if token == "DOUBLE" {
            DataType::Float64
        } else if token == "VertexId" {
            DataType::VertexId
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
    U64Vec(Vec<u64>),
    Double(f64),
    ID(DefaultId),
    String(String),
    Null,
}

impl Item {
    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Item::Int32(v) => Ok(*v as u64),
            Item::UInt32(v) => Ok(*v as u64),
            Item::Int64(v) => Ok(*v as u64),
            Item::UInt64(v) => Ok(*v),
            Item::Double(v) => Ok(*v as u64),
            Item::ID(v) => Ok(*v as u64),
            _ => Ok(0_u64),
        }
    }

    #[inline]
    pub fn as_u64_vec(&self) -> Result<Vec<u64>, CastError> {
        match self {
            Item::U64Vec(v) => Ok(v.clone()),
            _ => Ok(Vec::<u64>::new()),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Item::Int32(v) => Ok(*v),
            Item::UInt32(v) => Ok(*v as i32),
            Item::Int64(v) => Ok(*v as i32),
            Item::UInt64(v) => Ok(*v as i32),
            Item::Double(v) => Ok(*v as i32),
            Item::ID(v) => Ok(*v as i32),
            _ => Ok(0),
        }
    }
}

#[derive(Clone)]
pub enum RefItem<'a> {
    Int32(&'a i32),
    UInt32(&'a u32),
    Int64(&'a i64),
    UInt64(&'a u64),
    U64Vec(&'a Vec<u64>),
    Double(&'a f64),
    ID(&'a DefaultId),
    Null,
}

impl<'a> RefItem<'a> {
    pub fn to_owned(self) -> Item {
        match self {
            RefItem::Int32(v) => Item::Int32(*v),
            RefItem::UInt32(v) => Item::UInt32(*v),
            RefItem::Int64(v) => Item::Int64(*v),
            RefItem::UInt64(v) => Item::UInt64(*v),
            RefItem::U64Vec(v) => Item::U64Vec(v.clone()),
            RefItem::Double(v) => Item::Double(*v),
            RefItem::ID(v) => Item::ID(*v),
            RefItem::Null => Item::Null,
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
            RefItem::ID(v) => Ok(**v as u64),
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
            RefItem::ID(v) => Ok(**v as i32),
            _ => Ok(0),
        }
    }
}

/// Edge direction.
#[derive(Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(usize)]
pub enum Direction {
    /// An `Outgoing` edge is an outward edge *from* the current node.
    Outgoing = 0,
    /// An `Incoming` edge is an inbound edge *to* the current node.
    Incoming = 1,
}

impl Direction {
    /// Return the opposite `Direction`.
    #[inline]
    pub fn opposite(self) -> Direction {
        match self {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
        }
    }

    /// Return `0` for `Outgoing` and `1` for `Incoming`.
    #[inline]
    pub fn index(self) -> usize {
        (self as usize) & 0x1
    }
}

impl Clone for Direction {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone)]
pub struct AliasData {
    pub alias_index: i32,
    pub column_data: ColumnData,
}

impl Debug for AliasData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Alias index: {}, data: {:?}", self.alias_index, self.column_data)
    }
}

impl Encode for AliasData {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i32(self.alias_index)?;
        self.column_data.write_to(writer)?;
        Ok(())
    }
}

impl Decode for AliasData {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let alias_index = reader.read_i32()?;
        let column_data = ColumnData::read_from(reader)?;
        Ok(AliasData { alias_index, column_data })
    }
}

#[derive(Clone)]
pub enum ColumnData {
    BooleanArray(Vec<bool>),
    Int32Array(Vec<i32>),
    UInt32Array(Vec<u32>),
    Int64Array(Vec<i64>),
    UInt64Array(Vec<u64>),
    Float32Array(Vec<f32>),
    Float64Array(Vec<f64>),
    VertexIdArray(Vec<usize>),
    EdgeIdArray(Vec<(usize, usize)>),
    StringArray(Vec<String>),
    DateArray(Vec<i32>),
    TimestampArray(Vec<i64>),
    NullArray
}

impl Debug for ColumnData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnData::BooleanArray(data) => write!(f, "Boolean data, size: {}", data.len()),
            ColumnData::Int32Array(data) => write!(f, "Int32 data, size: {}", data.len()),
            ColumnData::UInt32Array(data) => write!(f, "UInt32 data, size: {}", data.len()),
            ColumnData::Int64Array(data) => write!(f, "Int64 data, size: {}", data.len()),
            ColumnData::UInt64Array(data) => write!(f, "UInt64 data, size: {}", data.len()),
            ColumnData::Float32Array(data) => write!(f, "Float32 data, size: {}", data.len()),
            ColumnData::Float64Array(data) => write!(f, "Float64 data, size: {}", data.len()),
            ColumnData::VertexIdArray(data) => write!(f, "VertexId data, size: {}", data.len()),
            ColumnData::EdgeIdArray(data) => write!(f, "EdgeId data, size: {}", data.len()),
            ColumnData::StringArray(data) => write!(f, "String data, size: {}", data.len()),
            ColumnData::DateArray(data) => write!(f, "Date data, size: {}", data.len()),
            ColumnData::TimestampArray(data) => write!(f, "Timestamp data, size: {}", data.len()),
            _ => write!(f, "Unknown data type"),
        }
    }
}

impl Encode for ColumnData {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            ColumnData::BooleanArray(data) => {
                writer.write_u8(0);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    if *i {
                        writer.write_u8(1);
                    } else {
                        writer.write_u8(0);
                    }
                }
            }
            ColumnData::Int32Array(data) => {
                writer.write_u8(1);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_i32(*i);
                }
            }
            ColumnData::UInt32Array(data) => {
                writer.write_u8(2);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_u32(*i);
                }
            }
            ColumnData::Int64Array(data) => {
                writer.write_u8(3);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_i64(*i);
                }
            }
            ColumnData::UInt64Array(data) => {
                writer.write_u8(4);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_u64(*i);
                }
            }
            ColumnData::Float32Array(data) => {
                writer.write_u8(5);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_f32(*i);
                }
            }
            ColumnData::Float64Array(data) => {
                writer.write_u8(6);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_f64(*i);
                }
            }
            ColumnData::VertexIdArray(data) => {
                writer.write_u8(7);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_u64(*i as u64);
                }
            }
            ColumnData::EdgeIdArray(data) => {
                writer.write_u8(8);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_u64(i.0 as u64);
                    writer.write_u64(i.1 as u64);
                }
            }
            ColumnData::StringArray(data) => {
                writer.write_u8(9);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    i.write_to(writer);
                }
            }
            ColumnData::DateArray(data) => {
                writer.write_u8(10);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_i32(*i);
                }
            }
            ColumnData::TimestampArray(data) => {
                writer.write_u8(11);
                writer.write_u64(data.len() as u64);
                for i in data.iter() {
                    writer.write_i64(*i);
                }
            }
            _ => todo!(),
        }
        Ok(())
    }
}

impl Decode for ColumnData {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let data_type = reader.read_u8()?;
        match data_type {
            0 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(if reader.read_u8()? == 1 { true } else { false });
                }
                Ok(ColumnData::BooleanArray(data))
            }
            1 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_i32()?);
                }
                Ok(ColumnData::Int32Array(data))
            }
            2 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_u32()?);
                }
                Ok(ColumnData::UInt32Array(data))
            }
            3 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_i64()?);
                }
                Ok(ColumnData::Int64Array(data))
            }
            4 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_u64()?);
                }
                Ok(ColumnData::UInt64Array(data))
            }
            5 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_f32()?);
                }
                Ok(ColumnData::Float32Array(data))
            }
            6 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_f64()?);
                }
                Ok(ColumnData::Float64Array(data))
            }
            7 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_u64()? as usize);
                }
                Ok(ColumnData::VertexIdArray(data))
            }
            8 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push((reader.read_u64()? as usize, reader.read_u64()? as usize));
                }
                Ok(ColumnData::EdgeIdArray(data))
            }
            9 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(String::read_from(reader)?);
                }
                Ok(ColumnData::StringArray(data))
            }
            10 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_i32()?);
                }
                Ok(ColumnData::DateArray(data))
            }
            11 => {
                let data_len = reader.read_u64()? as usize;
                let mut data = Vec::with_capacity(data_len);
                for i in 0..data_len {
                    data.push(reader.read_i64()?);
                }
                Ok(ColumnData::TimestampArray(data))
            }
            _ => todo!(),
        }
    }
}

impl ColumnData {
    pub fn as_ref(&self) -> ColumnDataRef {
        match self {
            ColumnData::BooleanArray(data) => ColumnDataRef::BooleanArray(&data),
            ColumnData::Int32Array(data) => ColumnDataRef::Int32Array(&data),
            ColumnData::UInt32Array(data) => ColumnDataRef::UInt32Array(&data),
            ColumnData::Int64Array(data) => ColumnDataRef::Int64Array(&data),
            ColumnData::UInt64Array(data) => ColumnDataRef::UInt64Array(&data),
            ColumnData::Float32Array(data) => ColumnDataRef::Float32Array(&data),
            ColumnData::Float64Array(data) => ColumnDataRef::Float64Array(&data),
            ColumnData::VertexIdArray(data) => ColumnDataRef::VertexIdArray(&data),
            ColumnData::EdgeIdArray(data) => ColumnDataRef::EdgeIdArray(&data),
            ColumnData::StringArray(data) => ColumnDataRef::StringArray(&data),
            ColumnData::DateArray(data) => ColumnDataRef::DateArray(&data),
            ColumnData::TimestampArray(data) => ColumnDataRef::TimestampArray(&data),
            _ => panic!("Unknown type"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::BooleanArray(data) => data.len(),
            ColumnData::Int32Array(data) => data.len(),
            ColumnData::UInt32Array(data) => data.len(),
            ColumnData::Int64Array(data) => data.len(),
            ColumnData::UInt64Array(data) => data.len(),
            ColumnData::Float32Array(data) => data.len(),
            ColumnData::Float64Array(data) => data.len(),
            ColumnData::VertexIdArray(data) => data.len(),
            ColumnData::EdgeIdArray(data) => data.len(),
            ColumnData::StringArray(data) => data.len(),
            ColumnData::DateArray(data) => data.len(),
            ColumnData::TimestampArray(data) => data.len(),
            _ => todo!(),
        }
    }

    pub fn push_item(&mut self, item: GraphItem) {
        match self {
            ColumnData::BooleanArray(data) => {
                if let GraphItem::Boolean(item) = item {
                    data.push(item);
                }
            }
            ColumnData::Int32Array(data) => {
                if let GraphItem::Int32(item) = item {
                    data.push(item);
                }
            }
            ColumnData::UInt32Array(data) => {
                if let GraphItem::UInt32(item) = item {
                    data.push(item);
                }
            }
            ColumnData::Int64Array(data) => {
                if let GraphItem::Int64(item) = item {
                    data.push(item);
                }
            }
            ColumnData::UInt64Array(data) => {
                if let GraphItem::UInt64(item) = item {
                    data.push(item);
                }
            }
            ColumnData::Float32Array(data) => {
                if let GraphItem::Float(item) = item {
                    data.push(item);
                }
            }
            ColumnData::Float64Array(data) => {
                if let GraphItem::Double(item) = item {
                    data.push(item);
                }
            }
            ColumnData::VertexIdArray(data) => {
                if let GraphItem::VertexId(item) = item {
                    data.push(item as usize);
                }
            }
            ColumnData::EdgeIdArray(data) => {
                if let GraphItem::EdgeId(item) = item {
                    data.push((item.0 as usize, item.1 as usize));
                }
            }
            ColumnData::StringArray(data) => {
                if let GraphItem::String(item) = item {
                    data.push(item);
                }
            }
            _ => todo!(),
        }
    }

    pub fn get_item(&self, index: usize) -> GraphItem {
        match self {
            ColumnData::BooleanArray(data) => GraphItem::Boolean(data[index]),
            ColumnData::Int32Array(data) => GraphItem::Int32(data[index]),
            ColumnData::UInt32Array(data) => GraphItem::UInt32(data[index]),
            ColumnData::Int64Array(data) => GraphItem::Int64(data[index]),
            ColumnData::UInt64Array(data) => GraphItem::UInt64(data[index]),
            ColumnData::Float32Array(data) => GraphItem::Float(data[index]),
            ColumnData::Float64Array(data) => GraphItem::Double(data[index]),
            ColumnData::VertexIdArray(data) => GraphItem::VertexId(data[index]),
            _ => todo!(),
        }
    }

    pub fn get_type(&self) -> DataType {
        match self {
            ColumnData::BooleanArray(_) => DataType::Boolean,
            ColumnData::Int32Array(_) => DataType::Int32,
            ColumnData::UInt32Array(_) => DataType::UInt32,
            ColumnData::Int64Array(_) => DataType::Int64,
            ColumnData::UInt64Array(_) => DataType::UInt64,
            ColumnData::Float32Array(_) => DataType::Float32,
            ColumnData::Float64Array(_) => DataType::Float64,
            ColumnData::VertexIdArray(_) => DataType::VertexId,
            ColumnData::EdgeIdArray(_) => DataType::EdgeId,
            ColumnData::StringArray(_) => DataType::String,
            ColumnData::DateArray(_) => DataType::Date,
            ColumnData::TimestampArray(_) => DataType::Timestamp,
            _ => todo!(),
        }
    }
}

pub enum ColumnDataRef<'a> {
    BooleanArray(&'a Vec<bool>),
    Int32Array(&'a Vec<i32>),
    UInt32Array(&'a Vec<u32>),
    Int64Array(&'a Vec<i64>),
    UInt64Array(&'a Vec<u64>),
    Float32Array(&'a Vec<f32>),
    Float64Array(&'a Vec<f64>),
    VertexIdArray(&'a Vec<usize>),
    EdgeIdArray(&'a Vec<(usize, usize)>),
    StringArray(&'a Vec<String>),
    DateArray(&'a Vec<i32>),
    TimestampArray(&'a Vec<i64>),
}

pub enum GraphIndexError {
    UpdateFailure(String),
}

impl Debug for GraphIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphIndexError::UpdateFailure(msg) => write!(f, "Failed when update idnex: {}", msg),
        }
    }
}

impl Display for GraphIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for GraphIndexError {}

pub fn str_to_data_type(data_type_str: &String) -> DataType {
    match data_type_str.as_ref() {
        "Boolean" => DataType::Boolean,
        "Int32" => DataType::Int32,
        "UInt32" => DataType::UInt32,
        "Int64" => DataType::Int64,
        "UInt64" => DataType::UInt64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "VertexId" => DataType::VertexId,
        "EdgeId" => DataType::EdgeId,
        "String" => DataType::String,
        _ => DataType::NULL,
    }
}

pub fn str_to_default_value(default_value: &String, data_type: DataType) -> Item {
    match data_type {
        DataType::Int32 => Item::Int32(default_value.parse::<i32>().unwrap()),
        DataType::UInt64 => Item::UInt64(default_value.parse::<u64>().unwrap()),
        DataType::String => Item::String(default_value.clone()),
        _ => Item::Int32(0),
    }
}
