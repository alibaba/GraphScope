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

use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::Path;

use crate::error::{GDBError, GDBResult};

/// A generic datatype for each item in a row
pub type ItemType = JsonValue;

/// One row of data, as a vector of `ItemType`
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct Row {
    data: Vec<ItemType>,
}

impl Row {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get(&self, index: usize) -> Option<&ItemType> {
        self.data.get(index)
    }

    pub fn push(&mut self, val: ItemType) {
        self.data.push(val)
    }
}

impl From<String> for Row {
    /// Create a `Row` with one single `String`-typed field
    fn from(item: String) -> Self {
        let val = json!(item);
        let data = vec![val];
        Row { data }
    }
}

impl From<u32> for Row {
    /// Create a `Row` with one single `u32`-typed field
    fn from(item: u32) -> Self {
        let val = json!(item);
        let data = vec![val];
        Row { data }
    }
}

impl From<u64> for Row {
    /// Create a `Row` with one single `u64`-typed field
    fn from(item: u64) -> Self {
        let val = json!(item);
        let data = vec![val];
        Row { data }
    }
}

impl From<Vec<ItemType>> for Row {
    /// Create a `Row` from a vector of items
    fn from(data: Vec<ItemType>) -> Self {
        Row { data }
    }
}

/// Serialize a `Row` of data into a byte array
impl TryInto<Vec<u8>> for Row {
    type Error = GDBError;

    fn try_into(self) -> GDBResult<Vec<u8>> {
        let mut raw_data = Vec::with_capacity(self.len());

        for item in self.data.into_iter() {
            raw_data.push(serde_cbor::to_vec(&item)?);
        }

        Ok(bincode::serialize(&raw_data)?)
    }
}

/// Deserialize from a byte array
impl TryFrom<Vec<u8>> for Row {
    type Error = GDBError;

    fn try_from(value: Vec<u8>) -> GDBResult<Self> {
        let raw_data = bincode::deserialize::<Vec<Vec<u8>>>(&value)?;
        let mut data = Vec::with_capacity(raw_data.len());
        for item in raw_data.into_iter() {
            data.push(serde_cbor::from_slice::<ItemType>(&item)?);
        }

        Ok(Self { data })
    }
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let _data_vec: GDBResult<Vec<u8>> = self.clone().try_into();
        if let Ok(data_vec) = _data_vec {
            data_vec.serialize(serializer)
        } else {
            Result::Err(S::Error::custom("Serialize `JsonValue` into `Vec<u8>` error!"))
        }
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = Vec::<u8>::deserialize(deserializer)?;
        if let Ok(row) = Row::try_from(inner) {
            Ok(row)
        } else {
            Result::Err(D::Error::custom("Deserialize `JsonValue` from `Vec<u8>` error!"))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RowRef<'a> {
    Ref(&'a Row),
    Owned(Row),
    Single(JsonValue),
    None,
}

impl<'a> RowRef<'a> {
    pub fn get(&self, field_index: usize) -> Option<&ItemType> {
        match self {
            RowRef::Ref(row) => row.get(field_index),
            RowRef::Owned(row) => row.get(field_index),
            RowRef::Single(val) => Some(val),
            RowRef::None => None,
        }
    }
}

/// The table structure, which maintain a couple of rows
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Table {
    Dense(Vec<Row>),
    Sparse(HashMap<usize, Row>),
}

/// Define the functions that operate on a `PropertyTable`
pub trait PropertyTableTrait {
    /// Get size of the Table
    fn len(&self) -> usize;

    /// To get a row from the table at the give index
    fn get_row(&self, index: usize) -> GDBResult<RowRef>;

    /// Inserts a key-value pair into the table.
    ///
    /// If the table did not have this key present, [`Ok(None)`] is returned.
    ///
    /// If the table did have this key present, the row is updated, and the old
    /// row is returned.
    ///
    /// `GDBError` will be thrown out in case of error
    fn insert(&mut self, index: usize, row: Row) -> GDBResult<Option<Row>>;

    /// Batch inserting a certain number of items
    /// Return the number of data that is successfully inserted
    fn insert_batches<Iter: Iterator<Item = (usize, Row)>>(
        &mut self, iter: Iter,
    ) -> GDBResult<usize> {
        let mut count = 0;
        for item in iter {
            if self.insert(item.0, item.1)?.is_none() {
                count += 1;
            }
        }

        Ok(count)
    }

    fn new<P: AsRef<Path>>(_path: P) -> Self;

    /// Export `Self`'s binary file to the given file
    fn export<P: AsRef<Path>>(&self, path: P) -> GDBResult<()>;

    /// Import the binary file in the given path as `Self`
    fn import<P: AsRef<Path>>(path: P) -> GDBResult<Self>
    where
        Self: std::marker::Sized;
}

/// A memory-based table to store the properties of an entity (vertex or edge)
#[derive(Clone, Serialize, Deserialize)]
pub struct PropertyTable {
    /// A table of data
    properties: Table,
}

impl PropertyTable {
    /// Create a new sparse table
    pub fn new_sparse() -> Self {
        Self { properties: Table::Sparse(HashMap::new()) }
    }

    /// Create a new dense table
    pub fn new_dense() -> Self {
        Self { properties: Table::Dense(Vec::new()) }
    }
}

impl PropertyTableTrait for PropertyTable {
    fn len(&self) -> usize {
        match &self.properties {
            Table::Sparse(data) => data.len(),
            Table::Dense(data) => data.len(),
        }
    }

    fn get_row(&self, index: usize) -> GDBResult<RowRef> {
        let _row = match &self.properties {
            Table::Sparse(data) => data.get(&index),
            Table::Dense(data) => data.get(index),
        };

        if let Some(row) = _row {
            Ok(RowRef::Ref(row))
        } else {
            Ok(RowRef::None)
        }
    }

    fn insert(&mut self, index: usize, row: Row) -> GDBResult<Option<Row>> {
        match &mut self.properties {
            Table::Sparse(data) => Ok(data.insert(index, row)),
            Table::Dense(data) => {
                if index >= data.len() {
                    let mut count = data.len();
                    data.reserve(index + 1);
                    while index > count {
                        data.push(Row::default());
                        count += 1;
                    }
                    data.push(row);

                    Ok(None)
                } else {
                    let old_val = &mut data[index];
                    let ret_val = old_val.clone();
                    *old_val = row;

                    Ok(Some(ret_val))
                }
            }
        }
    }

    fn new<P: AsRef<Path>>(_path: P) -> Self {
        // By default use the dense table
        PropertyTable::new_dense()
    }

    fn export<P: AsRef<Path>>(&self, path: P) -> GDBResult<()> {
        crate::io::export(&self, path)?;
        Ok(())
    }

    fn import<P: AsRef<Path>>(path: P) -> GDBResult<Self> {
        let mut table = crate::io::import::<Self, _>(path)?;
        match &mut table.properties {
            Table::Dense(inner) => inner.shrink_to_fit(),
            Table::Sparse(inner) => inner.shrink_to_fit(),
        }
        Ok(table)
    }
}

/// A table where each row has only one value of `u64` type, which is very common in edge
/// data of a graph
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SingleValueTable {
    property: HashMap<usize, u64>,
}

impl PropertyTableTrait for SingleValueTable {
    fn len(&self) -> usize {
        self.property.len()
    }

    fn get_row(&self, index: usize) -> GDBResult<RowRef> {
        if let Some(num) = self.property.get(&index) {
            Ok(RowRef::Single(json!(*num)))
        } else {
            Ok(RowRef::None)
        }
    }

    fn insert(&mut self, index: usize, row: Row) -> GDBResult<Option<Row>> {
        if row.is_empty() {
            return GDBResult::Err(GDBError::OutOfBoundError);
        }
        let mut _ret_val = None;
        let item = row.get(0).unwrap();
        if let Some(number) = item.as_u64() {
            _ret_val = self.property.insert(index, number).map(|num| Row::from(num));
        } else {
            return GDBResult::Err(GDBError::ParseError);
        }

        Ok(_ret_val)
    }

    fn new<P: AsRef<Path>>(_path: P) -> Self {
        Self { property: HashMap::new() }
    }

    fn export<P: AsRef<Path>>(&self, path: P) -> GDBResult<()> {
        crate::io::export(&self, path)?;
        Ok(())
    }

    fn import<P: AsRef<Path>>(path: P) -> GDBResult<Self> {
        let mut table = crate::io::import::<Self, _>(path)?;
        table.property.shrink_to_fit();

        Ok(table)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_property_table_dense() {
        let mut table = PropertyTable::new_dense();
        assert_eq!(table.len(), 0);
        assert_eq!(table.get_row(0).unwrap(), RowRef::None);

        assert!(table.insert(0, Row::default()).unwrap().is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(table.get_row(0).unwrap(), RowRef::Ref(&Row::default()));

        assert!(table.insert(2, Row::from("abc".to_string())).unwrap().is_none());
        assert_eq!(table.len(), 3);
        assert_eq!(table.get_row(1).unwrap(), RowRef::Ref(&Row::default()));
        assert_eq!(table.get_row(2).unwrap(), RowRef::Ref(&Row::from("abc".to_string())));
        // Try to insert a duplicated item, update and return the old value
        assert_eq!(table.insert(2, Row::default()).unwrap(), Some(Row::from("abc".to_string())));
        assert_eq!(table.get_row(2).unwrap(), RowRef::Ref(&Row::default()));
    }

    #[test]
    fn test_property_table_sparse() {
        let mut table = PropertyTable::new_sparse();
        assert_eq!(table.len(), 0);
        assert_eq!(table.get_row(0).unwrap(), RowRef::None);

        assert!(table.insert(0, Row::default()).unwrap().is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(table.get_row(0).unwrap(), RowRef::Ref(&Row::default()));

        assert!(table.insert(2, Row::from("abc".to_string())).unwrap().is_none());
        assert_eq!(table.len(), 2);

        assert_eq!(table.get_row(2).unwrap(), RowRef::Ref(&Row::from("abc".to_string())));
        // Try to insert a duplicated item, abort and return the old value
        assert_eq!(table.insert(2, Row::default()).unwrap(), Some(Row::from("abc".to_string())));
        assert_eq!(table.get_row(2).unwrap(), RowRef::Ref(&Row::default()));
    }
}
