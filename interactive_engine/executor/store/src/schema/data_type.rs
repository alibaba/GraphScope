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
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum DataType {
    Bool = 0,
    Char = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Float = 5,
    Double = 6,
    Bytes = 7,
    String = 8,
    Date = 9,
    Set = 10,
    ListInt = 11,
    ListLong = 12,
    ListFloat = 13,
    ListDouble = 14,
    ListString = 15,
    ListBytes = 16,
    Map = 100,
    Unknown = 1000,
}

impl Default for DataType {
    fn default() -> Self {
        DataType::Unknown
    }
}

impl DataType {
    pub(super) fn new(order: u32, expression: &str) -> Self {
        match order {
            0 => DataType::Bool,
            1 => DataType::Char,
            2 => DataType::Short,
            3 => DataType::Int,
            4 => DataType::Long,
            5 => DataType::Float,
            6 => DataType::Double,
            7 => DataType::Bytes,
            8 => DataType::String,
            9 => DataType::Date,
            10 => DataType::Set,
            11 => {
                match expression.to_lowercase().as_str() {
                    "int" => DataType::ListInt,
                    "long" => DataType::ListLong,
                    "float" => DataType::ListFloat,
                    "double" => DataType::ListDouble,
                    "string" => DataType::ListString,
                    "bytes" => DataType::ListBytes,
                    _ => DataType::Unknown,
                }
            }
            12 => DataType::ListLong,
            13 => DataType::ListFloat,
            14 => DataType::ListDouble,
            15 => DataType::ListString,
            16 => DataType::ListBytes,
            _ => DataType::Unknown,
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            DataType::Bool | DataType::Char => 1,
            DataType::Short => 2,
            DataType::Int | DataType::Float => 4,
            DataType::Long | DataType::Double => 8,
            _ => unimplemented!()
        }
    }

    pub fn is_fixed_len(&self) -> bool {
        match *self {
            DataType::Bool | DataType::Char |
            DataType::Short | DataType::Int |
            DataType::Long | DataType::Float |
            DataType::Double => true,
            _ => false,
        }
    }
}

pub fn parse_str_to_data_type(value: &str) -> Result<DataType, String> {
    match value.to_lowercase().as_str() {
        "bool" => Ok(DataType::Bool),
        "char" => Ok(DataType::Char),
        "short" => Ok(DataType::Short),
        "int" => Ok(DataType::Int),
        "long" => Ok(DataType::Long),
        "float" => Ok(DataType::Float),
        "double" => Ok(DataType::Double),
        "bytes" => Ok(DataType::Bytes),
        "string" => Ok(DataType::String),
        "date" => Ok(DataType::Date),
        v => {
            if v.starts_with("list<") {
                let tmp = &v[5..v.len() - 1];
                let sub_type = DataType::from(tmp);
                match sub_type {
                    DataType::Int => Ok(DataType::ListInt),
                    DataType::Long => Ok(DataType::ListLong),
                    DataType::Float => Ok(DataType::ListFloat),
                    DataType::Double => Ok(DataType::ListDouble),
                    DataType::String => Ok(DataType::ListString),
                    DataType::Bytes => Ok(DataType::ListBytes),
                    _ => Err(format!("data type {} not support yet", v))

                }
            } else if v.starts_with("s<") {
                //todo
                Err(format!("data type {} not support yet", v))
            } else if v.starts_with("m<") {
                //todo
                Err(format!("data type {} not support yet", v))
            } else {
                Err(format!("unknown data type {}", v))
            }
        },
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "bool" => DataType::Bool,
            "char" => DataType::Char,
            "short" => DataType::Short,
            "int" => DataType::Int,
            "long" => DataType::Long,
            "float" => DataType::Float,
            "double" => DataType::Double,
            "bytes" => DataType::Bytes,
            "string" => DataType::String,
            "date" => DataType::Date,
            v => {
                if v.starts_with("list<") {
                    let tmp = &v[5..v.len() - 1];
                    let sub_type = DataType::from(tmp);
                    if sub_type == DataType::Int {
                        DataType::ListInt
                    } else {
                        unimplemented!()
                    }
                } else if v.starts_with("s<") {
                    //todo
                    unimplemented!()
                } else if v.starts_with("m<") {
                    //todo
                    unimplemented!()
                } else {
                    DataType::Unknown
                }
            },
        }
    }
}
