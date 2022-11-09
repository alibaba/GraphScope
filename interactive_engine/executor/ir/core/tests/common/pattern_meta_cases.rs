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

use std::fs::File;

use ir_core::plan::meta::set_schema;
use ir_core::{plan::meta::Schema, JsonIO};

pub fn read_modern_graph_schema() -> Schema {
    let modern_schema_file = match File::open("resource/modern_schema.json") {
        Ok(file) => file,
        Err(_) => match File::open("core/resource/modern_schema.json") {
            Ok(file) => file,
            Err(_) => File::open("../core/resource/modern_schema.json").unwrap(),
        },
    };
    Schema::from_json(modern_schema_file).unwrap()
}

pub fn set_modern_graph_schema() {
    let modern_schema = read_modern_graph_schema();
    set_schema(modern_schema);
}

pub fn read_ldbc_graph_schema() -> Schema {
    let ldbc_schema_file = match File::open("resource/ldbc_schema.json") {
        Ok(file) => file,
        Err(_) => match File::open("core/resource/ldbc_schema.json") {
            Ok(file) => file,
            Err(_) => File::open("../core/resource/ldbc_schema.json").unwrap(),
        },
    };
    Schema::from_json(ldbc_schema_file).unwrap()
}

pub fn set_ldbc_graph_schema() {
    let ldbc_schema = read_ldbc_graph_schema();
    set_schema(ldbc_schema);
}
