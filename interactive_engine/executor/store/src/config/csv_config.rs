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

use crate::schema::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;
use serde_json;
use serde_json::{Value, Error};
use crate::api::prelude::*;
use std::sync::Arc;


#[derive(Debug, Default)]
pub struct CSVLoadConfig {
    pub label: LabelId,
    pub separator: String,
    pub properties: Vec<(usize, PropId, DataType)>,
    pub src_label: Option<LabelId>,
    pub dst_label: Option<LabelId>,
    pub max_index: usize,
}

impl CSVLoadConfig {
    pub fn from_file<P: AsRef<Path>>(schema: Arc<dyn Schema>, path: P) -> Result<Self, ConfigParseError> {
        let mut f = File::open(path)?;
        let mut text = String::new();
        f.read_to_string(&mut text)?;
        Self::from_str(schema, text.as_str())
    }

    pub fn from_str(schema: Arc<dyn Schema>, text: &str) -> Result<Self, ConfigParseError> {
        let json: Value = serde_json::from_str(text)?;
        let mut ret = CSVLoadConfig::new();
        let label = get_string_value(&json, "label")?;

        if let Some(type_id) = schema.get_label_id(label.as_str()) {
            ret.label = type_id;
        } else {
            return Err(ConfigParseError::from(format!(r#"label "{}" not found in schema: {:?}"#, label, schema)));
        }

        ret.separator = json.get("separator").map_or(",", |s| {
            s.as_str().unwrap_or(",")
        }).to_owned();

        if let Some(properties) = json["properties"].as_array() {
            for v in properties.iter() {
                let prop_name = get_string_value(v, "propertyName")?;
                if let Some (prop_id) = schema.get_prop_id(prop_name.as_str()) {
                    let data_type = get_string_value(v, "dataType")?;
                    let index = get_string_value(v, "index")?.parse::<usize>()?;
                    ret.properties.push((index, prop_id, parse_str_to_data_type(data_type.to_lowercase().as_str())?));
                } else {
                    return Err(ConfigParseError::from(format!(r#""{}" not found in schema"#, prop_name)));
                }
            }
            let t = get_string_value(&json, "type")?.to_lowercase();
            if t.as_str() == "edge" {
                let src_label = get_string_value(&json, "srcLabel")?;
                ret.src_label = Some(schema.get_label_id(src_label.as_str())
                    .ok_or(format!("{} not found", src_label))?);
                let dst_label = get_string_value(&json, "dstLabel")?;
                ret.dst_label = Some(schema.get_label_id(dst_label.as_str())
                    .ok_or(format!("{} not found", dst_label))?);
            }
        } else {
            return Err(ConfigParseError::from(r#""properties" not found in config"#));
        }
        for x in ret.properties.iter() {
            if x.0 > ret.max_index {
                ret.max_index = x.0;
            }
        }
        Ok(ret)
    }

    fn new() -> Self {
        Default::default()
    }
}

fn get_string_value(json: &Value, key: &str) -> Result<String, ConfigParseError> {
    if let Some(v) = json[key].as_str() {
        Ok(v.to_owned())
    } else {
        Err(ConfigParseError::from(format!(r#""{}" not found in config: {:?}"#, key, json)))
    }
}

#[derive(Debug)]
pub struct ConfigParseError {
    err_msg: String,
}

impl From<String> for ConfigParseError {
    fn from(err_msg: String) -> Self {
        ConfigParseError {
            err_msg,
        }
    }
}

impl ToString for ConfigParseError {
    fn to_string(&self) -> String {
        format!("ConfigParseError: ({})", self.err_msg)
    }
}

impl<'a> From<&'a str> for ConfigParseError {
    fn from(err_msg: &str) -> Self {
        ConfigParseError {
            err_msg: err_msg.to_owned()
        }
    }
}

impl From<Error> for ConfigParseError {
    fn from(err: Error) -> Self {
        ConfigParseError::from(err.to_string())
    }
}

impl From<::std::io::Error> for ConfigParseError {
    fn from(err: ::std::io::Error) -> Self {
        ConfigParseError::from(err.to_string())
    }
}

impl From<::std::num::ParseIntError> for ConfigParseError {
    fn from(err: ::std::num::ParseIntError) -> Self {
        ConfigParseError::from(err.to_string())
    }
}
