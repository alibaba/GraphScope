#![allow(dead_code)]
use crate::db::api::*;

pub fn create_string_only_type_def() -> TypeDef {
    let mut builder = TypeDefBuilder::new();
    builder.version(1)
        .add_property(1, 1, "str_only".to_string(), ValueType::String, None, false, "cmt".to_string());
    builder.build()
}

pub fn create_one_property_type_def(r#type: ValueType) -> TypeDef {
    let mut builder = TypeDefBuilder::new();
    builder.version(1)
        .add_property(1, 1, "one_property".to_string(), r#type, None, false, "cmt".to_string());
    builder.build()
}

pub fn multi_numeric_properties_type_def(count: usize) -> TypeDef {
    let mut builder = TypeDefBuilder::new();
    builder.version(1);
    let types = vec![ValueType::Int, ValueType::Long, ValueType::Float, ValueType::Double];
    for i in 0..count {
        let r#type = types[i % types.len()];
        builder.add_property(i as PropertyId + 1, i as PropertyId + 1, "multi_num".to_string(), r#type, None, false, "cmt".to_string());
    }
    builder.build()
}

pub fn multi_string_properties_type_def(count: usize) -> TypeDef {
    let mut builder = TypeDefBuilder::new();
    builder.version(1);
    for i in 0..count {
        builder.add_property(i as PropertyId + 1, i as PropertyId + 1, "multi_str".to_string(), ValueType::String, None, false, "cmt".to_string());
    }
    builder.build()
}
