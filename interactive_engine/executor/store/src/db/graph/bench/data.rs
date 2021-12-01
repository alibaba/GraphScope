use std::collections::HashMap;
use crate::db::api::*;

pub fn gen_properties(type_def: &TypeDef) -> HashMap<PropertyId, Value> {
    let mut ret = HashMap::new();
    for prop_def in type_def.get_prop_defs() {
        let v = gen_property(prop_def.r#type);
        ret.insert(prop_def.id, v);
    }
    ret
}

fn gen_property(r#type: ValueType) -> Value {
    match r#type {
        ValueType::Int => Value::int(100),
        ValueType::Long => Value::long(101),
        ValueType::Float => Value::float(10.1),
        ValueType::Double => Value::double(123.4),
        ValueType::String => Value::string(&String::from_utf8(vec!['a' as u8; 128]).unwrap()),
        _ => unimplemented!()
    }
}

pub fn gen_one_string_properties(type_def: &TypeDef, len: usize) -> HashMap<PropertyId, Value> {
    let prop_def = type_def.get_prop_defs().next().unwrap();
    assert_eq!(prop_def.r#type, ValueType::String);
    let mut ret = HashMap::new();
    ret.insert(prop_def.id, gen_string_property(len));
    ret
}

fn gen_string_property(len: usize) -> Value {
    Value::string(&String::from_utf8(vec!['a' as u8; len]).unwrap())
}

