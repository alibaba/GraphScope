#![allow(dead_code)]
use std::collections::HashMap;
use crate::db::api::*;

pub fn gen_vertex_properties(si: SnapshotId, label: LabelId, id: VertexId, type_def: &TypeDef) -> HashMap<PropertyId, Value> {
    let mut map = HashMap::new();
    for prop_def in type_def.get_prop_defs() {
        let p = vertex_prop(si, label, id, prop_def.r#type);
        map.insert(prop_def.id, p);
    }
    map
}

pub fn gen_vertex_update_properties(si: SnapshotId, label: LabelId, id: VertexId, type_def: &TypeDef) -> HashMap<PropertyId, Value> {
    let mut map = HashMap::new();
    let x = si as i64 + label as i64 + id as i64;
    let count = ValueType::count() as i64;
    for i in x..x+count/2 {
        let prop_id = (i % count) as PropertyId + 1;
        let prop_def = type_def.get_prop_def(prop_id).unwrap();
        let v = vertex_prop(si, label, id, prop_def.r#type);
        map.insert(prop_id, v);
    }
    map
}

pub fn gen_edge_properties(si: SnapshotId, edge_type: &EdgeKind, id: &EdgeId, type_def: &TypeDef) -> HashMap<PropertyId, Value> {
    let mut map = HashMap::new();
    for prop_def in type_def.get_prop_defs() {
        let p = edge_prop(si, edge_type, id, prop_def.r#type);
        map.insert(prop_def.id, p);
    }
    map
}

pub fn gen_edge_update_properties(si: SnapshotId, edge_type: &EdgeKind, id: &EdgeId, type_def: &TypeDef) -> HashMap<PropertyId, Value> {
    let mut map = HashMap::new();
    let x = si as i64 + (edge_type.edge_label_id +edge_type.src_vertex_label_id +edge_type.dst_vertex_label_id) as i64 + (id.src_id+id.dst_id+id.inner_id) as i64;
    let count = ValueType::count() as i64;
    for i in x..x+count/2 {
        let prop_id = (i % count) as PropertyId + 1;
        let prop_def = type_def.get_prop_def(prop_id).unwrap();
        let v = edge_prop(si, edge_type, id, prop_def.r#type);
        map.insert(prop_id, v);
    }
    map
}

pub fn gen_edge_prop_map(si: SnapshotId, edge_type: &EdgeKind, id: &EdgeId, prop_defs: &Vec<&PropDef>) -> HashMap<PropertyId, Value> {
    let mut map = HashMap::new();
    for prop_def in prop_defs {
        let p = edge_prop(si, edge_type, id, prop_def.r#type);
        map.insert(prop_def.id, p);
    }
    map
}

fn vertex_prop(si: SnapshotId, label: LabelId, id: VertexId, r#type: ValueType) -> Value {
    let s = si as i64;
    let x = label as i64;
    let y = id as i64;
    let s2 = si as f64;
    let x2 = x as f64;
    let y2 = y as f64;
    match r#type {
        ValueType::Bool => {
            let v = (x + y + s) % 2 == 0;
            Value::bool(v)
        },
        ValueType::Char => {
            let v = (x + y * 2 + s) % 128;
            Value::char(v as u8)
        },
        ValueType::Short => {
            let v = (x * 3 + y - 1 + s) % 20000;
            Value::short(v as i16)
        },
        ValueType::Int => {
            let v = (x * x + y * 23 + 123 + s) % 1000000007;
            Value::int(v as i32)
        },
        ValueType::Long => {
            let v = x * x * x + y * y - 1234 * x + 3333 * y + 1 + s;
            Value::long(v)
        },
        ValueType::Float => {
            let v = x2 * 1.24 + y2 / 3.0 + s2;
            Value::float(v as f32)
        },
        ValueType::Double => {
            let v = x2 * x2 / 0.222 + y2 * x2 / 0.311 + s2;
            Value::double(v)
        },
        ValueType::String => {
            let v = format!("{}#{}#{}", s, x, y);
            Value::string(&v)
        }
        ValueType::Bytes => {
            let v = format!("{}-{}-{}", s, x, y);
            Value::bytes(v.as_bytes())
        },
        ValueType::IntList => {
            let v = vec![x as i32, y as i32, (x + y) as i32, (x - y) as i32, s as i32];
            Value::int_list(&v)
        },
        ValueType::LongList => {
            let v = vec![y, x, y - x, y + x, s];
            Value::long_list(&v)
        },
        ValueType::FloatList => {
            let v = vec![x2 as f32 + 1.23, y2 as f32 * 2.2, (x2 * y2) as f32, (x2 / (y2 + 0.1)) as f32, s2 as f32];
            Value::float_list(&v)
        },
        ValueType::DoubleList => {
            let v = vec![x2 + y2, x2 - y2, x2 * y2 + 1.11, x2 / (y2 + 0.1), s2];
            Value::double_list(&v)
        },
        ValueType::StringList => {
            let v = vec![format!("{}", x), format!("{}", y), format!("{}_{}", x, y), format!("{}", s)];
            Value::string_list(&v)
        }
    }
}

fn edge_prop(si: SnapshotId, edge_type: &EdgeKind, id: &EdgeId, r#type: ValueType) -> Value {
    let s = si as i64;
    let x = (edge_type.edge_label_id + edge_type.src_vertex_label_id * 2 + edge_type.dst_vertex_label_id / 3) as i64;
    let y = (id.src_id * 2 + id.dst_id / 2 + id.inner_id) as i64;
    let s2 = si as f64;
    let x2 = x as f64;
    let y2 = y as f64;
    match r#type {
        ValueType::Bool => {
            let v = (x + y + s) % 2 == 0;
            Value::bool(v)
        },
        ValueType::Char => {
            let v = (x + y * 2 + s) % 128;
            Value::char(v as u8)
        },
        ValueType::Short => {
            let v = (x * 3 + y - 1 + s) % 20000;
            Value::short(v as i16)
        },
        ValueType::Int => {
            let v = (x * x + y * 23 + 123 + s) % 1000000007;
            Value::int(v as i32)
        },
        ValueType::Long => {
            let v = x * x * 2 + y * 100 - 1234 * x + 3333 * y + 1 + s;
            Value::long(v)
        },
        ValueType::Float => {
            let v = x2 * 1.24 + y2 / 3.0 + s2;
            Value::float(v as f32)
        },
        ValueType::Double => {
            let v = x2 * x2 / 0.222 + y2 * x2 / 0.311 + s2;
            Value::double(v)
        },
        ValueType::String => {
            let v = format!("{}#{}#{}", s, x, y);
            Value::string(&v)
        }
        ValueType::Bytes => {
            let v = format!("{}-{}-{}", s, x, y);
            Value::bytes(v.as_bytes())
        },
        ValueType::IntList => {
            let v = vec![x as i32, y as i32, (x + y) as i32, (x - y) as i32, s as i32];
            Value::int_list(&v)
        },
        ValueType::LongList => {
            let v = vec![y, x, y - x, y + x, s];
            Value::long_list(&v)
        },
        ValueType::FloatList => {
            let v = vec![x2 as f32 + 1.23, y2 as f32 * 2.2, (x2 * y2) as f32, (x2 / (y2 + 0.1)) as f32, s2 as f32];
            Value::float_list(&v)
        },
        ValueType::DoubleList => {
            let v = vec![x2 + y2, x2 - y2, x2 * y2 + 1.11, x2 / (y2 + 0.1), s2];
            Value::double_list(&v)
        },
        ValueType::StringList => {
            let v = vec![format!("{}", x), format!("{}", y), format!("{}_{}", x, y), format!("{}", s)];
            Value::string_list(&v)
        }
    }
}
