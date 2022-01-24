use maxgraph_common::util::{fs, Timer};
use maxgraph_store::db::api::{GraphConfigBuilder, TypeDefBuilder, ValueType, Value};
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use std::collections::HashMap;

fn main() {
    let path = format!("write_bench_data_dir");
    fs::rmr(&path).unwrap();
    let store = open_graph_store(&path);
    println!("store opened.");
    let mut type_def_builer = TypeDefBuilder::new();
    type_def_builer.version(1);
    type_def_builer.add_property(1, 1, "id".to_string(), ValueType::Long, None, true, "id".to_string());
    type_def_builer.add_property(2, 2, "name".to_string(), ValueType::String, None, false, "name".to_string());
    let type_def = type_def_builer.build();
    let label_id = 1;
    store.create_vertex_type(1, 1, label_id, &type_def, 1).unwrap();
    println!("schema created");
    let mut i = 2;
    let str_len = 100;
    let timer = Timer::new();
    let mut tmp_time = 0.0;
    let mut tmp_count = 0;
    loop {
        let snapshot_id = i;
        let vertex_id = i;
        let mut properties = HashMap::new();
        properties.insert(1, Value::long(i));
        properties.insert(2, Value::string(&String::from_utf8(vec!['a' as u8; str_len]).unwrap()));
        store.insert_overwrite_vertex(snapshot_id, vertex_id, label_id, &properties);
        i += 1;
        if i % 200000 == 0 {
            let write_count = i - tmp_count;
            let total_time = timer.elasped_secs();
            let t = total_time - tmp_time;
            println!("{:.0}\t{:.2}", total_time, write_count as f64 / t);
            tmp_count = i;
            tmp_time = total_time;
        }
    }
}

fn open_graph_store(path: &str) -> GraphStore {
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    let config = builder.build();
    GraphStore::open(&config, path).unwrap()
}

