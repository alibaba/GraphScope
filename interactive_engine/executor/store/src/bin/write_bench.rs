use maxgraph_common::util::{fs, Timer};
use maxgraph_store::db::api::{GraphConfigBuilder, TypeDefBuilder, ValueType, Value};
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    let write_buffer_mb = &args[1];
    let compaction_style = &args[2];
    let background_job = &args[3];

    println!("write_buffer_mb {}, compaction_style {}, background_job {}", write_buffer_mb, compaction_style, background_job);
    let path = format!("write_bench_data_dir");
    fs::rmr(&path).unwrap();
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    builder.add_storage_option("write_buffer_mb", write_buffer_mb);
    builder.add_storage_option("compaction_style", compaction_style);
    builder.add_storage_option("background_jobs", background_job);
    let config = builder.build();
    let store = GraphStore::open(&config, &path).unwrap();
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
    let val = "c".repeat(str_len);
    println!("{}\t{}\t{}", "time(sec)", "speed(record/s)", "total");
    loop {
        let snapshot_id = i;
        let vertex_id = i;
        let mut properties = HashMap::new();
        properties.insert(1, Value::long(i));
        properties.insert(2, Value::string(&val));
        let mut hasher = DefaultHasher::new();
        vertex_id.hash(&mut hasher);
        let hash_id = hasher.finish();
        store.insert_overwrite_vertex(snapshot_id, hash_id as i64, label_id, &properties).unwrap();
        i += 1;
        if i % 500000 == 0 {
            let write_count = i - tmp_count;
            let total_time = timer.elasped_secs();
            let t = total_time - tmp_time;
            println!("{:.0}\t{:.2}\t{:.0}", total_time, write_count as f64 / t, i);
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

