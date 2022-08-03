use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use maxgraph_store::db::api::{GraphConfigBuilder, TypeDefBuilder, Value, ValueType};
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::db::util::{fs, time};

fn main() {
    let args: Vec<String> = env::args().collect();

    let write_buffer_mb = &args[1];
    let compaction_style = &args[2];
    let max_write_buffer_num = &args[3];
    let level_zero_compaction_trigger = &args[4];
    let max_level_base_mb = &args[5];
    let background_job = &args[6];
    let thread_count = &args[7];

    println!(
        "write_buffer_mb {}, compaction_style {}, background_job {}, thread_count {}",
        write_buffer_mb, compaction_style, background_job, thread_count
    );
    let path = format!("write_bench_data_dir");
    fs::rmr(&path).unwrap();
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    builder.add_storage_option("store.rocksdb.compression.type", "none");
    builder.add_storage_option("store.rocksdb.stats.dump.period.sec", "60");
    builder.add_storage_option("store.rocksdb.write.buffer.mb", write_buffer_mb);
    builder.add_storage_option("store.rocksdb.compaction.style", compaction_style);
    builder.add_storage_option("store.rocksdb.background.jobs", background_job);
    builder.add_storage_option("store.rocksdb.max.write.buffer.num", max_write_buffer_num);
    builder.add_storage_option("store.rocksdb.level0.compaction.trigger", level_zero_compaction_trigger);
    builder.add_storage_option("store.rocksdb.max.level.base.mb", max_level_base_mb);
    let config = builder.build();
    let store = Arc::new(GraphStore::open(&config, &path).unwrap());
    println!("store opened.");
    let mut type_def_builer = TypeDefBuilder::new();
    type_def_builer.version(1);
    type_def_builer.add_property(1, 1, "id".to_string(), ValueType::Long, None, true, "id".to_string());
    type_def_builer.add_property(
        2,
        2,
        "name".to_string(),
        ValueType::String,
        None,
        false,
        "name".to_string(),
    );
    let type_def = type_def_builer.build();
    let label_id = 1;
    store
        .create_vertex_type(1, 1, label_id, &type_def, 1)
        .unwrap();
    println!("schema created");
    let str_len = 100;
    let timer = time::Timer::new();
    let mut tmp_time = 0.0;
    let mut tmp_count = 0;
    let val = "c".repeat(str_len);
    let mut handles = Vec::new();
    let total_count = Arc::new(AtomicU64::new(0));
    let snapshot_idx = Arc::new(AtomicI64::new(1));
    for i in 0..thread_count.parse().unwrap() {
        let task_id = i;
        let counter = total_count.clone();
        let val = val.clone();
        let store = store.clone();
        let snapshot_idx = snapshot_idx.clone();
        println!("task {} starting", task_id);
        let handle = std::thread::spawn(move || {
            let mut idx = i * 100000000000 + 2;
            loop {
                let snapshot_id = snapshot_idx.load(Ordering::SeqCst);
                let vertex_id = idx;
                let mut properties = HashMap::new();
                properties.insert(1, Value::long(i));
                properties.insert(2, Value::string(&val));
                let mut hasher = DefaultHasher::new();
                vertex_id.hash(&mut hasher);
                let hash_id = hasher.finish();
                store
                    .insert_overwrite_vertex(snapshot_id, hash_id as i64, label_id, &properties)
                    .unwrap();
                idx += 1;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
    println!("{}\t{}\t{}", "time(sec)", "speed(record/s)", "total");
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(3));
        let total_write = total_count.load(Ordering::Relaxed);
        let write_count = total_write - tmp_count;
        let total_time = timer.elasped_secs();
        let t = total_time - tmp_time;
        println!("{:.0}\t{:.2}\t{:.0}", total_time, write_count as f64 / t, total_write);
        tmp_count = total_write;
        tmp_time = total_time;
        snapshot_idx.fetch_add(1, Ordering::SeqCst);
    });
    for handle in handles {
        handle.join().unwrap();
    }
}
