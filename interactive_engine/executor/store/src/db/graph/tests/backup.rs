use crate::db::api::*;
use crate::db::graph::store::GraphStore;
use super::types;
use std::collections::HashMap;
use crate::db::api::multi_version_graph::MultiVersionGraph;
use crate::db::api::types::RocksVertex;

pub fn test_backup_engine<G: MultiVersionGraph>(graph: G, test_dir: &str) {
    let backup_path = format!("{}/backup", test_dir);
    let mut backup_engine = graph.open_backup_engine(&backup_path).unwrap();

    // insert data
    let schema_version = 1;
    graph.create_vertex_type(10, schema_version, 1, &types::create_test_type_def(1), schema_version).unwrap();
    let properties: HashMap<PropertyId, Value> = HashMap::new();
    graph.insert_overwrite_vertex(11, 1, 1, &properties).unwrap();
    graph.insert_overwrite_vertex(12, 2, 1, &properties).unwrap();

    // create the first backup
    let backup_1_id = backup_engine.create_new_backup().unwrap();
    // delete vertex id '2' and create the second backup
    graph.delete_vertex(13, 2, 1).unwrap();
    let backup_2_id = backup_engine.create_new_backup().unwrap();
    assert!(backup_2_id > backup_1_id);

    // verify backups
    let backup_list = backup_engine.get_backup_list();
    assert_eq!(backup_list.len(), 2);
    backup_list.iter().for_each(|i| {
        assert!(backup_engine.verify_backup(*i).is_ok());
    });

    // restore the first backup
    let restore_path_1 = format!("{}/restore_1", test_dir);
    backup_engine.restore_from_backup(&restore_path_1, backup_1_id).unwrap();
    // test backup
    let restore_store_1 = open_graph(&restore_path_1);
    assert_eq!(restore_store_1.get_vertex(15, 1, Some(1), None).unwrap().unwrap().get_vertex_id(), 1);
    assert_eq!(restore_store_1.get_vertex(16, 2, Some(1), None).unwrap().unwrap().get_vertex_id(), 2);

    // restore the latest(second) backup
    let restore_path_2 = format!("{}/restore_2", test_dir);
    backup_engine.restore_from_backup(&restore_path_2, backup_2_id).unwrap();
    // test backup
    let restore_store_2 = open_graph(&restore_path_2);
    assert_eq!(restore_store_2.get_vertex(17, 1, Some(1), None).unwrap().unwrap().get_vertex_id(), 1);
    assert!(restore_store_2.get_vertex(18, 2, Some(1), None).unwrap().is_none());
}

fn open_graph(path: &str) -> GraphStore {
    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    let config = builder.build();
    GraphStore::open(&config, path).unwrap()
}