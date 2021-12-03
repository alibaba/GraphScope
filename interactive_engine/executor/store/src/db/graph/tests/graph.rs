use std::collections::HashMap;
use crate::db::api::*;
use super::types;
use crate::db::api::multi_version_graph::MultiVersionGraph;

pub fn test_si_guard<G: MultiVersionGraph>(graph: G) {
    let mut schema_version = 1;
    graph.create_vertex_type(10, schema_version, 1, &types::create_test_type_def(1), schema_version).unwrap();
    schema_version += 1;
    graph.create_vertex_type(10, schema_version,2, &types::create_test_type_def(2), schema_version).unwrap();
    schema_version += 1;
    graph.create_vertex_type(12, schema_version,3, &types::create_test_type_def(3), schema_version).unwrap();
    schema_version += 1;
    assert!(graph.create_vertex_type(11, schema_version,4, &types::create_test_type_def(4), schema_version).is_err());
    schema_version += 1;
    graph.drop_vertex_type(15, schema_version,1).unwrap();
    schema_version += 1;
    assert!(graph.create_vertex_type(13, schema_version,5, &types::create_test_type_def(5), schema_version).is_err());
    let properties: HashMap<PropertyId, Value> = HashMap::new();
    graph.insert_overwrite_vertex(15, 1, 2, &properties).unwrap();
    graph.insert_overwrite_vertex(17, 1, 2, &properties).unwrap();
    schema_version += 1;
    assert!(graph.create_vertex_type(16, schema_version,5, &types::create_test_type_def(5), schema_version).is_err());
    schema_version += 1;
    assert!(graph.drop_vertex_type(16, schema_version,2).is_err());
    schema_version += 1;
    assert!(graph.insert_overwrite_vertex(16, 1, 2, &properties).is_err());
    schema_version += 1;
    assert!(graph.delete_vertex(16, 1, 2).is_err());
    schema_version += 1;
    assert!(graph.insert_update_vertex(16, 1, 2, &properties).is_err());
    schema_version += 1;

    graph.create_edge_type(20, schema_version,10, &types::create_test_type_def(10)).unwrap();
    schema_version += 1;
    let edge_type = EdgeKind::new(10, 11, 12);
    graph.add_edge_kind(20, schema_version,&edge_type, schema_version).unwrap();
    schema_version += 1;
    assert!(graph.insert_overwrite_vertex(16, 1, 2, &properties).is_err());
    graph.insert_update_edge(20, EdgeId::new(1,2,3), &edge_type, true, &properties).unwrap();
    graph.insert_update_edge(21, EdgeId::new(1,2,3), &edge_type, true, &properties).unwrap();
    graph.insert_overwrite_edge(21, EdgeId::new(1,2,3), &edge_type, true, &properties).unwrap();
    assert!(graph.insert_update_edge(20, EdgeId::new(1,2,3), &edge_type, true, &properties).is_err());
    assert!(graph.drop_edge_type(19, schema_version,10).is_err());
    schema_version += 1;
    assert!(graph.remove_edge_kind(19, schema_version,&edge_type).is_err());
    schema_version += 1;
    assert!(graph.delete_edge(19, EdgeId::new(1, 2, 3), &edge_type, true).is_err());
    assert!(graph.create_edge_type(19, schema_version,20, &types::create_test_type_def(2)).is_err());
    schema_version += 1;
    assert!(graph.add_edge_kind(19, schema_version,&edge_type, schema_version).is_err());
}

