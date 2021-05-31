use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::PartitionId;
use store::v2::global_graph::GlobalGraph;
use maxgraph_common::util;
use itertools::Itertools;
use maxgraph_store::db::api::GraphConfigBuilder;
use std::sync::Arc;

pub mod global_graph;
mod edge_iterator;
pub mod global_graph_schema;

pub fn create_global_graph(config: &StoreConfig, partition_ids: &Vec<PartitionId>) -> GlobalGraph {
    let paths = config.local_data_root.split(",").collect::<Vec<&str>>();
    let mut disks = Vec::with_capacity(paths.len());
    paths.iter().foreach(|path| {
        let graph_path = util::fs::path_join(*path, config.graph_name.as_str());
        let data_path = util::fs::path_join(graph_path.as_str(), "data");
        disks.push(data_path);
    });
    let mut config_builder = GraphConfigBuilder::new();
    config_builder.set_storage_engine("rocksdb");
    let graph_config = config_builder.build();
    GlobalGraph::new(disks, &graph_config, partition_ids).unwrap()
}
