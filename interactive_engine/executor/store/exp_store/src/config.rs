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

use crate::common::{Label, LabelId};
use crate::error::{GDBError, GDBResult};
use crate::graph_db_impl::{IndexData, LargeGraphDB, MutableGraphDB};
use crate::io::import;
use crate::schema::LDBCGraphSchema;
use crate::table::PropertyTableTrait;
use petgraph::graph::{DiGraph, IndexType};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub trait JsonConf<T: Serialize + DeserializeOwned = Self>: Serialize {
    fn from_json_file<P: AsRef<Path>>(path: P) -> std::io::Result<T> {
        let file = File::open(path)?;
        serde_json::from_reader::<File, T>(file).map_err(std::io::Error::from)
    }

    fn from_json(json: String) -> std::io::Result<T> {
        serde_json::from_str(&json).map_err(std::io::Error::from)
    }

    fn to_json_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let file = File::create(path)?;
        serde_json::to_writer_pretty::<File, Self>(file, self).map_err(std::io::Error::from)
    }

    fn to_json(&self) -> std::io::Result<String> {
        serde_json::to_string_pretty(self).map_err(std::io::Error::from)
    }
}

// for graph schema
pub const FILE_SCHEMA: &'static str = "schema.json";
pub const DIR_GRAPH_SCHEMA: &'static str = "graph_schema";

// for graph binary data
pub const DIR_BINARY_DATA: &'static str = "graph_data_bin";
pub const FILE_GRAPH_STRUCT: &'static str = "graph_struct";
pub const FILE_NODE_PPT_DATA: &'static str = "node_property";
pub const FILE_EDGE_PPT_DATA: &'static str = "edge_property";
pub const FILE_INDEX_DATA: &'static str = "index_data";
pub const PARTITION_PREFIX: &'static str = "partition_";

/// The configuration to open an graph database for loading and querying data.
/// Currently, we use `MutableGraphDB` by calling `Self::new()` for loading data,
/// and `LargeGraphDB` by calling `Self::open()` for querying data.
///
/// Given a `root_dir`, the graph is maintained as the following directory structures:
/// <`root_dir`>
/// ---- DIR_BINARY_DATA (graph_data_bin) # a directory of graph data
/// ---- ---- FILE_GRAPH_STRUCT (graph_struct) # a binary file that encodes graph structure
/// ---- ---- FILE_NODE_PPT_DATA (node_property) # a binary file that encodes vertices' properties
/// ---- ---- FILE_EDGE_PPT_DATA (edge_property) # a binary file that encodes edges' properties
/// ---- ---- FILE_INDEX_DATA (index_data) # a binary file that encodes any index data
/// ---- DIR_GRAPH_SCHEMA (graph_schema) # a directory of schema
/// ---- ---- FILE_SCHEMA (schema.json)  # a json file that contains the graph schema (user given)
///
/// # Example
/// ```
/// extern crate tempdir;
/// use graph_store::prelude::*;
///
/// let temp_dir = tempdir::TempDir::new("doc-test-config")
///                     .expect("Open temp folder error");
/// let root_dir = temp_dir.path();
/// let mut  mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
///     .root_dir(root_dir)
///     .partition(0)
///     .new();
///
/// mut_graph.add_vertex(0, [0, 0]);
/// mut_graph.add_vertex(1, [0, 0]);
/// mut_graph.add_edge(0, 1, 0);
///
/// // Shall see the above graph directories created under data/test_data
/// // Note that user must place the schema file as "data/test_data/graph_schema/schema.json"
/// mut_graph.export().expect("Export graph data error");
///
/// let graph: LargeGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
///     .root_dir(root_dir)
///     .schema_file("data/schema.json")
///     .partition(0)
///     .open()
///     .expect("Open graph error");
///
/// assert_eq!(
///     vec![1],
///     graph.get_out_vertices(0, Some(&vec![0])).map(|v| v.get_id()).collect::<Vec<DefaultId>>()
/// );
///
/// ```
#[derive(Clone, Debug)]
pub struct GraphDBConfig {
    /// The root directory in which the encoded data (and their directories are maintained)
    pub root_dir: PathBuf,
    /// The path to the schema file of the graph DB
    schema_file: PathBuf,
    /// The initial number of vertices, used only for building graphs via `MutableGraphDB`
    init_vertices: usize,
    /// The initial number of edges, used only for building graphs via `MutableGraphDB`
    init_edges: usize,
    /// The number of vertex labels, used only for building graphs via `MutableGraphDB`
    number_vertex_labels: usize,
    /// The partition id of this graph data
    partition: usize,
}

impl Default for GraphDBConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::default(),
            schema_file: PathBuf::default(),
            init_vertices: 100,
            init_edges: 1000,
            number_vertex_labels: 20,
            partition: 0,
        }
    }
}

impl GraphDBConfig {
    pub fn root_dir<P: AsRef<Path>>(mut self, root_dir: P) -> Self {
        self.root_dir = root_dir.as_ref().to_path_buf();
        self
    }

    pub fn schema_file<P: AsRef<Path>>(mut self, schema_path: P) -> Self {
        self.schema_file = schema_path.as_ref().to_path_buf();
        self
    }

    pub fn partition(mut self, partition: usize) -> Self {
        self.partition = partition;
        self
    }

    pub fn init_vertices(mut self, init_vertices: usize) -> Self {
        self.init_vertices = init_vertices;
        self
    }

    pub fn init_edges(mut self, init_edges: usize) -> Self {
        self.init_edges = init_edges;
        self
    }

    pub fn number_vertex_labels(mut self, number_vertex_labels: usize) -> Self {
        self.number_vertex_labels = number_vertex_labels;
        self
    }

    /// Open an existing **read-only** graph database from `Self::root_dir`.
    pub fn open<G, I, N, E>(&self) -> GDBResult<LargeGraphDB<G, I, N, E>>
    where
        G: IndexType + Serialize + DeserializeOwned + Send + Sync,
        I: IndexType + Serialize + DeserializeOwned + Send + Sync,
        N: PropertyTableTrait + Send + Sync + 'static,
        E: PropertyTableTrait + Send + Sync + 'static,
    {
        info!("Partition {:?} reading binary file...", self.partition);
        let timer = Instant::now();
        let mut graph_schema = LDBCGraphSchema::from_json_file(&self.schema_file)?;
        // trim useless fields include vertex's label, edges' start_id and end_id
        graph_schema.trim();

        let root_dir = self.root_dir.join(DIR_BINARY_DATA);
        let mut entries = std::fs::read_dir(&root_dir)?;

        // The opening logic is:
        //     * If the directory has more than one partitions of data, open the specified partition
        //     * If the directory has jut one partition of data, open it regardless of which
        //       partition has been specified
        let mut partition_dir = PathBuf::new();
        let mut which_part = 0;
        while let Some(entry) = entries.next() {
            let curr_path = entry?.path();
            let path_str = curr_path
                .file_name()
                .ok_or(GDBError::UnknownError)?
                .to_str()
                .ok_or(GDBError::UnknownError)?
                .to_string();

            if !path_str.starts_with(PARTITION_PREFIX) {
                // skip invalid data
                continue;
            } else {
                if !partition_dir.exists() {
                    partition_dir = curr_path;
                    // The partition directory will end with the suffix of "_<part-id>"
                    if let Some(idx) = path_str.rfind('_') {
                        which_part = path_str.as_str()[idx + 1..].parse::<usize>()?;
                    }
                } else {
                    // Means there are more than one partitions
                    partition_dir =
                        root_dir.join(format!("{}{}", PARTITION_PREFIX, self.partition));
                    // set which_part back to -1 and use self.partition as the partition id
                    which_part = self.partition;
                    break;
                }
            }
        }

        let file_graph_struct = partition_dir.join(FILE_GRAPH_STRUCT);
        let file_node_ppt_data = partition_dir.join(FILE_NODE_PPT_DATA);
        let file_edge_ppt_data = partition_dir.join(FILE_EDGE_PPT_DATA);
        let file_index_data = partition_dir.join(FILE_INDEX_DATA);

        let graph_handle =
            std::thread::spawn(move || import::<DiGraph<Label, LabelId, I>, _>(&file_graph_struct));
        let v_prop_handle = std::thread::spawn(move || N::import(&file_node_ppt_data));
        let e_prop_handle = std::thread::spawn(move || E::import(&file_edge_ppt_data));
        let index_handle =
            std::thread::spawn(move || import::<IndexData<G, I>, _>(&file_index_data));

        let graph = graph_handle.join()??;
        let vertex_prop_table = v_prop_handle.join()??;
        let edge_prop_table = e_prop_handle.join()??;
        let index_data = index_handle.join()??;

        let graph_db = LargeGraphDB {
            partition: which_part,
            graph,
            graph_schema: Arc::new(graph_schema),
            vertex_prop_table,
            edge_prop_table,
            index_data,
        };

        info!("Time elapsed: {:?}", timer.elapsed().as_secs_f64());

        Ok(graph_db)
    }

    /// New a graph database to build from the raw data
    pub fn new<G, I, N, E>(&self) -> MutableGraphDB<G, I, N, E>
    where
        G: Eq + IndexType + Send + Sync,
        I: IndexType + Send + Sync,
        N: PropertyTableTrait + Sync,
        E: PropertyTableTrait + Sync,
    {
        let graph = DiGraph::<_, _, I>::with_capacity(self.init_vertices, self.init_edges);
        let partition_dir = self
            .root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("{}{}", PARTITION_PREFIX, self.partition));

        let vertex_ppt_dir = partition_dir.join(FILE_NODE_PPT_DATA);
        let edge_ppt_dir = partition_dir.join(FILE_EDGE_PPT_DATA);

        let vertex_prop_table = N::new(&vertex_ppt_dir);
        let edge_prop_table = E::new(&edge_ppt_dir);

        MutableGraphDB {
            root_dir: self.root_dir.clone(),
            partition: self.partition,
            graph,
            vertex_prop_table,
            edge_prop_table,
            index_data: IndexData::new(self.number_vertex_labels),
        }
    }

    pub fn schema(&self) -> GDBResult<LDBCGraphSchema> {
        Ok(LDBCGraphSchema::from_json_file(&self.schema_file)?)
    }
}
