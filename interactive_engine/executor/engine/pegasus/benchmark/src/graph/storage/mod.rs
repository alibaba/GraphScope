use std::collections::HashMap;
use std::sync::Arc;

use pegasus_graph::graph::{Direction };
use pegasus_graph::MemLabeledTopoGraph;

use crate::graph::{Graph, Value, Vertex};

mod clickhouse;

pub trait PropsStore: Send + 'static {
    fn get_vertices(&self, v_type: &str, ids: &[u64]) -> Vec<(u64, HashMap<String, Value>)>;

    fn select_vertices<F: ToString>(&self, v_type: &str, ids: &[u64], filter: F) -> Vec<u64>;
}

pub struct PropertyGraph<P: PropsStore> {
    topo: Arc<MemLabeledTopoGraph>,
    pros: P,
}

impl<P: PropsStore> Graph for PropertyGraph<P> {

    fn get_neighbor_ids(
        &self, src_id: u64 , src_type: &str, edge_label: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        Box::new(
            self.topo
                .get_neighbors_through(src_id, src_type, edge_label, dir),
        )
    }

    fn get_vertices_by_ids(&self, v_type: &str, ids: &[u64]) -> Vec<Vertex> {
        let label_id = MemLabeledTopoGraph::get_label_id(v_type).expect("label not found");
        let props = self.pros.get_vertices(v_type, ids);
        let mut vertices = Vec::with_capacity(props.len());
        for (id, p)  in props {
            let id = (label_id, id).into();
            vertices.push(Vertex::new(id, p));
        }
        vertices
    }

    fn filter_vertex<F: ToString>(&self, v_type: &str, ids: &[u64], filter: F) -> Vec<u64> {
        self.pros.select_vertices(v_type, ids, filter)
    }
}
