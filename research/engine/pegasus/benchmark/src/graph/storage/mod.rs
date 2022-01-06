use std::sync::Arc;
use pegasus_graph::graph::{Direction, Vid};
use pegasus_graph::{MemLabeledTopoGraph};
use crate::graph::{FilterById, Graph, Vertex};

mod clickhouse;

pub trait PropsStore: Send + Sync + 'static {
    fn get_batch_vertices(&self, ids: &[Vid]) -> Vec<Vertex<Vid>>;

    fn get_id_filter<F: ToString>(&self, filter: F) -> Box<dyn FilterById<ID = Vid>>;
}

pub struct PropertyGraph<P: PropsStore> {
    topo: Arc<MemLabeledTopoGraph>,
    pros: P,
}

impl<P: PropsStore> Graph for PropertyGraph<P> {
    type VID = Vid;

    fn get_neighbor_ids(&self, src: Self::VID, edge_label: &str, dir: Direction) -> Box<dyn Iterator<Item=Self::VID> + Send + 'static> {
        Box::new(self.topo.get_neighbors_by_(src, edge_label, dir))
    }

    fn get_vertices_by_ids(&self, ids: &[Self::VID]) -> Vec<Vertex<Self::VID>> {

        self.pros.get_batch_vertices(ids)
    }

    fn prepare_filter_vertex<F: ToString>(&self, filter: F) -> Box<dyn FilterById<ID = Self::VID>> {
        self.pros.get_id_filter(filter)
    }
}


