use std::sync::Arc;
use crate::graph::{Graph, TodoGraph};

mod graph;
mod queries;


fn main() {
    let graph = load_graph::<TodoGraph>();
    crate::queries::ic1::ic1(0, "Chau".to_string(), graph);
}


fn load_graph<G: Graph>() -> Arc<G> {
    unimplemented!()
}