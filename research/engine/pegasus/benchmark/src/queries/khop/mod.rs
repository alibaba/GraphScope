use pegasus_graph::graph::Direction;
use pegasus::Data;
use crate::graph::Graph;

mod multi_src;
mod single_src;

#[inline]
pub fn one_hop<G: Graph>(id: G::VID, graph: &G) -> Box<dyn Iterator<Item = G::VID> + Send + 'static> where G::VID: Data {
    graph.get_neighbor_ids(id, "knows", Direction::Both)
}

pub use multi_src::packed_multi_src_k_hop;
pub use multi_src::unpacked_multi_src_k_hop;
pub use single_src::single_src_k_hop;
