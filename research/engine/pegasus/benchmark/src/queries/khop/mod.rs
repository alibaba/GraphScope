use pegasus_graph::graph::Direction;

use crate::graph::Graph;

mod multi_src;
mod single_src;

#[inline]
pub fn one_hop<G: Graph>(id: u64, graph: &G) -> Box<dyn Iterator<Item = u64> + Send + 'static>
{
    graph.get_neighbor_ids(id, "person", "knows", Direction::Both)
}

pub use multi_src::packed_multi_src_k_hop;
pub use multi_src::unpacked_multi_src_k_hop;
pub use single_src::single_src_k_hop;
