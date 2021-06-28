use maxgraph_store::api::{GlobalGraphQuery, Vertex, Edge};
use std::sync::Arc;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use gremlin_core::compiler::GremlinJobCompiler;

pub fn initialize_job_compiler<V, VI, E, EI>(_graph_query: Arc<dyn GlobalGraphQuery<V=V, E=E, VI=VI, EI=EI>>,
                                             _graph_partitioner: Arc<dyn GraphPartitionManager>,
                                             _num_servers: usize,
                                             _server_index: u64)
    -> GremlinJobCompiler
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static, {
    // TODO should be implement in integration
    unimplemented!()
}
