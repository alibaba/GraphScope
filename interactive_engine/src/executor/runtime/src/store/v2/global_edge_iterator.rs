use maxgraph_store::db::api::EdgeResultIter;
use store::LocalStoreEdge;

pub struct GlobalEdgeIteratorImpl {
}

impl Iterator for GlobalEdgeIteratorImpl {
    type Item = LocalStoreEdge;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

impl GlobalEdgeIteratorImpl {
    pub fn new(iter: Box<dyn Iterator<Item=LocalStoreEdge>>) -> Self {
        GlobalEdgeIteratorImpl {
        }
    }
}
