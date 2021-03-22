use maxgraph_store::db::api::{EdgeResultIter, Edge};

pub struct EdgeIterator<'a, E> where E: Edge {
    iter_vec: Vec<Box<dyn EdgeResultIter<E=E> + 'a>>,
}

impl<'a, E> Iterator for EdgeIterator<'a, E> where E: Edge {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

impl<'a, E> EdgeIterator<'a, E> where E: Edge {
    pub fn new(iter_vec: Vec<Box<dyn EdgeResultIter<E=E> + 'a>>) -> Self {
        EdgeIterator {
            iter_vec,
        }
    }
}
