use maxgraph_store::db::api::{EdgeResultIter, Edge, GraphError, EdgeWrapper};
use maxgraph_store::db::common::unsafe_util;

pub struct EdgeIterator<'a, E> where E: Edge {
    iter_vec: &'a [Box<dyn EdgeResultIter<E=E> + 'a>],
    cur: usize,
    err: Option<GraphError>,
}

impl<'a, E> Iterator for EdgeIterator<'a, E> where E: Edge {
    type Item = EdgeWrapper<'a, E>;

    fn next(&mut self) -> Option<EdgeWrapper<'a, E>> {
        if self.err.is_some() {
            return None;
        }
        loop {
            let iter = self.iter_vec.get(self.cur)?;
            let iter_mut = unsafe { unsafe_util::to_mut(iter) };
            if let Some(e) = iter_mut.next() {
                return Some(e);
            }
            if let Err(e) = iter.ok() {
                self.err = Some(e);
                return None;
            }
            self.cur += 1;
        }
    }
}

impl<'a, E> EdgeIterator<'a, E> where E: Edge {
    pub fn new(iter_vec: &'a [Box<dyn EdgeResultIter<E=E> + 'a>]) -> Self {
        EdgeIterator {
            iter_vec,
            cur: 0,
            err: None,
        }
    }
}
