use std::any::Any;
use std::collections::HashSet;

use crate::col_table::ColTable;
use crate::graph::IndexType;

pub struct NbrIter<'a, I> {
    inner: std::slice::Iter<'a, I>,
}

impl<'a, I> NbrIter<'a, I> {
    pub fn new(vec: &'a Vec<I>, start: usize, end: usize) -> Self {
        NbrIter { inner: vec[start..end].iter() }
    }
}

impl<'a, I: IndexType> Iterator for NbrIter<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct NbrOffsetIter<'a, I> {
    inner: std::slice::Iter<'a, I>,
    offset: usize,
}

impl<'a, I> NbrOffsetIter<'a, I> {
    pub fn new(vec: &'a Vec<I>, start: usize, end: usize) -> Self {
        NbrOffsetIter { inner: vec[start..end].iter(), offset: start }
    }
}

impl<'a, I: IndexType> Iterator for NbrOffsetIter<'a, I> {
    type Item = (I, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(x) => {
                let ret = (x.clone(), self.offset);
                self.offset += 1;
                Some(ret)
            }
            None => None,
        }
    }
}

pub trait CsrTrait<I: IndexType>: Send + Sync {
    fn vertex_num(&self) -> I;
    fn max_edge_offset(&self) -> usize;
    fn edge_num(&self) -> usize;
    fn degree(&self, u: I) -> usize;
    fn serialize(&self, path: &String);
    fn deserialize(&mut self, path: &String);

    fn get_edges(&self, u: I) -> Option<NbrIter<I>>;
    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>>;

    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn delete_vertices(&mut self, vertices: &HashSet<I>);
    fn delete_edges(&mut self, edges: &HashSet<(I, I)>, reverse: bool);
    fn delete_edges_with_props(&mut self, edges: &HashSet<(I, I)>, reverse: bool, table: &mut ColTable);
}

#[derive(Debug)]
pub enum CsrBuildError {
    OffsetOutOfCapacity,
    UnfinishedVertex,
}
