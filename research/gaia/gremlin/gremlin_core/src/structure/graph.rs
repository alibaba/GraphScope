//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//! http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::structure::{Direction, Edge, ElementFilter, Filter, Label, Vertex, ID};
use crate::{DynIter, DynResult, Element};

#[derive(Clone)]
pub struct QueryParams<E: Element + Send + Sync> {
    pub labels: Vec<Label>,
    pub limit: Option<usize>,
    pub props: Option<Vec<String>>,
    pub filter: Option<Arc<Filter<E, ElementFilter>>>,
}

impl<E: Element + Send + Sync> QueryParams<E> {
    pub fn new() -> Self {
        QueryParams { labels: vec![], limit: None, props: None, filter: None }
    }

    pub fn set_filter(&mut self, filter: Filter<E, ElementFilter>) {
        self.filter = Some(Arc::new(filter))
    }
}

pub trait Statement<I, O>: Send + 'static {
    fn exec(&self, next: I) -> DynResult<DynIter<O>>;
}

impl<I, O, F: 'static> Statement<I, O> for F
where
    F: Fn(I) -> DynResult<DynIter<O>> + Send + Sync,
{
    fn exec(&self, param: I) -> DynResult<DynIter<O>> {
        (self)(param)
    }
}

pub trait GraphProxy: Send + Sync {
    fn scan_vertex(
        &self, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Statement<ID, Vertex>>>;

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Statement<ID, Edge>>>;
}

use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref GRAPH_PROXY: AtomicPtr<Arc<dyn GraphProxy>> = AtomicPtr::default();
}

pub fn register_graph(graph: Arc<dyn GraphProxy>) {
    let ptr = Box::into_raw(Box::new(graph));
    GRAPH_PROXY.store(ptr, Ordering::SeqCst);
}

pub fn get_graph() -> Option<Arc<dyn GraphProxy>> {
    let ptr = GRAPH_PROXY.load(Ordering::SeqCst);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { (*ptr).clone() })
    }
}
