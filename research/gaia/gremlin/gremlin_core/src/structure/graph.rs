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

use crate::generated::common as pb_common;
use crate::generated::gremlin as pb;
use crate::structure::codec::{pb_chain_to_filter, ParseError};
use crate::structure::{
    Direction, Edge, ElementFilter, Filter, Label, LabelId, PropKey, Vertex, ID,
};
use crate::{DynIter, DynResult, Element, FromPb};
use dyn_type::Object;
use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryParams<E: Element + Send + Sync> {
    pub labels: Vec<Label>,
    pub limit: Option<usize>,
    pub props: Option<Vec<PropKey>>,
    pub filter: Option<Arc<Filter<E, ElementFilter>>>,
    pub partitions: Option<Vec<u64>>,
    pub extra_params: Option<HashMap<String, Object>>,
}

impl<E: Element + Send + Sync> Default for QueryParams<E> {
    fn default() -> Self {
        QueryParams {
            labels: vec![],
            limit: None,
            props: None,
            filter: None,
            partitions: None,
            extra_params: None,
        }
    }
}

impl<E: Element + Send + Sync> FromPb<Option<pb::QueryParams>> for QueryParams<E> {
    fn from_pb(query_params_pb: Option<pb::QueryParams>) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        query_params_pb.map_or(Ok(QueryParams::default()), |query_params_pb| {
            QueryParams::default()
                .with_labels(query_params_pb.labels)?
                .with_filter(query_params_pb.predicates)?
                .with_limit(query_params_pb.limit)?
                .with_required_properties(query_params_pb.required_properties)?
                .with_extra_params(query_params_pb.extra_params)
        })
    }
}

impl<E: Element + Send + Sync> QueryParams<E> {
    fn with_labels(
        mut self, labels_pb: Option<pb::query_params::Labels>,
    ) -> Result<Self, ParseError> {
        if let Some(labels_pb) = labels_pb {
            self.labels = labels_pb.labels.into_iter().map(|l| Label::Id(l as LabelId)).collect();
        }
        Ok(self)
    }

    fn with_filter(mut self, filter_chain_pb: Option<pb::FilterChain>) -> Result<Self, ParseError> {
        if let Some(ref filter_chain_pb) = filter_chain_pb {
            if let Some(filter) = pb_chain_to_filter(filter_chain_pb)? {
                self.filter = Some(Arc::new(filter));
            }
        }
        Ok(self)
    }

    fn with_limit(mut self, limit_pb: Option<pb::query_params::Limit>) -> Result<Self, ParseError> {
        if let Some(limit_pb) = limit_pb {
            self.limit = Some(limit_pb.limit as usize);
        }
        Ok(self)
    }

    // props specify the properties we query for, e.g.,
    // Some(vec![prop1, prop2]) indicates we need prop1 and prop2,
    // Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property,
    fn with_required_properties(
        mut self, required_properties_pb: Option<pb::PropKeys>,
    ) -> Result<Self, ParseError> {
        if let Some(required_properties_pb) = required_properties_pb {
            let mut prop_keys = vec![];
            for prop_key in required_properties_pb.prop_keys {
                let prop_key = PropKey::from_pb(prop_key)?;
                prop_keys.push(prop_key);
            }
            // the cases of we need all properties or some specific properties
            if required_properties_pb.is_all || !prop_keys.is_empty() {
                self.props = Some(prop_keys)
            }
        }
        Ok(self)
    }

    // Extra query params for different storages
    fn with_extra_params(
        mut self, extra_params_pb: Option<pb::query_params::ExtraParams>,
    ) -> Result<Self, ParseError> {
        if let Some(extra_params_pb) = extra_params_pb {
            let mut extra_params = HashMap::new();
            for param in extra_params_pb.params {
                let param_value = match param.value.unwrap().item.unwrap() {
                    pb_common::value::Item::Boolean(b) => Ok(b.into()),
                    pb_common::value::Item::I32(i) => Ok(i.into()),
                    pb_common::value::Item::I64(i) => Ok(i.into()),
                    pb_common::value::Item::F64(f) => Ok(f.into()),
                    pb_common::value::Item::Str(s) => Ok(s.into()),
                    pb_common::value::Item::Blob(b) => Ok(b.into()),
                    _ => Err(ParseError::OtherErr("Unsupported extra params".to_string())),
                }?;
                extra_params.insert(param.key, param_value);
            }
            self.extra_params = Some(extra_params);
        }
        Ok(self)
    }

    pub fn get_extra_param(&self, key: &str) -> Option<&Object> {
        if let Some(ref extra_params) = self.extra_params {
            extra_params.get(key)
        } else {
            None
        }
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

    fn scan_edge(
        &self, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>>;

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    fn get_edge(
        &self, ids: &[ID], params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>>;

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Statement<ID, Vertex>>>;

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Statement<ID, Edge>>>;
}

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
