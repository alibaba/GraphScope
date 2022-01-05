use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use pegasus_graph::graph::{Direction, Vid};

mod storage;

#[allow(dead_code)]
pub enum Str {
    Own(String),
    Ref(&'static str),
}

impl Str {
    pub fn as_str(&self) -> &str {
        match self {
            Str::Own(v) => v.as_str(),
            Str::Ref(v) => *v,
        }
    }
}

pub enum ILP {
    ID,
    Label,
    Property(Str),
}

impl From<&'static str> for ILP {
    fn from(v: &'static str) -> Self {
        if v == "~id" {
            ILP::ID
        } else if v == "~label" {
            ILP::Label
        } else {
            ILP::Property(Str::Ref(v))
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Value {
    Int(u64),
    Float(f64),
    Str(String),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.eq(b),
            (Value::Float(a), Value::Float(b)) => a.eq(b),
            (Value::Str(a), Value::Str(b)) => a.eq(b),
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

pub struct Vertex<VID> {
    pub id: VID,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

pub struct OrderBy {
    pub by: ILP,
    is_asc: bool,
}

impl OrderBy {
    pub fn asc_by<T: Into<ILP>>(by: T) -> Self {
        OrderBy { by: by.into(), is_asc: true }
    }

    #[allow(dead_code)]
    pub fn desc_by<T: Into<ILP>>(by: T) -> Self {
        OrderBy { by: by.into(), is_asc: false }
    }
}

impl<VID: VertexId> Vertex<VID> {
    pub fn cmp_by(&self, other: &Vertex<VID>, opts: &[OrderBy]) -> Ordering {
        for p in opts {
            let mut ord = match p.by {
                ILP::ID => self.id.get_id().cmp(&other.id.get_id()),
                ILP::Label => self.label.cmp(&other.label),
                ILP::Property(ref v) => {
                    let name = v.as_str();
                    if let Some(va) = self.properties.get(name) {
                        if let Some(vb) = other.properties.get(name) {
                            if let Some(ord) = va.partial_cmp(vb) {
                                ord
                            } else {
                                panic!("can't compare between {:?} and {:?}", va, vb);
                            }
                        } else {
                            panic!("property {} not found;", name)
                        }
                    } else {
                        panic!("property {} not found;", name)
                    }
                }
            };

            if !p.is_asc {
                ord = ord.reverse();
            }

            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

pub trait FilterById: Send + 'static {
    type ID;
    fn exec(&self, ids: &[Self::ID]) -> Box<dyn Iterator<Item = Self::ID> + Send + 'static>;
}

pub trait VertexId : Clone + Eq + PartialEq + Hash + Send + 'static {
    fn get_id(&self) -> u64;
}

impl VertexId for u64 {
    fn get_id(&self) -> u64 {
        *self
    }
}

impl VertexId for Vid {
    fn get_id(&self) -> u64 {
        self.vertex_id()
    }
}


pub trait Graph: Send + Sync + 'static {
    type VID: VertexId;

    fn get_neighbor_ids(
        &self, src: Self::VID, edge_label: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = Self::VID> + Send + 'static>;

    fn get_vertices_by_ids(&self, ids: &[Self::VID]) -> Vec<Vertex<Self::VID>>;

    fn prepare_filter_vertex<F: ToString>(&self, filter: F) -> Box<dyn FilterById<ID=Self::VID>>;
}

impl<G: ?Sized + Graph> Graph for Arc<G> {
    type VID = G::VID;

    fn get_neighbor_ids(
        &self, src: Self::VID, label: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = Self::VID> + Send + 'static> {
        (**self).get_neighbor_ids(src, label, dir)
    }

    fn get_vertices_by_ids(&self, ids: &[Self::VID]) -> Vec<Vertex<Self::VID>> {
        (**self).get_vertices_by_ids(ids)
    }

    fn prepare_filter_vertex<F: ToString>(&self, p: F) -> Box<dyn FilterById<ID=Self::VID>> {
        (**self).prepare_filter_vertex(p)
    }
}

// just for compile without warning;
pub struct TodoGraph;

impl Graph for TodoGraph {
    type VID = u64;

    fn get_neighbor_ids(
        &self, _: u64, _label: &str, _dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        todo!()
    }

    fn get_vertices_by_ids(&self, _ids: &[u64]) -> Vec<Vertex<Self::VID>> {
        todo!()
    }

    fn prepare_filter_vertex<P: ToString>(&self, _p: P) -> Box<dyn FilterById<ID = u64>> {
        todo!()
    }
}

impl Graph for pegasus_graph::MemIdTopoGraph {
    type VID = u64;

    fn get_neighbor_ids(&self, src: u64, _label: &str, _dir: Direction) -> Box<dyn Iterator<Item=u64> + Send + 'static> {
        Box::new(self.get_neighbors(src))
    }

    fn get_vertices_by_ids(&self, _ids: &[u64]) -> Vec<Vertex<Self::VID>> {
        unimplemented!()
    }

    fn prepare_filter_vertex<P: ToString>(&self, _p: P) -> Box<dyn FilterById<ID = u64>> {
        unimplemented!()
    }
}