use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use pegasus_graph::graph::Direction;

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

pub struct Vertex {
    pub id: u64,
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

impl Vertex {
    pub fn cmp_by(&self, other: &Vertex, opts: &[OrderBy]) -> Ordering {
        for p in opts {
            let mut ord = match p.by {
                ILP::ID => self.id.cmp(&other.id),
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
    fn exec(&self, ids: &[u64]) -> Box<dyn Iterator<Item = u64> + Send + 'static>;
}

pub trait Graph: Send + Sync + 'static {
    fn get_neighbor_ids(
        &self, src: u64, label: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static>;

    fn get_vertices_by_ids(&self, ids: &[u64]) -> Vec<Vertex>;

    fn prepare_filter_vertex<P: Debug>(&self, p: P) -> Box<dyn FilterById>;
}

impl<G: ?Sized + Graph> Graph for Arc<G> {
    fn get_neighbor_ids(
        &self, src: u64, label: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        (**self).get_neighbor_ids(src, label, dir)
    }

    fn get_vertices_by_ids(&self, ids: &[u64]) -> Vec<Vertex> {
        (**self).get_vertices_by_ids(ids)
    }

    fn prepare_filter_vertex<P: Debug>(&self, p: P) -> Box<dyn FilterById> {
        (**self).prepare_filter_vertex(p)
    }
}

// just for compile without warning;
pub struct TodoGraph;

impl Graph for TodoGraph {
    fn get_neighbor_ids(
        &self, _: u64, _label: &str, _dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        todo!()
    }

    fn get_vertices_by_ids(&self, _ids: &[u64]) -> Vec<Vertex> {
        todo!()
    }

    fn prepare_filter_vertex<P: Debug>(&self, _p: P) -> Box<dyn FilterById> {
        todo!()
    }
}
