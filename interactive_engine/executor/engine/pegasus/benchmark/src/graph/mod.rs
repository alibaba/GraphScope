use std::cell::{Ref, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use pegasus_graph::graph::{Direction, VID};

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
    pub id: VID,
    label: RefCell<Option<String>>,
    properties: HashMap<String, Value>,
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

    pub fn new(id: VID, properties: HashMap<String, Value>) -> Self {
        Vertex {
            id,
            label: RefCell::new(None),
            properties
        }
    }

    pub fn get_label(&self) -> Ref<String> {
        {
            let mut label = self.label.borrow_mut();
            if label.is_none() {
                label.replace(self.id.get_label().expect("label lost"));
            }
        }
        Ref::map(self.label.borrow(), |o| o.as_ref().expect("label lost"))
    }

    pub fn cmp_by(&self, other: &Vertex, opts: &[OrderBy]) -> Ordering {
        for p in opts {
            let mut ord = match p.by {
                ILP::ID => self.id.vertex_id().cmp(&other.id.vertex_id()),
                ILP::Label => self.get_label().cmp(&other.get_label()),
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

pub trait Graph: Send + 'static {

    fn get_neighbor_ids(
        &self, src: u64, src_type: &str, edge_type: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static>;

    fn get_vertices_by_ids(&self, v_type: &str, ids: &[u64]) -> Vec<Vertex>;

    fn filter_vertex<F: ToString>(&self, v_type: &str, ids: &[u64], filter: F) -> Vec<u64>;
}

impl<G: ?Sized + Sync + Graph> Graph for Arc<G> {

    fn get_neighbor_ids(
        &self, src: u64, src_type: &str, edge_type: &str, dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        (**self).get_neighbor_ids(src, src_type, edge_type, dir)
    }

    fn get_vertices_by_ids(&self, v_type: &str, ids: &[u64]) -> Vec<Vertex> {
        (**self).get_vertices_by_ids(v_type, ids)
    }

    fn filter_vertex<F: ToString>(&self, v_type: &str, ids: &[u64], filter: F) -> Vec<u64> {
        (**self).filter_vertex(v_type, ids, filter)
    }
}

// just for compile without warning;
pub struct TodoGraph;

impl Graph for TodoGraph {
    fn get_neighbor_ids(
        &self, _: u64, _: &str, _: &str, _dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        todo!()
    }

    fn get_vertices_by_ids(&self, _: &str, _ids: &[u64]) -> Vec<Vertex> {
        todo!()
    }

    fn filter_vertex<F: ToString>(&self, _v_type: &str, _ids: &[u64], _filter: F) -> Vec<u64> {
        todo!()
    }
}

impl Graph for pegasus_graph::MemIdTopoGraph {

    fn get_neighbor_ids(
        &self, src: u64, _: &str, _: &str, _dir: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + 'static> {
        Box::new(self.get_neighbors(src))
    }

    fn get_vertices_by_ids(&self, _: &str, _ids: &[u64]) -> Vec<Vertex> {
        unimplemented!()
    }

    fn filter_vertex<F: ToString>(&self, _v_type: &str, _ids: &[u64], _filter: F) -> Vec<u64> {
        unimplemented!()
    }
}
