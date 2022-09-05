use crate::api::{property::Property, Edge, PropId, Vertex};

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Const(Property),
    PropId(PropId),
    Label,
    Id,
}

impl Operand {
    pub(crate) fn get_const_property(&self) -> Option<&Property> {
        match self {
            Operand::Const(prop) => Some(prop),
            _ => None,
        }
    }
    pub(crate) fn extract_vertex_property<V: Vertex>(&self, vertex: &V) -> Option<Property> {
        match self {
            Operand::PropId(prop_id) => vertex.get_property(*prop_id),
            Operand::Label => {
                let label_id = vertex.get_label_id();
                Some(Property::Long(label_id as i64))
            }
            Operand::Id => {
                let v_id = vertex.get_id();
                Some(Property::Long(v_id))
            }
            _ => None,
        }
    }

    pub(crate) fn extract_edge_property<E: Edge>(&self, edge: &E) -> Option<Property> {
        match self {
            Operand::PropId(prop_id) => edge.get_property(*prop_id),
            Operand::Label => {
                let label_id = edge.get_label_id();
                Some(Property::Long(label_id as i64))
            }
            Operand::Id => {
                let v_id = edge.get_edge_id();
                Some(Property::Long(v_id))
            }
            _ => None,
        }
    }
}
