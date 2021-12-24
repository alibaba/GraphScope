pub struct VertexType(pub String);

pub struct EdgeType {
    pub src_vertex_type: VertexType,
    pub dst_vertex_type: VertexType,
    pub edge_type: String,
}

pub trait GraphMeta {
    fn get_vertex_types(&self) -> Vec<VertexType>;

    fn get_edge_types(&self) -> Vec<EdgeType>;
}
