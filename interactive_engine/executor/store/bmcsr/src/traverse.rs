use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use crate::bmcsr::BatchMutableCsr;
use crate::bmscsr::BatchMutableSingleCsr;
use crate::col_table::ColTable;
use crate::columns::DataType;
use crate::csr::CsrTrait;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::ldbc_parser::LDBCVertexParser;
use crate::schema::Schema;
use crate::types::LabelId;

fn traverse_vertices<G: Send + Sync + IndexType, I: Send + Sync + IndexType>(
    graph: &GraphDB<G, I>, output_dir: &str,
) {
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let output_dir_path = PathBuf::from_str(output_dir).unwrap();
    for n in vertex_label_names.iter() {
        if let Some(v_label) = graph.graph_schema.get_vertex_label_id(n) {
            let header = graph
                .graph_schema
                .get_vertex_header(v_label)
                .unwrap();
            let file_name = format!("{}.csv", n);
            let file_path = output_dir_path.join(file_name);
            let mut file = File::create(file_path).unwrap();

            let v_labels = vec![v_label];
            for v in graph.get_all_vertices(Some(&v_labels)) {
                let id = LDBCVertexParser::<G>::get_original_id(v.get_id());
                write!(file, "{}", id.index()).unwrap();
                for c in header {
                    if c.1 != DataType::ID {
                        write!(
                            file,
                            "|{}",
                            v.get_property(c.0.as_str())
                                .unwrap()
                                .to_string()
                        )
                        .unwrap();
                    }
                }
                writeln!(file).unwrap();
            }
        }
    }
}

fn output_csr<G, I>(
    graph: &GraphDB<G, I>, output_path: &str, csr: &BatchMutableCsr<I>, prop_table: Option<&ColTable>,
    label: LabelId, neighbor_label: LabelId, dir: Direction,
) where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    let mut file = File::create(output_path).unwrap();
    for v in 0..csr.vertex_num().index() {
        let src_global_id = graph.get_global_id(I::new(v), label).unwrap();
        let src_oid = LDBCVertexParser::<G>::get_original_id(src_global_id);
        if let Some(table) = prop_table {
            let col_num = table.col_num();
            if let Some(edges) = csr.get_edges_with_offset(I::new(v)) {
                for (nbr, offset) in edges {
                    let dst_global_id = graph
                        .get_global_id(nbr, neighbor_label)
                        .unwrap();
                    let dst_oid = LDBCVertexParser::<G>::get_original_id(dst_global_id);
                    if dir == Direction::Outgoing {
                        write!(file, "{}|{}", src_oid.index(), dst_oid.index()).unwrap();
                    } else {
                        write!(file, "{}|{}", dst_oid.index(), src_oid.index()).unwrap();
                    }
                    for c in 0..col_num {
                        write!(
                            file,
                            "|{}",
                            table
                                .get_item_by_index(c, offset)
                                .unwrap()
                                .to_string()
                        )
                        .unwrap();
                    }
                    writeln!(file).unwrap();
                }
            }
        } else {
            if let Some(edges) = csr.get_edges(I::new(v)) {
                for e in edges {
                    let dst_global_id = graph.get_global_id(*e, neighbor_label).unwrap();
                    let dst_oid = LDBCVertexParser::<G>::get_original_id(dst_global_id);
                    if dir == Direction::Outgoing {
                        writeln!(file, "{}|{}", src_oid.index(), dst_oid.index()).unwrap();
                    } else {
                        writeln!(file, "{}|{}", dst_oid.index(), src_oid.index()).unwrap();
                    }
                }
            }
        }
    }
}

fn output_single_csr<G, I>(
    graph: &GraphDB<G, I>, output_path: &str, csr: &BatchMutableSingleCsr<I>,
    prop_table: Option<&ColTable>, label: LabelId, neighbor_label: LabelId, dir: Direction,
) where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    let mut file = File::create(output_path).unwrap();
    for v in 0..csr.vertex_num().index() {
        let src_global_id = graph.get_global_id(I::new(v), label).unwrap();
        let src_oid = LDBCVertexParser::<G>::get_original_id(src_global_id);
        if let Some(table) = prop_table {
            let col_num = table.col_num();
            if let Some(edges) = csr.get_edges_with_offset(I::new(v)) {
                for (nbr, offset) in edges {
                    let dst_global_id = graph
                        .get_global_id(nbr, neighbor_label)
                        .unwrap();
                    let dst_oid = LDBCVertexParser::<G>::get_original_id(dst_global_id);
                    if dir == Direction::Outgoing {
                        write!(file, "{}|{}", src_oid.index(), dst_oid.index()).unwrap();
                    } else {
                        write!(file, "{}|{}", dst_oid.index(), src_oid.index()).unwrap();
                    }
                    for c in 0..col_num {
                        write!(
                            file,
                            "|{}",
                            table
                                .get_item_by_index(c, offset)
                                .unwrap()
                                .to_string()
                        )
                        .unwrap();
                    }
                    writeln!(file).unwrap();
                }
            }
        } else {
            if let Some(edges) = csr.get_edges(I::new(v)) {
                for e in edges {
                    let dst_global_id = graph.get_global_id(*e, neighbor_label).unwrap();
                    let dst_oid = LDBCVertexParser::<G>::get_original_id(dst_global_id);
                    if dir == Direction::Outgoing {
                        writeln!(file, "{}|{}", src_oid.index(), dst_oid.index()).unwrap();
                    } else {
                        writeln!(file, "{}|{}", dst_oid.index(), src_oid.index()).unwrap();
                    }
                }
            }
        }
    }
}

fn traverse_edges<G: Send + Sync + IndexType, I: Send + Sync + IndexType>(
    graph: &GraphDB<G, I>, output_dir: &str,
) {
    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for src_label in 0..vertex_label_num {
        let src_label_name = graph.graph_schema.vertex_label_names()[src_label].clone();
        for dst_label in 0..vertex_label_num {
            let dst_label_name = graph.graph_schema.vertex_label_names()[dst_label].clone();
            for edge_label in 0..edge_label_num {
                let edge_label_name = graph.graph_schema.edge_label_names()[edge_label].clone();
                if let Some(_) = graph.graph_schema.get_edge_header(
                    src_label as LabelId,
                    edge_label as LabelId,
                    dst_label as LabelId,
                ) {
                    let oe_file_name =
                        format!("oe_{}_{}_{}.csv", src_label_name, edge_label_name, dst_label_name);
                    let oe_file_path = PathBuf::from_str(output_dir)
                        .unwrap()
                        .join(oe_file_name);

                    let oe_index = graph.edge_label_to_index(
                        src_label as LabelId,
                        dst_label as LabelId,
                        edge_label as LabelId,
                        Direction::Outgoing,
                    );
                    if graph.graph_schema.is_single_oe(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let csr = graph.oe[oe_index]
                            .as_any()
                            .downcast_ref::<BatchMutableSingleCsr<I>>()
                            .unwrap();
                        output_single_csr(
                            graph,
                            oe_file_path.to_str().unwrap(),
                            csr,
                            graph.oe_edge_prop_table.get(&oe_index),
                            src_label as LabelId,
                            dst_label as LabelId,
                            Direction::Outgoing,
                        );
                    } else {
                        let csr = graph.oe[oe_index]
                            .as_any()
                            .downcast_ref::<BatchMutableCsr<I>>()
                            .unwrap();
                        info!("output oe csr: {}", oe_file_path.to_str().unwrap());
                        output_csr(
                            graph,
                            oe_file_path.to_str().unwrap(),
                            csr,
                            graph.oe_edge_prop_table.get(&oe_index),
                            src_label as LabelId,
                            dst_label as LabelId,
                            Direction::Outgoing,
                        );
                    }

                    let ie_file_name =
                        format!("ie_{}_{}_{}.csv", src_label_name, edge_label_name, dst_label_name);
                    let ie_file_path = PathBuf::from_str(output_dir)
                        .unwrap()
                        .join(ie_file_name);
                    // reverse src and dst label
                    let ie_index = graph.edge_label_to_index(
                        dst_label as LabelId,
                        src_label as LabelId,
                        edge_label as LabelId,
                        Direction::Incoming,
                    );
                    if graph.graph_schema.is_single_ie(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let csr = graph.ie[ie_index]
                            .as_any()
                            .downcast_ref::<BatchMutableSingleCsr<I>>()
                            .unwrap();
                        output_single_csr(
                            graph,
                            ie_file_path.to_str().unwrap(),
                            csr,
                            graph.ie_edge_prop_table.get(&ie_index),
                            dst_label as LabelId,
                            src_label as LabelId,
                            Direction::Incoming,
                        );
                    } else {
                        let csr = graph.ie[ie_index]
                            .as_any()
                            .downcast_ref::<BatchMutableCsr<I>>()
                            .unwrap();
                        info!("output ie csr: {}", ie_file_path.to_str().unwrap());
                        output_csr(
                            graph,
                            ie_file_path.to_str().unwrap(),
                            csr,
                            graph.ie_edge_prop_table.get(&ie_index),
                            dst_label as LabelId,
                            src_label as LabelId,
                            Direction::Incoming,
                        );
                    }
                }
            }
        }
    }
}

pub fn traverse<G: Send + Sync + IndexType, I: Send + Sync + IndexType>(
    graph: &GraphDB<G, I>, output_dir: &str,
) {
    traverse_vertices(graph, output_dir);
    traverse_edges(graph, output_dir);
}
