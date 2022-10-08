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

mod common;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::catalogue::{PatternDirection, PatternLabelId};

    use crate::common::pattern_meta_cases::*;

    /// Test whether the pattern meta from the modern graph obeys our expectation
    #[test]
    fn test_modern_graph_schema() {
        let modern_graph_schema = read_modern_graph_schema();
        let modern_pattern_meta = PatternMeta::from(&modern_graph_schema);
        assert_eq!(modern_pattern_meta.get_edge_types_num(), 2);
        assert_eq!(modern_pattern_meta.get_vertex_types_num(), 2);
        assert_eq!(
            modern_pattern_meta
                .adjacent_elabels_iter(0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            3
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_elabels_iter(1)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_vlabels_iter(0)
                .collect::<Vec<PatternLabelId>>()
                .len(),
            2
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_vlabels_iter(1)
                .collect::<Vec<PatternLabelId>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(0, 0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            2
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(0, 1)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(1, 0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
    }

    /// Test whether the pattern meta from the ldbc graph obeys our expectation
    #[test]
    fn test_ldbc_graph_schema() {
        let ldbc_graph_schema = read_ldbc_graph_schema();
        let ldbc_pattern_meta = PatternMeta::from(&ldbc_graph_schema);
        assert_eq!(
            ldbc_pattern_meta
                .edge_label_ids_iter()
                .collect::<Vec<PatternLabelId>>()
                .len()
                + ldbc_pattern_meta
                    .vertex_label_ids_iter()
                    .collect::<Vec<PatternLabelId>>()
                    .len(),
            ldbc_graph_schema
                .get_pattern_meta_info()
                .0
                .len()
        );
        let all_vertex_names = ldbc_pattern_meta.vertex_label_names_iter();
        for vertex_name in all_vertex_names {
            let v_id_from_schema = ldbc_graph_schema
                .get_table_id(vertex_name)
                .unwrap();
            let v_id_from_pattern_meta = ldbc_pattern_meta
                .get_vertex_label_id(vertex_name)
                .unwrap();
            assert_eq!(v_id_from_schema, v_id_from_pattern_meta);
        }
        let all_edge_names = ldbc_pattern_meta.edge_label_names_iter();
        for edge_name in all_edge_names {
            let e_id_from_schema = ldbc_graph_schema
                .get_table_id(edge_name)
                .unwrap();
            let e_id_from_pattern_meta = ldbc_pattern_meta
                .get_edge_label_id(edge_name)
                .unwrap();
            assert_eq!(e_id_from_schema, e_id_from_pattern_meta);
        }
        let all_edge_ids = ldbc_pattern_meta.edge_label_ids_iter();
        let mut vertex_vertex_edges = BTreeMap::new();
        for edge_id in all_edge_ids {
            let edge_associate_vertices = ldbc_pattern_meta.associated_vlabels_iter_by_elabel(edge_id);
            for (start_v_id, end_v_id) in edge_associate_vertices {
                vertex_vertex_edges
                    .entry((start_v_id, end_v_id))
                    .or_insert(vec![])
                    .push((edge_id, PatternDirection::Out));
                vertex_vertex_edges
                    .entry((end_v_id, start_v_id))
                    .or_insert(vec![])
                    .push((edge_id, PatternDirection::In));
            }
        }
        for ((start_v_id, end_v_id), mut connections) in vertex_vertex_edges {
            let mut edges_between_vertices: Vec<(PatternLabelId, PatternDirection)> = ldbc_pattern_meta
                .associated_elabels_iter_by_vlabel(start_v_id, end_v_id)
                .collect();
            assert_eq!(connections.len(), edges_between_vertices.len());
            connections.sort();
            edges_between_vertices.sort();
            for i in 0..connections.len() {
                assert_eq!(connections[i], edges_between_vertices[i]);
            }
        }
        let all_vertex_ids = ldbc_pattern_meta.vertex_label_ids_iter();
        let mut vertex_vertex_edges = BTreeMap::new();
        for vertex_id in all_vertex_ids {
            let adjacent_edges = ldbc_pattern_meta.adjacent_elabels_iter(vertex_id);
            for (edge_id, dir) in adjacent_edges {
                let edges_with_dirs = ldbc_pattern_meta.associated_vlabels_iter_by_elabel(edge_id);
                for (start_v_id, end_v_id) in edges_with_dirs {
                    if start_v_id == vertex_id && dir == PatternDirection::Out {
                        vertex_vertex_edges
                            .entry((start_v_id, end_v_id))
                            .or_insert(vec![])
                            .push((edge_id, PatternDirection::Out));
                    }
                    if end_v_id == vertex_id && dir == PatternDirection::In {
                        vertex_vertex_edges
                            .entry((end_v_id, start_v_id))
                            .or_insert(vec![])
                            .push((edge_id, PatternDirection::In));
                    }
                }
            }
        }
        for ((start_v_id, end_v_id), mut connections) in vertex_vertex_edges {
            let mut edges_between_vertices: Vec<(PatternLabelId, PatternDirection)> = ldbc_pattern_meta
                .associated_elabels_iter_by_vlabel(start_v_id, end_v_id)
                .map(|(edge_label, dir)| (edge_label, dir))
                .collect();
            assert_eq!(connections.len(), edges_between_vertices.len());
            connections.sort();
            edges_between_vertices.sort();
            for i in 0..connections.len() {
                assert_eq!(connections[i], edges_between_vertices[i]);
            }
        }
    }
}
