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

    use ir_core::catalogue::pattern::{PatternEdge, PatternVertex};
    use ir_core::catalogue::PatternDirection;

    use crate::common::pattern_cases::*;

    /// Test whether the structure of pattern_case1 is the same as our previous description
    #[test]
    fn test_pattern_case1_structure() {
        let pattern_case1 = build_pattern_case1();
        let edges_num = pattern_case1.get_edges_num();
        assert_eq!(edges_num, 2);
        let vertices_num = pattern_case1.get_vertices_num();
        assert_eq!(vertices_num, 2);
        let edges_with_label_0: Vec<&PatternEdge> = pattern_case1.edges_iter_by_label(0).collect();
        assert_eq!(edges_with_label_0.len(), 2);
        let vertices_with_label_0: Vec<&PatternVertex> = pattern_case1
            .vertices_iter_by_label(0)
            .collect();
        assert_eq!(vertices_with_label_0.len(), 2);
        let edge_0 = pattern_case1.get_edge(0).unwrap();
        assert_eq!(edge_0.get_id(), 0);
        assert_eq!(edge_0.get_labels()[0], 0);
        assert_eq!(edge_0.get_start_vertex().get_id(), 0);
        assert_eq!(edge_0.get_end_vertex().get_id(), 1);
        assert_eq!(edge_0.get_start_vertex().get_labels()[0], 0);
        assert_eq!(edge_0.get_end_vertex().get_labels()[0], 0);
        let edge_1 = pattern_case1.get_edge(1).unwrap();
        assert_eq!(edge_1.get_id(), 1);
        assert_eq!(edge_1.get_labels()[0], 0);
        assert_eq!(edge_1.get_start_vertex().get_id(), 1);
        assert_eq!(edge_1.get_end_vertex().get_id(), 0);
        assert_eq!(edge_1.get_start_vertex().get_labels()[0], 0);
        assert_eq!(edge_1.get_end_vertex().get_labels()[0], 0);
        let vertex_0 = pattern_case1.get_vertex(0).unwrap();
        assert_eq!(vertex_0.get_id(), 0);
        assert_eq!(vertex_0.get_labels()[0], 0);
        assert_eq!(pattern_case1.get_vertex_degree(0), 2);
        let mut vertex_0_adjacent_edges_iter = pattern_case1.adjacencies_iter(0);
        let vertex_0_adjacency_0 = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_0_adjacency_0.get_edge_id(), 0);
        assert_eq!(vertex_0_adjacency_0.get_adj_vertex().get_id(), 1);
        assert_eq!(vertex_0_adjacency_0.get_direction(), PatternDirection::Out);
        let vertex_0_adjacency_1 = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_0_adjacency_1.get_edge_id(), 1);
        assert_eq!(vertex_0_adjacency_1.get_adj_vertex().get_id(), 1);
        assert_eq!(vertex_0_adjacency_1.get_direction(), PatternDirection::In);
        let vertex_1 = pattern_case1.get_vertex(1).unwrap();
        assert_eq!(vertex_1.get_id(), 1);
        assert_eq!(vertex_1.get_labels()[0], 0);
        assert_eq!(pattern_case1.get_vertex_degree(1), 2);
        let mut vertex_1_adjacent_edges_iter = pattern_case1.adjacencies_iter(1);
        let vertex_1_adjacency_0 = vertex_1_adjacent_edges_iter.next().unwrap();
        let vertex_1_adjacency_1 = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_1_adjacency_0.get_edge_id(), 0);
        assert_eq!(vertex_1_adjacency_0.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_0.get_direction(), PatternDirection::In);

        assert_eq!(vertex_1_adjacency_1.get_edge_id(), 1);
        assert_eq!(vertex_1_adjacency_1.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_1.get_direction(), PatternDirection::Out);
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case1_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case1();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 3);
            assert_eq!(pattern.get_edges_num(), 3);
            // 3 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                3
            );
            // 3 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case2_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case2();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 3);
            assert_eq!(pattern.get_edges_num(), 3);
            // 2 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 University vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(12)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 knows edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 studyat edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(15)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case3_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case3();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 4);
            assert_eq!(pattern.get_edges_num(), 6);
            // 4 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                4
            );
            // 6 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                6
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    // fuzzy pattern test
    #[test]
    fn test_ldbc_pattern_from_pb_case4_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case4();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 4);
            assert_eq!(pattern.get_edges_num(), 4);
            // 2 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 City vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(9)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 Comment vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 has creator edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(0)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 1 likes edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(13)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 islocated edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(11)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case5_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case5();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 6);
            assert_eq!(pattern.get_edges_num(), 6);
            // 6 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                6
            );
            // 6 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                6
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    // fuzzy pattern test
    #[test]
    fn test_ldbc_pattern_from_pb_case6_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case6();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 6);
            assert_eq!(pattern.get_edges_num(), 6);
            // 4 Persons vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                4
            );
            // 1 City vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(9)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 Comment vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(2)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 has creator edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(0)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 1 likes edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(13)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 islocated edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(11)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
            // 2 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }
}
