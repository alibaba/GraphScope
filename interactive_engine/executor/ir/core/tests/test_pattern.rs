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
    use std::convert::TryFrom;

    use ir_core::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
    use ir_core::catalogue::{PatternDirection, PatternId};
    use ir_core::plan::meta::TagId;

    use crate::common::pattern_cases::*;

    const TAG_A: TagId = 0;
    const TAG_B: TagId = 1;
    const TAG_C: TagId = 2;
    const TAG_D: TagId = 3;

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
        assert_eq!(edge_0.get_label(), 0);
        assert_eq!(edge_0.get_start_vertex().get_id(), 0);
        assert_eq!(edge_0.get_end_vertex().get_id(), 1);
        assert_eq!(edge_0.get_start_vertex().get_label(), 0);
        assert_eq!(edge_0.get_end_vertex().get_label(), 0);
        let edge_1 = pattern_case1.get_edge(1).unwrap();
        assert_eq!(edge_1.get_id(), 1);
        assert_eq!(edge_1.get_label(), 0);
        assert_eq!(edge_1.get_start_vertex().get_id(), 1);
        assert_eq!(edge_1.get_end_vertex().get_id(), 0);
        assert_eq!(edge_1.get_start_vertex().get_label(), 0);
        assert_eq!(edge_1.get_end_vertex().get_label(), 0);
        let vertex_0 = pattern_case1.get_vertex(0).unwrap();
        assert_eq!(vertex_0.get_id(), 0);
        assert_eq!(vertex_0.get_label(), 0);
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
        assert_eq!(vertex_1.get_label(), 0);
        assert_eq!(pattern_case1.get_vertex_degree(1), 2);
        let mut vertex_1_adjacent_edges_iter = pattern_case1.adjacencies_iter(1);
        let vertex_1_adjacency_0 = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_1_adjacency_0.get_edge_id(), 1);
        assert_eq!(vertex_1_adjacency_0.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_0.get_direction(), PatternDirection::Out);
        let vertex_1_adjacency_1 = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_1_adjacency_1.get_edge_id(), 0);
        assert_eq!(vertex_1_adjacency_1.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_1.get_direction(), PatternDirection::In);
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
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex1, pattern_vertex3);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
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
            // check structure
            let pattern_vertex1 = PatternVertex::new(0, 12);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_edge1 = PatternEdge::new(0, 15, pattern_vertex2, pattern_vertex1);
            let pattern_edge2 = PatternEdge::new(1, 15, pattern_vertex3, pattern_vertex1);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
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
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex1, pattern_vertex3);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex1, pattern_vertex4);
            let pattern_edge5 = PatternEdge::new(4, 12, pattern_vertex2, pattern_vertex4);
            let pattern_edge6 = PatternEdge::new(5, 12, pattern_vertex3, pattern_vertex4);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_D)
                    .unwrap()
                    .get_id(),
                TAG_D as PatternId
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    // Ignore this case as it is fuzzy pattern, which is not supported yet.
    #[ignore]
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
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 9);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 2);
            let pattern_edge1 = PatternEdge::new(0, 11, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 11, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 13, pattern_vertex1, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 0, pattern_vertex4, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3, pattern_edge4])
                    .unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_D)
                    .unwrap()
                    .get_id(),
                TAG_D as PatternId
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
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_vertex5 = PatternVertex::new(4, 1);
            let pattern_vertex6 = PatternVertex::new(5, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex3, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex5, pattern_vertex4);
            let pattern_edge5 = PatternEdge::new(4, 12, pattern_vertex5, pattern_vertex6);
            let pattern_edge6 = PatternEdge::new(5, 12, pattern_vertex1, pattern_vertex6);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    // Ignore this case as it is fuzzy pattern, which is not supported yet.
    #[ignore]
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
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 9);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_vertex5 = PatternVertex::new(4, 1);
            let pattern_vertex6 = PatternVertex::new(5, 2);
            let pattern_edge1 = PatternEdge::new(0, 11, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 11, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex3, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex4, pattern_vertex5);
            let pattern_edge5 = PatternEdge::new(4, 0, pattern_vertex6, pattern_vertex5);
            let pattern_edge6 = PatternEdge::new(5, 13, pattern_vertex1, pattern_vertex6);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let pattern_code = pattern.encode_to();
            let pattern_for_comparison_code = pattern_for_comparison.encode_to();
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                TAG_A as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                TAG_B as PatternId
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                TAG_C as PatternId
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }
}
