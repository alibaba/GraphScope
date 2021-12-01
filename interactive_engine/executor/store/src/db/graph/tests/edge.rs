use crate::db::api::*;

use super::helper::GraphTestHelper;
use super::types;
use crate::db::api::multi_version_graph::MultiVersionGraph;

pub fn test_get_edge<G: MultiVersionGraph>(graph: G) {
    let tester = tester::GetEdgeTester::new(graph);
    tester.execute();
}

pub fn test_query_edges<G: MultiVersionGraph>(graph: G) {
    let tester = tester::QueryEdgesTester::new(graph);
    tester.execute();
}

pub fn test_get_in_out_edges<G: MultiVersionGraph>(graph: G) {
    let tester = tester::GetInOutEdgesTester::new(graph);
    tester.execute();
}

pub fn test_update_edge<G: MultiVersionGraph>(graph: G) {
    let tester = tester::UpdateEdgeTester::new(graph);
    tester.execute();
}

pub fn test_delete_edge<G: MultiVersionGraph>(graph: G) {
    let tester = tester::DeleteEdgeTester::new(graph);
    tester.execute();
}

pub fn test_drop_edge<G: MultiVersionGraph>(graph: G) {
    let tester = tester::DropEdgeTester::new(graph);
    tester.execute();
}

pub fn test_remove_edge_kind<G: MultiVersionGraph>(graph: G) {
    let tester = tester::RemoveEdgeKindTester::new(graph);
    tester.execute();
}

mod tester {
    use super::*;
    use super::common::*;
    use crate::db::api::multi_version_graph::MultiVersionGraph;

    pub struct QueryEdgesTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> QueryEdgesTester<G> {
        pub fn new(graph: G) -> Self {
            QueryEdgesTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();
            let label1 = 1;

            helper.create_edge_type(10, 10, label1, types::create_test_type_def(label1)).unwrap();
            let label1_edge_kinds = data_gen.edge_kinds(label1);
            assert_eq!(label1_edge_kinds.len(), 3);
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_edge_kinds(13, vec![label1_edge_kinds[0].clone()]);
            insert_helper.init_data_of_edge_kinds(16, vec![label1_edge_kinds[1].clone()]);
            insert_helper.init_data_of_edge_kinds(19, vec![label1_edge_kinds[2].clone()]);
            std::mem::drop(insert_helper);

            let label2 = 2;
            helper.create_edge_type(20, 20, label2, types::create_test_type_def(label2)).unwrap();
            let label2_edge_kinds = data_gen.edge_kinds(label2);
            assert_eq!(label2_edge_kinds.len(), 3);
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_edge_kinds(23, vec![label2_edge_kinds[0].clone()]);
            insert_helper.init_data_of_edge_kinds(26, vec![label2_edge_kinds[1].clone()]);
            insert_helper.init_data_of_edge_kinds(29, vec![label2_edge_kinds[2].clone()]);
            std::mem::drop(insert_helper);

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_query_edges(si, None, &vec![]);
                check_helper.check_query_edges_empty(si, Some(label1));
                check_helper.check_query_edges_empty(si, Some(label2));
                println!("check si#{} success", si);
            }
            for si in 10..35 {
                if si < 20 {
                    check_helper.check_query_edges_empty(si, Some(label2));
                    if si < 13 {
                        check_helper.check_query_edges(si, None, &vec![]);
                        check_helper.check_query_edges(si, Some(label1), &vec![]);
                    } else if si < 16 {
                        check_helper.check_query_edges(si, None, &vec![label1_edge_kinds[0].clone()]);
                        check_helper.check_query_edges(si, Some(label1), &vec![label1_edge_kinds[0].clone()]);
                    } else if si < 19 {
                        check_helper.check_query_edges(si, None, &vec![label1_edge_kinds[0].clone(), label1_edge_kinds[1].clone()]);
                        check_helper.check_query_edges(si, Some(label1), &vec![label1_edge_kinds[0].clone(), label1_edge_kinds[1].clone()]);
                    } else {
                        check_helper.check_query_edges(si, None, &label1_edge_kinds);
                        check_helper.check_query_edges(si, Some(label1), &label1_edge_kinds);
                    }
                } else {
                    check_helper.check_query_edges(si, Some(label1), &label1_edge_kinds);
                    if si < 23 {
                        check_helper.check_query_edges(si, Some(label2), &vec![]);
                        check_helper.check_query_edges(si, None, &label1_edge_kinds);
                    } else if si < 26 {
                        check_helper.check_query_edges(si, Some(label2), &vec![label2_edge_kinds[0].clone()]);
                        let mut edge_kinds = label1_edge_kinds.clone();
                        edge_kinds.push(label2_edge_kinds[0].clone());
                        check_helper.check_query_edges(si, None, &edge_kinds);
                    } else if si < 29 {
                        check_helper.check_query_edges(si, Some(label2), &vec![label2_edge_kinds[0].clone(), label2_edge_kinds[1].clone()]);
                        let mut edge_kinds = label1_edge_kinds.clone();
                        edge_kinds.push(label2_edge_kinds[0].clone());
                        edge_kinds.push(label2_edge_kinds[1].clone());
                        check_helper.check_query_edges(si, None, &edge_kinds);
                    } else {
                        check_helper.check_query_edges(si, Some(label2), &label2_edge_kinds);
                        let mut edge_kinds = label1_edge_kinds.clone();
                        edge_kinds.extend_from_slice(&label2_edge_kinds);
                        check_helper.check_query_edges(si, None, &edge_kinds);
                    }
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct GetInOutEdgesTester<G> {
        graph: G,
    }

    impl<G: MultiVersionGraph> GetInOutEdgesTester<G> {
        pub fn new(graph: G) -> Self {
            GetInOutEdgesTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();

            let label1 = 1;
            helper.create_edge_type(10, 10, label1, types::create_test_type_def(label1)).unwrap();
            let label1_edge_kinds = data_gen.edge_kinds(label1);
            assert_eq!(label1_edge_kinds.len(), 3);
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_edge_kinds(13, vec![label1_edge_kinds[0].clone()]);
            insert_helper.init_data_of_edge_kinds(16, vec![label1_edge_kinds[1].clone()]);
            insert_helper.init_data_of_edge_kinds(19, vec![label1_edge_kinds[2].clone()]);
            std::mem::drop(insert_helper);

            let label2 = 2;
            helper.create_edge_type(20, 20, label2, types::create_test_type_def(label2)).unwrap();
            let label2_edge_kinds = data_gen.edge_kinds(label2);
            assert_eq!(label2_edge_kinds.len(), 3);
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_edge_kinds(23, vec![label2_edge_kinds[0].clone()]);
            insert_helper.init_data_of_edge_kinds(26, vec![label2_edge_kinds[1].clone()]);
            insert_helper.init_data_of_edge_kinds(29, vec![label2_edge_kinds[2].clone()]);
            std::mem::drop(insert_helper);

            let check_helper =  EdgeCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_get_out_edges_empty(si, Some(label1));
                check_helper.check_get_in_edges_empty(si, Some(label1));
                check_helper.check_get_out_edges_empty(si, Some(label2));
                check_helper.check_get_in_edges_empty(si, Some(label2));
                let mut edge_kinds = Vec::new();
                edge_kinds.extend_from_slice(&label1_edge_kinds);
                edge_kinds.extend_from_slice(&label2_edge_kinds);
                check_helper.check_get_out_edges_none(si, None, &edge_kinds);
                check_helper.check_get_in_edges_none(si, None, &edge_kinds);
                println!("check si#{} success", si);
            }

            for si in 10..35 {
                if si < 20 {
                    check_helper.check_get_out_edges_empty(si, Some(label2));
                    check_helper.check_get_in_edges_empty(si, Some(label2));
                    if si < 13 {
                        check_helper.check_get_out_edges_none(si, Some(label1), &label1_edge_kinds);
                        check_helper.check_get_in_edges_none(si, Some(label1), &label1_edge_kinds);
                        check_helper.check_get_out_edges_none(si, None, &label1_edge_kinds);
                        check_helper.check_get_in_edges_none(si, None, &label1_edge_kinds);
                    } else if si < 16 {
                        let exists = vec![label1_edge_kinds[0].clone()];
                        let none = vec![label1_edge_kinds[1].clone(), label1_edge_kinds[2].clone()];
                        check_helper.check_get_out_edges_none(si, Some(label1), &none);
                        check_helper.check_get_in_edges_none(si, Some(label1), &none);
                        check_helper.check_get_out_edges(si, Some(label1), &exists);
                        check_helper.check_get_in_edges(si, Some(label1), &exists);
                        check_helper.check_get_out_edges(si, None, &exists);
                        check_helper.check_get_in_edges(si, None, &exists);
                    } else if si < 19 {
                        let exists = vec![label1_edge_kinds[0].clone(), label1_edge_kinds[1].clone()];
                        let none = vec![label1_edge_kinds[2].clone()];
                        check_helper.check_get_out_edges_none(si, Some(label1), &none);
                        check_helper.check_get_in_edges_none(si, Some(label1), &none);
                        check_helper.check_get_out_edges(si, Some(label1), &exists);
                        check_helper.check_get_in_edges(si, Some(label1), &exists);
                        check_helper.check_get_out_edges(si, None, &exists);
                        check_helper.check_get_in_edges(si, None, &exists);
                    } else {
                        check_helper.check_get_out_edges(si, Some(label1), &label1_edge_kinds);
                        check_helper.check_get_in_edges(si, Some(label1), &label1_edge_kinds);
                        check_helper.check_get_out_edges(si, None, &label1_edge_kinds);
                        check_helper.check_get_in_edges(si, None, &label1_edge_kinds);
                    }
                } else {
                    check_helper.check_get_out_edges(si, Some(label1), &label1_edge_kinds);
                    check_helper.check_get_in_edges(si, Some(label1), &label1_edge_kinds);
                    if si < 23 {
                        check_helper.check_get_out_edges_none(si, Some(label2), &label2_edge_kinds);
                        check_helper.check_get_in_edges_none(si, Some(label2), &label2_edge_kinds);
                        check_helper.check_get_out_edges(si, None, &label1_edge_kinds);
                        check_helper.check_get_in_edges(si, None, &label1_edge_kinds);
                    } else if si < 26 {
                        let label2_exists = vec![label2_edge_kinds[0].clone()];
                        let mut exists = Vec::new();
                        exists.extend_from_slice(&label1_edge_kinds);
                        exists.extend_from_slice(&label2_exists[..]);
                        let none = vec![label2_edge_kinds[1].clone(), label2_edge_kinds[2].clone()];
                        check_helper.check_get_out_edges_none(si, Some(label2), &none);
                        check_helper.check_get_in_edges_none(si, Some(label2), &none);
                        check_helper.check_get_out_edges(si, Some(label2), &label2_exists);
                        check_helper.check_get_in_edges(si, Some(label2), &label2_exists);
                        check_helper.check_get_out_edges(si, None, &exists);
                        check_helper.check_get_in_edges(si, None, &exists);
                    } else if si < 29 {
                        let label2_exists = vec![label2_edge_kinds[0].clone(), label2_edge_kinds[1].clone()];
                        let mut exists = Vec::new();
                        exists.extend_from_slice(&label1_edge_kinds);
                        exists.extend_from_slice(&label2_exists[..]);
                        let none = vec![label2_edge_kinds[2].clone()];
                        check_helper.check_get_out_edges_none(si, Some(label2), &none);
                        check_helper.check_get_in_edges_none(si, Some(label2), &none);
                        check_helper.check_get_out_edges(si, Some(label2), &label2_exists);
                        check_helper.check_get_in_edges(si, Some(label2), &label2_exists);
                        check_helper.check_get_out_edges(si, None, &exists);
                        check_helper.check_get_in_edges(si, None, &exists);
                    } else {
                        let mut exists = Vec::new();
                        exists.extend_from_slice(&label1_edge_kinds);
                        exists.extend_from_slice(&label2_edge_kinds);
                        check_helper.check_get_out_edges(si, Some(label2), &label2_edge_kinds);
                        check_helper.check_get_in_edges(si, Some(label2), &label2_edge_kinds);
                        check_helper.check_get_out_edges(si, None, &exists);
                        check_helper.check_get_in_edges(si, None, &exists);
                    }
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct UpdateEdgeTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> UpdateEdgeTester<G> {
        pub fn new(graph: G) -> Self {
            UpdateEdgeTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();

            let label1 = 1;
            let si = 10;
            let mut schema_version = 1;

            helper.create_edge_type(si, schema_version, label1, types::create_full_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_edge_kinds = data_gen.edge_kinds(label1);
            assert_eq!(label1_edge_kinds.len(), 3);
            for edge_kind in &label1_edge_kinds {
                helper.add_edge_kind(si, schema_version, edge_kind).unwrap();
                schema_version += 1;
                let ids = data_gen.edge_ids(edge_kind);
                helper.insert_edge(si, edge_kind, ids.into_iter()).unwrap();
            }
            helper.update_edge(13, &label1_edge_kinds[0], data_gen.edge_ids(&label1_edge_kinds[0]).into_iter()).unwrap();
            helper.update_edge(16, &label1_edge_kinds[1], data_gen.edge_ids(&label1_edge_kinds[1]).into_iter()).unwrap();
            helper.update_edge(19, &label1_edge_kinds[2], data_gen.edge_ids(&label1_edge_kinds[2]).into_iter()).unwrap();

            let label2 = 2;
            let si = 20;
            helper.create_edge_type(si, schema_version, label2, types::create_full_type_def(label2)).unwrap();
            schema_version += 1;
            let label2_edge_kinds = data_gen.edge_kinds(label2);
            assert_eq!(label2_edge_kinds.len(), 3);
            for edge_kind in &label2_edge_kinds {
                helper.add_edge_kind(si, schema_version, edge_kind).unwrap();
                schema_version += 1;
                let ids = data_gen.edge_ids(edge_kind);
                helper.insert_edge(si, edge_kind, ids.into_iter()).unwrap();
            }
            helper.update_edge(23, &label2_edge_kinds[0], data_gen.edge_ids(&label2_edge_kinds[0]).into_iter()).unwrap();
            helper.update_edge(26, &label2_edge_kinds[1], data_gen.edge_ids(&label2_edge_kinds[1]).into_iter()).unwrap();
            helper.update_edge(29, &label2_edge_kinds[2], data_gen.edge_ids(&label2_edge_kinds[2]).into_iter()).unwrap();

            let label3 = 3;
            let si = 30;
            helper.create_edge_type(si, schema_version, label3, types::create_full_type_def(label3)).unwrap();
            schema_version += 1;
            let label3_edge_kinds = data_gen.edge_kinds(label3);
            for edge_kind in &label3_edge_kinds {
                helper.add_edge_kind(si, schema_version, edge_kind).unwrap();
                schema_version += 1;
            }
            assert_eq!(label3_edge_kinds.len(), 3);
            helper.update_edge(33, &label3_edge_kinds[0], data_gen.edge_ids(&label3_edge_kinds[0]).into_iter()).unwrap();
            helper.update_edge(36, &label3_edge_kinds[1], data_gen.edge_ids(&label3_edge_kinds[1]).into_iter()).unwrap();
            helper.update_edge(39, &label3_edge_kinds[2], data_gen.edge_ids(&label3_edge_kinds[2]).into_iter()).unwrap();

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_all_data_err_of_labels(si, vec![label1, label2, label3]);
                println!("check si#{} success", si);
            }

            for si in 10..40 {
                if si < 20 {
                    check_helper.check_all_data_err_of_labels(si, vec![label2, label3]);
                    check_helper.check_all_data_of_labels(si, vec![label1]);
                } else if si < 30 {
                    check_helper.check_all_data_err_of_labels(si, vec![label3]);
                    check_helper.check_all_data_of_labels(si, vec![label1, label2]);
                } else {
                    if si < 33 {
                        check_helper.check_all_data_none_of_label(si, label3);
                        check_helper.check_all_data_of_labels(si, vec![label1, label2]);
                    } else {
                        let mut edge_kinds = Vec::new();
                        edge_kinds.extend_from_slice(&label1_edge_kinds);
                        edge_kinds.extend_from_slice(&label2_edge_kinds);
                        if si < 36 {
                            edge_kinds.extend_from_slice(&label3_edge_kinds[..1]);
                        } else if si < 39 {
                            edge_kinds.extend_from_slice(&label3_edge_kinds[..2]);
                        } else {
                            edge_kinds.extend_from_slice(&label3_edge_kinds[..3]);
                        }
                        check_helper.check_all_data_of_edge_kinds(si, &edge_kinds);
                    }
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct DeleteEdgeTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> DeleteEdgeTester<G> {
        pub fn new(graph: G) -> Self {
            DeleteEdgeTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();

            let label1 = 1;
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_label(10, label1);
            std::mem::drop(insert_helper);
            let label1_edge_kinds = data_gen.edge_kinds(label1);
            helper.delete_edge(13, &label1_edge_kinds[0], data_gen.delete_edge_ids(&label1_edge_kinds[0])).unwrap();
            helper.delete_edge(16, &label1_edge_kinds[1], data_gen.delete_edge_ids(&label1_edge_kinds[1])).unwrap();
            helper.delete_edge(19, &label1_edge_kinds[2], data_gen.delete_edge_ids(&label1_edge_kinds[2])).unwrap();

            let label2 = 2;
            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_label(20, label2);
            std::mem::drop(insert_helper);
            let label2_edge_kinds = data_gen.edge_kinds(label2);
            helper.delete_edge(23, &label2_edge_kinds[0], data_gen.delete_edge_ids(&label2_edge_kinds[0])).unwrap();
            helper.delete_edge(26, &label2_edge_kinds[1], data_gen.delete_edge_ids(&label2_edge_kinds[1])).unwrap();
            helper.delete_edge(29, &label2_edge_kinds[2], data_gen.delete_edge_ids(&label2_edge_kinds[2])).unwrap();

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_all_data_err_of_labels(si, vec![label1, label2]);
                println!("check si#{} success", si);
            }
            for si in 10..35 {
                if si < 20 {
                    check_helper.check_all_data_err_of_labels(si, vec![label2]);
                    let deleted_ids = if si < 13 {
                        vec![]
                    } else if si < 16 {
                        data_gen.delete_edge_ids(&label1_edge_kinds[0])
                    } else if si < 19 {
                        data_gen.all_delete_edge_ids(&label1_edge_kinds[0..2])
                    } else {
                        data_gen.all_delete_edge_ids(&label1_edge_kinds)
                    };
                    check_helper.check_get_edge_of_labels_with_deletion(si, &[label1], deleted_ids.clone());
                    check_helper.check_query_edges_with_deletion(si, Some(label1), &label1_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_out_edges_with_deletion(si, Some(label1), &label1_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_in_edges_with_deletion(si, Some(label1), &label1_edge_kinds, deleted_ids.clone());
                    check_helper.check_query_edges_with_deletion(si, None, &label1_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_out_edges_with_deletion(si, None, &label1_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_in_edges_with_deletion(si, None, &label1_edge_kinds, deleted_ids.clone());
                } else {
                    let label1_deleted_ids = data_gen.all_delete_edge_ids(&label1_edge_kinds);
                    check_helper.check_all_data_of_label_with_deletion(si, label1, label1_deleted_ids.clone());
                    let mut deleted_ids = if si < 23 {
                        vec![]
                    } else if si < 26 {
                        data_gen.delete_edge_ids(&label2_edge_kinds[0])
                    } else if si < 29 {
                        data_gen.all_delete_edge_ids(&label2_edge_kinds[0..2])
                    } else {
                        data_gen.all_delete_edge_ids(&label2_edge_kinds)
                    };
                    check_helper.check_get_edge_of_labels_with_deletion(si, &[label2], deleted_ids.clone());
                    check_helper.check_query_edges_with_deletion(si, Some(label2), &label2_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_out_edges_with_deletion(si, Some(label2), &label2_edge_kinds, deleted_ids.clone());
                    check_helper.check_get_in_edges_with_deletion(si, Some(label2), &label2_edge_kinds, deleted_ids.clone());
                    deleted_ids.extend_from_slice(&label1_deleted_ids);
                    let mut edge_kinds = Vec::new();
                    edge_kinds.extend_from_slice(&label1_edge_kinds);
                    edge_kinds.extend_from_slice(&label2_edge_kinds);
                    check_helper.check_query_edges_with_deletion(si, None, &edge_kinds, deleted_ids.clone());
                    check_helper.check_get_out_edges_with_deletion(si, None, &edge_kinds, deleted_ids.clone());
                    check_helper.check_get_in_edges_with_deletion(si, None, &edge_kinds, deleted_ids.clone());
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct GetEdgeTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> GetEdgeTester<G> {
        pub fn new(graph: G) -> Self {
            GetEdgeTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();

            let mut insert_helper = EdgeDataInsertHelper::new(&mut helper, &data_gen);
            insert_helper.init_data_of_label(10, 1);
            insert_helper.init_data_of_label(20, 2);
            insert_helper.init_data_of_label(30, 3);
            insert_helper.init_data_of_label(40, 4);
            insert_helper.init_data_of_label(50, 5);
            std::mem::drop(insert_helper);
            println!("init data success");

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            // at si 1-10, no labels exists
            for si in (1..10).step_by(3) {
                let labels = (1..=10).collect();
                check_helper.check_get_edge_err_of_labels(si, labels);
            }
            for si in (10..60).step_by(3) {
                let max_exists_si = (si as LabelId) / 10;
                let labels: Vec<i32> = (1..=max_exists_si).collect();
                check_helper.check_get_edge_of_labels(si, &labels);
                check_helper.check_get_not_exist_edge_of_labels(si, &labels);
                // these labels is not in graph at si
                let labels = (max_exists_si + 1..=10).collect();
                check_helper.check_get_edge_err_of_labels(si, labels);
                println!("check si#{} success", si);
            }
        }
    }

    pub struct DropEdgeTester<G> {
        graph: G,
    }

    impl<G: MultiVersionGraph> DropEdgeTester<G> {
        pub fn new(graph: G) -> Self {
            DropEdgeTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();

            let label1 = 1;
            let si = 10;
            let mut schema_version = 1;

            helper.create_edge_type(si, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            for edge_kind in data_gen.edge_kinds(label1) {
                helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                schema_version += 1;
                helper.insert_edge(si, &edge_kind, data_gen.edge_ids(&edge_kind).into_iter()).unwrap();
            }
            println!("insert data of label1 success");
            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in si..si + 3 {
                check_helper.check_all_data_of_labels(si, vec![label1]);
            }
            std::mem::drop(check_helper);
            println!("check data of label1 success");

            let label2 = 2;
            let si = 20;
            helper.create_edge_type(si, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            schema_version += 1;
            for edge_kind in data_gen.edge_kinds(label2) {
                helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                schema_version += 1;
                helper.insert_edge(si, &edge_kind, data_gen.edge_ids(&edge_kind).into_iter()).unwrap();
            }
            println!("insert data of label1 success");
            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in si..si + 3 {
                check_helper.check_all_data_of_labels(si, vec![label1, label2]);
            }
            std::mem::drop(check_helper);
            println!("check data of label1 and label2 success");

            helper.drop_edge_type(25, schema_version, label1).unwrap();

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in 10..30 {
                if si % 2 == 1 {
                    if si < 20 {
                        // only label1 exists
                        check_helper.check_all_data_of_labels(si, vec![label1]);
                    } else if si < 25 {
                        // label1 and label2 exists
                        check_helper.check_all_data_of_labels(si, vec![label1, label2]);
                    } else {
                        // label1 is dropped
                        check_helper.check_all_data_of_labels(si, vec![label2]);
                    }
                    println!("check si#{} success", si);
                }
            }
        }
    }

    pub struct RemoveEdgeKindTester<G> {
        graph: G,
    }

    impl<G: MultiVersionGraph> RemoveEdgeKindTester<G> {
        pub fn new(graph: G) -> Self {
            RemoveEdgeKindTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = EdgeDataGen::default();
            let si = 10;
            let label1 = 1;
            let mut schema_version = 1;
            helper.create_edge_type(si, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            for edge_kind in data_gen.edge_kinds(label1) {
                helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                schema_version += 1;
                helper.insert_edge(si, &edge_kind, data_gen.edge_ids(&edge_kind).into_iter()).unwrap();
            }
            println!("init data of label#{} success", label1);
            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in si..si + 3 {
                check_helper.check_all_data_of_labels(si, vec![label1]);
            }
            std::mem::drop(check_helper);
            println!("check data of label#{} success", label1);

            let si = 20;
            let label2 = 2;
            helper.create_edge_type(si, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            schema_version += 1;
            for edge_kind in data_gen.edge_kinds(label2) {
                helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                schema_version += 1;
                helper.insert_edge(si, &edge_kind, data_gen.edge_ids(&edge_kind).into_iter()).unwrap();
            }
            println!("init data of label#{} success", label2);
            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in si..si + 3 {
                check_helper.check_all_data_of_labels(si, vec![label1, label2]);
            }
            std::mem::drop(check_helper);
            println!("check data of label#{} success", label2);

            let mut label1_edge_kinds = data_gen.edge_kinds(label1);
            let mut label2_edge_kinds = data_gen.edge_kinds(label2);
            let label1_removed_edge_kind = label1_edge_kinds.pop().unwrap();
            let label2_removed_edge_kind = label2_edge_kinds.pop().unwrap();
            helper.remove_edge_kind(25, schema_version, &label1_removed_edge_kind).unwrap();
            schema_version += 1;
            helper.remove_edge_kind(25, schema_version, &label2_removed_edge_kind).unwrap();
            println!("remove edge type success");

            let check_helper = EdgeCheckHelper::new(&helper, &data_gen);
            for si in 10..30 {
                if si % 2 == 1 {
                    if si < 20 {
                        // no edge type of label1 removed
                        let edge_kinds = data_gen.edge_kinds(label1);
                        check_helper.check_all_data_of_edge_kinds(si, &edge_kinds);
                    } else if si < 25 {
                        // no edge type of label1 and label2 removed
                        let mut edge_kinds = Vec::new();
                        edge_kinds.extend(data_gen.edge_kinds(label1));
                        edge_kinds.extend(data_gen.edge_kinds(label2));
                        check_helper.check_all_data_of_edge_kinds(si, &edge_kinds);
                    } else {
                        // last edge type of label1 and label2 is removed
                        let mut all_edge_kinds = Vec::new();
                        all_edge_kinds.extend_from_slice(&label1_edge_kinds);
                        all_edge_kinds.extend_from_slice(&label2_edge_kinds);
                        check_helper.check_all_data_of_edge_kinds(si, &all_edge_kinds);
                    }
                    println!("check si#{} success", si);
                }
            }
        }
    }
}

mod common {
    use super::*;
    use std::collections::{HashSet, HashMap};
    use std::iter::FromIterator;
    use crate::db::api::multi_version_graph::MultiVersionGraph;

    pub struct EdgeDataInsertHelper<'a, 'b, G: MultiVersionGraph> {
        helper: &'a mut GraphTestHelper<'b, G>,
        data_gen: &'a EdgeDataGen,
    }

    impl<'a, 'b, G: MultiVersionGraph> EdgeDataInsertHelper<'a, 'b, G> {
        pub fn new(helper: &'a mut GraphTestHelper<'b, G>, data_gen: &'a EdgeDataGen) -> Self {
            EdgeDataInsertHelper {
                helper,
                data_gen,
            }
        }

        pub fn init_data_of_label(&mut self, si: SnapshotId, label: LabelId) {
            let mut schema_version = si;
            self.helper.create_edge_type(si, schema_version, label, types::create_test_type_def(label)).unwrap();
            schema_version += 1;
            for edge_kind in self.data_gen.edge_kinds(label) {
                self.helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                schema_version += 1;
                self.helper.insert_edge(si, &edge_kind, self.data_gen.edge_ids(&edge_kind).into_iter()).unwrap();
            }
        }

        pub fn init_data_of_edge_kinds(&mut self, si: SnapshotId, edge_kinds: Vec<EdgeKind>) {
            let schema_version = si;
            for edge_kind in edge_kinds {
                self.helper.add_edge_kind(si, schema_version, &edge_kind).unwrap();
                let ids = self.data_gen.edge_ids(&edge_kind).into_iter();
                self.helper.insert_edge(si, &edge_kind, ids).unwrap();
            }
        }
    }

    pub struct EdgeDataGen {
        edge_kinds_count_per_label: usize,
        src_count_per_edge_kind: usize,
        dst_count_per_edge_kind: usize,
        inner_count_per_edge_kind: usize,
    }

    impl EdgeDataGen {
        pub fn default() -> Self {
            EdgeDataGen {
                edge_kinds_count_per_label: 3,
                src_count_per_edge_kind: 3,
                dst_count_per_edge_kind: 3,
                inner_count_per_edge_kind: 3,
            }
        }

        pub fn edge_kinds(&self, label: LabelId) -> Vec<EdgeKind> {
            let mut ret = Vec::new();
            for i in 0..self.edge_kinds_count_per_label {
                let src_label = 1000 + i as LabelId;
                let dst_label = 5000 + i as LabelId;
                ret.push(EdgeKind::new(label, src_label, dst_label));
            }
            ret
        }

        pub fn edge_ids(&self, edge_kind: &EdgeKind) -> Vec<EdgeId> {
            let src_start = edge_kind.src_vertex_label_id as VertexId * 100;
            let dst_start = edge_kind.dst_vertex_label_id as VertexId * 100;
            let mut ret = Vec::new();
            let inner_id_start = edge_kind.edge_label_id as VertexId * 100;
            let src_count = self.src_count_per_edge_kind as VertexId;
            let dst_count = self.dst_count_per_edge_kind as VertexId;
            let inner_count = self.inner_count_per_edge_kind as i64;
            for src_id in src_start + 1..=src_start + src_count {
                for dst_id in dst_start + 1..=dst_start + dst_count {
                    for inner_id in inner_id_start + 1..=inner_id_start + inner_count {
                        ret.push(EdgeId::new(src_id, dst_id, inner_id));
                    }
                }
            }
            ret
        }

        pub fn delete_edge_ids(&self, edge_kind: &EdgeKind) -> Vec<EdgeId> {
            self.edge_ids(edge_kind).into_iter()
                .filter(|id| (id.src_id+id.dst_id+id.inner_id) % 2 == 0)
                .collect()
        }

        pub fn all_delete_edge_ids(&self, edge_kinds: &[EdgeKind]) -> Vec<EdgeId> {
            edge_kinds.iter().flat_map(|edge_kind| self.edge_ids(edge_kind).into_iter())
                .filter(|id| (id.src_id+id.dst_id+id.inner_id) % 2 == 0)
                .collect()
        }

        pub fn not_exist_edge_ids(&self, edge_kind: &EdgeKind) -> Vec<EdgeId> {
            let mut ret = self.edge_ids(edge_kind);
            ret.iter_mut().for_each(|id| id.inner_id = -id.inner_id);
            ret
        }

        #[allow(dead_code)]
        pub fn all_edge_ids(&self, labels: Vec<LabelId>) -> HashSet<EdgeId> {
            let mut set = HashSet::new();
            for label in labels {
                for edge_kind in self.edge_kinds(label) {
                    set.extend(self.edge_ids(&edge_kind));
                }
            }
            set
        }

        pub fn all_edge_ids_of_types(&self, edge_kinds: &Vec<EdgeKind>) -> HashSet<EdgeId> {
            let mut set = HashSet::new();
            for edge_kind in edge_kinds {
                set.extend(self.edge_ids(edge_kind));
            }
            set
        }

        pub fn all_out_edges(&self, labels: Vec<LabelId>) -> HashMap<VertexId, HashSet<EdgeId>> {
            let mut ret = HashMap::new();
            for label in labels {
                for edge_kind in self.edge_kinds(label) {
                    for edge_id in self.edge_ids(&edge_kind) {
                        ret.entry(edge_id.src_id).or_insert_with(|| HashSet::new()).insert(edge_id);
                    }
                }
            }
            ret
        }

        pub fn all_out_edges_of_types(&self, edge_kinds: &Vec<EdgeKind>) -> HashMap<VertexId, HashSet<EdgeId>> {
            let mut ret = HashMap::new();
            for edge_id in self.all_edge_ids_of_types(edge_kinds) {
                ret.entry(edge_id.src_id).or_insert_with(|| HashSet::new()).insert(edge_id);
            }
            ret
        }

        pub fn all_in_edges(&self, labels: Vec<LabelId>) -> HashMap<VertexId, HashSet<EdgeId>> {
            let mut ret = HashMap::new();
            for label in labels {
                for edge_kind in self.edge_kinds(label) {
                    for edge_id in self.edge_ids(&edge_kind) {
                        ret.entry(edge_id.dst_id).or_insert_with(|| HashSet::new()).insert(edge_id);
                    }
                }
            }
            ret
        }

        pub fn all_in_edges_of_types(&self, edge_kinds: &Vec<EdgeKind>) -> HashMap<VertexId, HashSet<EdgeId>> {
            let mut ret = HashMap::new();
            for edge_id in self.all_edge_ids_of_types(edge_kinds) {
                ret.entry(edge_id.dst_id).or_insert_with(|| HashSet::new()).insert(edge_id);
            }
            ret
        }
    }

    pub struct EdgeCheckHelper<'a, G: MultiVersionGraph> {
        helper: &'a GraphTestHelper<'a, G>,
        data_gen: &'a EdgeDataGen,
    }

    impl<'a, G: MultiVersionGraph> EdgeCheckHelper<'a, G> {
        pub fn new(helper: &'a GraphTestHelper<'a, G>, data_gen: &'a EdgeDataGen) -> Self {
            EdgeCheckHelper {
                helper,
                data_gen,
            }
        }

        pub fn check_all_data_err_of_labels(&self, si: SnapshotId, labels: Vec<LabelId>) {
            for label in labels.clone() {
                self.check_get_out_edges_empty(si, Some(label));
                self.check_get_in_edges_empty(si, Some(label));
                self.check_query_edges_empty(si, Some(label));
            }
            self.check_get_edge_err_of_labels(si, labels);
        }

        pub fn check_all_data_none_of_label(&self, si: SnapshotId, label: LabelId) {
            let edge_kinds = self.data_gen.edge_kinds(label);
            self.check_get_out_edges_none(si, Some(label), &edge_kinds);
            self.check_get_in_edges_none(si, Some(label), &edge_kinds);
            self.check_query_edges_none(si, Some(label));
            self.check_get_edge_none_of_label(si, label);
        }

        pub fn check_all_data_of_label_with_deletion(&self, si: SnapshotId, label: LabelId, deleted_ids: Vec<EdgeId>) {
            let edge_kinds = self.data_gen.edge_kinds(label);
            self.check_get_out_edges_with_deletion(si, Some(label), &edge_kinds, deleted_ids.clone());
            self.check_get_in_edges_with_deletion(si, Some(label), &edge_kinds, deleted_ids.clone());
            self.check_query_edges_with_deletion(si, Some(label), &edge_kinds, deleted_ids.clone());
            self.check_get_edge_of_edge_kinds_with_deletion(si, &edge_kinds, deleted_ids);
        }

        pub fn check_all_data_of_labels(&self, si: SnapshotId, labels: Vec<LabelId>) {
            let mut map = HashMap::new();
            for label in labels {
                map.insert(label, self.data_gen.edge_kinds(label));
            }
            self.check_all_data(si, map);
        }

        pub fn check_all_data_of_edge_kinds(&self, si: SnapshotId, edge_kinds: &Vec<EdgeKind>) {
            let map = self.group_edge_kinds(edge_kinds);
            self.check_all_data(si, map);
        }

        pub fn check_get_edge_of_labels(&self, si: SnapshotId, labels: &[LabelId]) {
            self.check_get_edge_of_labels_with_deletion(si, labels, vec![]);
        }

        pub fn check_get_edge_of_labels_with_deletion(&self, si: SnapshotId, labels: &[LabelId], deleted_ids: Vec<EdgeId>) {
            for label in labels {
                let edge_kinds = self.data_gen.edge_kinds(*label);
                self.check_get_edge_of_edge_kinds_with_deletion(si, &edge_kinds, deleted_ids.clone());
            }
        }

        pub fn check_get_edge_err_of_labels(&self, si: SnapshotId, labels: Vec<LabelId>) {
            for label in labels {
                let edge_kinds = self.data_gen.edge_kinds(label);
                self.check_get_edge_err_of_edge_kinds(si, &edge_kinds);
            }
        }

        pub fn check_get_not_exist_edge_of_labels(&self, si: SnapshotId, labels: &Vec<LabelId>) {
            for label in labels {
                let edge_kinds = self.data_gen.edge_kinds(*label);
                self.check_get_not_exist_edge_of_edge_kinds(si, &edge_kinds);
            }
        }

        pub fn check_get_edge_of_edge_kinds(&self, si: SnapshotId, edge_kinds: &Vec<EdgeKind>) {
            self.check_get_edge_of_edge_kinds_with_deletion(si, edge_kinds, vec![]);
        }

        pub fn check_get_edge_of_edge_kinds_with_deletion(&self, si: SnapshotId, edge_kinds: &Vec<EdgeKind>, deleted_ids: Vec<EdgeId>) {
            let deleted = HashSet::from_iter(deleted_ids);
            for edge_kind in edge_kinds {
                let ids = self.data_gen.edge_ids(edge_kind);
                let set: HashSet<EdgeId> = HashSet::from_iter(ids);
                self.helper.check_get_edge(si, &edge_kind, set.difference(&deleted));
            }
        }

        pub fn check_get_edge_none_of_label(&self, si: SnapshotId, label: LabelId) {
            let edge_kinds = self.data_gen.edge_kinds(label);
            self.check_get_edge_none_of_edge_kinds(si, &edge_kinds);
        }

        pub fn check_get_edge_none_of_edge_kinds(&self, si: SnapshotId, edge_kinds: &[EdgeKind]) {
            for edge_kind in edge_kinds {
                let ids = self.data_gen.edge_ids(edge_kind);
                self.helper.check_get_edge_none(si, edge_kind, ids.iter());
            }
        }

        pub fn check_get_edge_err_of_edge_kinds(&self, si: SnapshotId, edge_kinds: &Vec<EdgeKind>) {
            for edge_kind in edge_kinds {
                let ids = self.data_gen.edge_ids(edge_kind);
                self.helper.check_get_edge_err(si, &edge_kind, ids.iter());
            }
        }

        pub fn check_get_not_exist_edge_of_edge_kinds(&self, si: SnapshotId, edge_kinds: &Vec<EdgeKind>) {
            for edge_kind in edge_kinds {
                let ids = self.data_gen.not_exist_edge_ids(edge_kind);
                self.helper.check_get_edge_none(si, edge_kind, ids.iter());
            }
        }

        pub fn check_get_out_edges(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            for (src_id, ids) in self.data_gen.all_out_edges_of_types(&edge_kinds) {
                self.helper.check_get_out_edges(si, src_id, label, ids);
            }
        }

        pub fn check_get_out_edges_with_deletion(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>, deleted_ids: Vec<EdgeId>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            let deleted: HashSet<EdgeId> = HashSet::from_iter(deleted_ids);
            for (src_id, ids) in self.data_gen.all_out_edges_of_types(&edge_kinds) {
                let ids = ids.difference(&deleted).map(|id| *id).collect();
                self.helper.check_get_out_edges(si, src_id, label, ids);
            }
        }

        pub fn check_get_out_edges_none(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            for (src_id, _) in self.data_gen.all_out_edges_of_types(&edge_kinds) {
                self.helper.check_get_out_edges(si, src_id, label, HashSet::new());
            }
        }

        pub fn check_get_out_edges_empty(&self, si: SnapshotId, label: Option<LabelId>) {
            for (src_id, _) in self.data_gen.all_out_edges(vec![label.unwrap()]) {
                self.helper.check_get_out_edges_empty(si, src_id, label);
            }
        }

        pub fn check_get_in_edges(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>) {
            self.check_get_in_edges_with_deletion(si, label, edge_kinds, vec![]);
        }

        pub fn check_get_in_edges_with_deletion(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>, delete_ids: Vec<EdgeId>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            let deleted = HashSet::from_iter(delete_ids);
            for (dst_id, ids) in self.data_gen.all_in_edges_of_types(&edge_kinds) {
                let ids = ids.difference(&deleted).map(|id| *id).collect();
                self.helper.check_get_in_edges(si, dst_id, label, ids);
            }
        }

        pub fn check_get_in_edges_none(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            for (dst_id, _) in self.data_gen.all_in_edges_of_types(&edge_kinds) {
                self.helper.check_get_in_edges(si, dst_id, label, HashSet::new());
            }
        }

        pub fn check_get_in_edges_empty(&self, si: SnapshotId, label: Option<LabelId>) {
            for (dst_id, _) in self.data_gen.all_in_edges(vec![label.unwrap()]) {
                self.helper.check_get_in_edges_empty(si, dst_id, label);
            }
        }

        pub fn check_query_edges(&self, si: SnapshotId, label: Option<LabelId>, kinds: &Vec<EdgeKind>) {
            self.check_query_edges_with_deletion(si, label, kinds, vec![]);
        }

        pub fn check_query_edges_none(&self, si: SnapshotId, label: Option<LabelId>) {
            self.helper.check_query_edges(si, label, HashSet::new());
        }

        pub fn check_query_edges_with_deletion(&self, si: SnapshotId, label: Option<LabelId>, edge_kinds: &Vec<EdgeKind>, deleted_ids: Vec<EdgeId>) {
            assert!(label.is_none() || edge_kinds.iter().all(|t| t.edge_label_id == label.unwrap()));
            let ids = self.data_gen.all_edge_ids_of_types(edge_kinds);
            let deletion = HashSet::from_iter(deleted_ids);
            let ids = ids.difference(&deletion).map(|id| *id).collect();
            self.helper.check_query_edges(si, label, ids);
        }

        pub fn check_query_edges_empty(&self, si: SnapshotId, label: Option<LabelId>) {
            self.helper.check_query_edges_empty(si, label);
        }

        fn check_all_data(&self, si: SnapshotId, map: HashMap<LabelId, Vec<EdgeKind>>) {
            for (label, edge_kinds) in map.clone() {
                self.check_get_edge_of_edge_kinds(si, &edge_kinds);
                self.check_query_edges(si, Some(label), &edge_kinds);
                self.check_get_out_edges(si, Some(label), &edge_kinds);
                self.check_get_in_edges(si, Some(label), &edge_kinds);
            }
            let all_edge_kinds = map.values()
                .flat_map(|v| v.iter())
                .map(|t| t.clone())
                .collect();
            self.check_get_out_edges(si, None, &all_edge_kinds);
            self.check_get_in_edges(si, None, &all_edge_kinds);
            self.check_query_edges(si, None, &all_edge_kinds);
        }

        fn group_edge_kinds(&self, edge_kinds: &Vec<EdgeKind>) -> HashMap<LabelId, Vec<EdgeKind>> {
            let mut ret = HashMap::new();
            for edge_kind in edge_kinds {
                ret.entry(edge_kind.edge_label_id).or_insert_with(|| Vec::new()).push(edge_kind.clone());
            }
            ret
        }
    }
}
