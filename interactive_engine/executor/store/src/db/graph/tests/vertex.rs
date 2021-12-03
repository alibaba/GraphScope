use crate::db::api::*;
use super::types;
use super::helper::GraphTestHelper;
use crate::db::api::multi_version_graph::MultiVersionGraph;

pub fn test_get_vertex<G: MultiVersionGraph>(graph: G) {
    let tester = tester::GetVertexTester::new(graph);
    tester.execute();
}

pub fn test_query_vertices<G: MultiVersionGraph>(graph: G) {
    let tester = tester::QueryVerticesTester::new(graph);
    tester.execute();
}

pub fn test_update_vertex<G: MultiVersionGraph>(graph: G) {
    let tester = tester::UpdateVertexTester::new(graph);
    tester.execute();
}

pub fn test_delete_vertex<G: MultiVersionGraph>(graph: G) {
    let tester = tester::DeleteVertexTester::new(graph);
    tester.execute();
}

pub fn test_drop_vertex_type<G: MultiVersionGraph>(graph: G) {
    let tester = tester::DropVertexTypeTester::new(graph);
    tester.execute();
}

mod tester {
    use super::*;
    use super::common::*;
    use crate::db::api::multi_version_graph::MultiVersionGraph;

    pub struct GetVertexTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> GetVertexTester<G> {
        pub fn new(graph: G) -> Self {
            GetVertexTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = VertexDataGen::default();
            let mut schema_version = 1;
            let label1 = 1;
            helper.create_vertex_type(10, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_ids = data_gen.vertex_ids(label1);
            helper.insert_vertex(13, label1, label1_ids.clone()).unwrap();
            helper.insert_vertex(16, label1, label1_ids.clone()).unwrap();
            helper.insert_vertex(19, label1, label1_ids.clone()).unwrap();

            let label2 = 2;
            helper.create_vertex_type(20, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            let label2_ids = data_gen.vertex_ids(label2);
            helper.insert_vertex(23, label2, label2_ids.clone()).unwrap();
            helper.insert_vertex(26, label2, label2_ids.clone()).unwrap();
            helper.insert_vertex(29, label2, label2_ids.clone()).unwrap();

            let check_helper = VertexCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_get_vertex_err(si, label1);
                check_helper.check_get_vertex_err(si, label2);
                println!("check si#{} success", si);
            }

            for si in 10..30 {
                check_helper.check_get_vertex_err(si, 3);
                check_helper.check_get_vertex_err(si, 4);
                check_helper.check_get_not_exist_vertex(si, label1);
                if si < 20 {
                    check_helper.check_get_vertex_err(si, label2);
                    if si < 13 {
                        check_helper.check_get_vertex_none(si, label1);
                    } else {
                        check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    }
                } else {
                    check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    check_helper.check_get_not_exist_vertex(si, label2);
                    if si < 23 {
                        check_helper.check_get_vertex_none(si, label2);
                    } else {
                        check_helper.check_get_vertex(si, label2, label2_ids.clone());
                    }
                }
                println!("check si#{} success", si);
            }

        }
    }

    pub struct QueryVerticesTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> QueryVerticesTester<G> {
        pub fn new(graph: G) -> Self {
            QueryVerticesTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = VertexDataGen::default();
            let mut schema_version = 1;
            let label1 = 1;
            helper.create_vertex_type(10, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_ids = data_gen.vertex_ids(label1);
            helper.insert_vertex(13, label1, label1_ids.clone()).unwrap();
            helper.insert_vertex(16, label1, label1_ids.clone()).unwrap();
            helper.insert_vertex(19, label1, label1_ids.clone()).unwrap();

            let label2 = 2;
            helper.create_vertex_type(20, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            let label2_ids = data_gen.vertex_ids(label2);
            helper.insert_vertex(23, label2, label2_ids.clone()).unwrap();
            helper.insert_vertex(26, label2, label2_ids.clone()).unwrap();
            helper.insert_vertex(29, label2, label2_ids.clone()).unwrap();

            let check_helper = VertexCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_query_vertices_err(si, label1);
                check_helper.check_query_vertices_err(si, label2);
                println!("check si#{} success", si);
            }

            for si in 10..=30 {
                if si < 20 {
                    check_helper.check_query_vertices_err(si, label2);
                    if si < 13 {
                        check_helper.check_query_vertices_none(si, Some(label1));
                    } else {
                        check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                        check_helper.check_query_vertices(si, None, label1_ids.clone());
                    }
                } else {
                    check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                    if si < 23 {
                        check_helper.check_query_vertices_none(si, Some(label2));
                    } else {
                        let mut ids = Vec::new();
                        ids.extend_from_slice(&label1_ids);
                        ids.extend_from_slice(&label2_ids);
                        check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                        check_helper.check_query_vertices(si, None, ids);
                    }
                }
                println!("check si#{} success", si);
            }

        }
    }

    pub struct UpdateVertexTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> UpdateVertexTester<G> {
        pub fn new(graph: G) -> Self {
            UpdateVertexTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = VertexDataGen::default();

            let label1 = 1;
            let mut schema_version = 1;
            helper.create_vertex_type(10, schema_version, label1, types::create_full_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_ids = data_gen.vertex_ids(label1);
            helper.insert_vertex(10, label1, label1_ids.clone()).unwrap();
            helper.update_vertex(13, label1, label1_ids.clone()).unwrap();
            helper.update_vertex(16, label1, label1_ids.clone()).unwrap();
            helper.update_vertex(19, label1, label1_ids.clone()).unwrap();

            let label2 = 2;
            helper.create_vertex_type(20, schema_version, label2, types::create_full_type_def(label2)).unwrap();
            schema_version += 1;
            let label2_ids = data_gen.vertex_ids(label2);
            helper.insert_vertex(20, label2, label2_ids.clone()).unwrap();
            helper.update_vertex(23, label2, label2_ids.clone()).unwrap();
            helper.update_vertex(26, label2, label2_ids.clone()).unwrap();
            helper.update_vertex(29, label2, label2_ids.clone()).unwrap();

            let label3 = 3;
            helper.create_vertex_type(30, schema_version, label3, types::create_full_type_def(label3)).unwrap();
            let label3_ids = data_gen.vertex_ids(label3);
            helper.update_vertex(33, label3, label3_ids.clone()).unwrap();
            helper.update_vertex(36, label3, label3_ids.clone()).unwrap();
            helper.update_vertex(39, label3, label3_ids.clone()).unwrap();

            let check_helper = VertexCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_query_vertices_err(si, label1);
                check_helper.check_query_vertices_err(si, label2);
                check_helper.check_query_vertices_err(si, label3);
                check_helper.check_query_vertices_none(si, None);
                check_helper.check_get_vertex_err(si, label1);
                check_helper.check_get_vertex_err(si, label2);
                check_helper.check_get_vertex_err(si, label3);
                println!("check si#{} success", si);
            }

            for si in 10..=40 {
                if si < 20 {
                    check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                    check_helper.check_query_vertices_err(si, label2);
                    check_helper.check_query_vertices_err(si, label3);
                    check_helper.check_query_vertices(si, None, label1_ids.clone());
                    check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    check_helper.check_get_vertex_err(si, label2);
                    check_helper.check_get_vertex_err(si, label3);
                } else if si < 30 {
                    let mut ids = Vec::new();
                    ids.extend_from_slice(&label1_ids);
                    ids.extend_from_slice(&label2_ids);
                    check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                    check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                    check_helper.check_query_vertices_err(si, label3);
                    check_helper.check_query_vertices(si, None, ids);
                    check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    check_helper.check_get_vertex(si, label2, label2_ids.clone());
                    check_helper.check_get_vertex_err(si, label3);
                } else {
                    let mut ids = Vec::new();
                    ids.extend_from_slice(&label1_ids);
                    ids.extend_from_slice(&label2_ids);
                    check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                    check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                    check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    check_helper.check_get_vertex(si, label2, label2_ids.clone());
                    if si < 33 {
                        check_helper.check_query_vertices_none(si, Some(label3));
                        check_helper.check_get_vertex_none(si, label3);
                    } else {
                        ids.extend_from_slice(&label3_ids);
                        check_helper.check_query_vertices(si, Some(label3), label3_ids.clone());
                        check_helper.check_get_vertex(si, label3, label3_ids.clone());
                    }
                    check_helper.check_query_vertices(si, None, ids);
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct DeleteVertexTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> DeleteVertexTester<G> {
        pub fn new(graph: G) -> Self {
            DeleteVertexTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let data_gen = VertexDataGen::default();
            let mut schema_version = 1;

            let label1 = 1;
            helper.create_vertex_type(10, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_ids = data_gen.vertex_ids(label1);
            let label1_left_ids = data_gen.left_ids(label1);
            helper.insert_vertex(10, label1, label1_ids.clone()).unwrap();
            helper.delete_vertex(15, label1, data_gen.deleted_vertex_ids(label1)).unwrap();

            let label2 = 2;
            helper.create_vertex_type(20, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            let label2_ids = data_gen.vertex_ids(label2);
            let label2_left_ids = data_gen.left_ids(label2);
            helper.insert_vertex(20, label2, label2_ids.clone()).unwrap();
            helper.delete_vertex(25, label2, data_gen.deleted_vertex_ids(label2)).unwrap();

            let check_helper = VertexCheckHelper::new(&helper, &data_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_get_vertex_err(si, label1);
                check_helper.check_get_vertex_err(si, label2);
                check_helper.check_query_vertices_err(si, label1);
                check_helper.check_query_vertices_err(si, label2);
                check_helper.check_query_vertices_none(si, None);
                println!("check si#{} success", si);
            }

            for si in 10..=30 {
                if si < 20 {
                    check_helper.check_get_vertex_err(si, label2);
                    check_helper.check_query_vertices_err(si, label2);
                    if si < 15 {
                        check_helper.check_get_vertex(si, label1, label1_ids.clone());
                        check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                        check_helper.check_query_vertices(si, None, label1_ids.clone());
                    } else {
                        check_helper.check_get_vertex(si, label1, label1_left_ids.clone());
                        check_helper.check_query_vertices(si, Some(label1), label1_left_ids.clone());
                        check_helper.check_query_vertices(si, None, label1_left_ids.clone());
                    }
                } else {
                    let mut ids = Vec::new();
                    ids.extend_from_slice(&label1_left_ids);
                    check_helper.check_get_vertex(si, label1, label1_left_ids.clone());
                    check_helper.check_query_vertices(si, Some(label1), label1_left_ids.clone());
                    if si < 25 {
                        check_helper.check_get_vertex(si, label2, label2_ids.clone());
                        check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                        ids.extend_from_slice(&label2_ids);
                        check_helper.check_query_vertices(si, None, ids);
                    } else {
                        check_helper.check_get_vertex(si, label2, label2_left_ids.clone());
                        check_helper.check_query_vertices(si, Some(label2), label2_left_ids.clone());
                        ids.extend_from_slice(&label2_left_ids);
                        check_helper.check_query_vertices(si, None, ids);
                    }
                }
                println!("check si#{} success", si);
            }
        }
    }

    pub struct DropVertexTypeTester<G: MultiVersionGraph> {
        graph: G,
    }

    impl<G: MultiVersionGraph> DropVertexTypeTester<G> {
        pub fn new(graph: G) -> Self {
            DropVertexTypeTester {
                graph,
            }
        }

        pub fn execute(&self) {
            let mut helper = GraphTestHelper::new(&self.graph);
            let date_gen = VertexDataGen::default();

            let label1 = 1;
            let mut schema_version = 1;
            helper.create_vertex_type(10, schema_version, label1, types::create_test_type_def(label1)).unwrap();
            schema_version += 1;
            let label1_ids = date_gen.vertex_ids(label1);
            helper.insert_vertex(10, label1, label1_ids.clone()).unwrap();

            let label2 = 2;
            helper.create_vertex_type(10, schema_version, label2, types::create_test_type_def(label2)).unwrap();
            schema_version += 1;
            let label2_ids = date_gen.vertex_ids(label2);
            helper.insert_vertex(10, label2, label2_ids.clone()).unwrap();


            helper.drop_vertex_type(13, schema_version, label1).unwrap();
            schema_version += 1;
            helper.drop_vertex_type(16, schema_version, label2).unwrap();

            let check_helper = VertexCheckHelper::new(&helper, &date_gen);
            for si in (1..10).step_by(3) {
                check_helper.check_query_vertices_err(si, label1);
                check_helper.check_query_vertices_err(si, label2);
                check_helper.check_query_vertices_none(si, None);
                check_helper.check_get_vertex_err(si, label1);
                check_helper.check_get_vertex_err(si, label2);
                println!("check si#{} success", si);
            }

            for si in 10..20 {
                if si < 13 {
                    let mut ids = Vec::new();
                    ids.extend_from_slice(&label1_ids);
                    ids.extend_from_slice(&label2_ids);
                    check_helper.check_get_vertex(si, label1, label1_ids.clone());
                    check_helper.check_get_vertex(si, label2, label2_ids.clone());
                    check_helper.check_query_vertices(si, Some(label1), label1_ids.clone());
                    check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                    check_helper.check_query_vertices(si, None, ids);
                } else if si < 16 {
                    let ids = label2_ids.clone();
                    check_helper.check_get_vertex_err(si, label1);
                    check_helper.check_get_vertex(si, label2, label2_ids.clone());
                    check_helper.check_query_vertices_err(si, label1);
                    check_helper.check_query_vertices(si, Some(label2), label2_ids.clone());
                    check_helper.check_query_vertices(si, None, ids);
                } else {
                    check_helper.check_query_vertices_err(si, label1);
                    check_helper.check_query_vertices_err(si, label2);
                    check_helper.check_query_vertices_none(si, None);
                    check_helper.check_get_vertex_err(si, label1);
                    check_helper.check_get_vertex_err(si, label2);
                }
                println!("check si#{} success", si);
            }
        }
    }
}

mod common {
    use super::*;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    pub struct VertexDataGen {
        id_count_per_label: usize,
    }

    impl VertexDataGen {
        pub fn default() -> Self {
            VertexDataGen {
                id_count_per_label: 100,
            }
        }

        pub fn vertex_ids(&self, label: LabelId) -> Vec<VertexId> {
            let start = label as VertexId * 100000;
            (start+1..=start+(self.id_count_per_label as VertexId)).collect()
        }

        pub fn deleted_vertex_ids(&self, label: LabelId) -> Vec<VertexId> {
            self.vertex_ids(label).into_iter().filter(|id| *id % 2 == 0).collect()
        }

        pub fn left_ids(&self, label: LabelId) -> Vec<VertexId> {
            self.vertex_ids(label).into_iter().filter(|id| *id % 2 == 1).collect()
        }

        pub fn not_exists_ids(&self, label: LabelId) -> Vec<VertexId> {
            self.vertex_ids(label).into_iter().map(|id| -id).collect()
        }
    }

    pub struct VertexCheckHelper<'a, 'b, G: MultiVersionGraph> {
        helper: &'a GraphTestHelper<'b, G>,
        data_gen: &'a VertexDataGen,
    }

    impl<'a, 'b, G: MultiVersionGraph> VertexCheckHelper<'a, 'b, G> {
        pub fn new(helper: &'a GraphTestHelper<'b, G>, data_gen: &'a VertexDataGen) -> Self {
            VertexCheckHelper {
                helper,
                data_gen,
            }
        }

        pub fn check_get_vertex(&self, si: SnapshotId, label: LabelId, ids: Vec<VertexId>) {
            self.helper.check_get_vertex(si, label, &ids);
        }

        pub fn check_get_not_exist_vertex(&self, si: SnapshotId, label: LabelId) {
            let ids = self.data_gen.not_exists_ids(label);
            self.helper.check_get_vertex_none(si, label, &ids);
        }

        pub fn check_get_vertex_none(&self, si: SnapshotId, label: LabelId) {
            let ids = self.data_gen.vertex_ids(label);
            self.helper.check_get_vertex_none(si, label, &ids);
        }

        pub fn check_get_vertex_err(&self, si: SnapshotId, label: LabelId) {
            let ids = self.data_gen.vertex_ids(label);
            self.helper.check_get_vertex_err(si, label, &ids);
        }

        pub fn check_query_vertices_err(&self, si: SnapshotId, label: LabelId) {
            self.helper.check_query_vertices_empty(si, label);
        }

        pub fn check_query_vertices_none(&self, si: SnapshotId, label: Option<LabelId>) {
            self.helper.check_query_vertices(si, label, HashSet::new());
        }

        pub fn check_query_vertices(&self, si: SnapshotId, label: Option<LabelId>, ids: Vec<VertexId>) {
            let ids = HashSet::from_iter(ids);
            self.helper.check_query_vertices(si, label, ids);
        }
    }
}
