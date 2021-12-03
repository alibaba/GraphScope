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
mod test {
    use crate::common::test::*;
    use dyn_type::Object;
    use gremlin_core::process::traversal::traverser::Requirement;
    use gremlin_core::structure::{PropKey, Tag};

    // g.V().outE().inV()
    #[test]
    fn inv_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![2, 3, 3, 3, 4, 5]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("inv_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().inE().outV()
    #[test]
    fn outv_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 1, 4, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("outv_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().path()
    #[test]
    fn path_step_test_01() {
        initialize();
        let expected = vec![
            to_global_ids(vec![1]),
            to_global_ids(vec![2]),
            to_global_ids(vec![3]),
            to_global_ids(vec![4]),
            to_global_ids(vec![5]),
            to_global_ids(vec![6]),
        ];
        let mut test_job_factory = TestJobFactory::with_expect_path_result(expected);
        test_job_factory.set_requirement(Requirement::PATH);
        let pb_request = read_pb_request(gen_path("path_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by("id").as("a").select("a").by(id)
    #[test]
    fn select_step_test_01() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![1, 2, 3, 4, 5, 6]
            .into_iter()
            .map(|id| vec![(0 as Tag, vec![("".into(), (to_global_id(id) as i64).into())])])
            .collect();
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by("id").as("a").select("a").by(label)
    #[test]
    fn select_step_test_02() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![0, 0, 1, 0, 1, 0]
            .into_iter()
            .map(|label_id| vec![(0 as Tag, vec![("".into(), label_id.into())])])
            .collect();
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by("id").as("a").select("a").by("id")
    #[test]
    fn select_step_test_03() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![1, 2, 3, 4, 5, 6]
            .into_iter()
            .map(|id| vec![(0 as Tag, vec![("".into(), id.into())])])
            .collect();
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("SOFTWARE").order().by("id").as("a").select("a").by(valueMap("id","name"))
    #[test]
    fn select_step_test_04() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![
            vec![(0, vec![("id".into(), 3.into()), ("name".into(), "lop".into())])],
            vec![(0, vec![("id".into(), 5.into()), ("name".into(), "ripple".into())])],
        ];
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").hasLabel("SOFTWARE").order().by("id").as("b").select("a","b").by("id").by("name")
    #[test]
    fn select_step_test_05() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![
            vec![(0, vec![("".into(), 3.into())]), (1, vec![("".into(), "lop".into())])],
            vec![(0, vec![("".into(), 5.into())]), (1, vec![("".into(), "ripple".into())])],
        ];
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").hasLabel("SOFTWARE").order().by("id").as("b").select("a","b").by("id").by(valueMap("id","name"))
    #[test]
    fn select_step_test_6() {
        initialize();
        let expected: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>> = vec![
            vec![
                (0, vec![("".into(), 3.into())]),
                (1, vec![("id".into(), 3.into()), ("name".into(), "lop".into())]),
            ],
            vec![
                (0, vec![("".into(), 5.into())]),
                (1, vec![("id".into(), 5.into()), ("name".into(), "ripple".into())]),
            ],
        ];
        let mut test_job_factory = TestJobFactory::with_expect_get_properties(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().groupCount().unfold().select(keys).order()
    #[test]
    fn select_step_test_07() {
        initialize();
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("select_step_test_07")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().groupCount().unfold().select(values).order()
    #[test]
    fn select_step_test_08() {
        initialize();
        let expected = vec![1.into(), 1.into(), 1.into(), 1.into(), 1.into(), 1.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("select_step_test_08")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().values("id").as("a").select("a")
    #[test]
    fn select_step_test_09() {
        initialize();
        let expected = vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into(), 6.into()];
        let mut test_job_factory = TestJobFactory::with_expect_values(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_09")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").select("a")
    #[test]
    fn select_step_test_10() {
        initialize();
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("select_step_test_10")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().path().count(local)
    #[test]
    fn path_count_step_test_01() {
        initialize();
        let expected = vec![1.into(), 1.into(), 1.into(), 1.into(), 1.into(), 1.into()];
        let mut test_job_factory = TestJobFactory::with_expect_values(expected);
        test_job_factory.set_requirement(Requirement::PATH);
        let pb_request =
            read_pb_request(gen_path("path_count_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
