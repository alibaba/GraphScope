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
    use gremlin_core::process::traversal::traverser::Requirement;

    // g.V().hasLabel("PERSON")
    #[test]
    fn has_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 2, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasId(1)
    #[test]
    fn has_step_test_02() {
        initialize();
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("name", "marko")
    #[test]
    fn has_step_test_03() {
        initialize();
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("id", neq(1))
    #[test]
    fn has_step_test_04() {
        initialize();
        let mut expected = to_global_ids(vec![2, 3, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("age", lte(28).or(gte(32)))
    #[test]
    fn has_step_test_05() {
        initialize();
        let mut expected = to_global_ids(vec![2, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("age", inside(28,32))
    #[test]
    fn has_step_test_06() {
        initialize();
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("age", P.without(27))
    #[test]
    fn has_step_test_07() {
        initialize();
        let mut expected = to_global_ids(vec![1, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_07")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().has("age", P.within(27))
    #[test]
    fn has_step_test_08() {
        initialize();
        let mut expected = to_global_ids(vec![2]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("has_step_test_08")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").both("KNOWS").where(both("KNOWS").as("a"))
    #[test]
    fn where_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 2, 4]);
        expected.sort();
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("where_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").out("created").where(__.as("a").values("name").is("josh")).in("created")
    #[test]
    fn where_step_test_02() {
        initialize();
        let mut expected = to_global_ids(vec![1, 4, 4, 6]);
        expected.sort();
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("where_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").out("created").in("created").as("b").where("a", P.gt("b")).by("age")
    #[test]
    fn where_step_test_03() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 4]);
        expected.sort();
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("where_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().count().unfold().is(6)
    #[test]
    fn is_step_test_01() {
        initialize();
        let expected = vec![6.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("is_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().count().unfold().is(neq(6))
    #[test]
    fn is_step_test_02() {
        initialize();
        let expected = vec![];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("is_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().count().unfold().is(gt(5))
    #[test]
    fn is_step_test_03() {
        initialize();
        let expected = vec![6.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("is_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().count().unfold().is(lt(7))
    #[test]
    fn is_step_test_04() {
        initialize();
        let expected = vec![6.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("is_step_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
