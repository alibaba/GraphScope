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

    // g.V().order().by(id)
    #[test]
    fn order_step_test_01() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 4, 6, 3, 5]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        let pb_request = read_pb_request(gen_path("order_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by(label)
    #[test]
    fn order_step_test_02() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 4, 6, 3, 5]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        let pb_request = read_pb_request(gen_path("order_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by("id")
    #[test]
    fn order_step_test_03() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        let pb_request = read_pb_request(gen_path("order_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").order().by(select("a").by(id))
    #[test]
    fn order_step_test_04() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 4, 6, 3, 5]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("order_step_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").order().by(select("a").by(label))
    #[test]
    fn order_step_test_05() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 4, 6, 3, 5]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("order_step_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").order().by(select("a").by("id"))
    #[test]
    fn order_step_test_06() {
        initialize();
        let expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("order_step_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().order().by(both().count()).by("id")
    #[test]
    fn order_step_test_07() {
        initialize();
        let expected = to_global_ids(vec![2, 5, 6, 1, 3, 4]);
        let mut test_job_factory = TestJobFactory::with_expect_ids(expected);
        test_job_factory.set_ordered(true);
        let pb_request = read_pb_request(gen_path("order_step_test_07")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
