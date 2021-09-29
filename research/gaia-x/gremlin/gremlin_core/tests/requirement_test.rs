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

    // g.V() with path requirement
    #[test]
    fn path_requirement_test_01() {
        initialize();
        let expected = 1;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::PATH);
        let pb_request =
            read_pb_request(gen_path("path_requirement_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").out().as("b").out() with path requirement
    #[test]
    fn path_requirement_test_02() {
        initialize();
        let expected = 3;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::PATH);
        let pb_request =
            read_pb_request(gen_path("path_requirement_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").out().as("b").out().select("a").select("b") with label path requirement
    #[test]
    fn path_requirement_test_03() {
        initialize();
        let expected = 5;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::PATH);
        let pb_request =
            read_pb_request(gen_path("path_requirement_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V() with label path requirement
    #[test]
    fn labeled_path_requirement_test_01() {
        initialize();
        let expected = 0;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().as("a").out().as("b").out().select("a").select("b") with label path requirement
    #[test]
    fn labeled_path_requirement_test_02() {
        initialize();
        let expected = 2;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    /// This case should be passed when remove_tag optimization is turned on
    #[test]
    // g.V().as("a") with label path requirement
    fn labeled_path_requirement_test_03() {
        initialize();
        let expected = 0;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    /// This case should be passed when remove_tag optimization is turned on
    #[test]
    // g.V().as("a").out().as("b").out() with label path requirement
    fn labeled_path_requirement_test_04() {
        initialize();
        let expected = 0;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    /// This case should be passed when remove_tag optimization is turned off
    #[test]
    // g.V().as("a") with label path requirement
    fn labeled_path_requirement_test_05() {
        initialize();
        let expected = 1;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    /// This case should be passed when remove_tag optimization is turned off
    #[test]
    // g.V().as("a").out().as("b").out() with label path requirement
    fn labeled_path_requirement_test_06() {
        initialize();
        let expected = 2;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request =
            read_pb_request(gen_path("label_path_requirement_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V() with object requirement
    #[test]
    fn object_requirement_test_01() {
        initialize();
        let expected = 0;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::OBJECT);
        let pb_request =
            read_pb_request(gen_path("object_requirement_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().out().out() with object requirement
    #[test]
    fn object_requirement_test_02() {
        initialize();
        let expected = 0;
        let mut test_job_factory = TestJobFactory::with_expect_path_len(expected);
        test_job_factory.set_requirement(Requirement::OBJECT);
        let pb_request =
            read_pb_request(gen_path("object_requirement_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
