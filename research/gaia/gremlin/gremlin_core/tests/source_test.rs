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

    // g.V()
    #[test]
    fn source_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V(1)
    #[test]
    fn source_test_02() {
        initialize();
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").has("id",1)
    #[test]
    fn source_test_03() {
        initialize();
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.E()
    #[test]
    fn source_test_04() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2), (1, 4), (1, 3), (4, 5), (4, 3), (6, 3)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.E().hasLabel("knows")
    #[test]
    fn source_test_05() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2), (1, 4)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.E().hasLabel("knows").has("weight",gt(0.5))
    #[test]
    fn source_test_06() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 4)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.E(1)
    #[test]
    fn source_test_07() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("source_test_07")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
