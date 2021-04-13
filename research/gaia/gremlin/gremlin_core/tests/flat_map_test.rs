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

    // g.V().out()
    #[test]
    fn out_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![2, 3, 3, 3, 4, 5]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("out_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().in()
    #[test]
    fn in_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 1, 4, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("in_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().both()
    #[test]
    fn both_step_test_01() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 1, 2, 3, 3, 3, 4, 4, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("both_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().outE()
    #[test]
    fn oute_step_test_01() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2), (1, 3), (1, 4), (4, 3), (4, 5), (6, 3)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("oute_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().inE()
    #[test]
    fn ine_step_test_01() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2), (1, 3), (1, 4), (4, 3), (4, 5), (6, 3)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("ine_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().bothE("KNOWS")
    #[test]
    fn bothe_step_test_01() {
        initialize();
        let mut expected = eids_to_global_ids(vec![(1, 2), (1, 2), (1, 4), (1, 4)]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("bothe_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().values("id")
    #[test]
    fn values_step_test_01() {
        initialize();
        let expected = vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into(), 6.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("values_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
