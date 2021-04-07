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

    // g.V().outE().inV()
    #[test]
    fn inv_step_test_01() {
        let mut expected = to_global_ids(vec![2, 3, 3, 3, 4, 5]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("inv_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().inE().outV()
    #[test]
    fn outv_step_test_01() {
        let mut expected = to_global_ids(vec![1, 1, 1, 4, 4, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("outv_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().path()
    #[test]
    #[ignore]
    // TODO: expected value
    fn path_step_test_01() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("path_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").select("a").by(id)
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_01() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").select("a").by(label)
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_02() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_02")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").select("a").by("id")
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_03() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_03")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").select("a").by(valueMap("id","name"))
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_04() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_04")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().groupCount().select(keys)
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_05() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_05")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().groupCount().select(values)
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_06() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_06")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // select tag where tag refers to a value
    // g.V().values("id").as("a").select("a")
    #[test]
    #[ignore]
    // TODO: expected value
    fn select_step_test_07() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_07")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").select("a")
    #[test]
    fn select_step_test_08() {
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("select_step_test_08")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().path().count(local)
    #[test]
    #[ignore]
    // TODO: expected value
    fn path_count_step_test_01() {
        let expected = vec![];
        initialize();
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let service = start_test_service(test_job_factory);
        let pb_request =
            read_pb_request(gen_path("path_count_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }
}
