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

    // g.V().hasLabel("PERSON")
    #[test]
    fn has_step_test_01() {
        let mut expected = to_global_ids(vec![1, 2, 4, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().hasId(1)
    #[test]
    fn has_step_test_02() {
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_02")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().has("name", "marko")
    #[test]
    fn has_step_test_03() {
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_03")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().has("id", neq(1))
    #[test]
    fn has_step_test_04() {
        let mut expected = to_global_ids(vec![2, 3, 4, 5, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_04")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().has("age", lte(28).or(gte(32)))
    #[test]
    fn has_step_test_05() {
        let mut expected = to_global_ids(vec![2, 4, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_05")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().has("age", inside(28,32))
    #[test]
    fn has_step_test_06() {
        let mut expected = to_global_ids(vec![1]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_06")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().has("name", without("marko","josh"))
    // TODO: with_in not implemented
    #[test]
    #[ignore]
    fn has_step_test_07() {
        let mut expected = to_global_ids(vec![2, 3, 5, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("has_step_test_07")).expect("read pb failed");
        submit_query(&service, pb_request);
    }

    // g.V().as("a").both("KNOWS").where(both("KNOWS").as("a"))
    #[test]
    fn where_step_test_01() {
        let mut expected = to_global_ids(vec![1, 1, 2, 4]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("where_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }
}
