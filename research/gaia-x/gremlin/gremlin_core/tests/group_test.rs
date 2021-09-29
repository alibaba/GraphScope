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
    use gremlin_core::ID;

    // g.V().group().by().unfold().order().by(select(keys).values("id"))
    #[test]
    fn group_step_test_01() {
        initialize();
        let expected = vec![
            (to_global_id(1) as ID, to_global_ids(vec![1])),
            (to_global_id(2) as ID, to_global_ids(vec![2])),
            (to_global_id(3) as ID, to_global_ids(vec![3])),
            (to_global_id(4) as ID, to_global_ids(vec![4])),
            (to_global_id(5) as ID, to_global_ids(vec![5])),
            (to_global_id(6) as ID, to_global_ids(vec![6])),
        ];
        let test_job_factory = TestJobFactory::with_expect_map_result(expected);
        let pb_request = read_pb_request(gen_path("group_step_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().groupCount().unfold().order().by(select(keys).values("id"))
    #[test]
    fn group_step_test_02() {
        initialize();
        let expected = vec![
            (to_global_id(1) as ID, vec![1]),
            (to_global_id(2) as ID, vec![1]),
            (to_global_id(3) as ID, vec![1]),
            (to_global_id(4) as ID, vec![1]),
            (to_global_id(5) as ID, vec![1]),
            (to_global_id(6) as ID, vec![1]),
        ];
        let test_job_factory = TestJobFactory::with_expect_map_result(expected);
        let pb_request = read_pb_request(gen_path("group_step_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().group().by(id).unfold().order().by(keys)
    #[test]
    fn group_step_test_03() {
        initialize();
        let expected = vec![
            (to_global_id(1) as ID, to_global_ids(vec![1])),
            (to_global_id(2) as ID, to_global_ids(vec![2])),
            (to_global_id(4) as ID, to_global_ids(vec![4])),
            (to_global_id(6) as ID, to_global_ids(vec![6])),
            (to_global_id(3) as ID, to_global_ids(vec![3])),
            (to_global_id(5) as ID, to_global_ids(vec![5])),
        ];
        let test_job_factory = TestJobFactory::with_expect_map_result(expected);
        let pb_request = read_pb_request(gen_path("group_step_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().group().by(label).unfold().order().by(keys)
    #[test]
    fn group_step_test_04() {
        initialize();
        let expected =
            vec![(0 as ID, to_global_ids(vec![1, 2, 4, 6])), (1 as ID, to_global_ids(vec![3, 5]))];
        let test_job_factory = TestJobFactory::with_expect_map_result(expected);
        let pb_request = read_pb_request(gen_path("group_step_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().group().by("id").unfold().order().by(keys)
    #[test]
    fn group_step_test_05() {
        initialize();
        let expected = vec![
            (1 as ID, to_global_ids(vec![1])),
            (2 as ID, to_global_ids(vec![2])),
            (3 as ID, to_global_ids(vec![3])),
            (4 as ID, to_global_ids(vec![4])),
            (5 as ID, to_global_ids(vec![5])),
            (6 as ID, to_global_ids(vec![6])),
        ];
        let test_job_factory = TestJobFactory::with_expect_map_result(expected);
        let pb_request = read_pb_request(gen_path("group_step_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
