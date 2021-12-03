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

    // g.V().out().order().by(id)
    #[test]
    fn out_step_test_w2() {
        initialize();
        let mut expected = to_global_ids(vec![2, 3, 3, 3, 4, 5]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("out_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    // g.V().in().order().by(id)
    #[test]
    fn in_step_test_w2() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 1, 4, 4, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("in_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    // g.V().both().order().by(id)
    #[test]
    fn both_step_test_w2() {
        initialize();
        let mut expected = to_global_ids(vec![1, 1, 1, 2, 3, 3, 3, 4, 4, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("both_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    // g.V().group().by().unfold().order().by(select(keys).values("id"))
    #[test]
    fn group_step_test_w2() {
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
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    // g.V().out().values("id").order()
    #[test]
    fn values_step_test_w2() {
        initialize();
        let expected = vec![2.into(), 3.into(), 3.into(), 3.into(), 4.into(), 5.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("values_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    // g.V().out().out().count()
    #[test]
    fn count_step_test_w2() {
        initialize();
        let expected = vec![2.into()];
        let test_job_factory = TestJobFactory::with_expect_values(expected);
        let pb_request = read_pb_request(gen_path("count_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    #[test]
    // g.V().union(identity(),identity()).dedup().order().by(id)
    fn dedup_step_test_w2() {
        initialize();
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let pb_request = read_pb_request(gen_path("dedup_step_test_w2")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }
}
