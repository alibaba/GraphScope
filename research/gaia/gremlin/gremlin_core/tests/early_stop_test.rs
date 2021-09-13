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

    #[test]
    // g.V(1).repeat(both('KNOWS')).times(5).dedup().limit(1)
    fn early_stop_test_01() {
        initialize();
        let expected = 1;
        let test_job_factory = TestJobFactory::with_expect_result_num(expected);
        let pb_request = read_pb_request(gen_path("early_stop_test_01")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }

    #[test]
    // g.V(1).where(both('KNOWS').both('KNOWS').both('KNOWS')).limit(1)
    fn early_stop_test_02() {
        initialize();
        let expected = 1;
        let test_job_factory = TestJobFactory::with_expect_result_num(expected);
        let pb_request = read_pb_request(gen_path("early_stop_test_02")).expect("read pb failed");
        run_test_with_worker_num(test_job_factory, pb_request, 2);
    }
}
