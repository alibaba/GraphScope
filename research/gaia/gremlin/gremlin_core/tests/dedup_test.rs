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
    // g.V().union(identity(),identity()).dedup()
    fn dedup_step_test_01() {
        let mut expected = to_global_ids(vec![1, 2, 3, 4, 5, 6]);
        expected.sort();
        initialize();
        let test_job_factory = TestJobFactory::with_expect_ids(expected);
        let service = start_test_service(test_job_factory);
        let pb_request = read_pb_request(gen_path("dedup_step_test_01")).expect("read pb failed");
        submit_query(&service, pb_request);
    }
}
