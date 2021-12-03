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

    // g.V().hasLabel("PERSON").order().by("id") with property optimization turned on
    #[test]
    fn property_opt_test_01() {
        initialize();
        // expected property with saved property ("id") and unsaved property ("name" and "age")
        let expected_props = (vec!["id".into()], vec!["name".into(), "age".into()]);
        let test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        let pb_request = read_pb_request(gen_path("property_opt_test_01")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").order().by("id") with property optimization turned off
    #[test]
    fn property_opt_test_02() {
        initialize();
        // expect that all property ("id", "name" and "age") are saved
        let expected_props = (vec!["id".into(), "name".into(), "age".into()], vec![]);
        let test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        let pb_request = read_pb_request(gen_path("property_opt_test_02")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").order().by("id").by("name") with property optimization turned on
    #[test]
    fn property_opt_test_03() {
        initialize();
        // expected property with saved property ("id" and "name") and unsaved property ("age")
        let expected_props = (vec!["id".into(), "name".into()], vec!["age".into()]);
        let test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        let pb_request = read_pb_request(gen_path("property_opt_test_03")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").order().by("id").by("name") with property optimization turned off
    #[test]
    fn property_opt_test_04() {
        initialize();
        // expect that all property ("id", "name" and "age") are saved
        let expected_props = (vec!["id".into(), "name".into(), "age".into()], vec![]);
        let test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        let pb_request = read_pb_request(gen_path("property_opt_test_04")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").as("a").order().by("id").values("name").select("a") with property optimization turned on
    #[test]
    fn property_opt_test_05() {
        initialize();
        // expected property with saved property ("id" and "name") and unsaved property ("age")
        let expected_props = (vec!["id".into(), "name".into()], vec!["age".into()]);
        let mut test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("property_opt_test_05")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    //  g.V().hasLabel("PERSON").as("a").order().by("id").values("name").select("a") with property optimization turned off
    #[test]
    fn property_opt_test_06() {
        initialize();
        // expect that all property ("id", "name" and "age") are saved
        let expected_props = (vec!["id".into(), "name".into(), "age".into()], vec![]);
        let mut test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("property_opt_test_06")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").as("a").filter(has("name", "josh").has("age", 32)).values("id").select("a") with property optimization turned on
    #[test]
    fn property_opt_test_07() {
        initialize();
        // expected property with saved property ("id") and unsaved property ("age" and "name")
        let expected_props = (vec!["id".into()], vec!["name".into(), "age".into()]);
        let mut test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("property_opt_test_07")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }

    // g.V().hasLabel("PERSON").as("a").filter(has("name", "josh").has("age", 32)).values("id").select("a") with property optimization turned off
    #[test]
    fn property_opt_test_08() {
        initialize();
        // expect that all property ("id", "name" and "age") are saved
        let expected_props = (vec!["id".into(), "name".into(), "age".into()], vec![]);
        let mut test_job_factory = TestJobFactory::with_expect_property_opt(expected_props);
        test_job_factory.set_requirement(Requirement::LABELED_PATH);
        let pb_request = read_pb_request(gen_path("property_opt_test_08")).expect("read pb failed");
        run_test(test_job_factory, pb_request);
    }
}
