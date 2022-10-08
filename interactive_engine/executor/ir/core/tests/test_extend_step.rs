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
mod tests {
    use crate::common::{extend_step_cases::*, pattern_cases::*, pattern_meta_cases::*};

    /// Test whether pattern1 + extend_step = pattern2
    #[test]
    fn test_pattern_case1_case2_extend_de_extend() {
        let pattern1 = build_pattern_case1();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_extend_step_case1(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_pattern_case2();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_pattern_case8_case9_extend_de_extend() {
        let pattern1 = build_pattern_case8();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_extend_step_case2(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_pattern_case9();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case3_extend_de_extend_1() {
        let pattern1 = build_modern_pattern_case1();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case1(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case3();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case3_extend_de_extend_2() {
        let pattern1 = build_modern_pattern_case1();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case2(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case3();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case4_extend_de_extend() {
        let pattern1 = build_modern_pattern_case1();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case3(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case4();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case2_case4_extend_de_extend() {
        let pattern1 = build_modern_pattern_case2();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case4(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case4();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case3_case5_extend_de_extend() {
        let pattern1 = build_modern_pattern_case3();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case6(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case5();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case4_case5_extend_de_extend() {
        let pattern1 = build_modern_pattern_case4();
        let pattern1_code = pattern1.encode_to();
        let extend_step = build_modern_extend_step_case5(&pattern1);
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case5();
        let pattern2_code = pattern2.encode_to();
        let pattern_after_extend_code = pattern_after_extend.encode_to();
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code)
            .unwrap();
        let pattern_after_de_extend_code = pattern_after_de_extend.encode_to();
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case1() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_only_pattern = build_modern_pattern_case1();
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 3);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case2() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let software_only_pattern = build_modern_pattern_case2();
        let all_extend_steps = software_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 1);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case3() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_knows_person = build_modern_pattern_case3();
        let all_extend_steps = person_knows_person.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 11);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case4() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_created_software = build_modern_pattern_case4();
        let all_extend_steps = person_created_software.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 6);
    }

    #[test]
    fn test_get_extend_steps_of_ldbc_case1() {
        let ldbc_pattern_meta = get_ldbc_pattern_meta();
        let person_knows_person = build_ldbc_pattern_case1();
        let all_extend_steps = person_knows_person.get_extend_steps(&ldbc_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 44);
    }
}
