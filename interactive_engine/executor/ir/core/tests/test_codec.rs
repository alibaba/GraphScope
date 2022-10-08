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
    use ir_core::catalogue::pattern::*;

    use crate::common::pattern_cases::*;

    #[test]
    fn test_encode_decode_one_vertex_pattern() {
        // Pattern has label 2
        let pattern = Pattern::from(PatternVertex::new(0, 2));
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case1() {
        let pattern = build_pattern_case1();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case2() {
        let pattern = build_pattern_case2();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case3() {
        let pattern = build_pattern_case3();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case4() {
        let pattern = build_pattern_case4();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case5() {
        let pattern = build_pattern_case5();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case1_vec_u8() {
        let pattern = build_pattern_case1();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case2_vec_u8() {
        let pattern = build_pattern_case2();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case3_vec_u8() {
        let pattern = build_pattern_case3();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case4_vec_u8() {
        let pattern = build_pattern_case4();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case5_vec_u8() {
        let pattern = build_pattern_case5();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case6_vec_u8() {
        let pattern = build_pattern_case6();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case7_vec_u8() {
        let pattern = build_pattern_case7();
        let code1 = pattern.encode_to();
        let pattern = Pattern::decode_from(&code1).unwrap();
        let code2 = pattern.encode_to();
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case1_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case2_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case3_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case4_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case5_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case6_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case7_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case8_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case9_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case10_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case11_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case12_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case13_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case14_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case15_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case16_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case17_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case18_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let pattern_code1 = pattern.encode_to();
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let pattern_code2 = pattern.encode_to();
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode = Pattern::decode_from(&pattern_code1).unwrap();
        let pattern_code_from_decode = pattern_from_decode.encode_to();
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }
}
