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

use core::f64;

use crate::object::Primitives;

// if the results occur overflow, the function will panic.
impl std::ops::Add for Primitives {
    type Output = Primitives;

    fn add(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(_), _) | (_, Double(_)) => Double(self.as_f64().unwrap() + other.as_f64().unwrap()),
            (Float(_), _) | (_, Float(_)) => Float(self.as_f32().unwrap() + other.as_f32().unwrap()),
            (ULLong(_), _) | (_, ULLong(_)) => {
                if self.is_negative() {
                    ULLong(other.as_u128().unwrap() - self.as_i128().unwrap().abs() as u128)
                } else if other.is_negative() {
                    ULLong(self.as_u128().unwrap() - other.as_i128().unwrap().abs() as u128)
                } else {
                    ULLong(self.as_u128().unwrap() + other.as_u128().unwrap())
                }
            }
            (ULong(_), _) | (_, ULong(_)) => {
                if self.is_negative() {
                    ULong(other.as_u64().unwrap() - self.as_i64().unwrap().abs() as u64)
                } else if other.is_negative() {
                    ULong(self.as_u64().unwrap() - other.as_i64().unwrap().abs() as u64)
                } else {
                    ULong(self.as_u64().unwrap() + other.as_u64().unwrap())
                }
            }
            (Long(_), _) | (_, Long(_)) => Long(self.as_i64().unwrap() + other.as_i64().unwrap()),
            (UInteger(_), _) | (_, UInteger(_)) => {
                if self.is_negative() {
                    UInteger(other.as_u32().unwrap() - self.as_i32().unwrap().abs() as u32)
                } else if other.is_negative() {
                    UInteger(self.as_u32().unwrap() - other.as_i32().unwrap().abs() as u32)
                } else {
                    UInteger(self.as_u32().unwrap() + other.as_u32().unwrap())
                }
            }
            (Integer(_), _) | (_, Integer(_)) => Integer(self.as_i32().unwrap() + other.as_i32().unwrap()),
            (Byte(_), _) => Byte(self.as_i8().unwrap() + other.as_i8().unwrap()),
        }
    }
}

impl std::ops::Sub for Primitives {
    type Output = Primitives;

    fn sub(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(_), _) | (_, Double(_)) => Double(self.as_f64().unwrap() - other.as_f64().unwrap()),
            (Float(_), _) | (_, Float(_)) => Float(self.as_f32().unwrap() - other.as_f32().unwrap()),
            (ULLong(_), _) | (_, ULLong(_)) => {
                if self.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else if other.is_negative() {
                    ULLong(self.as_u128().unwrap() + other.as_i128().unwrap().abs() as u128)
                } else {
                    ULLong(self.as_u128().unwrap() - other.as_u128().unwrap())
                }
            }
            (ULong(_), _) | (_, ULong(_)) => {
                if self.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else if other.is_negative() {
                    ULong(self.as_u64().unwrap() + other.as_i64().unwrap().abs() as u64)
                } else {
                    ULong(self.as_u64().unwrap() - other.as_u64().unwrap())
                }
            }
            (Long(_), _) | (_, Long(_)) => Long(self.as_i64().unwrap() - other.as_i64().unwrap()),
            (UInteger(_), _) | (_, UInteger(_)) => {
                if self.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else if other.is_negative() {
                    UInteger(self.as_u32().unwrap() + other.as_i32().unwrap().abs() as u32)
                } else {
                    UInteger(self.as_u32().unwrap() - other.as_u32().unwrap())
                }
            }
            (Integer(_), _) | (_, Integer(_)) => Integer(self.as_i32().unwrap() - other.as_i32().unwrap()),
            (Byte(_), _) => Byte(self.as_i8().unwrap() - other.as_i8().unwrap()),
        }
    }
}

impl std::ops::Mul for Primitives {
    type Output = Primitives;

    fn mul(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(_), _) | (_, Double(_)) => Double(self.as_f64().unwrap() * other.as_f64().unwrap()),
            (Float(_), _) | (_, Float(_)) => Float(self.as_f32().unwrap() * other.as_f32().unwrap()),
            (ULLong(_), _) | (_, ULLong(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    ULLong(self.as_u128().unwrap() * other.as_u128().unwrap())
                }
            }
            (ULong(_), _) | (_, ULong(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    ULong(self.as_u64().unwrap() * other.as_u64().unwrap())
                }
            }

            (Long(_), _) | (_, Long(_)) => Long(self.as_i64().unwrap() * other.as_i64().unwrap()),
            (UInteger(_), _) | (_, UInteger(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    UInteger(self.as_u32().unwrap() * other.as_u32().unwrap())
                }
            }
            (Integer(_), _) | (_, Integer(_)) => Integer(self.as_i32().unwrap() * other.as_i32().unwrap()),
            (Byte(_), _) => Byte(self.as_i8().unwrap() * other.as_i8().unwrap()),
        }
    }
}

impl std::ops::Div for Primitives {
    type Output = Primitives;

    fn div(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        if other == Byte(0)
            || other == Integer(0)
            || other == UInteger(0)
            || other == Long(0)
            || other == ULong(0)
            || other == ULLong(0)
            || other == Double(0.0)
            || other == Float(0.0)
        {
            if self.is_negative() {
                return Double(f64::NEG_INFINITY);
            } else {
                return Double(f64::INFINITY);
            }
        }
        match (self, other) {
            (Double(_), _) | (_, Double(_)) => Double(self.as_f64().unwrap() / other.as_f64().unwrap()),
            (Float(_), _) | (_, Float(_)) => Float(self.as_f32().unwrap() / other.as_f32().unwrap()),
            (ULLong(_), _) | (_, ULLong(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    ULLong(self.as_u128().unwrap() / other.as_u128().unwrap())
                }
            }
            (ULong(_), _) | (_, ULong(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    ULong(self.as_u64().unwrap() / other.as_u64().unwrap())
                }
            }

            (Long(_), _) | (_, Long(_)) => Long(self.as_i64().unwrap() / other.as_i64().unwrap()),
            (UInteger(_), _) | (_, UInteger(_)) => {
                if self.is_negative() || other.is_negative() {
                    // must be negative and will overflow
                    panic!("overflow");
                } else {
                    UInteger(self.as_u32().unwrap() / other.as_u32().unwrap())
                }
            }
            (Integer(_), _) | (_, Integer(_)) => Integer(self.as_i32().unwrap() / other.as_i32().unwrap()),
            (Byte(_), _) => Byte(self.as_i8().unwrap() / other.as_i8().unwrap()),
        }
    }
}

impl std::ops::Rem for Primitives {
    type Output = Primitives;

    fn rem(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a % b),
            (Byte(a), Integer(b)) => Integer(a as i32 % b),
            (Byte(a), Long(b)) => Long(a as i64 % b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 % b),
            (Byte(a), Double(b)) => Double(a as f64 % b),
            (Integer(a), Byte(b)) => Integer(a % b as i32),
            (Integer(a), Integer(b)) => Integer(a % b),
            (Integer(a), Long(b)) => Long(a as i64 % b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 % b),
            (Integer(a), Double(b)) => Double(a as f64 % b),
            (Long(a), Byte(b)) => Long(a % b as i64),
            (Long(a), Integer(b)) => Long(a % b as i64),
            (Long(a), Long(b)) => Long(a % b),
            (Long(a), ULLong(b)) => ULLong(a as u128 % b),
            (Long(a), Double(b)) => Double(a as f64 % b),
            (ULLong(a), Byte(b)) => ULLong(a % b as u128),
            (ULLong(a), Integer(b)) => ULLong(a % b as u128),
            (ULLong(a), Long(b)) => ULLong(a % b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a % b),
            // u128 as f64, can overflow
            (ULLong(a), Double(b)) => Double(a as f64 % b),
            (Double(a), Byte(b)) => Double(a % b as f64),
            (Double(a), Integer(b)) => Double(a % b as f64),
            (Double(a), Long(b)) => Double(a % b as f64),
            // u128 as f64, can overflow
            (Double(a), ULLong(b)) => Double(a % b as f64),
            (Double(a), Double(b)) => Double(a % b),
            _ => unimplemented!(),
        }
    }
}

impl std::ops::Neg for Primitives {
    type Output = Primitives;

    fn neg(self) -> Self::Output {
        use super::Primitives::*;
        match self {
            Byte(a) => Byte(-a),
            Integer(a) => Integer(-a),
            Long(a) => Long(-a),
            Float(a) => Float(-a),
            Double(a) => Double(-a),
            // TODO: may overflow
            UInteger(a) => Integer(-(a as i32)),
            ULong(a) => Long(-(a as i64)),
            ULLong(a) => Long(-(a as i128) as i64),
        }
    }
}

/// The exponential operator
pub trait Exp {
    type Output;

    fn exp(self, other: Self) -> Self::Output;
}

fn exp_positive(this: Primitives, other: Primitives) -> Primitives {
    use super::Primitives::*;
    match (this, other) {
        (Byte(a), Byte(b)) => Byte(a.pow(b as u32)),
        (Byte(a), Integer(b)) => Integer((a as i32).pow(b as u32)),
        (Byte(a), Long(b)) => Long((a as i64).pow(b as u32)),
        (Byte(a), ULLong(b)) => ULLong((a as u128).pow(b as u32)),
        (Integer(a), Byte(b)) => Integer(a.pow(b as u32)),
        (Integer(a), Integer(b)) => Integer(a.pow(b as u32)),
        (Integer(a), Long(b)) => Long((a as i64).pow(b as u32)),
        (Integer(a), ULLong(b)) => ULLong((a as u128).pow(b as u32)),
        (Long(a), Byte(b)) => Long(a.pow(b as u32)),
        (Long(a), Integer(b)) => Long(a.pow(b as u32)),
        (Long(a), Long(b)) => Long(a.pow(b as u32)),
        (Long(a), ULLong(b)) => ULLong((a as u128).pow(b as u32)),
        (ULLong(a), Byte(b)) => ULLong(a.pow(b as u32)),
        (ULLong(a), Integer(b)) => ULLong(a.pow(b as u32)),
        (ULLong(a), Long(b)) => ULLong(a.pow(b as u32)),
        (ULLong(a), ULLong(b)) => ULLong(a.pow(b as u32)),
        (Double(a), Byte(b)) => Double(a.powi(b as i32)),
        (Double(a), Integer(b)) => Double(a.powi(b as i32)),
        (Double(a), Long(b)) => Double(a.powi(b as i32)),
        (Double(a), ULLong(b)) => Double(a.powi(b as i32)),
        (Byte(a), Double(b)) => Double((a as f64).powf(b)),
        (Integer(a), Double(b)) => Double((a as f64).powf(b)),
        (Long(a), Double(b)) => Double((a as f64).powf(b)),
        (ULLong(a), Double(b)) => Double((a as f64).powf(b)),
        (Double(a), Double(b)) => Double((a as f64).powf(b)),
        _ => unimplemented!(),
    }
}

impl Exp for Primitives {
    type Output = Primitives;

    fn exp(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        if other >= Integer(0) {
            exp_positive(self, other)
        } else {
            // -minus has no problem on UULong in this case
            let pos = exp_positive(self, -other);
            Double(1.0) / pos
        }
    }
}

/// Bit operations
pub trait BitOperand {
    type Output;

    fn bit_and(self, other: Self) -> Self::Output;
    fn bit_or(self, other: Self) -> Self::Output;
    fn bit_xor(self, other: Self) -> Self::Output;
    fn bit_left_shift(self, other: Self) -> Self::Output;
    fn bit_right_shift(self, other: Self) -> Self::Output;
}

impl BitOperand for Primitives {
    type Output = Primitives;

    fn bit_and(self, other: Self) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a & b),
            (Byte(a), Integer(b)) => Integer((a as i32) & b),
            (Byte(a), Long(b)) => Long((a as i64) & b),
            (Byte(a), ULLong(b)) => ULLong((a as u128) & b),
            (Integer(a), Byte(b)) => Integer(a & (b as i32)),
            (Integer(a), Integer(b)) => Integer(a & b),
            (Integer(a), Long(b)) => Long((a as i64) & b),
            (Integer(a), ULLong(b)) => ULLong((a as u128) & b),
            (Long(a), Byte(b)) => Long(a & (b as i64)),
            (Long(a), Integer(b)) => Long(a & (b as i64)),
            (Long(a), Long(b)) => Long(a & b),
            (Long(a), ULLong(b)) => ULLong((a as u128) & b),
            (ULLong(a), Byte(b)) => ULLong(a & (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a & (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a & (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a & b),
            (Float(_), _) | (_, Float(_)) | (Double(_), _) | (_, Double(_)) => {
                panic!("cannot apply bit operations on `Float` or `Double` type")
            }
            _ => unimplemented!(),
        }
    }

    fn bit_or(self, other: Self) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a | b),
            (Byte(a), Integer(b)) => Integer((a as i32) | b),
            (Byte(a), Long(b)) => Long((a as i64) | b),
            (Byte(a), ULLong(b)) => ULLong((a as u128) | b),
            (Integer(a), Byte(b)) => Integer(a | (b as i32)),
            (Integer(a), Integer(b)) => Integer(a | b),
            (Integer(a), Long(b)) => Long((a as i64) | b),
            (Integer(a), ULLong(b)) => ULLong((a as u128) | b),
            (Long(a), Byte(b)) => Long(a | (b as i64)),
            (Long(a), Integer(b)) => Long(a | (b as i64)),
            (Long(a), Long(b)) => Long(a | b),
            (Long(a), ULLong(b)) => ULLong((a as u128) | b),
            (ULLong(a), Byte(b)) => ULLong(a | (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a | (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a | (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a | b),
            (Float(_), _) | (_, Float(_)) | (Double(_), _) | (_, Double(_)) => {
                panic!("cannot apply bit operations on `Float` or `Double` type")
            }
            _ => unimplemented!(),
        }
    }

    fn bit_xor(self, other: Self) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a ^ b),
            (Byte(a), Integer(b)) => Integer((a as i32) ^ b),
            (Byte(a), Long(b)) => Long((a as i64) ^ b),
            (Byte(a), ULLong(b)) => ULLong((a as u128) ^ b),
            (Integer(a), Byte(b)) => Integer(a ^ (b as i32)),
            (Integer(a), Integer(b)) => Integer(a ^ b),
            (Integer(a), Long(b)) => Long((a as i64) ^ b),
            (Integer(a), ULLong(b)) => ULLong((a as u128) ^ b),
            (Long(a), Byte(b)) => Long(a ^ (b as i64)),
            (Long(a), Integer(b)) => Long(a ^ (b as i64)),
            (Long(a), Long(b)) => Long(a ^ b),
            (Long(a), ULLong(b)) => ULLong((a as u128) ^ b),
            (ULLong(a), Byte(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a ^ b),
            (Float(_), _) | (_, Float(_)) | (Double(_), _) | (_, Double(_)) => {
                panic!("cannot apply bit operations on `Float` or `Double` type")
            }
            _ => unimplemented!(),
        }
    }

    fn bit_left_shift(self, other: Self) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a << b),
            (Byte(a), Integer(b)) => Integer((a as i32) << b),
            (Byte(a), Long(b)) => Long((a as i64) << b),
            (Byte(a), ULLong(b)) => ULLong((a as u128) << b),
            (Integer(a), Byte(b)) => Integer(a << (b as i32)),
            (Integer(a), Integer(b)) => Integer(a << b),
            (Integer(a), Long(b)) => Long((a as i64) << b),
            (Integer(a), ULLong(b)) => ULLong((a as u128) << b),
            (Long(a), Byte(b)) => Long(a << (b as i64)),
            (Long(a), Integer(b)) => Long(a << (b as i64)),
            (Long(a), Long(b)) => Long(a << b),
            (Long(a), ULLong(b)) => ULLong((a as u128) << b),
            (ULLong(a), Byte(b)) => ULLong(a << (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a << (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a << (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a << b),
            (Float(_), _) | (_, Float(_)) | (Double(_), _) | (_, Double(_)) => {
                panic!("cannot apply bit operations on `Float` or `Double` type")
            }
            _ => unimplemented!(),
        }
    }

    fn bit_right_shift(self, other: Self) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a >> b),
            (Byte(a), Integer(b)) => Integer((a as i32) >> b),
            (Byte(a), Long(b)) => Long((a as i64) >> b),
            (Byte(a), ULLong(b)) => ULLong((a as u128) >> b),
            (Integer(a), Byte(b)) => Integer(a >> (b as i32)),
            (Integer(a), Integer(b)) => Integer(a >> b),
            (Integer(a), Long(b)) => Long((a as i64) >> b),
            (Integer(a), ULLong(b)) => ULLong((a as u128) >> b),
            (Long(a), Byte(b)) => Long(a >> (b as i64)),
            (Long(a), Integer(b)) => Long(a >> (b as i64)),
            (Long(a), Long(b)) => Long(a >> b),
            (Long(a), ULLong(b)) => ULLong((a as u128) >> b),
            (ULLong(a), Byte(b)) => ULLong(a >> (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a >> (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a >> (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a >> b),
            (Float(_), _) | (_, Float(_)) | (Double(_), _) | (_, Double(_)) => {
                panic!("cannot apply bit operations on `Float` or `Double` type")
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
// test arithmetic operations
mod tests {
    use super::*;
    use std::panic;

    #[test]
    fn test_add_signed_with_unsigned() {
        // i32 + u32 -> u32
        let x = Primitives::Integer(-10);
        let y = Primitives::UInteger(20);

        if let Primitives::UInteger(result) = x + y {
            assert_eq!(result, 10);
        } else {
            panic!("Expected UInteger result");
        }

        // i32 + u64 -> u64
        let x = Primitives::Integer(-20);
        let y = Primitives::ULong(30);

        if let Primitives::ULong(result) = x + y {
            assert_eq!(result, 10);
        } else {
            panic!("Expected Long result");
        }

        // i64 + u32 -> i64
        let x = Primitives::Long(-20);
        let y = Primitives::UInteger(30);

        if let Primitives::Long(result) = x + y {
            assert_eq!(result, 10);
        } else {
            panic!("Expected Long result");
        }

        // i64 + u64 -> u64
        let x = Primitives::Long(-20);
        let y = Primitives::ULong(30);
        if let Primitives::ULong(result) = x + y {
            assert_eq!(result, 10);
        } else {
            panic!("Expected Long result");
        }
    }

    #[test]
    fn test_add_overflow() {
        // i32 + i32 -> i32 overflow
        let x = Primitives::Integer(i32::MAX);
        let y = Primitives::Integer(1);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // i64 + i64 -> i64 overflow
        let x = Primitives::Long(i64::MAX);
        let y = Primitives::Long(1);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // u32 + u32 -> u32 overflow
        let x = Primitives::UInteger(u32::MAX);
        let y = Primitives::UInteger(1);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // u64 + u64 -> u64 overflow
        let x = Primitives::ULong(u64::MAX);
        let y = Primitives::ULong(1);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // i32 + u32 -> u32 overflow
        let x = Primitives::Integer(-10);
        let y = Primitives::UInteger(5);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // i32 + u64 -> u64 overflow
        let x = Primitives::Integer(-10);
        let y = Primitives::ULong(5);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // i64 + u32 -> i64 not overflow
        let x = Primitives::Long(-10);
        let y = Primitives::UInteger(5);
        let res = x + y;
        if let Primitives::Long(result) = res {
            assert_eq!(result, -5);
        } else {
            panic!("Expected Long result");
        }

        // i64 + u32 -> i64 overflow
        let x = Primitives::Long(i64::MAX);
        let y = Primitives::UInteger(20);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());

        // i64 + u64 -> u64 overflow
        let x = Primitives::Long(-10);
        let y = Primitives::ULong(5);
        let res = panic::catch_unwind(|| x + y);
        assert!(res.is_err());
    }

    #[test]
    fn test_sub_signed_with_unsigned() {
        // i32 - u32 or u32 - i32, -> u32
        let x = Primitives::Integer(-10);
        let y = Primitives::UInteger(20);
        // -10 - 20
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());
        // 20 - (-10)
        if let Primitives::UInteger(result) = y - x {
            assert_eq!(result, 30);
        } else {
            panic!("Expected UInteger result");
        }

        // i32 - u64 or u64 - i32, -> u64
        let x = Primitives::Integer(-20);
        let y = Primitives::ULong(30);

        // -20 - 30
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());
        // 30 - (-20)
        if let Primitives::ULong(result) = y - x {
            assert_eq!(result, 50);
        } else {
            panic!("Expected Long result");
        }

        // i64 - u32 or u32 - i64, -> i64
        let x = Primitives::Long(-20);
        let y = Primitives::UInteger(30);

        // -20 - 30
        if let Primitives::Long(result) = x - y {
            assert_eq!(result, -50);
        } else {
            panic!("Expected Long result");
        }
        // 30 - (-20)
        if let Primitives::Long(result) = y - x {
            assert_eq!(result, 50);
        } else {
            panic!("Expected Long result");
        }

        // i64 - u64 or u64 - i64, -> u64
        let x = Primitives::Long(-20);
        let y = Primitives::ULong(30);
        // -20 - 30
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());
        // 30 - (-20)
        if let Primitives::ULong(result) = y - x {
            assert_eq!(result, 50);
        } else {
            panic!("Expected Long result");
        }
    }

    #[test]
    fn test_sub_overflow() {
        // i32 - i32 -> i32 overflow
        let x = Primitives::Integer(i32::MIN);
        let y = Primitives::Integer(1);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // i64 - i64 -> i64 overflow
        let x = Primitives::Long(i64::MIN);
        let y = Primitives::Long(1);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // u32 - u32 -> u32 overflow
        let x = Primitives::UInteger(0);
        let y = Primitives::UInteger(1);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // u64 - u64 -> u64 overflow
        let x = Primitives::ULong(0);
        let y = Primitives::ULong(1);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // i32 - u32 -> u32 overflow
        let x = Primitives::Integer(-10);
        let y = Primitives::UInteger(20);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // i32 - u64 -> u64 overflow
        let x = Primitives::Integer(-10);
        let y = Primitives::ULong(20);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // i64 - u32 -> i64 not overflow
        let x = Primitives::Long(-10);
        let y = Primitives::UInteger(20);
        let res = x - y;
        if let Primitives::Long(result) = res {
            assert_eq!(result, -30);
        } else {
            panic!("Expected Long result");
        }

        // i64 - u32 -> i64 overflow
        let x = Primitives::Long(i64::MIN);
        let y = Primitives::UInteger(20);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // i64 - u64 -> u64 overflow
        let x = Primitives::Long(-10);
        let y = Primitives::ULong(20);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());
    }

    #[test]
    fn test_div_overflow() {
        // divide_uint32_int32_overflow 4294967295/-1
        let x = Primitives::UInteger(u32::MAX);
        let y = Primitives::Integer(-1);
        let res = panic::catch_unwind(|| x / y);
        assert!(res.is_err());

        // divide_int32_int32_overflow -2147483648/-1
        let x = Primitives::Integer(i32::MIN);
        let y = Primitives::Integer(-1);
        let res = panic::catch_unwind(|| x / y);
        assert!(res.is_err());

        // divide_int64_int32_overflow|-9223372036854775808L,-1|overflow
        let x = Primitives::Long(i64::MIN);
        let y = Primitives::Integer(-1);
        let res = panic::catch_unwind(|| x / y);
        assert!(res.is_err());
    }

    #[test]
    fn test_float_cmp() {
        let epsilon = 1e-10;

        let x = Primitives::Float(3.14);
        let y = Primitives::Double(3.14);
        assert_ne!(x, y);

        // minus_int32_double|12,13.12d|-1.12d
        let x = Primitives::Integer(12);
        let y = Primitives::Double(13.12);
        let expected = -1.12f64;
        let res = x - y;
        if let Primitives::Double(result) = res {
            assert_ne!(result, expected);
            assert!((result - expected).abs() < epsilon);
        } else {
            panic!("Expected Double result");
        }

        // minus_float_double|12.0f,13.12d|-1.12d
        let x = Primitives::Float(12.0);
        let y = Primitives::Double(13.12);

        let expected = -1.12f64;
        let res = x - y;
        if let Primitives::Double(result) = res {
            assert_ne!(result, expected);
            assert!((result - expected).abs() < epsilon);
        } else {
            panic!("Expected Double result");
        }

        // divide_int32_double|2,1.12d|1.79d
        let epsilon = 1e-2;
        let x = Primitives::Integer(2);
        let y = Primitives::Double(1.12);
        let expected = 1.79f64;
        let res = x / y;
        if let Primitives::Double(result) = res {
            assert_ne!(result, expected);
            assert!((result - expected).abs() < epsilon);
        } else {
            panic!("Expected Double result");
        }
        //divide_float_double|2.0f,1.12d|1.79d
        let epsilon = 1e-2;
        let x = Primitives::Float(2.0);
        let y = Primitives::Double(1.12);
        let expected = 1.79f64;
        let res = x / y;
        if let Primitives::Double(result) = res {
            assert_ne!(result, expected);
            assert!((result - expected).abs() < epsilon);
        } else {
            panic!("Expected Double result");
        }
    }
}
