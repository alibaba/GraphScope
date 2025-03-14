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

// If the results occur overflow, the function will panic.
// You can catch the panic in the caller function.
impl std::ops::Add for Primitives {
    type Output = Primitives;

    fn add(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(a), Double(b)) => Double(a + b),
            (Double(a), Float(b)) => Double(a + b as f64),
            (Double(a), ULLong(b)) => Double(a + b as f64),
            (Double(a), ULong(b)) => Double(a + b as f64),
            (Double(a), Long(b)) => Double(a + b as f64),
            (Double(a), UInteger(b)) => Double(a + b as f64),
            (Double(a), Integer(b)) => Double(a + b as f64),
            (Double(a), Byte(b)) => Double(a + b as f64),
            (Float(a), Double(b)) => Double(a as f64 + b),
            (ULLong(a), Double(b)) => Double(a as f64 + b),
            (ULong(a), Double(b)) => Double(a as f64 + b),
            (Long(a), Double(b)) => Double(a as f64 + b),
            (UInteger(a), Double(b)) => Double(a as f64 + b),
            (Integer(a), Double(b)) => Double(a as f64 + b),
            (Byte(a), Double(b)) => Double(a as f64 + b),
            (Float(a), Float(b)) => Float(a + b),
            (Float(a), ULLong(b)) => Float(a + b as f32),
            (Float(a), ULong(b)) => Float(a + b as f32),
            (Float(a), Long(b)) => Float(a + b as f32),
            (Float(a), UInteger(b)) => Float(a + b as f32),
            (Float(a), Integer(b)) => Float(a + b as f32),
            (Float(a), Byte(b)) => Float(a + b as f32),
            (ULLong(a), Float(b)) => Float(a as f32 + b),
            (ULong(a), Float(b)) => Float(a as f32 + b),
            (Long(a), Float(b)) => Float(a as f32 + b),
            (UInteger(a), Float(b)) => Float(a as f32 + b),
            (Integer(a), Float(b)) => Float(a as f32 + b),
            (Byte(a), Float(b)) => Float(a as f32 + b),
            (ULLong(a), ULLong(b)) => ULLong(a + b),
            (ULLong(a), ULong(b)) => ULLong(a + b as u128),
            (ULLong(a), Long(b)) => {
                if b < 0 {
                    ULLong(a - b.abs() as u128)
                } else {
                    ULLong(a + b as u128)
                }
            }
            (ULLong(a), UInteger(b)) => ULLong(a + b as u128),
            (ULLong(a), Integer(b)) => {
                if b < 0 {
                    ULLong(a - b.abs() as u128)
                } else {
                    ULLong(a + b as u128)
                }
            }
            (ULLong(a), Byte(b)) => {
                if b < 0 {
                    ULLong(a - b.abs() as u128)
                } else {
                    ULLong(a + b as u128)
                }
            }
            (ULong(a), ULLong(b)) => ULLong(a as u128 + b),
            (Long(a), ULLong(b)) => {
                if a < 0 {
                    ULLong(b - a.abs() as u128)
                } else {
                    ULLong(a as u128 + b)
                }
            }
            (UInteger(a), ULLong(b)) => ULLong(a as u128 + b),
            (Integer(a), ULLong(b)) => {
                if a < 0 {
                    ULLong(b - a.abs() as u128)
                } else {
                    ULLong(a as u128 + b)
                }
            }
            (Byte(a), ULLong(b)) => {
                if a < 0 {
                    ULLong(b - a.abs() as u128)
                } else {
                    ULLong(a as u128 + b)
                }
            }
            (ULong(a), ULong(b)) => ULong(a + b),
            (ULong(a), Long(b)) => {
                if b < 0 {
                    ULong(a - b.abs() as u64)
                } else {
                    ULong(a + b as u64)
                }
            }
            (ULong(a), UInteger(b)) => ULong(a + b as u64),
            (ULong(a), Integer(b)) => {
                if b < 0 {
                    ULong(a - b.abs() as u64)
                } else {
                    ULong(a + b as u64)
                }
            }
            (ULong(a), Byte(b)) => {
                if b < 0 {
                    ULong(a - b.abs() as u64)
                } else {
                    ULong(a + b as u64)
                }
            }
            (Long(a), ULong(b)) => {
                if a < 0 {
                    ULong(b - a.abs() as u64)
                } else {
                    ULong(a as u64 + b)
                }
            }
            (UInteger(a), ULong(b)) => ULong(a as u64 + b),
            (Integer(a), ULong(b)) => {
                if a < 0 {
                    ULong(b - a.abs() as u64)
                } else {
                    ULong(a as u64 + b)
                }
            }
            (Byte(a), ULong(b)) => {
                if a < 0 {
                    ULong(b - a.abs() as u64)
                } else {
                    ULong(a as u64 + b)
                }
            }
            (Long(a), Long(b)) => Long(a + b),
            (Long(a), UInteger(b)) => Long(a + b as i64),
            (Long(a), Integer(b)) => Long(a + b as i64),
            (Long(a), Byte(b)) => Long(a + b as i64),
            (UInteger(a), Long(b)) => Long(a as i64 + b),
            (Integer(a), Long(b)) => Long(a as i64 + b),
            (Byte(a), Long(b)) => Long(a as i64 + b),
            (UInteger(a), UInteger(b)) => UInteger(a + b),
            (UInteger(a), Integer(b)) => {
                if b < 0 {
                    UInteger(a - b.abs() as u32)
                } else {
                    UInteger(a + b as u32)
                }
            }
            (UInteger(a), Byte(b)) => {
                if b < 0 {
                    UInteger(a - b.abs() as u32)
                } else {
                    UInteger(a + b as u32)
                }
            }
            (Integer(a), UInteger(b)) => {
                if a < 0 {
                    UInteger(b - a.abs() as u32)
                } else {
                    UInteger(a as u32 + b)
                }
            }
            (Byte(a), UInteger(b)) => {
                if a < 0 {
                    UInteger(b - a.abs() as u32)
                } else {
                    UInteger(a as u32 + b)
                }
            }
            (Integer(a), Integer(b)) => Integer(a + b),
            (Integer(a), Byte(b)) => Integer(a + b as i32),
            (Byte(a), Integer(b)) => Integer(a as i32 + b),
            (Byte(a), Byte(b)) => Byte(a + b),
        }
    }
}

// If the results occur overflow, the function will panic.
// You can catch the panic in the caller function.
impl std::ops::Sub for Primitives {
    type Output = Primitives;

    fn sub(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(a), Double(b)) => Double(a - b),
            (Double(a), Float(b)) => Double(a - b as f64),
            (Double(a), ULLong(b)) => Double(a - b as f64),
            (Double(a), ULong(b)) => Double(a - b as f64),
            (Double(a), Long(b)) => Double(a - b as f64),
            (Double(a), UInteger(b)) => Double(a - b as f64),
            (Double(a), Integer(b)) => Double(a - b as f64),
            (Double(a), Byte(b)) => Double(a - b as f64),
            (Float(a), Double(b)) => Double(a as f64 - b),
            (ULLong(a), Double(b)) => Double(a as f64 - b),
            (ULong(a), Double(b)) => Double(a as f64 - b),
            (Long(a), Double(b)) => Double(a as f64 - b),
            (UInteger(a), Double(b)) => Double(a as f64 - b),
            (Integer(a), Double(b)) => Double(a as f64 - b),
            (Byte(a), Double(b)) => Double(a as f64 - b),
            (Float(a), Float(b)) => Float(a - b),
            (Float(a), ULLong(b)) => Float(a - b as f32),
            (Float(a), ULong(b)) => Float(a - b as f32),
            (Float(a), Long(b)) => Float(a - b as f32),
            (Float(a), UInteger(b)) => Float(a - b as f32),
            (Float(a), Integer(b)) => Float(a - b as f32),
            (Float(a), Byte(b)) => Float(a - b as f32),
            (ULLong(a), Float(b)) => Float(a as f32 - b),
            (ULong(a), Float(b)) => Float(a as f32 - b),
            (Long(a), Float(b)) => Float(a as f32 - b),
            (UInteger(a), Float(b)) => Float(a as f32 - b),
            (Integer(a), Float(b)) => Float(a as f32 - b),
            (Byte(a), Float(b)) => Float(a as f32 - b),
            (ULLong(a), ULLong(b)) => ULLong(a - b),
            (ULLong(a), ULong(b)) => ULLong(a - b as u128),
            (ULLong(a), Long(b)) => {
                if b < 0 {
                    ULLong(a + b.abs() as u128)
                } else {
                    // if a < b, a - b will overflow
                    ULLong(a - b as u128)
                }
            }
            (ULLong(a), UInteger(b)) => ULLong(a - b as u128),
            (ULLong(a), Integer(b)) => {
                if b < 0 {
                    ULLong(a + b.abs() as u128)
                } else {
                    ULLong(a - b as u128)
                }
            }
            (ULLong(a), Byte(b)) => {
                if b < 0 {
                    ULLong(a + b.abs() as u128)
                } else {
                    ULLong(a - b as u128)
                }
            }
            (ULong(a), ULLong(b)) => ULLong(a as u128 - b),
            // could be an unexpected result if a < 0, so as the follows when we do the subtraction between a signed negative number and an unsigned number
            (Long(a), ULLong(b)) => ULLong(a as u128 - b),
            (UInteger(a), ULLong(b)) => ULLong(a as u128 - b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 - b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 - b),
            (ULong(a), ULong(b)) => ULong(a - b),
            (ULong(a), Long(b)) => {
                if b < 0 {
                    ULong(a + b.abs() as u64)
                } else {
                    ULong(a - b as u64)
                }
            }
            (ULong(a), UInteger(b)) => ULong(a - b as u64),
            (ULong(a), Integer(b)) => {
                if b < 0 {
                    ULong(a + b.abs() as u64)
                } else {
                    ULong(a - b as u64)
                }
            }
            (ULong(a), Byte(b)) => {
                if b < 0 {
                    ULong(a + b.abs() as u64)
                } else {
                    ULong(a - b as u64)
                }
            }
            (Long(a), ULong(b)) => ULong(a as u64 - b),
            (UInteger(a), ULong(b)) => ULong(a as u64 - b),
            (Integer(a), ULong(b)) => ULong(a as u64 - b),
            (Byte(a), ULong(b)) => ULong(a as u64 - b),
            (Long(a), Long(b)) => Long(a - b),
            (Long(a), UInteger(b)) => Long(a - b as i64),
            (Long(a), Integer(b)) => Long(a - b as i64),
            (Long(a), Byte(b)) => Long(a - b as i64),
            (UInteger(a), Long(b)) => Long(a as i64 - b),
            (Integer(a), Long(b)) => Long(a as i64 - b),
            (Byte(a), Long(b)) => Long(a as i64 - b),
            (UInteger(a), UInteger(b)) => UInteger(a - b),
            (UInteger(a), Integer(b)) => {
                if b < 0 {
                    UInteger(a + b.abs() as u32)
                } else {
                    UInteger(a - b as u32)
                }
            }
            (UInteger(a), Byte(b)) => {
                if b < 0 {
                    UInteger(a + b.abs() as u32)
                } else {
                    UInteger(a - b as u32)
                }
            }
            (Integer(a), UInteger(b)) => UInteger(a as u32 - b),
            (Byte(a), UInteger(b)) => UInteger(a as u32 - b),
            (Integer(a), Integer(b)) => Integer(a - b),
            (Integer(a), Byte(b)) => Integer(a - b as i32),
            (Byte(a), Integer(b)) => Integer(a as i32 - b),
            (Byte(a), Byte(b)) => Byte(a - b),
        }
    }
}

// If the results occur overflow, the function will panic.
// You can catch the panic in the caller function.
impl std::ops::Mul for Primitives {
    type Output = Primitives;

    fn mul(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Double(a), Double(b)) => Double(a * b),
            (Double(a), Float(b)) => Double(a * b as f64),
            (Double(a), ULLong(b)) => Double(a * b as f64),
            (Double(a), ULong(b)) => Double(a * b as f64),
            (Double(a), Long(b)) => Double(a * b as f64),
            (Double(a), UInteger(b)) => Double(a * b as f64),
            (Double(a), Integer(b)) => Double(a * b as f64),
            (Double(a), Byte(b)) => Double(a * b as f64),
            (Float(a), Double(b)) => Double(a as f64 * b),
            (ULLong(a), Double(b)) => Double(a as f64 * b),
            (ULong(a), Double(b)) => Double(a as f64 * b),
            (Long(a), Double(b)) => Double(a as f64 * b),
            (UInteger(a), Double(b)) => Double(a as f64 * b),
            (Integer(a), Double(b)) => Double(a as f64 * b),
            (Byte(a), Double(b)) => Double(a as f64 * b),
            (Float(a), Float(b)) => Float(a * b),
            (Float(a), ULLong(b)) => Float(a * b as f32),
            (Float(a), ULong(b)) => Float(a * b as f32),
            (Float(a), Long(b)) => Float(a * b as f32),
            (Float(a), UInteger(b)) => Float(a * b as f32),
            (Float(a), Integer(b)) => Float(a * b as f32),
            (Float(a), Byte(b)) => Float(a * b as f32),
            (ULLong(a), Float(b)) => Float(a as f32 * b),
            (ULong(a), Float(b)) => Float(a as f32 * b),
            (Long(a), Float(b)) => Float(a as f32 * b),
            (UInteger(a), Float(b)) => Float(a as f32 * b),
            (Integer(a), Float(b)) => Float(a as f32 * b),
            (Byte(a), Float(b)) => Float(a as f32 * b),
            (ULLong(a), ULLong(b)) => ULLong(a * b),
            (ULLong(a), ULong(b)) => ULLong(a * b as u128),
            // could be an unexpected result if b < 0, so as the follows when we do the multiplication between a signed negative number and an unsigned number
            (ULLong(a), Long(b)) => ULLong(a * b as u128),
            (ULLong(a), UInteger(b)) => ULLong(a * b as u128),
            (ULLong(a), Integer(b)) => ULLong(a * b as u128),
            (ULLong(a), Byte(b)) => ULLong(a * b as u128),
            (ULong(a), ULLong(b)) => ULLong(a as u128 * b),
            (Long(a), ULLong(b)) => ULLong(a as u128 * b),
            (UInteger(a), ULLong(b)) => ULLong(a as u128 * b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 * b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 * b),
            (ULong(a), ULong(b)) => ULong(a * b),
            (ULong(a), Long(b)) => ULong(a * b as u64),
            (ULong(a), UInteger(b)) => ULong(a * b as u64),
            (ULong(a), Integer(b)) => ULong(a * b as u64),
            (ULong(a), Byte(b)) => ULong(a * b as u64),
            (Long(a), ULong(b)) => ULong(a as u64 * b),
            (UInteger(a), ULong(b)) => ULong(a as u64 * b),
            (Integer(a), ULong(b)) => ULong(a as u64 * b),
            (Byte(a), ULong(b)) => ULong(a as u64 * b),
            (Long(a), Long(b)) => Long(a * b),
            (Long(a), UInteger(b)) => Long(a * b as i64),
            (Long(a), Integer(b)) => Long(a * b as i64),
            (Long(a), Byte(b)) => Long(a * b as i64),
            (UInteger(a), Long(b)) => Long(a as i64 * b),
            (Integer(a), Long(b)) => Long(a as i64 * b),
            (Byte(a), Long(b)) => Long(a as i64 * b),
            (UInteger(a), UInteger(b)) => UInteger(a * b),
            (UInteger(a), Integer(b)) => UInteger(a * b as u32),
            (UInteger(a), Byte(b)) => UInteger(a * b as u32),
            (Integer(a), UInteger(b)) => UInteger(a as u32 * b),
            (Byte(a), UInteger(b)) => UInteger(a as u32 * b),
            (Integer(a), Integer(b)) => Integer(a * b),
            (Integer(a), Byte(b)) => Integer(a * b as i32),
            (Byte(a), Integer(b)) => Integer(a as i32 * b),
            (Byte(a), Byte(b)) => Byte(a * b),
        }
    }
}

// If the results occur overflow, the function will panic.
// You can catch the panic in the caller function.
impl std::ops::Div for Primitives {
    type Output = Primitives;

    fn div(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        // when divide by zero,
        // if it is a integer division, it should panic
        // if it is a float division, it should return f64::INFINITY, f64::NEG_INFINITY, or f64::NAN, following IEEE 754 standard
        // currently, we follow the IEEE 754 standard for all division
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
            } else if self == Byte(0)
                || self == Integer(0)
                || self == UInteger(0)
                || self == Long(0)
                || self == ULong(0)
                || self == ULLong(0)
                || self == Double(0.0)
                || self == Float(0.0)
            {
                return Double(f64::NAN);
            } else {
                return Double(f64::INFINITY);
            }
        }
        match (self, other) {
            (Double(a), Double(b)) => Double(a / b),
            (Double(a), Float(b)) => Double(a / b as f64),
            (Double(a), ULLong(b)) => Double(a / b as f64),
            (Double(a), ULong(b)) => Double(a / b as f64),
            (Double(a), Long(b)) => Double(a / b as f64),
            (Double(a), UInteger(b)) => Double(a / b as f64),
            (Double(a), Integer(b)) => Double(a / b as f64),
            (Double(a), Byte(b)) => Double(a / b as f64),
            (Float(a), Double(b)) => Double(a as f64 / b),
            (ULLong(a), Double(b)) => Double(a as f64 / b),
            (ULong(a), Double(b)) => Double(a as f64 / b),
            (Long(a), Double(b)) => Double(a as f64 / b),
            (UInteger(a), Double(b)) => Double(a as f64 / b),
            (Integer(a), Double(b)) => Double(a as f64 / b),
            (Byte(a), Double(b)) => Double(a as f64 / b),
            (Float(a), Float(b)) => Float(a / b),
            (Float(a), ULLong(b)) => Float(a / b as f32),
            (Float(a), ULong(b)) => Float(a / b as f32),
            (Float(a), Long(b)) => Float(a / b as f32),
            (Float(a), UInteger(b)) => Float(a / b as f32),
            (Float(a), Integer(b)) => Float(a / b as f32),
            (Float(a), Byte(b)) => Float(a / b as f32),
            (ULLong(a), Float(b)) => Float(a as f32 / b),
            (ULong(a), Float(b)) => Float(a as f32 / b),
            (Long(a), Float(b)) => Float(a as f32 / b),
            (UInteger(a), Float(b)) => Float(a as f32 / b),
            (Integer(a), Float(b)) => Float(a as f32 / b),
            (Byte(a), Float(b)) => Float(a as f32 / b),
            (ULLong(a), ULLong(b)) => ULLong(a / b),
            (ULLong(a), ULong(b)) => ULLong(a / b as u128),
            // could be an unexpected result if b < 0, so as the follows when we do the division between a signed negative number and an unsigned number
            (ULLong(a), Long(b)) => ULLong(a / b as u128),
            (ULLong(a), UInteger(b)) => ULLong(a / b as u128),
            (ULLong(a), Integer(b)) => ULLong(a / b as u128),
            (ULLong(a), Byte(b)) => ULLong(a / b as u128),
            (ULong(a), ULLong(b)) => ULLong(a as u128 / b),
            (Long(a), ULLong(b)) => ULLong(a as u128 / b),
            (UInteger(a), ULLong(b)) => ULLong(a as u128 / b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 / b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 / b),
            (ULong(a), ULong(b)) => ULong(a / b),
            (ULong(a), Long(b)) => ULong(a / b as u64),
            (ULong(a), UInteger(b)) => ULong(a / b as u64),
            (ULong(a), Integer(b)) => ULong(a / b as u64),
            (ULong(a), Byte(b)) => ULong(a / b as u64),
            (Long(a), ULong(b)) => ULong(a as u64 / b),
            (UInteger(a), ULong(b)) => ULong(a as u64 / b),
            (Integer(a), ULong(b)) => ULong(a as u64 / b),
            (Byte(a), ULong(b)) => ULong(a as u64 / b),
            (Long(a), Long(b)) => Long(a / b),
            (Long(a), UInteger(b)) => Long(a / b as i64),
            (Long(a), Integer(b)) => Long(a / b as i64),
            (Long(a), Byte(b)) => Long(a / b as i64),
            (UInteger(a), Long(b)) => Long(a as i64 / b),
            (Integer(a), Long(b)) => Long(a as i64 / b),
            (Byte(a), Long(b)) => Long(a as i64 / b),
            (UInteger(a), UInteger(b)) => UInteger(a / b),
            (UInteger(a), Integer(b)) => UInteger(a / b as u32),
            (UInteger(a), Byte(b)) => UInteger(a / b as u32),
            (Integer(a), UInteger(b)) => UInteger(a as u32 / b),
            (Byte(a), UInteger(b)) => UInteger(a as u32 / b),
            (Integer(a), Integer(b)) => Integer(a / b),
            (Integer(a), Byte(b)) => Integer(a / b as i32),
            (Byte(a), Integer(b)) => Integer(a as i32 / b),
            (Byte(a), Byte(b)) => Byte(a / b),
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
    use std::{panic, u32};

    #[test]
    fn test_simple() {
        let a: f64 = -1.0;
        let b: u32 = a as u32;
        assert_eq!(b, 0);

        let a: i64 = -1;
        let b: u32 = a as u32;
        assert_eq!(b, 4294967295);

        // this will overflow and cannot pass the compilation
        // let a: u128 = 1;
        // let b: i64 = 2;
        // let c: u128 = a - b as u128;
    }

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
        let x = Primitives::ULLong(1);
        let y = Primitives::Long(2);
        let res = panic::catch_unwind(|| x - y);
        assert!(res.is_err());

        // u32 - i32, -> u32
        let x = Primitives::Integer(-10);
        let y = Primitives::UInteger(20);
        // 20 - (-10)
        if let Primitives::UInteger(result) = y - x {
            assert_eq!(result, 30);
        } else {
            panic!("Expected UInteger result");
        }

        // u64 - i32, -> u64
        let x = Primitives::Integer(-20);
        let y = Primitives::ULong(30);
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

        // u64 - i64, -> u64
        let x = Primitives::Long(-20);
        let y = Primitives::ULong(30);
        // 30 - (-20)
        if let Primitives::ULong(result) = y - x {
            assert_eq!(result, 50);
        } else {
            panic!("Expected Long result");
        }

        // i32 - u32 -> u32, when i32 is negative
        let x = Primitives::Integer(-1);
        let y = Primitives::UInteger(0);
        // -1_i32 - 0_u32, won't overflow, but the result is not correct
        assert_eq!(x - y, Primitives::UInteger(u32::MAX));

        // i32 - u64 -> u64, when i32 is negative
        let x = Primitives::Integer(-1);
        let y = Primitives::ULong(0);
        // -1_i32 - 0_u64, won't overflow, but the result is not correct
        assert_eq!(x - y, Primitives::ULong(u64::MAX));

        // i64 - u64 -> u64, when i64 is negative
        let x = Primitives::Long(-1);
        let y = Primitives::ULong(0);
        // -1_i64 - 0_u64, won't overflow, but the result is not correct
        assert_eq!(x - y, Primitives::ULong(u64::MAX));
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
    }

    #[test]
    fn test_div_overflow() {
        // divide_uint32_int32_overflow 4294967295/-1
        let x = Primitives::UInteger(u32::MAX);
        let y = Primitives::Integer(-1);
        // divide u32 by a negative number, won't panic, but the result is not correct
        assert_eq!(x / y, Primitives::UInteger(1));

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
