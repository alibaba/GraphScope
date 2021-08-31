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

use crate::object::Primitives;

impl std::ops::Add for Primitives {
    type Output = Primitives;

    fn add(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a + b),
            (Byte(a), Integer(b)) => Integer(a as i32 + b),
            (Byte(a), Long(b)) => Long(a as i64 + b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 + b),
            (Byte(a), Float(b)) => Float(a as f64 + b),
            (Integer(a), Byte(b)) => Integer(a + b as i32),
            (Integer(a), Integer(b)) => Integer(a + b),
            (Integer(a), Long(b)) => Long(a as i64 + b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 + b),
            (Integer(a), Float(b)) => Float(a as f64 + b),
            (Long(a), Byte(b)) => Long(a + b as i64),
            (Long(a), Integer(b)) => Long(a + b as i64),
            (Long(a), Long(b)) => Long(a + b),
            (Long(a), ULLong(b)) => ULLong(a as u128 + b),
            (Long(a), Float(b)) => Float(a as f64 + b),
            (ULLong(a), Byte(b)) => ULLong(a + b as u128),
            (ULLong(a), Integer(b)) => ULLong(a + b as u128),
            (ULLong(a), Long(b)) => ULLong(a + b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a + b),
            // u128 as f64, can overflow
            (ULLong(a), Float(b)) => Float(a as f64 + b),
            (Float(a), Byte(b)) => Float(a + b as f64),
            (Float(a), Integer(b)) => Float(a + b as f64),
            (Float(a), Long(b)) => Float(a + b as f64),
            // u128 as f64, can overflow
            (Float(a), ULLong(b)) => Float(a + b as f64),
            (Float(a), Float(b)) => Float(a + b),
        }
    }
}

impl std::ops::Sub for Primitives {
    type Output = Primitives;

    fn sub(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a - b),
            (Byte(a), Integer(b)) => Integer(a as i32 - b),
            (Byte(a), Long(b)) => Long(a as i64 - b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 - b),
            (Byte(a), Float(b)) => Float(a as f64 - b),
            (Integer(a), Byte(b)) => Integer(a - b as i32),
            (Integer(a), Integer(b)) => Integer(a - b),
            (Integer(a), Long(b)) => Long(a as i64 - b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 - b),
            (Integer(a), Float(b)) => Float(a as f64 - b),
            (Long(a), Byte(b)) => Long(a - b as i64),
            (Long(a), Integer(b)) => Long(a - b as i64),
            (Long(a), Long(b)) => Long(a - b),
            (Long(a), ULLong(b)) => ULLong(a as u128 - b),
            (Long(a), Float(b)) => Float(a as f64 - b),
            (ULLong(a), Byte(b)) => ULLong(a - b as u128),
            (ULLong(a), Integer(b)) => ULLong(a - b as u128),
            (ULLong(a), Long(b)) => ULLong(a - b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a - b),
            // u128 as f64, can overflow
            (ULLong(a), Float(b)) => Float(a as f64 - b),
            (Float(a), Byte(b)) => Float(a - b as f64),
            (Float(a), Integer(b)) => Float(a - b as f64),
            (Float(a), Long(b)) => Float(a - b as f64),
            // u128 as f64, can overflow
            (Float(a), ULLong(b)) => Float(a - b as f64),
            (Float(a), Float(b)) => Float(a - b),
        }
    }
}

impl std::ops::Mul for Primitives {
    type Output = Primitives;

    fn mul(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a * b),
            (Byte(a), Integer(b)) => Integer(a as i32 * b),
            (Byte(a), Long(b)) => Long(a as i64 * b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 * b),
            (Byte(a), Float(b)) => Float(a as f64 * b),
            (Integer(a), Byte(b)) => Integer(a * b as i32),
            (Integer(a), Integer(b)) => Integer(a * b),
            (Integer(a), Long(b)) => Long(a as i64 * b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 * b),
            (Integer(a), Float(b)) => Float(a as f64 * b),
            (Long(a), Byte(b)) => Long(a * b as i64),
            (Long(a), Integer(b)) => Long(a * b as i64),
            (Long(a), Long(b)) => Long(a * b),
            (Long(a), ULLong(b)) => ULLong(a as u128 * b),
            (Long(a), Float(b)) => Float(a as f64 * b),
            (ULLong(a), Byte(b)) => ULLong(a * b as u128),
            (ULLong(a), Integer(b)) => ULLong(a * b as u128),
            (ULLong(a), Long(b)) => ULLong(a * b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a * b),
            // u128 as f64, can overflow
            (ULLong(a), Float(b)) => Float(a as f64 * b),
            (Float(a), Byte(b)) => Float(a * b as f64),
            (Float(a), Integer(b)) => Float(a * b as f64),
            (Float(a), Long(b)) => Float(a * b as f64),
            // u128 as f64, can overflow
            (Float(a), ULLong(b)) => Float(a * b as f64),
            (Float(a), Float(b)) => Float(a * b),
        }
    }
}

impl std::ops::Div for Primitives {
    type Output = Primitives;

    fn div(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a / b),
            (Byte(a), Integer(b)) => Integer(a as i32 / b),
            (Byte(a), Long(b)) => Long(a as i64 / b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 / b),
            (Byte(a), Float(b)) => Float(a as f64 / b),
            (Integer(a), Byte(b)) => Integer(a / b as i32),
            (Integer(a), Integer(b)) => Integer(a / b),
            (Integer(a), Long(b)) => Long(a as i64 / b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 / b),
            (Integer(a), Float(b)) => Float(a as f64 / b),
            (Long(a), Byte(b)) => Long(a / b as i64),
            (Long(a), Integer(b)) => Long(a / b as i64),
            (Long(a), Long(b)) => Long(a / b),
            (Long(a), ULLong(b)) => ULLong(a as u128 / b),
            (Long(a), Float(b)) => Float(a as f64 / b),
            (ULLong(a), Byte(b)) => ULLong(a / b as u128),
            (ULLong(a), Integer(b)) => ULLong(a / b as u128),
            (ULLong(a), Long(b)) => ULLong(a / b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a / b),
            // u128 as f64, can overflow
            (ULLong(a), Float(b)) => Float(a as f64 / b),
            (Float(a), Byte(b)) => Float(a / b as f64),
            (Float(a), Integer(b)) => Float(a / b as f64),
            (Float(a), Long(b)) => Float(a / b as f64),
            // u128 as f64, can overflow
            (Float(a), ULLong(b)) => Float(a / b as f64),
            (Float(a), Float(b)) => Float(a / b),
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
            (Byte(a), Float(b)) => Float(a as f64 % b),
            (Integer(a), Byte(b)) => Integer(a % b as i32),
            (Integer(a), Integer(b)) => Integer(a % b),
            (Integer(a), Long(b)) => Long(a as i64 % b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 % b),
            (Integer(a), Float(b)) => Float(a as f64 % b),
            (Long(a), Byte(b)) => Long(a % b as i64),
            (Long(a), Integer(b)) => Long(a % b as i64),
            (Long(a), Long(b)) => Long(a % b),
            (Long(a), ULLong(b)) => ULLong(a as u128 % b),
            (Long(a), Float(b)) => Float(a as f64 % b),
            (ULLong(a), Byte(b)) => ULLong(a % b as u128),
            (ULLong(a), Integer(b)) => ULLong(a % b as u128),
            (ULLong(a), Long(b)) => ULLong(a % b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a % b),
            // u128 as f64, can overflow
            (ULLong(a), Float(b)) => Float(a as f64 % b),
            (Float(a), Byte(b)) => Float(a % b as f64),
            (Float(a), Integer(b)) => Float(a % b as f64),
            (Float(a), Long(b)) => Float(a % b as f64),
            // u128 as f64, can overflow
            (Float(a), ULLong(b)) => Float(a % b as f64),
            (Float(a), Float(b)) => Float(a % b),
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
            ULLong(a) => Long(-(a as i128) as i64),
        }
    }
}

/// The exponential operator
pub trait Exp {
    type Output;

    fn exp(self, other: Self) -> Self::Output;
}

impl Exp for Primitives {
    type Output = Primitives;

    fn exp(self, other: Primitives) -> Self::Output {
        use super::Primitives::*;
        match (self, other) {
            (Byte(a), Byte(b)) => Byte(a ^ b),
            (Byte(a), Integer(b)) => Integer(a as i32 ^ b),
            (Byte(a), Long(b)) => Long(a as i64 ^ b),
            (Byte(a), ULLong(b)) => ULLong(a as u128 ^ b),
            (Integer(a), Byte(b)) => Integer(a ^ (b as i32)),
            (Integer(a), Integer(b)) => Integer(a ^ b),
            (Integer(a), Long(b)) => Long(a as i64 ^ b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 ^ b),
            (Long(a), Byte(b)) => Long(a ^ (b as i64)),
            (Long(a), Integer(b)) => Long(a ^ (b as i64)),
            (Long(a), Long(b)) => Long(a ^ b),
            (Long(a), ULLong(b)) => ULLong(a as u128 ^ b),
            (ULLong(a), Byte(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), Integer(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), Long(b)) => ULLong(a ^ (b as u128)),
            (ULLong(a), ULLong(b)) => ULLong(a ^ b),
            (Float(_), _) | (_, Float(_)) => unimplemented!("no implementation for `f64 ^ f64`"),
        }
    }
}
