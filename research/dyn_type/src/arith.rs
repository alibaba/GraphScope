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
        (Float(a), Byte(b)) => Float(a.powi(b as i32)),
        (Float(a), Integer(b)) => Float(a.powi(b as i32)),
        (Float(a), Long(b)) => Float(a.powi(b as i32)),
        (Float(a), ULLong(b)) => Float(a.powi(b as i32)),
        (Byte(a), Float(b)) => Float((a as f64).powf(b)),
        (Integer(a), Float(b)) => Float((a as f64).powf(b)),
        (Long(a), Float(b)) => Float((a as f64).powf(b)),
        (ULLong(a), Float(b)) => Float((a as f64).powf(b)),
        (Float(a), Float(b)) => Float((a as f64).powf(b)),
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
            Float(1.0) / pos
        }
    }
}
