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
            (Byte(a), Double(b)) => Double(a as f64 + b),
            (Integer(a), Byte(b)) => Integer(a + b as i32),
            (Integer(a), Integer(b)) => Integer(a + b),
            (Integer(a), Long(b)) => Long(a as i64 + b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 + b),
            (Integer(a), Double(b)) => Double(a as f64 + b),
            (Long(a), Byte(b)) => Long(a + b as i64),
            (Long(a), Integer(b)) => Long(a + b as i64),
            (Long(a), Long(b)) => Long(a + b),
            (Long(a), ULLong(b)) => ULLong(a as u128 + b),
            (Long(a), Double(b)) => Double(a as f64 + b),
            (ULLong(a), Byte(b)) => ULLong(a + b as u128),
            (ULLong(a), Integer(b)) => ULLong(a + b as u128),
            (ULLong(a), Long(b)) => ULLong(a + b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a + b),
            // u128 as f64, can overflow
            (ULLong(a), Double(b)) => Double(a as f64 + b),
            (Double(a), Byte(b)) => Double(a + b as f64),
            (Double(a), Integer(b)) => Double(a + b as f64),
            (Double(a), Long(b)) => Double(a + b as f64),
            // u128 as f64, can overflow
            (Double(a), ULLong(b)) => Double(a + b as f64),
            (Double(a), Double(b)) => Double(a + b),
            (Byte(_), UInteger(_)) => todo!(),
            (Byte(_), ULong(_)) => todo!(),
            (Byte(_), Float(_)) => todo!(),
            (Integer(_), UInteger(_)) => todo!(),
            (Integer(_), ULong(_)) => todo!(),
            (Integer(_), Float(_)) => todo!(),
            (UInteger(_), Byte(_)) => todo!(),
            (UInteger(_), Integer(_)) => todo!(),
            (UInteger(_), UInteger(_)) => todo!(),
            (UInteger(_), Long(_)) => todo!(),
            (UInteger(_), ULong(_)) => todo!(),
            (UInteger(_), ULLong(_)) => todo!(),
            (UInteger(_), Float(_)) => todo!(),
            (UInteger(_), Double(_)) => todo!(),
            (Long(_), UInteger(_)) => todo!(),
            (Long(_), ULong(_)) => todo!(),
            (Long(_), Float(_)) => todo!(),
            (ULong(_), Byte(_)) => todo!(),
            (ULong(_), Integer(_)) => todo!(),
            (ULong(_), UInteger(_)) => todo!(),
            (ULong(_), Long(_)) => todo!(),
            (ULong(_), ULong(_)) => todo!(),
            (ULong(_), ULLong(_)) => todo!(),
            (ULong(_), Float(_)) => todo!(),
            (ULong(_), Double(_)) => todo!(),
            (ULLong(_), UInteger(_)) => todo!(),
            (ULLong(_), ULong(_)) => todo!(),
            (ULLong(_), Float(_)) => todo!(),
            (Float(_), Byte(_)) => todo!(),
            (Float(_), Integer(_)) => todo!(),
            (Float(_), UInteger(_)) => todo!(),
            (Float(_), Long(_)) => todo!(),
            (Float(_), ULong(_)) => todo!(),
            (Float(_), ULLong(_)) => todo!(),
            (Float(_), Float(_)) => todo!(),
            (Float(_), Double(_)) => todo!(),
            (Double(_), UInteger(_)) => todo!(),
            (Double(_), ULong(_)) => todo!(),
            (Double(_), Float(_)) => todo!(),
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
            (Byte(a), Double(b)) => Double(a as f64 - b),
            (Integer(a), Byte(b)) => Integer(a - b as i32),
            (Integer(a), Integer(b)) => Integer(a - b),
            (Integer(a), Long(b)) => Long(a as i64 - b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 - b),
            (Integer(a), Double(b)) => Double(a as f64 - b),
            (Long(a), Byte(b)) => Long(a - b as i64),
            (Long(a), Integer(b)) => Long(a - b as i64),
            (Long(a), Long(b)) => Long(a - b),
            (Long(a), ULLong(b)) => ULLong(a as u128 - b),
            (Long(a), Double(b)) => Double(a as f64 - b),
            (ULLong(a), Byte(b)) => ULLong(a - b as u128),
            (ULLong(a), Integer(b)) => ULLong(a - b as u128),
            (ULLong(a), Long(b)) => ULLong(a - b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a - b),
            // u128 as f64, can overflow
            (ULLong(a), Double(b)) => Double(a as f64 - b),
            (Double(a), Byte(b)) => Double(a - b as f64),
            (Double(a), Integer(b)) => Double(a - b as f64),
            (Double(a), Long(b)) => Double(a - b as f64),
            // u128 as f64, can overflow
            (Double(a), ULLong(b)) => Double(a - b as f64),
            (Double(a), Double(b)) => Double(a - b),
            (Byte(_), UInteger(_)) => todo!(),
            (Byte(_), ULong(_)) => todo!(),
            (Byte(_), Float(_)) => todo!(),
            (Integer(_), UInteger(_)) => todo!(),
            (Integer(_), ULong(_)) => todo!(),
            (Integer(_), Float(_)) => todo!(),
            (UInteger(_), Byte(_)) => todo!(),
            (UInteger(_), Integer(_)) => todo!(),
            (UInteger(_), UInteger(_)) => todo!(),
            (UInteger(_), Long(_)) => todo!(),
            (UInteger(_), ULong(_)) => todo!(),
            (UInteger(_), ULLong(_)) => todo!(),
            (UInteger(_), Float(_)) => todo!(),
            (UInteger(_), Double(_)) => todo!(),
            (Long(_), UInteger(_)) => todo!(),
            (Long(_), ULong(_)) => todo!(),
            (Long(_), Float(_)) => todo!(),
            (ULong(_), Byte(_)) => todo!(),
            (ULong(_), Integer(_)) => todo!(),
            (ULong(_), UInteger(_)) => todo!(),
            (ULong(_), Long(_)) => todo!(),
            (ULong(_), ULong(_)) => todo!(),
            (ULong(_), ULLong(_)) => todo!(),
            (ULong(_), Float(_)) => todo!(),
            (ULong(_), Double(_)) => todo!(),
            (ULLong(_), UInteger(_)) => todo!(),
            (ULLong(_), ULong(_)) => todo!(),
            (ULLong(_), Float(_)) => todo!(),
            (Float(_), Byte(_)) => todo!(),
            (Float(_), Integer(_)) => todo!(),
            (Float(_), UInteger(_)) => todo!(),
            (Float(_), Long(_)) => todo!(),
            (Float(_), ULong(_)) => todo!(),
            (Float(_), ULLong(_)) => todo!(),
            (Float(_), Float(_)) => todo!(),
            (Float(_), Double(_)) => todo!(),
            (Double(_), UInteger(_)) => todo!(),
            (Double(_), ULong(_)) => todo!(),
            (Double(_), Float(_)) => todo!(),
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
            (Byte(a), Double(b)) => Double(a as f64 * b),
            (Integer(a), Byte(b)) => Integer(a * b as i32),
            (Integer(a), Integer(b)) => Integer(a * b),
            (Integer(a), Long(b)) => Long(a as i64 * b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 * b),
            (Integer(a), Double(b)) => Double(a as f64 * b),
            (Long(a), Byte(b)) => Long(a * b as i64),
            (Long(a), Integer(b)) => Long(a * b as i64),
            (Long(a), Long(b)) => Long(a * b),
            (Long(a), ULLong(b)) => ULLong(a as u128 * b),
            (Long(a), Double(b)) => Double(a as f64 * b),
            (ULLong(a), Byte(b)) => ULLong(a * b as u128),
            (ULLong(a), Integer(b)) => ULLong(a * b as u128),
            (ULLong(a), Long(b)) => ULLong(a * b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a * b),
            // u128 as f64, can overflow
            (ULLong(a), Double(b)) => Double(a as f64 * b),
            (Double(a), Byte(b)) => Double(a * b as f64),
            (Double(a), Integer(b)) => Double(a * b as f64),
            (Double(a), Long(b)) => Double(a * b as f64),
            // u128 as f64, can overflow
            (Double(a), ULLong(b)) => Double(a * b as f64),
            (Double(a), Double(b)) => Double(a * b),
            (Byte(_), UInteger(_)) => todo!(),
            (Byte(_), ULong(_)) => todo!(),
            (Byte(_), Float(_)) => todo!(),
            (Integer(_), UInteger(_)) => todo!(),
            (Integer(_), ULong(_)) => todo!(),
            (Integer(_), Float(_)) => todo!(),
            (UInteger(_), Byte(_)) => todo!(),
            (UInteger(_), Integer(_)) => todo!(),
            (UInteger(_), UInteger(_)) => todo!(),
            (UInteger(_), Long(_)) => todo!(),
            (UInteger(_), ULong(_)) => todo!(),
            (UInteger(_), ULLong(_)) => todo!(),
            (UInteger(_), Float(_)) => todo!(),
            (UInteger(_), Double(_)) => todo!(),
            (Long(_), UInteger(_)) => todo!(),
            (Long(_), ULong(_)) => todo!(),
            (Long(_), Float(_)) => todo!(),
            (ULong(_), Byte(_)) => todo!(),
            (ULong(_), Integer(_)) => todo!(),
            (ULong(_), UInteger(_)) => todo!(),
            (ULong(_), Long(_)) => todo!(),
            (ULong(_), ULong(_)) => todo!(),
            (ULong(_), ULLong(_)) => todo!(),
            (ULong(_), Float(_)) => todo!(),
            (ULong(_), Double(_)) => todo!(),
            (ULLong(_), UInteger(_)) => todo!(),
            (ULLong(_), ULong(_)) => todo!(),
            (ULLong(_), Float(_)) => todo!(),
            (Float(_), Byte(_)) => todo!(),
            (Float(_), Integer(_)) => todo!(),
            (Float(_), UInteger(_)) => todo!(),
            (Float(_), Long(_)) => todo!(),
            (Float(_), ULong(_)) => todo!(),
            (Float(_), ULLong(_)) => todo!(),
            (Float(_), Float(_)) => todo!(),
            (Float(_), Double(_)) => todo!(),
            (Double(_), UInteger(_)) => todo!(),
            (Double(_), ULong(_)) => todo!(),
            (Double(_), Float(_)) => todo!(),
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
            (Byte(a), Double(b)) => Double(a as f64 / b),
            (Integer(a), Byte(b)) => Integer(a / b as i32),
            (Integer(a), Integer(b)) => Integer(a / b),
            (Integer(a), Long(b)) => Long(a as i64 / b),
            (Integer(a), ULLong(b)) => ULLong(a as u128 / b),
            (Integer(a), Double(b)) => Double(a as f64 / b),
            (Long(a), Byte(b)) => Long(a / b as i64),
            (Long(a), Integer(b)) => Long(a / b as i64),
            (Long(a), Long(b)) => Long(a / b),
            (Long(a), ULLong(b)) => ULLong(a as u128 / b),
            (Long(a), Double(b)) => Double(a as f64 / b),
            (ULLong(a), Byte(b)) => ULLong(a / b as u128),
            (ULLong(a), Integer(b)) => ULLong(a / b as u128),
            (ULLong(a), Long(b)) => ULLong(a / b as u128),
            (ULLong(a), ULLong(b)) => ULLong(a / b),
            // u128 as f64, can overflow
            (ULLong(a), Double(b)) => Double(a as f64 / b),
            (Double(a), Byte(b)) => Double(a / b as f64),
            (Double(a), Integer(b)) => Double(a / b as f64),
            (Double(a), Long(b)) => Double(a / b as f64),
            // u128 as f64, can overflow
            (Double(a), ULLong(b)) => Double(a / b as f64),
            (Double(a), Double(b)) => Double(a / b),
            (Byte(_), UInteger(_)) => todo!(),
            (Byte(_), ULong(_)) => todo!(),
            (Byte(_), Float(_)) => todo!(),
            (Integer(_), UInteger(_)) => todo!(),
            (Integer(_), ULong(_)) => todo!(),
            (Integer(_), Float(_)) => todo!(),
            (UInteger(_), Byte(_)) => todo!(),
            (UInteger(_), Integer(_)) => todo!(),
            (UInteger(_), UInteger(_)) => todo!(),
            (UInteger(_), Long(_)) => todo!(),
            (UInteger(_), ULong(_)) => todo!(),
            (UInteger(_), ULLong(_)) => todo!(),
            (UInteger(_), Float(_)) => todo!(),
            (UInteger(_), Double(_)) => todo!(),
            (Long(_), UInteger(_)) => todo!(),
            (Long(_), ULong(_)) => todo!(),
            (Long(_), Float(_)) => todo!(),
            (ULong(_), Byte(_)) => todo!(),
            (ULong(_), Integer(_)) => todo!(),
            (ULong(_), UInteger(_)) => todo!(),
            (ULong(_), Long(_)) => todo!(),
            (ULong(_), ULong(_)) => todo!(),
            (ULong(_), ULLong(_)) => todo!(),
            (ULong(_), Float(_)) => todo!(),
            (ULong(_), Double(_)) => todo!(),
            (ULLong(_), UInteger(_)) => todo!(),
            (ULLong(_), ULong(_)) => todo!(),
            (ULLong(_), Float(_)) => todo!(),
            (Float(_), Byte(_)) => todo!(),
            (Float(_), Integer(_)) => todo!(),
            (Float(_), UInteger(_)) => todo!(),
            (Float(_), Long(_)) => todo!(),
            (Float(_), ULong(_)) => todo!(),
            (Float(_), ULLong(_)) => todo!(),
            (Float(_), Float(_)) => todo!(),
            (Float(_), Double(_)) => todo!(),
            (Double(_), UInteger(_)) => todo!(),
            (Double(_), ULong(_)) => todo!(),
            (Double(_), Float(_)) => todo!(),
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
            (Byte(_), UInteger(_)) => todo!(),
            (Byte(_), ULong(_)) => todo!(),
            (Byte(_), Float(_)) => todo!(),
            (Integer(_), UInteger(_)) => todo!(),
            (Integer(_), ULong(_)) => todo!(),
            (Integer(_), Float(_)) => todo!(),
            (UInteger(_), Byte(_)) => todo!(),
            (UInteger(_), Integer(_)) => todo!(),
            (UInteger(_), UInteger(_)) => todo!(),
            (UInteger(_), Long(_)) => todo!(),
            (UInteger(_), ULong(_)) => todo!(),
            (UInteger(_), ULLong(_)) => todo!(),
            (UInteger(_), Float(_)) => todo!(),
            (UInteger(_), Double(_)) => todo!(),
            (Long(_), UInteger(_)) => todo!(),
            (Long(_), ULong(_)) => todo!(),
            (Long(_), Float(_)) => todo!(),
            (ULong(_), Byte(_)) => todo!(),
            (ULong(_), Integer(_)) => todo!(),
            (ULong(_), UInteger(_)) => todo!(),
            (ULong(_), Long(_)) => todo!(),
            (ULong(_), ULong(_)) => todo!(),
            (ULong(_), ULLong(_)) => todo!(),
            (ULong(_), Float(_)) => todo!(),
            (ULong(_), Double(_)) => todo!(),
            (ULLong(_), UInteger(_)) => todo!(),
            (ULLong(_), ULong(_)) => todo!(),
            (ULLong(_), Float(_)) => todo!(),
            (Float(_), Byte(_)) => todo!(),
            (Float(_), Integer(_)) => todo!(),
            (Float(_), UInteger(_)) => todo!(),
            (Float(_), Long(_)) => todo!(),
            (Float(_), ULong(_)) => todo!(),
            (Float(_), ULLong(_)) => todo!(),
            (Float(_), Float(_)) => todo!(),
            (Float(_), Double(_)) => todo!(),
            (Double(_), UInteger(_)) => todo!(),
            (Double(_), ULong(_)) => todo!(),
            (Double(_), Float(_)) => todo!(),
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
        (Byte(_), UInteger(_)) => todo!(),
        (Byte(_), ULong(_)) => todo!(),
        (Byte(_), Float(_)) => todo!(),
        (Integer(_), UInteger(_)) => todo!(),
        (Integer(_), ULong(_)) => todo!(),
        (Integer(_), Float(_)) => todo!(),
        (UInteger(_), Byte(_)) => todo!(),
        (UInteger(_), Integer(_)) => todo!(),
        (UInteger(_), UInteger(_)) => todo!(),
        (UInteger(_), Long(_)) => todo!(),
        (UInteger(_), ULong(_)) => todo!(),
        (UInteger(_), ULLong(_)) => todo!(),
        (UInteger(_), Float(_)) => todo!(),
        (UInteger(_), Double(_)) => todo!(),
        (Long(_), UInteger(_)) => todo!(),
        (Long(_), ULong(_)) => todo!(),
        (Long(_), Float(_)) => todo!(),
        (ULong(_), Byte(_)) => todo!(),
        (ULong(_), Integer(_)) => todo!(),
        (ULong(_), UInteger(_)) => todo!(),
        (ULong(_), Long(_)) => todo!(),
        (ULong(_), ULong(_)) => todo!(),
        (ULong(_), ULLong(_)) => todo!(),
        (ULong(_), Float(_)) => todo!(),
        (ULong(_), Double(_)) => todo!(),
        (ULLong(_), UInteger(_)) => todo!(),
        (ULLong(_), ULong(_)) => todo!(),
        (ULLong(_), Float(_)) => todo!(),
        (Float(_), Byte(_)) => todo!(),
        (Float(_), Integer(_)) => todo!(),
        (Float(_), UInteger(_)) => todo!(),
        (Float(_), Long(_)) => todo!(),
        (Float(_), ULong(_)) => todo!(),
        (Float(_), ULLong(_)) => todo!(),
        (Float(_), Float(_)) => todo!(),
        (Float(_), Double(_)) => todo!(),
        (Double(_), UInteger(_)) => todo!(),
        (Double(_), ULong(_)) => todo!(),
        (Double(_), Float(_)) => todo!(),
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
