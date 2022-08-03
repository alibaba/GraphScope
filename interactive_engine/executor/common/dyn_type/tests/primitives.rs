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

#[cfg(test)]
mod tests {
    use dyn_type::Primitives;

    #[test]
    fn test_primitive_byte_as() {
        let a = Primitives::Byte(8);
        assert_eq!(a.as_i8().unwrap(), 8_i8);
        assert_eq!(a.as_i16().unwrap(), 8_i16);
        assert_eq!(a.as_i32().unwrap(), 8_i32);
        assert_eq!(a.as_i64().unwrap(), 8_i64);
        assert_eq!(a.as_i128().unwrap(), 8_i128);
        assert_eq!(a.as_u8().unwrap(), 8_u8);
        assert_eq!(a.as_u16().unwrap(), 8_u16);
        assert_eq!(a.as_u32().unwrap(), 8_u32);
        assert_eq!(a.as_u64().unwrap(), 8_u64);
        assert_eq!(a.as_usize().unwrap(), 8_usize);
        assert_eq!(a.as_u128().unwrap(), 8_u128);
        assert_eq!(a.as_f64().unwrap(), 8.0);
    }

    #[test]
    fn test_primitive_integer_as() {
        let a = Primitives::Integer(8);
        assert_eq!(a.as_i8().unwrap(), 8_i8);
        assert_eq!(a.as_i16().unwrap(), 8_i16);
        assert_eq!(a.as_i32().unwrap(), 8_i32);
        assert_eq!(a.as_i64().unwrap(), 8_i64);
        assert_eq!(a.as_i128().unwrap(), 8_i128);
        assert_eq!(a.as_u8().unwrap(), 8_u8);
        assert_eq!(a.as_u16().unwrap(), 8_u16);
        assert_eq!(a.as_u32().unwrap(), 8_u32);
        assert_eq!(a.as_u64().unwrap(), 8_u64);
        assert_eq!(a.as_usize().unwrap(), 8_usize);
        assert_eq!(a.as_u128().unwrap(), 8_u128);
        assert_eq!(a.as_f64().unwrap(), 8.0);
    }

    #[test]
    fn test_primitive_long_as() {
        let a = Primitives::Long(8);
        assert_eq!(a.as_i8().unwrap(), 8_i8);
        assert_eq!(a.as_i16().unwrap(), 8_i16);
        assert_eq!(a.as_i32().unwrap(), 8_i32);
        assert_eq!(a.as_i64().unwrap(), 8_i64);
        assert_eq!(a.as_i128().unwrap(), 8_i128);
        assert_eq!(a.as_u8().unwrap(), 8_u8);
        assert_eq!(a.as_u16().unwrap(), 8_u16);
        assert_eq!(a.as_u32().unwrap(), 8_u32);
        assert_eq!(a.as_u64().unwrap(), 8_u64);
        assert_eq!(a.as_usize().unwrap(), 8_usize);
        assert_eq!(a.as_u128().unwrap(), 8_u128);
        assert_eq!(a.as_f64().unwrap(), 8.0);
    }

    #[test]
    fn test_primitive_float_as() {
        let a = Primitives::Float(8.1);
        assert!(a.as_i8().is_err());
        assert!(a.as_i16().is_err());
        assert!(a.as_i32().is_err());
        assert!(a.as_i64().is_err());
        assert!(a.as_i128().is_err());
        assert!(a.as_u8().is_err());
        assert!(a.as_u16().is_err());
        assert!(a.as_u32().is_err());
        assert!(a.as_u64().is_err());
        assert!(a.as_usize().is_err());
        assert!(a.as_u128().is_err());
        assert_eq!(a.as_f64().unwrap(), 8.1);
    }

    #[test]
    fn test_primitive_overflow() {
        let a = Primitives::Integer(128);
        // 128 should overflow i8
        assert!(a.as_i8().is_err());
        assert_eq!(a.as_u8().unwrap(), 128_u8);
        let a = Primitives::Integer(256);
        assert!(a.as_i8().is_err());
        // 256 should overflow u8
        assert!(a.as_u8().is_err());
        let a = Primitives::Integer(32768);
        assert!(a.as_i8().is_err());
        assert!(a.as_u8().is_err());
        // 32768 should overflow i16
        assert!(a.as_i16().is_err());
        assert_eq!(a.as_u16().unwrap(), 32768_u16);
        let a = Primitives::Integer(65536);
        assert!(a.as_i8().is_err());
        assert!(a.as_u8().is_err());
        assert!(a.as_i16().is_err());
        // 65536 should overflow u16
        assert!(a.as_u16().is_err());
    }

    #[test]
    fn test_primitive_get() {
        let a = Primitives::Integer(8);
        assert_eq!(a.get::<i8>().unwrap(), 8_i8);
        assert_eq!(a.get::<i16>().unwrap(), 8_i16);
        assert_eq!(a.get::<i32>().unwrap(), 8_i32);
        assert_eq!(a.get::<i64>().unwrap(), 8_i64);
        assert_eq!(a.get::<i128>().unwrap(), 8_i128);
        assert_eq!(a.get::<u8>().unwrap(), 8_u8);
        assert_eq!(a.get::<u16>().unwrap(), 8_u16);
        assert_eq!(a.get::<u32>().unwrap(), 8_u32);
        assert_eq!(a.get::<u64>().unwrap(), 8_u64);
        assert_eq!(a.get::<usize>().unwrap(), 8_usize);
        assert_eq!(a.get::<u128>().unwrap(), 8_u128);
        assert_eq!(a.get::<f64>().unwrap(), 8.0);
    }

    #[test]
    fn test_equal() {
        let a = Primitives::Byte(8);
        assert_eq!(a, Primitives::Byte(8));
        assert_eq!(a, Primitives::Integer(8));
        assert_eq!(a, Primitives::Long(8));
        assert_eq!(a, Primitives::Float(8.0));

        assert_ne!(a, Primitives::Byte(9));
        assert_ne!(a, Primitives::Integer(9));
        assert_ne!(a, Primitives::Long(9));
        assert_ne!(a, Primitives::Float(8.1));

        let a = Primitives::Integer(8);
        assert_eq!(a, Primitives::Byte(8));
        assert_eq!(a, Primitives::Integer(8));
        assert_eq!(a, Primitives::Long(8));
        assert_eq!(a, Primitives::Float(8.0));

        let a = Primitives::Long(8);
        assert_eq!(a, Primitives::Byte(8));
        assert_eq!(a, Primitives::Integer(8));
        assert_eq!(a, Primitives::Long(8));
        assert_eq!(a, Primitives::Float(8.0));

        let a = Primitives::Float(8.0);
        assert_eq!(a, Primitives::Byte(8));
        assert_eq!(a, Primitives::Integer(8));
        assert_eq!(a, Primitives::Long(8));
        assert_eq!(a, Primitives::Float(8.0));

        let a = Primitives::Float(8.1);
        assert_ne!(a, Primitives::Byte(8));
        assert_ne!(a, Primitives::Integer(8));
        assert_ne!(a, Primitives::Long(8));
        assert_ne!(a, Primitives::Float(8.0));
    }

    #[test]
    fn test_compare() {
        let a = Primitives::Byte(8);
        assert!(a < Primitives::Byte(9));
        assert!(a < Primitives::Integer(9));
        assert!(a < Primitives::Long(9));
        assert!(a < Primitives::Float(8.1));

        assert!(a > Primitives::Byte(7));
        assert!(a > Primitives::Integer(7));
        assert!(a > Primitives::Long(7));
        assert!(a > Primitives::Float(7.9));

        let a = Primitives::Float(8.0);
        assert!(a > Primitives::Byte(7));
        assert!(a > Primitives::Integer(7));
        assert!(a > Primitives::Long(7));
        assert!(a > Primitives::Float(7.9));
    }
}
