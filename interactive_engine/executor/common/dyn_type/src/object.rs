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

use core::any::TypeId;
use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use chrono::{DateTime, Datelike, Days, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::{try_downcast, try_downcast_ref, CastError, DynType};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawType {
    Byte,
    Integer,
    UInt,
    Long,
    ULong,
    ULLong,
    Float,
    Double,
    String,
    Date,
    Time,
    DateTime,
    DateTimeWithTz,
    Blob(usize),
    Vector,
    KV,
    None,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub enum Primitives {
    Byte(i8),
    Integer(i32),
    UInteger(u32),
    Long(i64),
    ULong(u64),
    ULLong(u128),
    Float(f32),
    Double(f64),
}

impl ToString for Primitives {
    fn to_string(&self) -> String {
        use Primitives::*;
        match self {
            Byte(i) => i.to_string(),
            Integer(i) => i.to_string(),
            Long(i) => i.to_string(),
            ULLong(i) => i.to_string(),
            Float(i) => i.to_string(),
            UInteger(i) => i.to_string(),
            ULong(i) => i.to_string(),
            Double(i) => i.to_string(),
        }
    }
}

lazy_static! {
    static ref I8: TypeId = TypeId::of::<i8>();
    static ref U8: TypeId = TypeId::of::<u8>();
    static ref I16: TypeId = TypeId::of::<i16>();
    static ref U16: TypeId = TypeId::of::<u16>();
    static ref I32: TypeId = TypeId::of::<i32>();
    static ref U32: TypeId = TypeId::of::<u32>();
    static ref I64: TypeId = TypeId::of::<i64>();
    static ref U64: TypeId = TypeId::of::<u64>();
    static ref USIZE: TypeId = TypeId::of::<usize>();
    static ref I128: TypeId = TypeId::of::<i128>();
    static ref U128: TypeId = TypeId::of::<u128>();
    static ref F32: TypeId = TypeId::of::<f32>();
    static ref F64: TypeId = TypeId::of::<f64>();
}

macro_rules! try_transmute {
    ($var: expr, $ty: ty, $e : expr) => {
        if TypeId::of::<$ty>() == $var.type_id() {
            Ok(unsafe { std::mem::transmute($var) })
        } else {
            Err(CastError::new::<$ty>($e))
        }
    };
}

impl Primitives {
    #[inline]
    pub fn raw_type(&self) -> RawType {
        match self {
            Primitives::Byte(_) => RawType::Byte,
            Primitives::Integer(_) => RawType::Integer,
            Primitives::Long(_) => RawType::Long,
            Primitives::ULLong(_) => RawType::ULLong,
            Primitives::Float(_) => RawType::Float,
            Primitives::UInteger(_) => RawType::UInt,
            Primitives::ULong(_) => RawType::ULong,
            Primitives::Double(_) => RawType::Double,
        }
    }

    pub fn is_negative(&self) -> bool {
        match self {
            Primitives::Byte(v) => *v < 0,
            Primitives::Integer(v) => *v < 0,
            Primitives::Long(v) => *v < 0,
            Primitives::UInteger(_) => false,
            Primitives::ULong(_) => false,
            Primitives::ULLong(_) => false,
            Primitives::Float(v) => *v < 0.0,
            Primitives::Double(v) => *v < 0.0,
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Result<bool, CastError> {
        Ok(self.as_u8()? != 0_u8)
    }

    #[inline]
    pub fn as_i8(&self) -> Result<i8, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v),
            Primitives::Integer(v) => i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::Integer)),
            Primitives::UInteger(v) => i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::UInt)),
            Primitives::Long(v) => i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::Long)),
            Primitives::ULong(v) => i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::ULong)),
            Primitives::ULLong(v) => i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<i8>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<i8>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Result<i16, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i16),
            Primitives::Integer(v) => {
                i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::Integer))
            }
            Primitives::UInteger(v) => i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::UInt)),
            Primitives::Long(v) => i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::Long)),
            Primitives::ULong(v) => i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::ULong)),
            Primitives::ULLong(v) => i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<i16>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<i16>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i32),
            Primitives::Integer(v) => Ok(*v),
            Primitives::UInteger(v) => i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::UInt)),
            Primitives::Long(v) => i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::Long)),
            Primitives::ULong(v) => i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::ULong)),
            Primitives::ULLong(v) => i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<i32>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<i32>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i64),
            Primitives::Integer(v) => Ok(*v as i64),
            Primitives::UInteger(v) => Ok(*v as i64),
            Primitives::Long(v) => Ok(*v),
            Primitives::ULong(v) => i64::try_from(*v).map_err(|_| CastError::new::<i64>(RawType::ULong)),
            Primitives::ULLong(v) => i64::try_from(*v).map_err(|_| CastError::new::<i64>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<i64>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<i64>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i128),
            Primitives::Integer(v) => Ok(*v as i128),
            Primitives::UInteger(v) => Ok(*v as i128),
            Primitives::Long(v) => Ok(*v as i128),
            Primitives::ULong(v) => Ok(*v as i128),
            Primitives::ULLong(v) => {
                i128::try_from(*v).map_err(|_| CastError::new::<i128>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i128>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<i128>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_u8(&self) -> Result<u8, CastError> {
        match self {
            Primitives::Byte(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Byte)),
            Primitives::Integer(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Integer)),
            Primitives::UInteger(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::UInt)),
            Primitives::Long(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Long)),
            Primitives::ULong(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::ULong)),
            Primitives::ULLong(v) => u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<u8>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<u8>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Result<u16, CastError> {
        match self {
            Primitives::Byte(v) => u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Byte)),
            Primitives::Integer(v) => {
                u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Integer))
            }
            Primitives::UInteger(v) => u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::UInt)),
            Primitives::Long(v) => u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Long)),
            Primitives::ULong(v) => u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::ULong)),
            Primitives::ULLong(v) => u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<u16>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<u16>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Result<u32, CastError> {
        match self {
            Primitives::Byte(v) => u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Byte)),
            Primitives::Integer(v) => {
                u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Integer))
            }
            Primitives::UInteger(v) => Ok(*v),
            Primitives::Long(v) => u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Long)),
            Primitives::ULong(v) => u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::ULong)),
            Primitives::ULLong(v) => u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<u32>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<u32>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Primitives::Byte(v) => u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Byte)),
            Primitives::Integer(v) => {
                u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Integer))
            }
            Primitives::UInteger(v) => Ok(*v as u64),
            Primitives::Long(v) => u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Long)),
            Primitives::ULong(v) => Ok(*v),
            Primitives::ULLong(v) => u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::ULLong)),
            Primitives::Float(_) => Err(CastError::new::<u64>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<u64>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_usize(&self) -> Result<usize, CastError> {
        self.as_u64().map(|v| v as usize)
    }

    #[inline]
    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            Primitives::Byte(v) => u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Byte)),
            Primitives::Integer(v) => {
                u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Integer))
            }
            Primitives::UInteger(v) => Ok(*v as u128),
            Primitives::Long(v) => u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Long)),
            Primitives::ULong(v) => Ok(*v as u128),
            Primitives::ULLong(v) => Ok(*v),
            Primitives::Float(_) => Err(CastError::new::<u128>(RawType::Float)),
            Primitives::Double(_) => Err(CastError::new::<u128>(RawType::Double)),
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Result<f32, CastError> {
        match self {
            Primitives::Byte(v) => f32::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::Byte)),
            Primitives::Integer(v) => {
                let t = i16::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::Integer))?;
                f32::try_from(t).map_err(|_| CastError::new::<f32>(RawType::Integer))
            }
            Primitives::UInteger(v) => {
                let t = u16::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::UInt))?;
                f32::try_from(t).map_err(|_| CastError::new::<f32>(RawType::UInt))
            }
            Primitives::Long(v) => {
                let t = i16::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::Long))?;
                f32::try_from(t).map_err(|_| CastError::new::<f32>(RawType::Long))
            }
            Primitives::ULong(v) => {
                let t = u16::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::ULong))?;
                f32::try_from(t).map_err(|_| CastError::new::<f32>(RawType::ULong))
            }
            Primitives::ULLong(v) => {
                let t = u16::try_from(*v).map_err(|_| CastError::new::<f32>(RawType::ULLong))?;
                f32::try_from(t).map_err(|_| CastError::new::<f32>(RawType::ULLong))
            }
            Primitives::Float(v) => Ok(*v),
            Primitives::Double(v) => Ok(*v as f32),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            Primitives::Byte(v) => f64::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Byte)),
            Primitives::Integer(v) => {
                f64::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Integer))
            }
            Primitives::UInteger(v) => f64::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::UInt)),
            Primitives::Long(v) => {
                let t = i32::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Long))?;
                f64::try_from(t).map_err(|_| CastError::new::<f64>(RawType::Long))
            }
            Primitives::ULong(v) => {
                let t = u32::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::ULong))?;
                f64::try_from(t).map_err(|_| CastError::new::<f64>(RawType::ULong))
            }
            Primitives::ULLong(v) => {
                let t = u32::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::ULLong))?;
                f64::try_from(t).map_err(|_| CastError::new::<f64>(RawType::ULLong))
            }
            Primitives::Float(v) => Ok(*v as f64),
            Primitives::Double(v) => Ok(*v),
        }
    }

    #[inline]
    pub fn get<T: 'static + Clone>(&self) -> Result<T, CastError> {
        let type_id = TypeId::of::<T>();
        if type_id == *I8 {
            return self.as_i8().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *U8 {
            return self.as_u8().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *I16 {
            return self.as_i16().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *U16 {
            return self.as_u16().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *I32 {
            return self.as_i32().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *U32 {
            return self.as_u32().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *I64 {
            return self.as_i64().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *U64 {
            return self.as_u64().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *USIZE {
            return self.as_usize().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *I128 {
            return self.as_i128().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *U128 {
            return self.as_u128().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *F32 {
            return self.as_f32().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        if type_id == *F64 {
            return self.as_f64().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&v) };
                t.clone()
            });
        }

        Err(CastError::new::<T>(self.raw_type()))
    }
}

impl PartialEq for Primitives {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Primitives::Byte(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other.as_i8().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Integer(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other.as_i32().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::UInteger(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other.as_u32().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Long(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other.as_i64().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::ULong(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other.as_u64().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::ULLong(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                Primitives::Float(o) => (*v as f32).eq(o),
                _ => other
                    .as_u128()
                    .map(|o| o == *v)
                    .unwrap_or(false),
            },
            Primitives::Float(v) => match other {
                Primitives::Double(o) => (*v as f64).eq(o),
                _ => other.as_f32().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Double(v) => other.as_f64().map(|o| o == *v).unwrap_or(false),
        }
    }
}

impl Eq for Primitives {}

impl PartialOrd for Primitives {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Primitives::Byte(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_i8().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Integer(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_i32().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::UInteger(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_u32().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Long(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_i64().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::ULong(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_u64().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::ULLong(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                // may overflow
                Primitives::Float(o) => (*v as f32).partial_cmp(o),
                _ => other.as_u128().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Float(v) => match other {
                Primitives::Double(o) => (*v as f64).partial_cmp(o),
                _ => other
                    .as_f32()
                    .map(|o| v.partial_cmp(&o))
                    .unwrap_or(None),
            },
            Primitives::Double(v) => other
                .as_f64()
                .map(|o| v.partial_cmp(&o))
                .unwrap_or(None),
        }
    }
}

#[derive(Clone, Debug, Hash)]
pub enum DateTimeFormats {
    // preserve a date format (ISO format) like 2019-01-01
    Date(NaiveDate),
    // preserve a time format (ISO format) like 00:00:00.000
    Time(NaiveTime),
    // preserve a date time format (ISO format) with UTC timezone, like 2019-01-01 00:00:00.000
    DateTime(NaiveDateTime),
    // preserve a date time format (ISO format) with specified timezone, like 2019-01-01T00:00:00.000+08:00
    DateTimeWithTz(DateTime<FixedOffset>),
}

impl DateTimeFormats {
    #[inline]
    pub fn raw_type(&self) -> RawType {
        match self {
            DateTimeFormats::Date(_) => RawType::Date,
            DateTimeFormats::Time(_) => RawType::Time,
            DateTimeFormats::DateTime(_) => RawType::DateTime,
            DateTimeFormats::DateTimeWithTz(_) => RawType::DateTimeWithTz,
        }
    }

    // the date32 is stored as YYYYMMDD, e.g., 20100102
    pub fn from_date32(date32: i32) -> Result<Self, CastError> {
        NaiveDate::from_ymd_opt(date32 / 10000, ((date32 % 10000) / 100) as u32, (date32 % 100) as u32)
            .map(|d| DateTimeFormats::Date(d))
            .ok_or_else(|| CastError::new::<DateTimeFormats>(RawType::Integer))
    }

    // the date32 is stored as int32 days since 1970-01-01
    pub fn from_date32_dur(date32: i32) -> Result<Self, CastError> {
        let naive_date = if date32 < 0 {
            NaiveDate::default().checked_sub_days(Days::new(date32.abs() as u64))
        } else {
            NaiveDate::default().checked_add_days(Days::new(date32 as u64))
        };
        naive_date
            .map(|d| DateTimeFormats::Date(d))
            .ok_or_else(|| CastError::new::<DateTimeFormats>(RawType::Integer))
    }

    // the time32 is stored HHMMSSsss, e.g., 121314100
    pub fn from_time32(time32: i32) -> Result<Self, CastError> {
        NaiveTime::from_hms_milli_opt(
            (time32 / 10000000) as u32,
            ((time32 % 10000000) / 100000) as u32,
            ((time32 % 100000) / 1000) as u32,
            (time32 % 1000) as u32,
        )
        .map(|t| DateTimeFormats::Time(t))
        .ok_or_else(|| CastError::new::<DateTimeFormats>(RawType::Integer))
    }

    pub fn from_timestamp_millis(timestamp: i64) -> Result<Self, CastError> {
        NaiveDateTime::from_timestamp_millis(timestamp)
            .map(|dt| DateTimeFormats::DateTime(dt))
            .ok_or_else(|| CastError::new::<DateTimeFormats>(RawType::Long))
    }

    // we pre-assume some date/time/datetime formats according to ISO formats.
    pub fn from_str(str: &str) -> Result<Self, CastError> {
        // `1996-12-19`
        if let Ok(date) = NaiveDate::parse_from_str(str, "%Y-%m-%d") {
            Ok(DateTimeFormats::Date(date))
        }
        // `16:39:57.123`
        else if let Ok(time) = NaiveTime::parse_from_str(str, "%H:%M:%S%.f") {
            Ok(DateTimeFormats::Time(time))
        }
        // `1996-12-19 16:39:57.123`
        else if let Ok(dt) = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f") {
            Ok(DateTimeFormats::DateTime(dt))
        }
        // `1996-12-19T16:39:57.123+08:00`
        else if let Ok(dt) = DateTime::parse_from_rfc3339(str) {
            Ok(DateTimeFormats::DateTimeWithTz(dt))
        } else {
            Err(CastError::new::<DateTimeFormats>(RawType::String))
        }
    }

    #[inline]
    pub fn as_date(&self) -> Result<NaiveDate, CastError> {
        match self {
            DateTimeFormats::Date(d) => Ok(*d),
            DateTimeFormats::Time(_) => Err(CastError::new::<NaiveTime>(RawType::Time)),
            DateTimeFormats::DateTime(dt) => Ok(dt.date()),
            DateTimeFormats::DateTimeWithTz(dt) => Ok(dt.date_naive()),
        }
    }

    #[inline]
    pub fn as_time(&self) -> Result<NaiveTime, CastError> {
        match self {
            DateTimeFormats::Date(_) => Err(CastError::new::<NaiveDate>(RawType::Date)),
            DateTimeFormats::Time(t) => Ok(*t),
            DateTimeFormats::DateTime(dt) => Ok(dt.time()),
            DateTimeFormats::DateTimeWithTz(dt) => Ok(dt.time()),
        }
    }

    #[inline]
    pub fn as_date_time(&self) -> Result<NaiveDateTime, CastError> {
        match self {
            DateTimeFormats::Date(_) => Err(CastError::new::<NaiveDate>(RawType::DateTime)),
            DateTimeFormats::Time(_) => Err(CastError::new::<NaiveTime>(RawType::DateTime)),
            DateTimeFormats::DateTime(dt) => Ok(*dt),
            DateTimeFormats::DateTimeWithTz(dt) => Ok(dt.naive_local()),
        }
    }

    #[inline]
    pub fn as_date_time_with_tz(&self) -> Result<DateTime<FixedOffset>, CastError> {
        match self {
            DateTimeFormats::Date(_) => Err(CastError::new::<NaiveDate>(RawType::DateTimeWithTz)),
            DateTimeFormats::Time(_) => Err(CastError::new::<NaiveTime>(RawType::DateTimeWithTz)),
            DateTimeFormats::DateTime(dt) => dt
                .and_local_timezone(FixedOffset::east_opt(0).unwrap())
                .single()
                .ok_or_else(|| CastError::new::<NaiveDateTime>(RawType::DateTimeWithTz)),
            DateTimeFormats::DateTimeWithTz(dt) => Ok(*dt),
        }
    }

    pub fn year(&self) -> Option<i32> {
        match self {
            DateTimeFormats::Date(d) => Some(d.year()),
            DateTimeFormats::Time(_) => None,
            DateTimeFormats::DateTime(dt) => Some(dt.year()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.year()),
        }
    }

    pub fn month(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(d) => Some(d.month()),
            DateTimeFormats::Time(_) => None,
            DateTimeFormats::DateTime(dt) => Some(dt.month()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.month()),
        }
    }

    pub fn day(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(d) => Some(d.day()),
            DateTimeFormats::Time(_) => None,
            DateTimeFormats::DateTime(dt) => Some(dt.day()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.day()),
        }
    }

    pub fn hour(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(_) => None,
            DateTimeFormats::Time(t) => Some(t.hour()),
            DateTimeFormats::DateTime(dt) => Some(dt.hour()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.hour()),
        }
    }

    pub fn minute(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(_) => None,
            DateTimeFormats::Time(t) => Some(t.minute()),
            DateTimeFormats::DateTime(dt) => Some(dt.minute()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.minute()),
        }
    }

    pub fn second(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(_) => None,
            DateTimeFormats::Time(t) => Some(t.second()),
            DateTimeFormats::DateTime(dt) => Some(dt.second()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.second()),
        }
    }

    pub fn millisecond(&self) -> Option<u32> {
        match self {
            DateTimeFormats::Date(_) => None,
            DateTimeFormats::Time(t) => Some(t.nanosecond() / 1_000_000),
            DateTimeFormats::DateTime(dt) => Some(dt.nanosecond() / 1_000_000),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.nanosecond() / 1_000_000),
        }
    }

    pub fn timestamp(&self) -> Option<i64> {
        match self {
            DateTimeFormats::Date(d) => d.and_hms_opt(0, 0, 0).map(|dt| dt.timestamp()),
            DateTimeFormats::Time(_) => None,
            DateTimeFormats::DateTime(dt) => Some(dt.timestamp()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.timestamp()),
        }
    }

    pub fn timestamp_millis(&self) -> Option<i64> {
        match self {
            DateTimeFormats::Date(d) => d
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.timestamp_millis()),
            DateTimeFormats::Time(_) => None,
            DateTimeFormats::DateTime(dt) => Some(dt.timestamp_millis()),
            DateTimeFormats::DateTimeWithTz(dt) => Some(dt.timestamp_millis()),
        }
    }
}

impl PartialEq for DateTimeFormats {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DateTimeFormats::Date(l), DateTimeFormats::Date(r)) => l.eq(r),
            (DateTimeFormats::Time(l), DateTimeFormats::Time(r)) => l.eq(r),
            (DateTimeFormats::DateTime(l), DateTimeFormats::DateTime(r)) => l.eq(r),
            (DateTimeFormats::DateTimeWithTz(l), DateTimeFormats::DateTimeWithTz(r)) => l.eq(r),
            (DateTimeFormats::DateTime(l), DateTimeFormats::DateTimeWithTz(r)) => l.eq(&r.naive_utc()),
            (DateTimeFormats::DateTimeWithTz(l), DateTimeFormats::DateTime(r)) => l.naive_utc().eq(r),
            _ => false,
        }
    }
}

impl Eq for DateTimeFormats {}

impl PartialOrd for DateTimeFormats {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DateTimeFormats::Date(l), DateTimeFormats::Date(r)) => l.partial_cmp(r),
            (DateTimeFormats::Time(l), DateTimeFormats::Time(r)) => l.partial_cmp(r),
            (DateTimeFormats::DateTime(l), DateTimeFormats::DateTime(r)) => l.partial_cmp(r),
            (DateTimeFormats::DateTimeWithTz(l), DateTimeFormats::DateTimeWithTz(r)) => l.partial_cmp(r),
            (DateTimeFormats::DateTime(l), DateTimeFormats::DateTimeWithTz(r)) => {
                l.partial_cmp(&r.naive_utc())
            }
            (DateTimeFormats::DateTimeWithTz(l), DateTimeFormats::DateTime(r)) => {
                l.naive_utc().partial_cmp(r)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Object {
    Primitive(Primitives),
    String(String),
    Vector(Vec<Object>),
    KV(BTreeMap<Object, Object>),
    Blob(Box<[u8]>),
    DateFormat(DateTimeFormats),
    DynOwned(Box<dyn DynType>),
    None,
}

impl_as_any!(Object);

impl ToString for Object {
    fn to_string(&self) -> String {
        match self {
            Object::Primitive(p) => p.to_string(),
            Object::String(s) => s.to_string(),
            Object::Vector(v) => format!("{:?}", v),
            Object::KV(kv) => format!("{:?}", kv),
            Object::Blob(b) => format!("{:?}", b),
            Object::DateFormat(d) => format!("{:?}", d),
            Object::DynOwned(_) => "unknown dynamic type".to_string(),
            Object::None => "".to_string(),
        }
    }
}

/// Try to borrow an immutable reference of [crate::Object].
#[derive(Debug, Clone, Copy)]
pub enum BorrowObject<'a> {
    Primitive(Primitives),
    String(&'a str),
    Vector(&'a [Object]),
    KV(&'a BTreeMap<Object, Object>),
    Blob(&'a [u8]),
    DateFormat(&'a DateTimeFormats),
    /// To borrow from `Object::DynOwned`, and it can be cloned back to `Object::DynOwned`
    DynRef(&'a Box<dyn DynType>),
    None,
}

impl<'a> ToString for BorrowObject<'a> {
    fn to_string(&self) -> String {
        use BorrowObject::*;
        match self {
            Primitive(p) => p.to_string(),
            String(s) => s.to_string(),
            Vector(v) => format!("{:?}", v),
            KV(kv) => format!("{:?}", kv),
            Blob(b) => format!("{:?}", b),
            DateFormat(df) => format!("{:?}", df),
            DynRef(_) => "unknown dynamic type".to_string(),
            None => "None".to_string(),
        }
    }
}

macro_rules! contains_single {
    ($self: expr, $single: expr, $ty: ident) => {
        match $self {
            $crate::$ty::Vector(vec) => match $single {
                $crate::$ty::Primitive(_) | $crate::$ty::String(_) => {
                    for val in vec.iter() {
                        if $single == val {
                            return true;
                        }
                    }
                    false
                }
                _ => false,
            },
            $crate::$ty::String(str1) => match $single {
                $crate::$ty::String(str2) => str1.contains(str2),
                _ => false,
            },
            _ => false,
        }
    };
}

impl Object {
    pub fn raw_type(&self) -> RawType {
        match self {
            Object::Primitive(p) => p.raw_type(),
            Object::String(_) => RawType::String,
            Object::Vector(_) => RawType::Vector,
            Object::KV(_) => RawType::KV,
            Object::Blob(b) => RawType::Blob(b.len()),
            Object::DateFormat(df) => df.raw_type(),
            Object::DynOwned(_) => RawType::Unknown,
            Object::None => RawType::None,
        }
    }

    pub fn as_borrow(&self) -> BorrowObject {
        match self {
            Object::Primitive(p) => BorrowObject::Primitive(*p),
            Object::String(v) => BorrowObject::String(v.as_str()),
            Object::Vector(v) => BorrowObject::Vector(v.as_slice()),
            Object::KV(kv) => BorrowObject::KV(kv),
            Object::Blob(v) => BorrowObject::Blob(v.as_ref()),
            Object::DateFormat(d) => BorrowObject::DateFormat(d),
            Object::DynOwned(v) => BorrowObject::DynRef(v),
            Object::None => BorrowObject::None,
        }
    }

    #[inline]
    pub fn as_primitive(&self) -> Result<Primitives, CastError> {
        match self {
            Object::Primitive(p) => Ok(*p),
            Object::DynOwned(x) => try_downcast!(x, Primitives),
            _ => Err(CastError::new::<Primitives>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Result<bool, CastError> {
        Ok(self.as_u8()? != 0_u8)
    }

    #[inline]
    pub fn as_i8(&self) -> Result<i8, CastError> {
        match self {
            Object::Primitive(p) => p.as_i8(),
            Object::DynOwned(x) => try_downcast!(x, i8),
            _ => Err(CastError::new::<i8>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Result<i16, CastError> {
        match self {
            Object::Primitive(p) => p.as_i16(),
            Object::DynOwned(x) => try_downcast!(x, i16),
            _ => Err(CastError::new::<i16>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Object::Primitive(p) => p.as_i32(),
            Object::DynOwned(x) => try_downcast!(x, i32),
            _ => Err(CastError::new::<i32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            Object::Primitive(p) => p.as_i64(),
            Object::DynOwned(x) => try_downcast!(x, i64),
            _ => Err(CastError::new::<i64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            Object::Primitive(p) => p.as_i128(),
            Object::DynOwned(x) => try_downcast!(x, i128),
            _ => Err(CastError::new::<i128>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u8(&self) -> Result<u8, CastError> {
        match self {
            Object::Primitive(p) => p.as_u8(),
            Object::DynOwned(x) => try_downcast!(x, u8),
            _ => Err(CastError::new::<u8>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Result<u16, CastError> {
        match self {
            Object::Primitive(p) => p.as_u16(),
            Object::DynOwned(x) => try_downcast!(x, u16),
            _ => Err(CastError::new::<u16>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Result<u32, CastError> {
        match self {
            Object::Primitive(p) => p.as_u32(),
            Object::DynOwned(x) => try_downcast!(x, u32),
            _ => Err(CastError::new::<u32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Object::Primitive(p) => p.as_u64(),
            Object::DynOwned(x) => try_downcast!(x, u64),
            _ => Err(CastError::new::<u64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            Object::Primitive(p) => p.as_u128(),
            Object::DynOwned(x) => try_downcast!(x, u128),
            _ => Err(CastError::new::<u128>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Result<f32, CastError> {
        match self {
            Object::Primitive(p) => p.as_f32(),
            Object::DynOwned(x) => try_downcast!(x, f32),
            _ => Err(CastError::new::<f32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            Object::Primitive(p) => p.as_f64(),
            Object::DynOwned(x) => try_downcast!(x, f64),
            _ => Err(CastError::new::<f64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            Object::String(str) => Ok(Cow::Borrowed(str.as_str())),
            Object::Blob(b) => Ok(String::from_utf8_lossy(b)),
            Object::DynOwned(x) => try_downcast!(x, String, as_str).map(|r| Cow::Borrowed(r)),
            Object::None => Ok(Cow::Borrowed("")),
            _ => Err(CastError::new::<String>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            Object::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            Object::String(str) => Ok(str.as_bytes()),
            Object::Blob(v) => Ok(v.as_ref()),
            Object::DynOwned(x) => try_downcast!(x, Vec<u8>, as_slice),
            Object::None => Ok(&[]),
            _ => Err(CastError::new::<&[u8]>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_date_format(&self) -> Result<&DateTimeFormats, CastError> {
        match self {
            Object::DateFormat(d) => Ok(d),
            Object::DynOwned(x) => try_downcast_ref!(x, DateTimeFormats),
            _ => Err(CastError::new::<DateTimeFormats>(self.raw_type())),
        }
    }

    pub fn get<T: DynType + Clone>(&self) -> Result<OwnedOrRef<T>, CastError> {
        match self {
            Object::Primitive(p) => {
                let v = p.get::<T>()?;
                Ok(OwnedOrRef::Owned(v))
            }
            Object::String(x) => try_transmute!(x, T, RawType::String).map(|v| OwnedOrRef::Ref(v)),
            Object::Blob(x) => try_transmute!(x, T, RawType::Blob(x.len())).map(|v| OwnedOrRef::Ref(v)),
            Object::DynOwned(x) => try_downcast_ref!(x, T).map(|v| OwnedOrRef::Ref(v)),
            _ => Err(CastError::new::<OwnedOrRef<T>>(self.raw_type())),
        }
    }

    pub fn take_string(self) -> Result<String, CastError> {
        match self {
            Object::String(str) => Ok(str),
            Object::DynOwned(mut x) => {
                if let Some(v) = x.try_downcast_mut::<String>() {
                    Ok(std::mem::replace(v, "".to_owned()))
                } else {
                    Err(CastError::new::<i32>(RawType::Unknown))
                }
            }
            _ => Err(CastError::new::<String>(self.raw_type())),
        }
    }

    pub fn take_vector(self) -> Result<Vec<Object>, CastError> {
        match self {
            Object::Vector(vec) => Ok(vec),
            Object::DynOwned(mut x) => {
                if let Some(v) = x.try_downcast_mut::<Vec<Object>>() {
                    Ok(std::mem::replace(v, Vec::new()))
                } else {
                    Err(CastError::new::<Vec<Object>>(RawType::Unknown))
                }
            }
            _ => Err(CastError::new::<Vec<Object>>(self.raw_type())),
        }
    }

    fn contains_single(&self, single: &Object) -> bool {
        contains_single!(self, single, Object)
    }

    pub fn contains(&self, obj: &Object) -> bool {
        match obj {
            Object::Vector(vec) => {
                for val in vec.iter() {
                    if !self.contains_single(val) {
                        return false;
                    }
                }
                true
            }
            Object::Primitive(_) | Object::String(_) => self.contains_single(obj),
            _ => false,
        }
    }
}

impl<'a> BorrowObject<'a> {
    pub fn raw_type(&self) -> RawType {
        match self {
            BorrowObject::Primitive(p) => p.raw_type(),
            BorrowObject::String(_) => RawType::String,
            BorrowObject::Vector(_) => RawType::Vector,
            BorrowObject::KV(_) => RawType::KV,
            BorrowObject::Blob(b) => RawType::Blob(b.len()),
            BorrowObject::DateFormat(df) => df.raw_type(),
            BorrowObject::DynRef(_) => RawType::Unknown,
            BorrowObject::None => RawType::None,
        }
    }

    #[inline]
    pub fn as_primitive(&self) -> Result<Primitives, CastError> {
        match self {
            BorrowObject::Primitive(p) => Ok(*p),
            BorrowObject::DynRef(x) => try_downcast!(x, Primitives),
            _ => Err(CastError::new::<Primitives>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_bool(&self) -> Result<bool, CastError> {
        Ok(self.as_u8()? != 0_u8)
    }

    #[inline]
    pub fn as_i8(&self) -> Result<i8, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i8(),
            BorrowObject::DynRef(x) => try_downcast!(x, i8),
            _ => Err(CastError::new::<i8>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u8(&self) -> Result<u8, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u8(),
            BorrowObject::DynRef(x) => try_downcast!(x, u8),
            _ => Err(CastError::new::<u8>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Result<i16, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i16(),
            BorrowObject::DynRef(x) => try_downcast!(x, i16),
            _ => Err(CastError::new::<i16>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Result<u16, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u16(),
            BorrowObject::DynRef(x) => try_downcast!(x, u16),
            _ => Err(CastError::new::<u16>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i32(),
            BorrowObject::DynRef(x) => try_downcast!(x, i32),
            _ => Err(CastError::new::<i32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Result<u32, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u32(),
            BorrowObject::DynRef(x) => try_downcast!(x, u32),
            _ => Err(CastError::new::<u32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i64(),
            BorrowObject::DynRef(x) => try_downcast!(x, i64),
            _ => Err(CastError::new::<i64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u64(),
            BorrowObject::DynRef(x) => try_downcast!(x, u64),
            _ => Err(CastError::new::<u64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i128(),
            BorrowObject::DynRef(x) => try_downcast!(x, i128),
            _ => Err(CastError::new::<i128>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u128(),
            BorrowObject::DynRef(x) => try_downcast!(x, u128),
            _ => Err(CastError::new::<u128>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Result<f32, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_f32(),
            BorrowObject::DynRef(x) => try_downcast!(x, f32),
            _ => Err(CastError::new::<f32>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_f64(),
            BorrowObject::DynRef(x) => try_downcast!(x, f64),
            _ => Err(CastError::new::<f64>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            BorrowObject::String(str) => Ok(Cow::Borrowed(*str)),
            BorrowObject::Blob(b) => Ok(String::from_utf8_lossy(b)),
            BorrowObject::DynRef(x) => try_downcast!(x, String, as_str).map(|r| Cow::Borrowed(r)),
            BorrowObject::None => Ok(Cow::Borrowed("")),
            _ => Err(CastError::new::<String>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            BorrowObject::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            BorrowObject::String(v) => Ok(v.as_bytes()),
            BorrowObject::Blob(v) => Ok(*v),
            BorrowObject::DynRef(v) => try_downcast!(v, Vec<u8>, as_slice),
            BorrowObject::None => Ok(&[]),
            _ => Err(CastError::new::<&[u8]>(self.raw_type())),
        }
    }

    #[inline]
    pub fn as_date_format(&self) -> Result<&DateTimeFormats, CastError> {
        match self {
            BorrowObject::DateFormat(d) => Ok(d),
            BorrowObject::DynRef(x) => try_downcast_ref!(x, DateTimeFormats),
            _ => Err(CastError::new::<DateTimeFormats>(self.raw_type())),
        }
    }

    pub fn try_to_owned(&self) -> Option<Object> {
        match *self {
            BorrowObject::Primitive(p) => Some(Object::Primitive(p)),
            BorrowObject::String(s) => Some(Object::String(s.to_owned())),
            BorrowObject::Vector(v) => Some(Object::Vector(v.to_vec())),
            BorrowObject::KV(kv) => Some(Object::KV(kv.clone())),
            BorrowObject::Blob(b) => Some(Object::Blob(b.to_vec().into_boxed_slice())),
            BorrowObject::DynRef(d) => Some(Object::DynOwned((*d).clone())),
            BorrowObject::None => Some(Object::None),
            BorrowObject::DateFormat(df) => Some(Object::DateFormat(df.clone())),
        }
    }

    fn contains_single(&self, single: &BorrowObject) -> bool {
        contains_single!(self, single, BorrowObject)
    }

    pub fn contains(&self, obj: &BorrowObject) -> bool {
        match obj {
            BorrowObject::Vector(vec) => {
                for val in vec.iter() {
                    if !self.contains_single(&val.as_borrow()) {
                        return false;
                    }
                }
                true
            }
            BorrowObject::Primitive(_) | BorrowObject::String(_) => self.contains_single(obj),
            _ => false,
        }
    }
}

impl<'a> PartialEq<Object> for BorrowObject<'a> {
    fn eq(&self, other: &Object) -> bool {
        self.eq(&other.as_borrow())
    }
}

impl<'a> PartialEq<BorrowObject<'a>> for Object {
    fn eq(&self, other: &BorrowObject<'a>) -> bool {
        self.as_borrow().eq(other)
    }
}

macro_rules! eq {
    ($self:expr, $other:expr, $ty:ident) => {
        match $self {
            $crate::$ty::Primitive(p) => $other
                .as_primitive()
                .map(|o| p == &o)
                .unwrap_or(false),
            $crate::$ty::Blob(v) => $other
                .as_bytes()
                .map(|o| o.eq(v.as_ref()))
                .unwrap_or(false),
            $crate::$ty::String(v) => $other
                .as_str()
                .map(|o| o.eq(&(*v)))
                .unwrap_or(false),
            $crate::$ty::Vector(v1) => {
                if let $crate::$ty::Vector(v2) = $other {
                    v1 == v2
                } else {
                    false
                }
            }
            $crate::$ty::KV(kv1) => {
                if let $crate::$ty::KV(kv2) = $other {
                    kv1 == kv2
                } else {
                    false
                }
            }
            $crate::$ty::None => {
                if let $crate::$ty::None = $other {
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    };
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        eq!(self, other, Object)
    }
}

impl Eq for Object {}

macro_rules! partial_cmp {
    ($self:expr, $other:expr, $ty:ident) => {
        match $self {
            $crate::$ty::Primitive(p) => {
                if let $crate::$ty::None = $other {
                    Some(Ordering::Greater)
                } else {
                    $other
                        .as_primitive()
                        .map(|o| p.partial_cmp(&o))
                        .unwrap_or(None)
                }
            }
            $crate::$ty::Blob(v) => $other
                .as_bytes()
                .map(|o| v.as_ref().partial_cmp(o))
                .unwrap_or(None),
            $crate::$ty::String(v) => $other
                .as_str()
                .map(|o| (&(**v)).partial_cmp(&(*o)))
                .unwrap_or(None),
            $crate::$ty::None => {
                if let $crate::$ty::None = $other {
                    Some(Ordering::Equal)
                } else {
                    Some(Ordering::Less)
                }
            }
            $crate::$ty::Vector(v1) => {
                if let $crate::$ty::Vector(v2) = $other {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            $crate::$ty::KV(kv1) => {
                if let $crate::$ty::KV(kv2) = $other {
                    kv1.partial_cmp(kv2)
                } else {
                    None
                }
            }
            _ => None,
        }
    };
}

macro_rules! cmp {
    ($self:expr, $other:expr, $ty:ident) => {
        if let Some(ord) = $self.partial_cmp($other) {
            ord
        } else {
            match ($self, $other) {
                ($crate::$ty::None, $crate::$ty::None) => Ordering::Equal,
                ($crate::$ty::None, _) => Ordering::Less,
                (_, $crate::$ty::None) => Ordering::Greater,
                (_, _) => Ordering::Less,
            }
        }
    };
}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp!(self, other, Object)
    }
}

impl Ord for Object {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp!(self, other, Object)
    }
}

impl<'a> PartialEq for BorrowObject<'a> {
    fn eq(&self, other: &Self) -> bool {
        eq!(self, other, BorrowObject)
    }
}

impl<'a> Eq for BorrowObject<'a> {}

impl<'a> PartialOrd for BorrowObject<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        partial_cmp!(self, other, BorrowObject)
    }
}

impl<'a> Ord for BorrowObject<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp!(self, other, BorrowObject)
    }
}

/// Map a double number as a combination of mantissa, exponent and sign as integers.
/// This function has once been in Rust's standard library, but got deprecated.
/// We borrow it to allow hashing the `[crate::dyn_type::Object]` that can be a double number.
#[inline(always)]
fn integer_decode(val: f64) -> (u64, i16, i8) {
    let bits: u64 = unsafe { std::mem::transmute(val) };
    let sign: i8 = if bits >> 63 == 0 { 1 } else { -1 };
    let mut exponent: i16 = ((bits >> 52) & 0x7ff) as i16;
    let mantissa = if exponent == 0 {
        (bits & 0xfffffffffffff) << 1
    } else {
        (bits & 0xfffffffffffff) | 0x10000000000000
    };

    exponent -= 1023 + 52;
    (mantissa, exponent, sign)
}

/// Returns the mantissa, base 2 exponent, and sign as integers, respectively. The original number can be recovered by sign * mantissa * 2 ^ exponent.
#[inline(always)]
fn integer_decode_f32(val: f32) -> (u64, i16, i8) {
    let bits: u32 = unsafe { std::mem::transmute(val) };
    let sign: i8 = if bits >> 31 == 0 { 1 } else { -1 };
    let mut exponent: i16 = ((bits >> 23) & 0xff) as i16;
    let mantissa = if exponent == 0 { (bits & 0x7fffff) << 1 } else { (bits & 0x7fffff) | 0x800000 };

    exponent -= 127 + 23;
    (mantissa as u64, exponent, sign)
}

macro_rules! hash {
    ($self:expr, $state:expr, $ty:ident) => {
        match $self {
            $crate::$ty::Primitive(p) => match p {
                $crate::Primitives::Byte(v) => {
                    v.hash($state);
                }
                $crate::Primitives::Integer(v) => {
                    v.hash($state);
                }
                $crate::Primitives::UInteger(v) => {
                    v.hash($state);
                }
                $crate::Primitives::Long(v) => {
                    v.hash($state);
                }
                $crate::Primitives::ULong(v) => {
                    v.hash($state);
                }
                $crate::Primitives::Float(v) => {
                    integer_decode_f32(*v).hash($state);
                }
                $crate::Primitives::Double(v) => {
                    integer_decode(*v).hash($state);
                }
                $crate::Primitives::ULLong(v) => {
                    v.hash($state);
                }
            },
            $crate::$ty::String(s) => {
                s.hash($state);
            }
            $crate::$ty::Blob(b) => {
                b.hash($state);
            }
            $crate::$ty::Vector(v) => {
                v.hash($state);
            }
            $crate::$ty::KV(kv) => {
                for pair in kv.iter() {
                    pair.hash($state);
                }
            }
            $crate::$ty::None => {
                "".hash($state);
            }
            $crate::$ty::DateFormat(df) => {
                df.hash($state);
            }
            _ => unimplemented!(),
        }
    };
}

impl Hash for Object {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash!(self, state, Object)
    }
}

impl<'a> Hash for BorrowObject<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash!(self, state, BorrowObject)
    }
}

impl<'a> From<Option<BorrowObject<'a>>> for BorrowObject<'a> {
    fn from(obj_opt: Option<BorrowObject<'a>>) -> Self {
        match obj_opt {
            Some(obj) => obj,
            None => BorrowObject::None,
        }
    }
}

impl From<i8> for Object {
    fn from(v: i8) -> Self {
        Object::Primitive(Primitives::Byte(v))
    }
}

impl<'a> From<i8> for BorrowObject<'a> {
    fn from(v: i8) -> Self {
        BorrowObject::Primitive(Primitives::Byte(v))
    }
}

impl From<bool> for Object {
    fn from(v: bool) -> Self {
        if v {
            Object::Primitive(Primitives::Byte(1))
        } else {
            Object::Primitive(Primitives::Byte(0))
        }
    }
}

impl<'a> From<bool> for BorrowObject<'a> {
    fn from(v: bool) -> Self {
        if v {
            BorrowObject::Primitive(Primitives::Byte(1))
        } else {
            BorrowObject::Primitive(Primitives::Byte(0))
        }
    }
}

impl From<i32> for Object {
    fn from(i: i32) -> Self {
        Object::Primitive(Primitives::Integer(i))
    }
}

impl From<u32> for Object {
    fn from(i: u32) -> Self {
        Object::Primitive(Primitives::UInteger(i))
    }
}

impl<'a> From<i32> for BorrowObject<'a> {
    fn from(i: i32) -> Self {
        BorrowObject::Primitive(Primitives::Integer(i))
    }
}

impl<'a> From<u32> for BorrowObject<'a> {
    fn from(i: u32) -> Self {
        BorrowObject::Primitive(Primitives::UInteger(i))
    }
}

impl From<i64> for Object {
    fn from(i: i64) -> Self {
        Object::Primitive(Primitives::Long(i))
    }
}

impl From<u64> for Object {
    fn from(i: u64) -> Self {
        Object::Primitive(Primitives::ULong(i))
    }
}

impl<'a> From<i64> for BorrowObject<'a> {
    fn from(i: i64) -> Self {
        BorrowObject::Primitive(Primitives::Long(i))
    }
}

impl<'a> From<u64> for BorrowObject<'a> {
    fn from(i: u64) -> Self {
        BorrowObject::Primitive(Primitives::ULong(i))
    }
}

impl From<f32> for Object {
    fn from(i: f32) -> Self {
        Object::Primitive(Primitives::Float(i))
    }
}

impl From<f64> for Object {
    fn from(i: f64) -> Self {
        Object::Primitive(Primitives::Double(i))
    }
}

impl From<f32> for BorrowObject<'_> {
    fn from(i: f32) -> Self {
        BorrowObject::Primitive(Primitives::Float(i))
    }
}

impl<'a> From<f64> for BorrowObject<'a> {
    fn from(i: f64) -> Self {
        BorrowObject::Primitive(Primitives::Double(i))
    }
}

impl From<usize> for Object {
    fn from(i: usize) -> Self {
        Object::from(i as u64)
    }
}

impl<'a> From<usize> for BorrowObject<'a> {
    fn from(i: usize) -> Self {
        BorrowObject::from(i as u64)
    }
}

impl From<u128> for Object {
    fn from(u: u128) -> Self {
        Object::Primitive(Primitives::ULLong(u))
    }
}

impl<'a> From<u128> for BorrowObject<'a> {
    fn from(i: u128) -> Self {
        BorrowObject::Primitive(Primitives::ULLong(i as u128))
    }
}

impl From<Vec<u8>> for Object {
    fn from(v: Vec<u8>) -> Self {
        Object::Blob(v.into_boxed_slice())
    }
}

impl From<Box<[u8]>> for Object {
    fn from(v: Box<[u8]>) -> Self {
        Object::Blob(v)
    }
}

impl<'a> From<&'a [u8]> for BorrowObject<'a> {
    fn from(blob: &'a [u8]) -> Self {
        BorrowObject::Blob(blob)
    }
}

impl From<&str> for Object {
    fn from(s: &str) -> Self {
        Object::String(s.to_owned())
    }
}

impl<'a> From<&'a str> for BorrowObject<'a> {
    fn from(s: &'a str) -> Self {
        BorrowObject::String(s)
    }
}

impl From<String> for Object {
    fn from(s: String) -> Self {
        Object::String(s)
    }
}

impl From<Primitives> for Object {
    fn from(v: Primitives) -> Self {
        Object::Primitive(v)
    }
}

impl<T: Into<Object>> From<Vec<T>> for Object {
    fn from(vec: Vec<T>) -> Self {
        Object::Vector(
            vec.into_iter()
                .map(|val| val.into())
                .collect_vec(),
        )
    }
}

impl<K: Into<Object>, V: Into<Object>> From<Vec<(K, V)>> for Object {
    fn from(tuples: Vec<(K, V)>) -> Self {
        Object::KV(
            tuples
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect::<BTreeMap<_, _>>(),
        )
    }
}

impl From<NaiveDate> for Object {
    fn from(date: NaiveDate) -> Self {
        Object::DateFormat(DateTimeFormats::Date(date))
    }
}

impl From<NaiveDateTime> for Object {
    fn from(date_time: NaiveDateTime) -> Self {
        Object::DateFormat(DateTimeFormats::DateTime(date_time))
    }
}

impl From<NaiveTime> for Object {
    fn from(time: NaiveTime) -> Self {
        Object::DateFormat(DateTimeFormats::Time(time))
    }
}

impl From<DateTime<FixedOffset>> for Object {
    fn from(date_time_with_tz: DateTime<FixedOffset>) -> Self {
        Object::DateFormat(DateTimeFormats::DateTimeWithTz(date_time_with_tz))
    }
}

impl From<DateTimeFormats> for Object {
    fn from(date_time_formats: DateTimeFormats) -> Self {
        Object::DateFormat(date_time_formats)
    }
}

pub enum OwnedOrRef<'a, T> {
    Owned(T),
    Ref(&'a T),
}

impl<'a, T> Deref for OwnedOrRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedOrRef::Owned(v) => v,
            OwnedOrRef::Ref(v) => *v,
        }
    }
}

impl<'a, T: PartialEq> PartialEq<T> for OwnedOrRef<'a, T> {
    fn eq(&self, other: &T) -> bool {
        match self {
            OwnedOrRef::Owned(v) => v.eq(other),
            OwnedOrRef::Ref(v) => (*v).eq(other),
        }
    }
}

impl<'a, T: PartialOrd> PartialOrd<T> for OwnedOrRef<'a, T> {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        match self {
            OwnedOrRef::Owned(v) => v.partial_cmp(other),
            OwnedOrRef::Ref(v) => (*v).partial_cmp(other),
        }
    }
}

impl<'a, T: std::fmt::Debug> std::fmt::Debug for OwnedOrRef<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("OwnedOrRef");
        match self {
            OwnedOrRef::Owned(v) => debug.field("Owned", v),
            OwnedOrRef::Ref(v) => debug.field("Ref", v),
        };
        debug.finish()
    }
}
