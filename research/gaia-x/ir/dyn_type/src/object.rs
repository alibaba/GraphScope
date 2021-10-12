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

use crate::{try_downcast, try_downcast_ref, CastError, DynType};
use core::any::TypeId;
use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawType {
    Byte,
    Integer,
    Long,
    ULLong,
    Float,
    String,
    Blob(usize),
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub enum Primitives {
    Byte(i8),
    Integer(i32),
    Long(i64),
    ULLong(u128),
    Float(f64),
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
            Primitives::Integer(v) => {
                i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::Integer))
            }
            Primitives::Long(v) => {
                i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                i8::try_from(*v).map_err(|_| CastError::new::<i8>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i8>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Result<i16, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i16),
            Primitives::Integer(v) => {
                i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::Integer))
            }
            Primitives::Long(v) => {
                i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                i16::try_from(*v).map_err(|_| CastError::new::<i16>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i16>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i32),
            Primitives::Integer(v) => Ok(*v),
            Primitives::Long(v) => {
                i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                i32::try_from(*v).map_err(|_| CastError::new::<i32>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i32>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i64),
            Primitives::Integer(v) => Ok(*v as i64),
            Primitives::Long(v) => Ok(*v),
            Primitives::ULLong(v) => {
                i64::try_from(*v).map_err(|_| CastError::new::<i64>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i64>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i128),
            Primitives::Integer(v) => Ok(*v as i128),
            Primitives::Long(v) => Ok(*v as i128),
            Primitives::ULLong(v) => {
                i128::try_from(*v).map_err(|_| CastError::new::<i128>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<i128>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_u8(&self) -> Result<u8, CastError> {
        match self {
            Primitives::Byte(v) => {
                u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Integer))
            }
            Primitives::Long(v) => {
                u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                u8::try_from(*v).map_err(|_| CastError::new::<u8>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<u8>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Result<u16, CastError> {
        match self {
            Primitives::Byte(v) => {
                u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Integer))
            }
            Primitives::Long(v) => {
                u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                u16::try_from(*v).map_err(|_| CastError::new::<u16>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<u16>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Result<u32, CastError> {
        match self {
            Primitives::Byte(v) => {
                u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Integer))
            }
            Primitives::Long(v) => {
                u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                u32::try_from(*v).map_err(|_| CastError::new::<u32>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<u32>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Primitives::Byte(v) => {
                u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Integer))
            }
            Primitives::Long(v) => {
                u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                u64::try_from(*v).map_err(|_| CastError::new::<u64>(RawType::ULLong))
            }
            Primitives::Float(_) => Err(CastError::new::<u64>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_usize(&self) -> Result<usize, CastError> {
        self.as_u64().map(|v| v as usize)
    }

    #[inline]
    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            Primitives::Byte(v) => {
                u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Integer))
            }
            Primitives::Long(v) => {
                u128::try_from(*v).map_err(|_| CastError::new::<u128>(RawType::Long))
            }
            Primitives::ULLong(v) => Ok(*v),
            Primitives::Float(_) => Err(CastError::new::<u128>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            Primitives::Byte(v) => {
                f64::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Byte))
            }
            Primitives::Integer(v) => {
                f64::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Integer))
            }
            Primitives::Long(v) => {
                let t = i16::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::Long))?;
                f64::try_from(t).map_err(|_| CastError::new::<f64>(RawType::Long))
            }
            Primitives::ULLong(v) => {
                let t = i16::try_from(*v).map_err(|_| CastError::new::<f64>(RawType::ULLong))?;
                f64::try_from(t).map_err(|_| CastError::new::<f64>(RawType::ULLong))
            }
            Primitives::Float(v) => Ok(*v),
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
            return self.as_f64().map(|v| {
                let t: &T = unsafe { std::mem::transmute(&(v as f32)) };
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
                Primitives::Float(o) => (*v as f64).eq(o),
                _ => other.as_i8().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Integer(v) => match other {
                Primitives::Float(o) => (*v as f64).eq(o),
                _ => other.as_i32().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Long(v) => match other {
                Primitives::Float(o) => (*v as f64).eq(o),
                _ => other.as_i64().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::ULLong(v) => match other {
                Primitives::Float(o) => (*v as f64).eq(o),
                _ => other.as_u128().map(|o| o == *v).unwrap_or(false),
            },
            Primitives::Float(v) => other.as_f64().map(|o| o == *v).unwrap_or(false),
        }
    }
}

impl Eq for Primitives {}

impl PartialOrd for Primitives {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Primitives::Byte(v) => match other {
                Primitives::Float(o) => (*v as f64).partial_cmp(o),
                _ => other.as_i8().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Integer(v) => match other {
                Primitives::Float(o) => (*v as f64).partial_cmp(o),
                _ => other.as_i32().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Long(v) => match other {
                Primitives::Float(o) => (*v as f64).partial_cmp(o),
                _ => other.as_i64().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::ULLong(v) => match other {
                Primitives::Float(o) => (*v as f64).partial_cmp(o),
                _ => other.as_u128().map(|o| v.cmp(&o)).ok(),
            },
            Primitives::Float(v) => other.as_f64().map(|o| v.partial_cmp(&o)).unwrap_or(None),
        }
    }
}

/// copy from std::any::Any;
impl dyn DynType {
    pub fn is<T: DynType>(&self) -> bool {
        // Get `TypeId` of the type this function is instantiated with.
        let t = TypeId::of::<T>();

        // Get `TypeId` of the type in the trait dyn_type (`self`).
        let concrete = self.type_id();

        // Compare both `TypeId`s on equality.
        t == concrete
    }

    pub fn try_downcast_ref<T: DynType>(&self) -> Option<&T> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(&*(self as *const dyn DynType as *const T)) }
        } else {
            None
        }
    }

    pub fn try_downcast_mut<T: DynType>(&mut self) -> Option<&mut T> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(&mut *(self as *mut dyn DynType as *mut T)) }
        } else {
            None
        }
    }
}

// Is dyn type needed in dyn_type;
#[derive(Clone, Debug)]
pub enum Object {
    Primitive(Primitives),
    String(String),
    Blob(Box<[u8]>),
    DynOwned(Box<dyn DynType>),
}

impl ToString for Object {
    fn to_string(&self) -> String {
        use Object::*;
        match self {
            Primitive(p) => p.to_string(),
            String(s) => s.to_string(),
            Blob(_) => unimplemented!(),
            DynOwned(_) => unimplemented!(),
        }
    }
}

/// Try to borrow an immutable reference of [crate::Object].
#[derive(Debug, Clone, Copy)]
pub enum BorrowObject<'a> {
    Primitive(Primitives),
    String(&'a str),
    Blob(&'a [u8]),
    /// To borrow from `Object::DynOwned`, and it can be cloned back to `Object::DynOwned`
    DynRef(&'a Box<dyn DynType>),
}

impl<'a> ToString for BorrowObject<'a> {
    fn to_string(&self) -> String {
        use BorrowObject::*;
        match self {
            Primitive(p) => p.to_string(),
            String(s) => s.to_string(),
            Blob(_) => unimplemented!(),
            DynRef(_) => unimplemented!(),
        }
    }
}

impl Object {
    pub fn raw_type(&self) -> RawType {
        match self {
            Object::Primitive(p) => p.raw_type(),
            Object::String(_) => RawType::String,
            Object::Blob(b) => RawType::Blob(b.len()),
            Object::DynOwned(_) => RawType::Unknown,
        }
    }

    pub fn as_borrow(&self) -> BorrowObject {
        match self {
            Object::Primitive(p) => BorrowObject::Primitive(*p),
            Object::String(v) => BorrowObject::String(v.as_str()),
            Object::Blob(v) => BorrowObject::Blob(v.as_ref()),
            Object::DynOwned(v) => BorrowObject::DynRef(v),
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
        if let Object::Primitive(p) = self {
            p.as_i8()
        } else {
            Err(CastError::new::<i8>(self.raw_type()))
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
        if let Object::Primitive(p) = self {
            p.as_u8()
        } else {
            Err(CastError::new::<u8>(self.raw_type()))
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
            Object::Primitive(p) => Err(CastError::new::<String>(p.raw_type())),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            Object::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            Object::String(str) => Ok(str.as_bytes()),
            Object::Blob(v) => Ok(v.as_ref()),
            Object::DynOwned(x) => try_downcast!(x, Vec<u8>, as_slice),
        }
    }

    pub fn get<T: DynType + Clone>(&self) -> Result<OwnedOrRef<T>, CastError> {
        match self {
            Object::Primitive(p) => {
                let v = p.get::<T>()?;
                Ok(OwnedOrRef::Owned(v))
            }
            Object::String(x) => try_transmute!(x, T, RawType::String).map(|v| OwnedOrRef::Ref(v)),
            Object::Blob(x) => {
                try_transmute!(x, T, RawType::Blob(x.len())).map(|v| OwnedOrRef::Ref(v))
            }
            Object::DynOwned(x) => try_downcast_ref!(x, T).map(|v| OwnedOrRef::Ref(v)),
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
            Object::Primitive(p) => Err(CastError::new::<String>(p.raw_type())),
            Object::Blob(_) => unimplemented!(),
        }
    }
}

impl<'a> BorrowObject<'a> {
    pub fn raw_type(&self) -> RawType {
        match self {
            BorrowObject::Primitive(p) => p.raw_type(),
            BorrowObject::String(_) => RawType::String,
            BorrowObject::Blob(b) => RawType::Blob(b.len()),
            BorrowObject::DynRef(_) => RawType::Unknown,
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
            BorrowObject::Primitive(p) => Err(CastError::new::<String>(p.raw_type())),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            BorrowObject::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            BorrowObject::String(v) => Ok(v.as_bytes()),
            BorrowObject::Blob(v) => Ok(*v),
            BorrowObject::DynRef(v) => try_downcast!(v, Vec<u8>, as_slice),
        }
    }

    pub fn try_to_owned(&self) -> Option<Object> {
        match self {
            BorrowObject::Primitive(p) => Some(Object::Primitive(*p)),
            BorrowObject::String(s) => Some(Object::String((*s).to_owned())),
            BorrowObject::Blob(b) => Some(Object::Blob(b.to_vec().into_boxed_slice())),
            BorrowObject::DynRef(d) => Some(Object::DynOwned((*d).clone())),
        }
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Object::Primitive(p) => other.as_primitive().map(|o| p == &o).unwrap_or(false),
            Object::Blob(v) => other.as_bytes().map(|o| o.eq(v.as_ref())).unwrap_or(false),
            Object::String(v) => other.as_str().map(|o| o.eq(v.as_str())).unwrap_or(false),
            // TODO(longbin) Should be able to compare a DynType
            Object::DynOwned(_) => false,
        }
    }
}

impl Eq for Object {}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Object::Primitive(p) => other
                .as_primitive()
                .map(|o| p.partial_cmp(&o))
                .unwrap_or(None),
            Object::Blob(v) => other
                .as_bytes()
                .map(|o| v.as_ref().partial_cmp(o))
                .unwrap_or(None),
            Object::String(v) => other
                .as_str()
                .map(|o| v.as_str().partial_cmp(o.as_ref()))
                .unwrap_or(None),
            // TODO(longbin) Should be able to compare a DynType
            Object::DynOwned(_) => None,
        }
    }
}

impl<'a> PartialEq for BorrowObject<'a> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            BorrowObject::Primitive(p) => other.as_primitive().map(|o| p == &o).unwrap_or(false),
            BorrowObject::String(v) => other.as_str().map(|o| o.eq(*v)).unwrap_or(false),
            BorrowObject::Blob(v) => other.as_bytes().map(|o| *v == o).unwrap_or(false),
            // TODO(longbin) Should be able to compare a DynType
            BorrowObject::DynRef(_) => false,
        }
    }
}

impl<'a> PartialOrd for BorrowObject<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            BorrowObject::Primitive(p) => other
                .as_primitive()
                .map(|o| p.partial_cmp(&o))
                .unwrap_or(None),
            BorrowObject::String(v) => other
                .as_str()
                .map(|o| (*v).partial_cmp(o.as_ref()))
                .unwrap_or(None),
            BorrowObject::Blob(v) => other
                .as_bytes()
                .map(|o| (*v).partial_cmp(o))
                .unwrap_or(None),
            // TODO(longbin) Should be able to compare a DynType
            BorrowObject::DynRef(_) => None,
        }
    }
}

/// Map a float number as a combination of mantissa, exponent and sign as integers.
/// This function has once been in Rust's standard library, but got deprecated.
/// We borrow it to allow hashing the `[crate::dyn_type::Object]` that can be a float number.
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

impl Hash for Object {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Object::Primitive(p) => match p {
                Primitives::Byte(v) => {
                    v.hash(state);
                }
                Primitives::Integer(v) => {
                    v.hash(state);
                }
                Primitives::Long(v) => {
                    v.hash(state);
                }
                Primitives::Float(v) => {
                    integer_decode(*v).hash(state);
                }
                Primitives::ULLong(v) => {
                    v.hash(state);
                }
            },
            Object::String(s) => {
                s.hash(state);
            }
            Object::Blob(b) => {
                b.hash(state);
            }
            // TODO(longbin) Should be able to hash a DynType
            Object::DynOwned(_) => {
                unimplemented!()
            }
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

impl<'a> From<i32> for BorrowObject<'a> {
    fn from(i: i32) -> Self {
        BorrowObject::Primitive(Primitives::Integer(i))
    }
}

impl From<i64> for Object {
    fn from(i: i64) -> Self {
        Object::Primitive(Primitives::Long(i))
    }
}

impl<'a> From<i64> for BorrowObject<'a> {
    fn from(i: i64) -> Self {
        BorrowObject::Primitive(Primitives::Long(i))
    }
}

impl From<f64> for Object {
    fn from(i: f64) -> Self {
        Object::Primitive(Primitives::Float(i))
    }
}

impl<'a> From<f64> for BorrowObject<'a> {
    fn from(i: f64) -> Self {
        BorrowObject::Primitive(Primitives::Float(i))
    }
}

impl From<u64> for Object {
    fn from(i: u64) -> Self {
        if i <= (i64::MAX as u64) {
            Object::Primitive(Primitives::Long(i as i64))
        } else {
            Object::Primitive(Primitives::ULLong(i as u128))
        }
    }
}

impl<'a> From<u64> for BorrowObject<'a> {
    fn from(i: u64) -> Self {
        if i <= (i64::MAX as u64) {
            BorrowObject::Primitive(Primitives::Long(i as i64))
        } else {
            BorrowObject::Primitive(Primitives::ULLong(i as u128))
        }
    }
}

impl From<usize> for Object {
    fn from(i: usize) -> Self {
        Object::from(i as u64)
    }
}

impl<'a> From<usize> for BorrowObject<'a> {
    fn from(i: usize) -> Self {
        if i <= (i64::MAX as usize) {
            BorrowObject::Primitive(Primitives::Long(i as i64))
        } else {
            BorrowObject::Primitive(Primitives::ULLong(i as u128))
        }
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

impl<'a> From<BorrowObject<'a>> for Object {
    fn from(s: BorrowObject<'a>) -> Self {
        match s {
            BorrowObject::Primitive(p) => Object::Primitive(p),
            BorrowObject::Blob(blob) => Object::Blob(blob.to_vec().into_boxed_slice()),
            BorrowObject::String(s) => Object::String(s.to_string()),
            _ => unimplemented!(),
        }
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
