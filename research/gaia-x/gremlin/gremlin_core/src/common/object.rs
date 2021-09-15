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

use crate::common::serde_dyn::de_dyn_obj;
use crate::common::DynType;
use crate::generated::common as pb;
use crate::structure::Label;
use core::any::TypeId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use serde_json::Value;
use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;

#[derive(Debug, Clone, Copy)]
pub enum RawType {
    Byte,
    Integer,
    Long,
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
    Float(f64),
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

impl Encode for Primitives {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Primitives::Byte(b) => {
                writer.write_u8(0)?;
                b.write_to(writer)?;
            }
            Primitives::Integer(i) => {
                writer.write_u8(1)?;
                i.write_to(writer)?;
            }
            Primitives::Long(l) => {
                writer.write_u8(2)?;
                l.write_to(writer)?;
            }
            Primitives::Float(f) => {
                writer.write_u8(3)?;
                f.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Primitives {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let b = <i8>::read_from(reader)?;
                Ok(Primitives::Byte(b))
            }
            1 => {
                let i = <i32>::read_from(reader)?;
                Ok(Primitives::Integer(i))
            }
            2 => {
                let l = <i64>::read_from(reader)?;
                Ok(Primitives::Long(l))
            }
            3 => {
                let f = <f64>::read_from(reader)?;
                Ok(Primitives::Float(f))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Primitives {
    #[inline]
    pub fn raw_type(&self) -> RawType {
        match self {
            Primitives::Byte(_) => RawType::Byte,
            Primitives::Integer(_) => RawType::Integer,
            Primitives::Long(_) => RawType::Long,
            Primitives::Float(_) => RawType::Float,
        }
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
            Primitives::Float(_) => Err(CastError::new::<i32>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i64),
            Primitives::Integer(v) => Ok(*v as i64),
            Primitives::Long(v) => Ok(*v),
            Primitives::Float(_) => Err(CastError::new::<i64>(RawType::Float)),
        }
    }

    #[inline]
    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            Primitives::Byte(v) => Ok(*v as i128),
            Primitives::Integer(v) => Ok(*v as i128),
            Primitives::Long(v) => Ok(*v as i128),
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
            Primitives::Float(_) => Err(CastError::new::<u64>(RawType::Float)),
        }
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
            Primitives::Byte(v) => other.as_i8().map(|o| o == *v).unwrap_or(false),
            Primitives::Integer(v) => other.as_i32().map(|o| o == *v).unwrap_or(false),
            Primitives::Long(v) => other.as_i64().map(|o| o == *v).unwrap_or(false),
            Primitives::Float(v) => other.as_f64().map(|o| o == *v).unwrap_or(false),
        }
    }
}

impl PartialOrd for Primitives {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Primitives::Byte(v) => other.as_i8().map(|o| v.cmp(&o)).ok(),
            Primitives::Integer(v) => other.as_i32().map(|o| v.cmp(&o)).ok(),
            Primitives::Long(v) => other.as_i64().map(|o| v.cmp(&o)).ok(),
            Primitives::Float(v) => other.as_f64().map(|o| v.partial_cmp(&o)).unwrap_or(None),
        }
    }
}

/// copy from std::any::Any;
impl dyn DynType {
    pub fn is<T: DynType>(&self) -> bool {
        // Get `TypeId` of the type this function is instantiated with.
        let t = TypeId::of::<T>();

        // Get `TypeId` of the type in the trait object (`self`).
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

const I32_LEN: usize = std::mem::size_of::<i32>();
const I64_LEN: usize = std::mem::size_of::<i64>();
const F64_LEN: usize = std::mem::size_of::<f64>();
const I128_LEN: usize = std::mem::size_of::<i128>();
const U128_LEN: usize = std::mem::size_of::<u128>();

macro_rules! try_downcast {
    ($var: expr, $ty: ty) => {
        if let Some(v) = $var.try_downcast_ref::<$ty>() {
            Ok(*v)
        } else {
            Err(CastError::new::<$ty>(RawType::Unknown))
        }
    };

    ($var: expr, $ty: ty, $op: ident) => {
        if let Some(v) = $var.try_downcast_ref::<$ty>() {
            Ok(v.$op())
        } else {
            Err(CastError::new::<$ty>(RawType::Unknown))
        }
    };
}

macro_rules! try_downcast_ref {
    ($var: expr, $ty: ty) => {
        if let Some(v) = $var.try_downcast_ref::<$ty>() {
            Ok(v)
        } else {
            Err(CastError::new::<$ty>(RawType::Unknown))
        }
    };
}

// Is dyn type needed in object;
#[derive(Clone, Debug)]
pub enum Object {
    Primitive(Primitives),
    String(String),
    Blob(Box<[u8]>),
    UnknownOwned(Box<dyn DynType>),
    UnknownRef(&'static dyn DynType),
}

pub enum BorrowObject<'a> {
    Primitive(Primitives),
    String(&'a str),
    Blob(&'a [u8]),
    Unknown(&'a dyn DynType),
}

impl Object {
    pub fn raw_type(&self) -> RawType {
        match self {
            Object::Primitive(p) => p.raw_type(),
            Object::String(_) => RawType::String,
            Object::Blob(b) => RawType::Blob(b.len()),
            Object::UnknownOwned(_) => RawType::Unknown,
            Object::UnknownRef(_) => RawType::Unknown,
        }
    }

    pub fn as_borrow(&self) -> BorrowObject {
        match self {
            Object::Primitive(p) => BorrowObject::Primitive(*p),
            Object::String(v) => BorrowObject::String(v.as_str()),
            Object::Blob(v) => BorrowObject::Blob(v.as_ref()),
            Object::UnknownOwned(v) => BorrowObject::Unknown(v.deref()),
            Object::UnknownRef(v) => BorrowObject::Unknown(*v),
        }
    }

    pub fn as_primitive(&self) -> Result<Primitives, CastError> {
        match self {
            Object::Primitive(p) => Ok(*p),
            Object::String(_) => Err(CastError::new::<Primitives>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<Primitives>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, Primitives),
            Object::UnknownRef(x) => try_downcast!(x, Primitives),
        }
    }

    pub fn as_i8(&self) -> Result<i8, CastError> {
        if let Object::Primitive(p) = self {
            p.as_i8()
        } else {
            Err(CastError::new::<i8>(self.raw_type()))
        }
    }

    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            Object::Primitive(p) => p.as_i32(),
            Object::String(_) => Err(CastError::new::<i32>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<i32>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, i32),
            Object::UnknownRef(x) => try_downcast!(x, i32),
        }
    }

    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            Object::Primitive(p) => p.as_i64(),
            Object::String(_) => Err(CastError::new::<i64>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<i64>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, i64),
            Object::UnknownRef(x) => try_downcast!(x, i64),
        }
    }

    pub fn as_u64(&self) -> Result<u64, CastError> {
        match self {
            Object::Primitive(p) => p.as_u64(),
            Object::String(_) => Err(CastError::new::<u64>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<u64>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, u64),
            Object::UnknownRef(x) => try_downcast!(x, u64),
        }
    }

    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            Object::Primitive(p) => p.as_i128(),
            Object::String(_) => Err(CastError::new::<i128>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<i128>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, i128),
            Object::UnknownRef(x) => try_downcast!(x, i128),
        }
    }

    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            Object::Primitive(p) => p.as_u128(),
            Object::String(_) => Err(CastError::new::<u128>(RawType::String)),
            Object::Blob(v) => Err(CastError::new::<u128>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, u128),
            Object::UnknownRef(x) => try_downcast!(x, u128),
        }
    }

    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            Object::Primitive(p) => p.as_f64(),
            Object::Blob(v) => Err(CastError::new::<f64>(RawType::Blob(v.len()))),
            Object::UnknownOwned(x) => try_downcast!(x, f64),
            Object::UnknownRef(x) => try_downcast!(x, f64),
            _ => Err(CastError::new::<f64>(self.raw_type())),
        }
    }

    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            Object::String(str) => Ok(Cow::Borrowed(str.as_str())),
            Object::Blob(b) => Ok(String::from_utf8_lossy(b)),
            Object::UnknownOwned(x) => try_downcast!(x, String, as_str).map(|r| Cow::Borrowed(r)),
            Object::UnknownRef(x) => try_downcast!(x, String, as_str).map(|r| Cow::Borrowed(r)),
            Object::Primitive(p) => Err(CastError::new::<String>(p.raw_type())),
        }
    }

    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            Object::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            Object::String(str) => Ok(str.as_bytes()),
            Object::Blob(v) => Ok(v.as_ref()),
            Object::UnknownOwned(x) => try_downcast!(x, Vec<u8>, as_slice),
            Object::UnknownRef(x) => try_downcast!(x, Vec<u8>, as_slice),
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
            Object::UnknownOwned(x) => try_downcast_ref!(x, T).map(|v| OwnedOrRef::Ref(v)),
            Object::UnknownRef(x) => try_downcast_ref!(x, T).map(|v| OwnedOrRef::Ref(v)),
        }
    }

    pub fn take_string(self) -> Result<String, CastError> {
        match self {
            Object::String(str) => Ok(str),
            Object::UnknownOwned(mut x) => {
                if let Some(v) = x.try_downcast_mut::<String>() {
                    Ok(std::mem::replace(v, "".to_owned()))
                } else {
                    Err(CastError::new::<i32>(RawType::Unknown))
                }
            }
            Object::UnknownRef(x) => try_downcast!(x, String, to_owned),
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
            BorrowObject::Unknown(_) => RawType::Unknown,
        }
    }

    pub fn as_primitive(&self) -> Result<Primitives, CastError> {
        match self {
            BorrowObject::Primitive(p) => Ok(*p),
            BorrowObject::String(_) => Err(CastError::new::<Primitives>(RawType::String)),
            BorrowObject::Blob(v) => Err(CastError::new::<Primitives>(RawType::Blob(v.len()))),
            BorrowObject::Unknown(x) => try_downcast!(x, Primitives),
        }
    }

    pub fn as_i32(&self) -> Result<i32, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i32(),
            BorrowObject::String(_) => Err(CastError::new::<i32>(RawType::String)),
            BorrowObject::Blob(v) => {
                if v.len() == I32_LEN {
                    let mut b = [0u8; I32_LEN];
                    b.copy_from_slice(v.as_ref());
                    Ok(i32::from_le_bytes(b))
                } else {
                    Err(CastError::new::<i32>(RawType::Blob(v.len())))
                }
            }
            BorrowObject::Unknown(x) => try_downcast!(x, i32),
        }
    }

    pub fn as_i64(&self) -> Result<i64, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i64(),
            BorrowObject::String(_) => Err(CastError::new::<i64>(RawType::String)),
            BorrowObject::Blob(v) => {
                if v.len() == I64_LEN {
                    let mut b = [0u8; I64_LEN];
                    b.copy_from_slice(v.as_ref());
                    Ok(i64::from_le_bytes(b))
                } else {
                    Err(CastError::new::<i64>(RawType::Blob(v.len())))
                }
            }
            BorrowObject::Unknown(x) => try_downcast!(x, i64),
        }
    }

    pub fn as_i128(&self) -> Result<i128, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_i128(),
            BorrowObject::String(_) => Err(CastError::new::<i128>(RawType::String)),
            BorrowObject::Blob(v) => {
                if v.len() == I128_LEN {
                    let mut b = [0u8; I128_LEN];
                    b.copy_from_slice(v.as_ref());
                    Ok(i128::from_le_bytes(b))
                } else {
                    Err(CastError::new::<i128>(RawType::Blob(v.len())))
                }
            }
            BorrowObject::Unknown(x) => try_downcast!(x, i128),
        }
    }

    pub fn as_u128(&self) -> Result<u128, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_u128(),
            BorrowObject::String(_) => Err(CastError::new::<u128>(RawType::String)),
            BorrowObject::Blob(v) => {
                if v.len() == U128_LEN {
                    let mut b = [0u8; U128_LEN];
                    b.copy_from_slice(v.as_ref());
                    Ok(u128::from_le_bytes(b))
                } else {
                    Err(CastError::new::<i128>(RawType::Blob(v.len())))
                }
            }
            BorrowObject::Unknown(x) => try_downcast!(x, u128),
        }
    }

    pub fn as_f64(&self) -> Result<f64, CastError> {
        match self {
            BorrowObject::Primitive(p) => p.as_f64(),
            BorrowObject::String(_) => Err(CastError::new::<f64>(RawType::String)),
            BorrowObject::Blob(v) => {
                if v.len() == F64_LEN {
                    let mut b = [0u8; F64_LEN];
                    b.copy_from_slice(v.as_ref());
                    Ok(f64::from_le_bytes(b))
                } else {
                    Err(CastError::new::<f64>(RawType::Blob(v.len())))
                }
            }
            BorrowObject::Unknown(x) => try_downcast!(x, f64),
        }
    }

    pub fn as_str(&self) -> Result<Cow<'_, str>, CastError> {
        match self {
            BorrowObject::String(str) => Ok(Cow::Borrowed(*str)),
            BorrowObject::Blob(b) => Ok(String::from_utf8_lossy(b)),
            BorrowObject::Unknown(x) => try_downcast!(x, String, as_str).map(|r| Cow::Borrowed(r)),
            BorrowObject::Primitive(p) => Err(CastError::new::<String>(p.raw_type())),
        }
    }

    pub fn as_bytes(&self) -> Result<&[u8], CastError> {
        match self {
            BorrowObject::Primitive(p) => Err(CastError::new::<&[u8]>(p.raw_type())),
            BorrowObject::String(v) => Ok(v.as_bytes()),
            BorrowObject::Blob(v) => Ok(*v),
            BorrowObject::Unknown(v) => try_downcast!(v, Vec<u8>, as_slice),
        }
    }

    pub fn try_to_owned(&self) -> Option<Object> {
        match self {
            BorrowObject::Primitive(p) => Some(Object::Primitive(*p)),
            BorrowObject::String(s) => Some(Object::String((*s).to_owned())),
            BorrowObject::Blob(b) => Some(Object::Blob(b.to_vec().into_boxed_slice())),
            BorrowObject::Unknown(_) => None,
        }
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Object::Primitive(p) => other.as_primitive().map(|o| p == &o).unwrap_or(false),
            Object::Blob(v) => other.as_bytes().map(|o| o.eq(v.as_ref())).unwrap_or(false),
            Object::String(v) => other.as_str().map(|o| o.eq(v.as_str())).unwrap_or(false),
            Object::UnknownOwned(_) => false,
            Object::UnknownRef(_) => false,
        }
    }
}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Object::Primitive(p) => other.as_primitive().map(|o| p.partial_cmp(&o)).unwrap_or(None),
            Object::Blob(v) => other.as_bytes().map(|o| v.as_ref().partial_cmp(o)).unwrap_or(None),
            Object::String(v) => {
                other.as_str().map(|o| v.as_str().partial_cmp(o.as_ref())).unwrap_or(None)
            }
            Object::UnknownOwned(_) => None,
            Object::UnknownRef(_) => None,
        }
    }
}

impl<'a> PartialEq for BorrowObject<'a> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            BorrowObject::Primitive(p) => other.as_primitive().map(|o| p == &o).unwrap_or(false),
            BorrowObject::String(v) => other.as_str().map(|o| o.eq(*v)).unwrap_or(false),
            BorrowObject::Blob(v) => other.as_bytes().map(|o| *v == o).unwrap_or(false),
            BorrowObject::Unknown(_) => false,
        }
    }
}

impl<'a> PartialOrd for BorrowObject<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            BorrowObject::Primitive(p) => {
                other.as_primitive().map(|o| p.partial_cmp(&o)).unwrap_or(None)
            }
            BorrowObject::String(v) => {
                other.as_str().map(|o| (*v).partial_cmp(o.as_ref())).unwrap_or(None)
            }
            BorrowObject::Blob(v) => other.as_bytes().map(|o| (*v).partial_cmp(o)).unwrap_or(None),
            BorrowObject::Unknown(_) => None,
        }
    }
}

/// Map a float number as a combination of mantissa, exponent and sign as integers.
/// This function has once been in Rust's standard library, but got deprecated.
/// We borrow it to allow hashing the `[crate::object::Object]` that can be a float number.
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
            },
            Object::String(s) => {
                s.hash(state);
            }
            Object::Blob(b) => {
                b.hash(state);
            }
            Object::UnknownOwned(_o) => {
                //   TODO: hash UnknownOwned object
            }
            Object::UnknownRef(_o) => {
                // TODO: hash UnknownRef object
            }
        }
    }
}

impl Encode for Object {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Object::Primitive(p) => {
                writer.write_u8(0)?;
                p.write_to(writer)?;
                Ok(())
            }
            Object::String(str) => {
                writer.write_u8(1)?;
                str.write_to(writer)?;
                Ok(())
            }
            Object::Blob(b) => {
                writer.write_u8(2)?;
                let len = b.len();
                writer.write_u64(len as u64)?;
                writer.write_all(&(**b))?;
                Ok(())
            }
            Object::UnknownOwned(dyn_type) => {
                writer.write_u8(3)?;
                let bytes = (**dyn_type).to_bytes()?;
                bytes.write_to(writer)?;
                Ok(())
            }
            Object::UnknownRef(_) => {
                // TODO(yyy): not in use now
                Err(io::Error::new(io::ErrorKind::Other, "not supported"))
            }
        }
    }
}

impl Decode for Object {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let p = <Primitives>::read_from(reader)?;
                Ok(Object::Primitive(p))
            }
            1 => {
                let str = <String>::read_from(reader)?;
                Ok(Object::String(str))
            }
            2 => {
                let len = <u64>::read_from(reader)?;
                let mut b = vec![];
                for _i in 0..len {
                    let ele = <u8>::read_from(reader)?;
                    b.push(ele);
                }
                Ok(Object::Blob(b.into_boxed_slice()))
            }
            3 => {
                let bytes = <Vec<u8>>::read_from(reader)?;
                let mut bytes_reader = &bytes[0..];
                let number = <u64>::read_from(&mut bytes_reader)?;
                let t: TypeId = unsafe { std::mem::transmute(number) };
                let obj = de_dyn_obj(&t, &mut bytes_reader)?;
                Ok(Object::UnknownOwned(obj))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "not supported")),
        }
    }
}

#[derive(Debug)]
pub struct CastError {
    pub kind: RawType,
    target: &'static str,
}

impl CastError {
    pub fn new<T>(kind: RawType) -> Self {
        let target = std::any::type_name::<T>();
        CastError { kind, target }
    }
}

impl Display for CastError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.kind {
            RawType::Byte => write!(f, "can't cast i8 into {}", self.target),
            RawType::Integer => write!(f, "can't cast i32 into {}", self.target),
            RawType::Long => write!(f, "can't cast i64 into {}", self.target),
            RawType::Float => write!(f, "can't cast f64 into {}", self.target),
            RawType::Blob(len) => write!(f, "can't cast Blob({}) into {}", len, self.target),
            RawType::String => write!(f, "can't cast String into {}", self.target),
            RawType::Unknown => write!(f, "can't cast unknown dyn type into {}", self.target),
        }
    }
}

impl std::error::Error for CastError {}

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
            let b = i.to_le_bytes().to_vec().into_boxed_slice();
            Object::Blob(b)
        }
    }
}

impl From<u128> for Object {
    fn from(i: u128) -> Self {
        if i <= (i64::MAX as u128) {
            Object::Primitive(Primitives::Long(i as i64))
        } else {
            let b = i.to_le_bytes().to_vec().into_boxed_slice();
            Object::Blob(b)
        }
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

impl From<&str> for Object {
    fn from(s: &str) -> Self {
        Object::String(s.to_owned())
    }
}

impl From<String> for Object {
    fn from(s: String) -> Self {
        Object::String(s)
    }
}

impl From<Label> for Object {
    fn from(label: Label) -> Self {
        match label {
            Label::Str(s) => Object::String(s),
            Label::Id(id) => Object::Primitive(Primitives::Integer(id as i32)),
        }
    }
}

impl From<&Label> for Object {
    fn from(label: &Label) -> Self {
        match label {
            Label::Str(s) => Object::String(s.to_string()),
            Label::Id(id) => Object::Primitive(Primitives::Integer(*id as i32)),
        }
    }
}

impl From<&pb::Value> for Option<Object> {
    fn from(raw: &pb::Value) -> Self {
        match &raw.item {
            Some(pb::value::Item::Blob(blob)) => {
                let mut bytes = vec![0; blob.len()];
                bytes.copy_from_slice(blob);
                Some(bytes.into())
            }
            Some(pb::value::Item::Boolean(item)) => Some((*item).into()),
            Some(pb::value::Item::I32(item)) => Some((*item).into()),
            Some(pb::value::Item::I64(item)) => Some((*item).into()),
            Some(pb::value::Item::F64(item)) => Some((*item).into()),
            Some(pb::value::Item::Str(item)) => Some(item.as_str().into()),
            Some(pb::value::Item::I32Array(_)) => unimplemented!(),
            Some(pb::value::Item::I64Array(_)) => unimplemented!(),
            Some(pb::value::Item::F64Array(_)) => unimplemented!(),
            Some(pb::value::Item::StrArray(_)) => unimplemented!(),
            Some(pb::value::Item::None(_)) => None,
            _ => None,
        }
    }
}

impl From<&serde_json::Value> for Object {
    fn from(val: &serde_json::Value) -> Self {
        match val {
            serde_json::Value::Bool(item) => Object::from(*item as i8),
            Value::Number(num) => {
                if num.is_i64() {
                    Object::from(num.as_i64().unwrap())
                } else if num.is_u64() {
                    Object::from(num.as_u64().unwrap() as u128)
                } else {
                    Object::from(num.as_f64().unwrap())
                }
            }
            Value::String(item) => Object::from(item.as_str()),
            Value::Array(_) => unimplemented!(),
            Value::Object(_) => unimplemented!(),
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

// impl<T: DynType + Send + Sync + 'static> From<Box<T>> for Object {
//     fn from(v: Box<T>) -> Self {
//         Object::Typed(v as Box<dyn DynType + Send + Sync + 'static>)
//     }
// }
//
// impl From<Box<dyn DynType + Send + Sync + 'static>> for Object {
//     fn from(v: Box<dyn DynType + Send + Sync>) -> Self {
//         Object::Typed(v)
//     }
// }

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::serde_dyn::register_type;

    #[test]
    fn test_primitive_get() {
        let a = Primitives::Integer(8);
        let b: u64 = a.get().unwrap();
        assert_eq!(b, 8);
        let c: u128 = a.get().unwrap();
        assert_eq!(c, 8);
    }

    #[test]
    fn test_owned_or_ref() {
        let a = Object::Primitive(Primitives::Integer(8));
        let left = 0u128;
        let right = a.get().unwrap();
        assert_eq!(left.partial_cmp(&*right), Some(Ordering::Less));
        assert_eq!(right.partial_cmp(&left), Some(Ordering::Greater));
        assert_eq!(*&*right, 8u128);
    }

    #[test]
    fn test_ser_dyn_type() {
        struct MockDynType<T> {
            inner: T,
        }

        impl<T: Clone> Clone for MockDynType<T> {
            fn clone(&self) -> Self {
                MockDynType { inner: self.inner.clone() }
            }
        }

        impl<T: Encode> Encode for MockDynType<T> {
            fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
                self.inner.write_to(writer)?;
                Ok(())
            }
        }

        impl<T: Decode> Decode for MockDynType<T> {
            fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
                let inner = T::read_from(reader)?;
                Ok(MockDynType { inner })
            }
        }

        impl<T: Debug> Debug for MockDynType<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{:?}", self.inner)
            }
        }

        let dyn_ty_obj = MockDynType::<u64> { inner: 1024 };
        let obj = Object::UnknownOwned(Box::new(dyn_ty_obj));
        register_type::<MockDynType<u64>>().expect("register type failed");
        let mut bytes = vec![];
        obj.write_to(&mut bytes).unwrap();

        let mut reader = &bytes[0..];
        let de = Object::read_from(&mut reader).unwrap();
        let dyn_ty_obj_de: OwnedOrRef<MockDynType<u64>> = de.get().unwrap();
        assert_eq!(dyn_ty_obj_de.inner, 1024);
    }
}
