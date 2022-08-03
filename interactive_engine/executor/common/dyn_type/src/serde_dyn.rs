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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::sync::RwLock;

use pegasus_common::codec::{Decode, Encode};

use crate::DynType;

lazy_static! {
    static ref TYPE_TABLE: RwLock<HashMap<TypeId, Box<dyn Ph>>> = RwLock::new(HashMap::new());
}

/// The register_type fn is used to register types used in DynType for serializing and deserializing;
///
/// # Examples
/// ```
///
/// use dyn_type::object::Object;
/// use dyn_type::{register_type, OwnedOrRef};
/// use pegasus_common::codec::{Encode, Decode};
///
/// let dyn_ty_obj = vec![0_u64, 1, 2, 3];
/// let obj = Object::DynOwned(Box::new(dyn_ty_obj));
/// register_type::<Vec<u64>>().expect("register type failed");
/// let mut bytes = vec![];
/// obj.write_to(&mut bytes).unwrap();
///
/// let mut reader = &bytes[0..];
/// let de = Object::read_from(&mut reader).unwrap();
/// let dyn_ty_obj_de: OwnedOrRef<Vec<u64>> = de.get().unwrap();
/// assert_eq!(dyn_ty_obj_de.as_slice(), &[0_u64, 1, 2, 3]);
/// ```
///
pub fn register_type<T: 'static + Decode + DynType>() -> io::Result<()> {
    let ty_id = TypeId::of::<T>();
    let ph: PhImpl<T> = PhImpl { _ph: std::marker::PhantomData };
    if let Ok(mut table) = TYPE_TABLE.write() {
        table.insert(ty_id, Box::new(ph));
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
    }
}

pub fn de_dyn_obj(ty_id: &TypeId, bytes: &[u8]) -> io::Result<Box<dyn DynType>> {
    if let Ok(table) = TYPE_TABLE.read() {
        if let Some(ph_impl) = table.get(ty_id) {
            let res = ph_impl.from_bytes(bytes);
            res
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "type not exists"))
        }
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "lock poisoned"))
    }
}

pub trait Ph: Send + Sync {
    fn from_bytes(&self, bytes: &[u8]) -> io::Result<Box<dyn DynType>>;
}

pub struct PhImpl<T: Decode + DynType> {
    _ph: std::marker::PhantomData<T>,
}

impl<T: Decode + DynType> Ph for PhImpl<T> {
    fn from_bytes(&self, bytes: &[u8]) -> io::Result<Box<dyn DynType>> {
        let mut obj_bytes = &bytes[0..];
        let obj = T::read_from(&mut obj_bytes)?;
        Ok(Box::new(obj))
    }
}

impl<T: Any + Send + Sync + Clone + Debug + Encode> DynType for T {
    fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut bytes = vec![];
        let t = TypeId::of::<T>();
        let number: u64 = unsafe { std::mem::transmute(t) };
        number.write_to(&mut bytes)?;
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod test {
    use pegasus_common::codec::{ReadExt, WriteExt};

    use super::*;
    use crate::{Object, OwnedOrRef};

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
        let obj = Object::DynOwned(Box::new(dyn_ty_obj));
        register_type::<MockDynType<u64>>().expect("register type failed");
        let mut bytes = vec![];
        obj.write_to(&mut bytes).unwrap();

        let mut reader = &bytes[0..];
        let de = Object::read_from(&mut reader).unwrap();
        let dyn_ty_obj_de: OwnedOrRef<MockDynType<u64>> = de.get().unwrap();
        assert_eq!(dyn_ty_obj_de.inner, 1024);
    }
}
