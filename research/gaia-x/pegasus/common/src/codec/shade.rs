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

use std::error::Error;
use std::fmt::{Debug, Display};

use crate::codec::{Decode, Encode};
use crate::io::{ReadExt, WriteExt};

pub trait ShadeCodec {}

pub struct UnimplError<T> {
    _ph: std::marker::PhantomData<T>,
}

impl<T> Debug for UnimplError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "encode or decode is not implement of {}", std::any::type_name::<T>())
    }
}

impl<T> Display for UnimplError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "encode or decode is not implement of {}", std::any::type_name::<T>())
    }
}

impl<T> Error for UnimplError<T> {}

unsafe impl<T> Send for UnimplError<T> {}
unsafe impl<T> Sync for UnimplError<T> {}

// impl<T> Into<Box<dyn Error + Send + Sync>> for UnimplError<T> {
//     fn into(self) -> Box<dyn Error + Send + Sync> {
//         Box::new(self)
//     }
// }

impl<T> UnimplError<T> {
    pub fn new() -> Self {
        UnimplError { _ph: std::marker::PhantomData }
    }
}

impl<T: ShadeCodec + 'static> Encode for T {
    fn write_to<W: WriteExt>(&self, _: &mut W) -> std::io::Result<()> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, UnimplError::<T>::new()))
    }
}

impl<T: ShadeCodec + Sized + 'static> Decode for T {
    fn read_from<R: ReadExt>(_: &mut R) -> std::io::Result<Self> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, UnimplError::<T>::new()))
    }
}
