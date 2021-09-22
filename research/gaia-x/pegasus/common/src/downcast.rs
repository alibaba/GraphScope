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

pub use std::any::Any;

/// Util for downcast abstraction;
pub trait AsAny: 'static {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn as_any_ref(&self) -> &dyn Any;
}

impl<T: ?Sized + AsAny> AsAny for Box<T> {
    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        (**self).as_any_mut()
    }

    #[inline]
    fn as_any_ref(&self) -> &dyn Any {
        (**self).as_any_ref()
    }
}

impl<T: Sized + AsAny> AsAny for Vec<T> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

#[macro_export]
macro_rules! impl_as_any {
    // this evil monstrosity matches <A, B: T, C: S+T>
    ($ty:ident < $( $N:ident $(: $b0:ident $(+$b:ident)* )? ),* >) =>
    {
        impl< $( $N $(: $b0 $(+$b)* )? ),* >
            AsAny
            for $ty< $( $N ),* >
        {
            fn as_any_ref(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }
    };
    // match when no type parameters are present
    ($ty:ident) => {
        impl_as_any!($ty<>);
    };
}

impl_as_any!(bool);
impl_as_any!(u8);
impl_as_any!(i8);
impl_as_any!(u16);
impl_as_any!(i16);
impl_as_any!(u32);
impl_as_any!(i32);
impl_as_any!(u64);
impl_as_any!(i64);
impl_as_any!(u128);
impl_as_any!(i128);
