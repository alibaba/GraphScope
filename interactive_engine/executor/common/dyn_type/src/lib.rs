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

#[macro_use]
extern crate lazy_static;
extern crate pegasus_common;

extern crate dyn_clonable;

pub mod arith;
pub mod error;
pub mod object;
#[macro_use]
pub mod macros;
pub mod serde;
pub mod serde_dyn;

use std::any::{Any, TypeId};
use std::fmt::Debug;
use std::io;

use dyn_clonable::*;
pub use error::CastError;
pub use object::{BorrowObject, Object, OwnedOrRef, Primitives};
pub use serde_dyn::{de_dyn_obj, register_type};

#[clonable]
pub trait DynType: Any + Send + Sync + Clone + Debug {
    fn to_bytes(&self) -> io::Result<Vec<u8>>;
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
