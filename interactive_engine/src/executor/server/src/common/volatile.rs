//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#[derive(Copy, Clone)]
pub struct Volatile<T> {
    inner: T,
}

impl<T: Copy> Volatile<T> {
    pub fn new(t: T) -> Self {
        Volatile {
            inner: t,
        }
    }

    #[inline]
    pub fn read(&self) -> T {
        unsafe {
            ::std::ptr::read_volatile(&self.inner as *const T)
        }
    }

    #[inline]
    pub fn write(&self, t: T) {
        unsafe {
            ::std::ptr::write_volatile(&self.inner as *const T as *mut T, t);
        }
    }
}
