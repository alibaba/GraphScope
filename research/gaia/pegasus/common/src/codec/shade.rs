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

use crate::codec::{Decode, Encode};
use crate::io::{ReadExt, WriteExt};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

pub struct ShadeCodec<T> {
    shaded: T,
}

impl<T> ShadeCodec<T> {
    #[inline]
    pub fn take(self) -> T {
        self.shaded
    }
}

impl<T> Encode for ShadeCodec<T> {
    fn write_to<W: WriteExt>(&self, _: &mut W) -> std::io::Result<()> {
        unreachable!("codec is shaded;");
    }
}

impl<T> Decode for ShadeCodec<T> {
    fn read_from<R: ReadExt>(_: &mut R) -> std::io::Result<Self> {
        unreachable!("codec is shaded")
    }
}

#[inline]
pub fn shade_codec<T>(entry: T) -> ShadeCodec<T> {
    ShadeCodec { shaded: entry }
}

impl<T: Clone> Clone for ShadeCodec<T> {
    fn clone(&self) -> Self {
        ShadeCodec { shaded: self.shaded.clone() }
    }
}

impl<T: Debug> Debug for ShadeCodec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.shaded.fmt(f)
    }
}

impl<T> Deref for ShadeCodec<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.shaded
    }
}

impl<T> DerefMut for ShadeCodec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shaded
    }
}
