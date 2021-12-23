//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::fmt::Debug;
use std::io;

use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::FnExecResult;

pub trait Accumulator<I, O>: Send + Debug {
    fn accum(&mut self, next: I) -> FnExecResult<()>;

    fn finalize(&mut self) -> FnExecResult<O>;
}

impl<I, O, A: Accumulator<I, O> + ?Sized> Accumulator<I, O> for Box<A> {
    fn accum(&mut self, next: I) -> FnExecResult<()> {
        (**self).accum(next)
    }

    fn finalize(&mut self) -> FnExecResult<O> {
        (**self).finalize()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Count<D> {
    pub value: u64,
    pub _ph: std::marker::PhantomData<D>,
}

impl<D> Debug for Count<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "count={}", self.value)
    }
}

impl<D: Send + 'static> Accumulator<D, u64> for Count<D> {
    fn accum(&mut self, _next: D) -> FnExecResult<()> {
        self.value += 1;
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<u64> {
        let value = self.value;
        self.value = 0;
        Ok(value)
    }
}

impl<D> Encode for Count<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.value.write_to(writer)?;
        Ok(())
    }
}

impl<D> Decode for Count<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let value = u64::read_from(reader)?;
        Ok(Count { value, _ph: std::marker::PhantomData })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ToList<D> {
    pub inner: Vec<D>,
}

impl<D: Debug> Debug for ToList<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<D: Debug + Send + 'static> Accumulator<D, Vec<D>> for ToList<D> {
    fn accum(&mut self, next: D) -> FnExecResult<()> {
        self.inner.push(next);
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Vec<D>> {
        Ok(std::mem::replace(&mut self.inner, vec![]))
    }
}

impl<D: Encode> Encode for ToList<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.inner.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for ToList<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let inner = <Vec<D>>::read_from(reader)?;
        Ok(ToList { inner })
    }
}
