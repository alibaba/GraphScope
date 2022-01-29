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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::ops::Add;

use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::{FnExecError, FnExecResult};

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

#[derive(Clone)]
pub struct ToSet<D: Eq + Hash> {
    pub inner: HashSet<D>,
}

unsafe impl<D: Send + Eq + Hash> Send for ToSet<D> {}

impl<D: Debug + Eq + Hash> Debug for ToSet<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<D: Debug + Eq + Hash + Send + 'static> Accumulator<D, Vec<D>> for ToSet<D> {
    fn accum(&mut self, next: D) -> FnExecResult<()> {
        self.inner.insert(next);
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Vec<D>> {
        let mut result = Vec::with_capacity(self.inner.len());
        let set = std::mem::replace(&mut self.inner, HashSet::new());
        result.extend(set.into_iter());
        Ok(result)
    }
}

impl<D: Encode + Eq + Hash> Encode for ToSet<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32(self.inner.len() as u32)?;
        for data in self.inner.iter() {
            data.write_to(writer)?;
        }
        Ok(())
    }
}

impl<D: Decode + Eq + Hash> Decode for ToSet<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let len = reader.read_u32()?;
        let mut inner = HashSet::with_capacity(len as usize);
        for _ in 0..len {
            let data = <D>::read_from(reader)?;
            inner.insert(data);
        }
        Ok(ToSet { inner })
    }
}

#[derive(Clone)]
pub struct Maximum<D> {
    pub max: Option<D>,
}

impl<D: Debug> Debug for Maximum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "max={:?}", self.max)
    }
}

unsafe impl<D: Send> Send for Maximum<D> {}

impl<D: Debug + Send + PartialOrd + 'static> Accumulator<D, Option<D>> for Maximum<D> {
    fn accum(&mut self, next: D) -> FnExecResult<()> {
        if let Some(pre) = self.max.take() {
            match &pre.partial_cmp(&next) {
                Some(Ordering::Less) => {
                    self.max = Some(next);
                }
                Some(Ordering::Equal) => self.max = Some(pre),
                Some(Ordering::Greater) => self.max = Some(pre),
                None => Err(FnExecError::accum_error("Data cannot be compared in Maximum Accumulator"))?,
            }
        } else {
            self.max = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Option<D>> {
        Ok(self.max.take())
    }
}

impl<D: Encode> Encode for Maximum<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.max.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for Maximum<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let max = <Option<D>>::read_from(reader)?;
        Ok(Maximum { max })
    }
}

#[derive(Clone)]
pub struct Minimum<D> {
    pub min: Option<D>,
}

impl<D: Debug> Debug for Minimum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "min={:?}", self.min)
    }
}

unsafe impl<D: Send> Send for Minimum<D> {}

impl<D: Debug + Send + PartialOrd + 'static> Accumulator<D, Option<D>> for Minimum<D> {
    fn accum(&mut self, next: D) -> FnExecResult<()> {
        if let Some(pre) = self.min.take() {
            match &pre.partial_cmp(&next) {
                Some(Ordering::Less) => {
                    self.min = Some(pre);
                }
                Some(Ordering::Equal) => self.min = Some(pre),
                Some(Ordering::Greater) => self.min = Some(next),
                None => Err(FnExecError::accum_error("Data cannot be compared in Minimum Accumulator"))?,
            }
        } else {
            self.min = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Option<D>> {
        Ok(self.min.take())
    }
}

impl<D: Encode> Encode for Minimum<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.min.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for Minimum<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let min = <Option<D>>::read_from(reader)?;
        Ok(Minimum { min })
    }
}

#[derive(Clone)]
pub struct DataSum<D> {
    pub seed: Option<D>,
}

impl<D: Debug> Debug for DataSum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sum={:?}", self.seed)
    }
}

impl<D: Send + Debug + Add<Output = D> + 'static> Accumulator<D, Option<D>> for DataSum<D> {
    fn accum(&mut self, next: D) -> FnExecResult<()> {
        if let Some(seed) = self.seed.take() {
            self.seed = Some(seed.add(next));
        } else {
            self.seed = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Option<D>> {
        Ok(self.seed.take())
    }
}

impl<D: Encode> Encode for DataSum<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.seed.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for DataSum<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let seed = <Option<D>>::read_from(reader)?;
        Ok(DataSum { seed })
    }
}
