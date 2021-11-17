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

use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::ops::Add;

#[derive(Debug)]
pub struct AccumError {
    desc: String,
}

impl std::fmt::Display for AccumError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Get key failed: {}", self.desc)
    }
}

impl std::error::Error for AccumError {}

impl From<String> for AccumError {
    fn from(desc: String) -> Self {
        AccumError { desc }
    }
}

impl From<&str> for AccumError {
    fn from(desc: &str) -> Self {
        desc.to_string().into()
    }
}

pub trait Accumulator<I, O>: Send + Debug {
    fn accum(&mut self, next: I) -> Result<(), AccumError>;

    fn finalize(&mut self) -> O;
}

impl<I, O, A: Accumulator<I, O> + ?Sized> Accumulator<I, O> for Box<A> {
    fn accum(&mut self, next: I) -> Result<(), AccumError> {
        (**self).accum(next)
    }

    fn finalize(&mut self) -> O {
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
    fn accum(&mut self, _next: D) -> Result<(), AccumError> {
        self.value += 1;
        Ok(())
    }

    fn finalize(&mut self) -> u64 {
        let value = self.value;
        self.value = 0;
        value
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
        Ok(Count {
            value,
            _ph: std::marker::PhantomData,
        })
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
    fn accum(&mut self, next: D) -> Result<(), AccumError> {
        self.inner.push(next);
        Ok(())
    }

    fn finalize(&mut self) -> Vec<D> {
        std::mem::replace(&mut self.inner, vec![])
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

#[derive(Debug)]
pub struct ToSet<D: Eq + Hash + Debug> {
    pub inner: HashSet<D>,
}

impl<D: Eq + Hash + Debug + Send + 'static> Accumulator<D, Vec<D>> for ToSet<D> {
    fn accum(&mut self, next: D) -> Result<(), AccumError> {
        self.inner.insert(next);
        Ok(())
    }

    fn finalize(&mut self) -> Vec<D> {
        let mut result = Vec::with_capacity(self.inner.len());
        let set = std::mem::replace(&mut self.inner, HashSet::new());
        result.extend(set.into_iter());
        result
    }
}

pub struct Maximum<D> {
    pub max: Option<D>,
}

impl<D: Debug> Debug for Maximum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "max={:?}", self.max)
    }
}

unsafe impl<D: Send> Send for Maximum<D> {}

impl<D: Debug + Send + Ord + 'static> Accumulator<D, Option<D>> for Maximum<D> {
    fn accum(&mut self, next: D) -> Result<(), AccumError> {
        if let Some(pre) = self.max.take() {
            match &pre.cmp(&next) {
                Ordering::Less => {
                    self.max = Some(next);
                }
                Ordering::Equal => self.max = Some(pre),
                Ordering::Greater => self.max = Some(pre),
            }
        } else {
            self.max = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.max.take()
    }
}

pub struct Minimum<D> {
    pub min: Option<D>,
}

impl<D: Debug> Debug for Minimum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "min={:?}", self.min)
    }
}

unsafe impl<D: Send> Send for Minimum<D> {}

impl<D: Debug + Send + Ord + 'static> Accumulator<D, Option<D>> for Minimum<D> {
    fn accum(&mut self, next: D) -> Result<(), AccumError> {
        if let Some(pre) = self.min.take() {
            match &pre.cmp(&next) {
                Ordering::Less => {
                    self.min = Some(pre);
                }
                Ordering::Equal => self.min = Some(pre),
                Ordering::Greater => self.min = Some(next),
            }
        } else {
            self.min = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.min.take()
    }
}

pub struct DataSum<D> {
    pub seed: Option<D>,
}

impl<D: Debug> Debug for DataSum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sum={:?}", self.seed)
    }
}

impl<D: Send + Debug + Add<Output = D> + 'static> Accumulator<D, Option<D>> for DataSum<D> {
    fn accum(&mut self, next: D) -> Result<(), AccumError> {
        if let Some(seed) = self.seed.take() {
            self.seed = Some(seed.add(next));
        } else {
            self.seed = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.seed.take()
    }
}
