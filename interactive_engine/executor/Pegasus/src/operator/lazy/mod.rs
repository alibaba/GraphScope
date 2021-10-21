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

use super::*;
use std::vec::IntoIter;

pub trait Transform<I, O> : Send  + Clone {
    fn exec(&self, input: I) -> Option<Box<dyn Iterator<Item = O> + Send>>;
}

impl<I, O, T: ?Sized + Transform<I, O> + Sync> Transform<I, O> for Arc<T> {
    #[inline]
    fn exec(&self, input: I) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (**self).exec(input)
    }
}

impl<I, O, F, T> Transform<I, O> for F
    where T: IntoIterator<Item = O> + Send + 'static, T::IntoIter: Send,
          F: Fn(I) -> Option<T> + Send + Clone
{
    #[inline]
    fn exec(&self, input: I) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (*self)(input).map(|it| Box::new(it.into_iter()) as Box<dyn Iterator<Item = O> + Send>)
    }
}

/// Transformer transform each input record into an iterator of output records;
pub struct Transformer<I, O, T> {
    source: IntoIter<I>,
    target: Option<Box<dyn Iterator<Item = O> + Send>>,
    func: T,
    next: Option<O>,
}

impl<I, O, T> Transformer<I, O, T> where I: Data, O: Data, T: Transform<I, O> + 'static
{
    pub fn new(input: Vec<I>, func: T) -> Self {
        Transformer {
            source: input.into_iter(),
            target: None,
            func,
            next: None,
        }
    }

    pub fn has_next(&mut self) -> bool {
        if self.next.is_none() {
            self.next = self.pull();
        }
        self.next.is_some()
    }

    #[inline]
    pub fn next(&mut self) -> Option<O> {
        self.next.take()
    }

    fn pull(&mut self) -> Option<O> {
        loop {
            if let Some(ref mut target) = self.target {
                let output = target.next();
                if output.is_some() {
                    return output;
                }
            }

            if !self.transform() {
                self.target = None;
                return None;
            }
        }
    }

    #[inline]
    fn transform(&mut self) -> bool {
        while let Some(input) = self.source.next() {
            if let Some(outputs) = self.func.exec(input) {
                self.target.replace(outputs);
                return true;
            }
        }

        false
    }
}

impl<I, O, T> Iterator for Transformer<I, O, T> where I: Data, O: Data, T: Transform<I, O> + 'static {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        if self.has_next() {
            self.next()
        } else {
            None
        }
    }
}

pub trait STransform<I, O, S> : Send {
    fn exec(&self, i: I, s: &mut S) -> Option<Box<dyn Iterator<Item = O> + Send>>;
}

impl<I, O, S, T: ?Sized + STransform<I, O, S> + Sync> STransform<I, O, S> for Arc<T> {
    #[inline]
    fn exec(&self, i: I, s: &mut S) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (**self).exec(i, s)
    }
}

impl<I, O, S, T, F> STransform<I, O, S> for F
    where T: IntoIterator<Item = O> + Send + 'static, T::IntoIter: Send,
          F: Fn(I, &mut S) -> Option<T> + Send + 'static
{
    #[inline]
    fn exec(&self, i: I, s: &mut S) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (*self)(i, s).map(|it| Box::new(it.into_iter()) as Box<dyn Iterator<Item = O> + Send>)
    }
}

pub struct StateTransformer<I, O, S> {
    source: IntoIter<I>,
    state: S,
    target: Option<Box<dyn Iterator<Item = O> + Send>>,
    func: Box<dyn STransform<I, O, S>>,
    next: Option<O>,
}

impl<I, O, S> StateTransformer<I, O, S>
    where I: Data, O: Data, S: OperatorState,
{
    pub fn new<F: STransform<I, O, S> + 'static>(input: Vec<I>, state: S, func: F) -> Self {
       StateTransformer {
           source: input.into_iter(),
           state,
           target: None,
           func: Box::new(func),
           next: None
       }
    }

    pub fn has_next(&mut self) -> bool {
        if self.next.is_none() {
            self.next = self.pull();
        }
        self.next.is_some()
    }

    #[inline]
    pub fn next(&mut self) -> Option<O> {
        self.next.take()
    }

    #[inline]
    pub fn return_state(self) -> S {
        self.state
    }

    fn pull(&mut self) -> Option<O> {
        loop {
            if let Some(ref mut target) = self.target {
                let output = target.next();
                if output.is_some() {
                    return output;
                }
            }

            if !self.transform() {
                self.target = None;
                return None;
            }

        }
    }

    #[inline]
    fn transform(&mut self) -> bool {
        let state = &mut self.state;
        while let Some(input) = self.source.next() {
            if let Some(outputs) = self.func.exec(input, state) {
                self.target.replace(outputs);
                return true;
            }
        }

        false
    }
}

pub trait Notify<S: OperatorState, D: Data> : Send {
    fn on_notify(&self, state: S) -> Box<dyn Iterator<Item = D> + Send>;
}

impl<S, D, T, F> Notify<S, D> for F
    where S: OperatorState, D: Data,
          T: IntoIterator<Item = D> + Send + 'static, T::IntoIter: Send,
          F: Fn(S) -> T + Send + 'static
{
    #[inline(always)]
    fn on_notify(&self, state: S) -> Box<dyn Iterator<Item = D> + Send> {
        let it = (*self)(state).into_iter();
        Box::new(it) as Box<dyn Iterator<Item = D> + Send>
    }
}

pub mod unary;
pub mod binary;

#[cfg(test)]
mod test {
    use super::*;

    fn function(item: u32) -> Option<Box<dyn Iterator<Item = u32> + Send>> {
        let result = vec![item;64];
        let iter = result.into_iter();
        Some(Box::new(iter))
    }

    #[test]
    fn test_transformer() {
        let mut trans = Transformer::new(vec![1u32,2,3], function);
        assert!(trans.has_next());
        let mut count = 0;
        while trans.has_next() {
            let r = trans.next().expect("has next error");
            assert!(r <= 3);
            count += 1;
        }

        assert_eq!(count, 192);
    }

    #[test]
    fn test_state_transformer() {
        let func = |item, sum: &mut u32| {
            *sum += item;
            function(item)
        };

        let mut trans = StateTransformer::new(vec![1u32, 2, 3],0u32,func);
        assert!(trans.has_next());
        let mut count = 0;
        while trans.has_next() {
            let r = trans.next().expect("has next error");
            assert!(r <= 3);
            count += 1;
        }
        assert_eq!(count, 192);
        let sum = trans.return_state();
        assert_eq!(sum, 6);
    }
}
