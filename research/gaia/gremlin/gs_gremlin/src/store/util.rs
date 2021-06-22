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

use gremlin_core::DynError;

pub struct IterList<T, I>
where
    T: Iterator<Item = I>,
{
    iters: Vec<T>,
    curr_iter: Option<T>,
}

unsafe impl<T, I> Send for IterList<T, I> where T: Iterator<Item = I> {}

impl<T, I> IterList<T, I>
where
    T: Iterator<Item = I>,
{
    pub fn new(iters: Vec<T>) -> Self {
        IterList {
            iters,
            curr_iter: None,
        }
    }
}

impl<T, I> Iterator for IterList<T, I>
where
    T: Iterator<Item = I>,
{
    type Item = I;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            if let Some(ref mut iter) = self.curr_iter {
                if let Some(x) = iter.next() {
                    return Some(x);
                } else {
                    if let Some(iter_val) = self.iters.pop() {
                        *iter = iter_val;
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(iter_val) = self.iters.pop() {
                    self.curr_iter = Some(iter_val);
                } else {
                    return None;
                }
            }
        }
    }
}

/// A tricky bypassing of Rust's compiler. It is useful to simplify throwing a `DynError`
/// from a `&str` as `Err(str_to_dyn_err('some str'))`
pub fn str_to_dyn_error(str: &str) -> DynError {
    let err: Box<dyn std::error::Error + Send + Sync> = str.into();
    err
}
