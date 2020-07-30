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

pub mod lazy_unary_notify;

use std::sync::Arc;

use pegasus::tag::Tag;


pub trait UTransform<I, O> : Send {
    fn exec(&mut self, input: I, notify: Option<Tag>) -> Option<Box<dyn Iterator<Item = O> + Send>>;
}

impl<I, O, F, T> UTransform<I, O> for F
    where T: IntoIterator<Item = O> + Send + 'static, T::IntoIter: Send,
          F: FnMut(I, Option<Tag>) -> Option<T> + Send
{
    #[inline]
    fn exec(&mut self, input: I, notify: Option<Tag>) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (*self)(input, notify).map(|it| Box::new(it.into_iter()) as Box<dyn Iterator<Item = O> + Send>)
    }
}

