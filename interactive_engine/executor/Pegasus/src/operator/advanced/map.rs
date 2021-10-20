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
use crate::operator::unary::Unary;
use crate::communication::Pipeline;
use crate::operator::lazy::unary::LazyUnary;

pub trait Map<I: Data, A> {

    fn map<O: Data, F: Fn(I) -> O + Send + 'static>(&self, func: F) -> Stream<O, A>;

    fn map_in<F: Fn(&mut I) + Send + 'static>(&self, func: F) -> Stream<I, A>;

    fn flat_map<O, T, F>(&self, func: F) -> Stream<O, A>
        where O: Data, T: IntoIterator<Item = O> + Send + 'static, T::IntoIter: Send,
              F: Fn(I) -> Option<T> + Send + Clone + 'static;
}

impl<I: Data, A: DataflowBuilder> Map<I, A> for Stream<I, A> {
    fn map<O: Data, F: Fn(I) -> O + Send + 'static>(&self, func: F) -> Stream<O, A> {
        self.unary("map", Pipeline, |info| {
            info.set_pass();
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let mut session = output.session(&dataset);
                    for datum in dataset.data() {
                        let r = func(datum);
                        session.give(r)?;
                    }
                    Ok(session.has_capacity())
                })?;
                Ok(())
            }
        })
    }

    fn map_in<F: Fn(&mut I) + Send + 'static>(&self, func: F) -> Stream<I, A> {
        self.unary("map_in_place", Pipeline, |info| {
            info.set_pass();
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let mut session = output.session(&dataset);
                    let mut dataset = dataset.data();
                    for datum in dataset.iter_mut() {
                        func(datum)
                    }
                    session.give_batch(dataset)?;
                    Ok(session.has_capacity())
                })?;
                Ok(())
            }
        })
    }

    fn flat_map<O, T, F>(&self, func: F) -> Stream<O, A>
        where O: Data, T: IntoIterator<Item=O> + Send + 'static, T::IntoIter: Send,
              F: Fn(I) -> Option<T> + Send + Clone + 'static
    {
        self.lazy_unary("flat_map", Pipeline, |info| {
            info.set_expand();
            func
        })
    }
}
