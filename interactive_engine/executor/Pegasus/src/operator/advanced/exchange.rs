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
use crate::communication::{Exchange as ExchangeQ, Aggregate};
use crate::operator::unary::Unary;

pub trait Exchange<D: Data, A> {
    fn exchange<F>(&self, route: F) -> Stream<D, A> where F: FnMut(&D) -> u64 + Send + 'static;

    fn aggregate_to(&self, target: usize) -> Stream<D, A>;
}

impl<D: Data, A: DataflowBuilder> Exchange<D, A> for Stream<D, A> {
    fn exchange<F>(&self, route: F) -> Stream<D, A> where F: FnMut(&D) -> u64 + Send + 'static {
        let ex = ExchangeQ::new(route);
        self.unary("exchange", ex, |info| {
            info.set_pass();
            |input, output| {
                input.for_each_batch(|dataset| {
                    let mut session = output.session(&dataset);
                    session.give_batch(dataset.data())?;
                    Ok(true)
                })?;
                Ok(())
            }
        })
    }

    fn aggregate_to(&self, target: usize) -> Stream<D, A> {
        self.unary(&format!("aggregate-{}", target), Aggregate::new(target), |info| {
            info.set_pass();
            |input, output| {
                input.for_each_batch(|dataset| {
                    output.session(&dataset).give_batch(dataset.data())?;
                    Ok(true)
                })?;
                Ok(())
            }
        })
    }
}
