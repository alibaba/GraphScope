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
use crossbeam_channel::Sender;
use crate::operator::unary::Unary;
use crate::communication::Pipeline;

pub trait Output<D: Data, A> {
    fn sink<F>(&self, func: F) where F: Fn(u32, Vec<D>) + Send + Sync + 'static;

    fn sink_to<S: Sink<D> + Send + 'static>(&self, sink: S);
}

pub trait Sink<T: Data> {
    fn sink(&self, tag: u32, data: Vec<T>);
}

impl<T: Data> Sink<T> for Sender<(u32, Vec<T>)> {
    fn sink(&self, tag: u32, data: Vec<T>) {
        self.send((tag, data)).unwrap()
    }
}

impl<D: Data, A: DataflowBuilder> Output<D, A> for Stream<D, A> {

    fn sink<F>(&self, func: F) where F: Fn(u32, Vec<D>) + Send + Sync + 'static {
        self.unary("sink", Pipeline, |info| {
            info.set_sink();
            move |input, _out: &mut OutputHandle<()>| {
                input.for_each_batch(|data_set| {
                    let (t, d) = data_set.take();
                    let tag = t.current();
                    func(tag, d);
                    Ok(true)
                })?;
                Ok(())
            }
        });
    }

    fn sink_to<S: Sink<D> + Send + 'static>(&self, sink: S) {
        self.unary("sink", Pipeline, |info| {
            info.set_sink();
            move |input, _output: &mut OutputHandle<()>| {
                input.for_each_batch(|data_set| {
                    let (t, d) = data_set.take();
                    let tag = t.current();
                    sink.sink(tag, d);
                    Ok(true)
                })?;
                Ok(())
            }
        });
    }
}
