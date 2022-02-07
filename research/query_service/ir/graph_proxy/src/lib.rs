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

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
pub use exp_store::{create_demo_graph, QueryExpGraph, SimplePartition};
pub use gs_store::{QueryMaxGraph, QueryVineyard};
use pegasus::api::function::{DynIter, FnResult};
use runtime::graph::Statement;
use runtime::IRJobCompiler;

mod exp_store;
mod gs_store;

pub trait InitializeJobCompiler {
    fn initialize_job_compiler(&self) -> IRJobCompiler;
}

pub fn from_fn<I, O, F>(func: F) -> Box<dyn Statement<I, O>>
where
    F: Fn(I) -> FnResult<DynIter<O>> + Send + Sync + 'static,
{
    Box::new(func) as Box<dyn Statement<I, O>>
}

#[macro_export]
macro_rules! limit_n {
    ($iter: expr, $n: expr) => {
        if let Some(limit) = $n {
            let r = $iter.take(limit);
            Box::new(r)
        } else {
            Box::new($iter)
        }
    };
}

#[macro_export]
macro_rules! filter_limit {
    ($iter: expr, $f: expr, $n: expr) => {
        if let Some(ref f) = $f {
            use runtime::expr::eval_pred::EvalPred;
            let f = f.clone();
            let r = $iter.filter(move |v| f.eval_bool(Some(v)).unwrap_or(false));
            limit_n!(r, $n)
        } else {
            let r = $iter;
            limit_n!(r, $n)
        }
    };
}
