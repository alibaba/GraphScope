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
pub use adapters::{
    create_exp_store, create_gs_store, GrootMultiPartition, SimplePartition, VineyardGraphWriter,
    VineyardMultiPartition,
};
pub use errors::{GraphProxyError, GraphProxyResult};
use rand::prelude::ThreadRng;
use rand::Rng;

mod adapters;
pub mod apis;
mod errors;
pub mod utils;

struct Rand {
    rng: ThreadRng,
}

impl Rand {
    pub fn new() -> Self {
        Rand { rng: rand::thread_rng() }
    }

    fn gen_bool(&mut self, p: f64) -> bool {
        self.rng.gen_bool(p)
    }
}

unsafe impl Send for Rand {}

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
            use crate::utils::expr::eval_pred::EvalPred;
            let f = f.clone();
            let r = $iter.filter(move |v| f.eval_bool(Some(v)).unwrap_or(false));
            limit_n!(r, $n)
        } else {
            let r = $iter;
            limit_n!(r, $n)
        }
    };
}

#[macro_export]
macro_rules! sample_limit {
    ($iter: expr, $s: expr, $n: expr) => {
        if let Some(ratio) = $s {
            let mut rng = Rand::new();
            let r = $iter.filter(move |_| rng.gen_bool(ratio));
            limit_n!(r, $n)
        } else {
            let r = $iter;
            limit_n!(r, $n)
        }
    };
}

#[macro_export]
macro_rules! filter_sample_limit {
    ($iter: expr, $f: expr, $s: expr, $n: expr) => {
        if let Some(ref f) = $f {
            use crate::utils::expr::eval_pred::EvalPred;
            let f = f.clone();
            let r = $iter.filter(move |v| f.eval_bool(Some(v)).unwrap_or(false));
            sample_limit!(r, $s, $n)
        } else {
            let r = $iter;
            sample_limit!(r, $s, $n)
        }
    };
}
