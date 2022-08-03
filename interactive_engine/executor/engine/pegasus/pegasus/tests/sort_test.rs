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
//

#[macro_use]
extern crate lazy_static;

use pegasus::api::{Map, Sink};
use pegasus::JobConf;

lazy_static! {
    pub static ref MAP: std::collections::HashMap<u32, Vec<(u32, f32)>> = vec![
        (1, vec![(2, 0.5f32), (3, 0.4f32), (4, 1.0f32)]),
        (2, vec![]),
        (3, vec![]),
        (4, vec![(3, 0.4f32), (5, 1.0f32)]),
        (5, vec![]),
        (6, vec![(3, 0.2f32)])
    ]
    .into_iter()
    .collect();
}

#[test]
fn modern_graph_sort_by_test() {
    use pegasus::api::SortBy;

    let mut conf = JobConf::new("modern_graph_order_by_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1..7).filter(move |x| *x % num_workers == index))?
                .flat_map(|v| Ok(MAP.get(&v).unwrap().iter().cloned()))?
                .sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap())?
                .map(|x| Ok(x.1))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<f32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1.0, 1.0, 0.5, 0.4, 0.4, 0.2]);
}
