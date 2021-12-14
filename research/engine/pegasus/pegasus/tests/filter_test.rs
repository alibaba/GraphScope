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

use pegasus::api::{Map, Sink, Filter};
use pegasus::JobConf;
use pegasus::result::ResultStream;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref MAP: std::collections::HashMap<u32, (Vec<u32>, Vec<u32>)> = vec![
        (1, (vec![2, 3, 4], vec![])),
        (2, (vec![], vec![1])),
        (3, (vec![], vec![1, 4, 6])),
        (4, (vec![3, 5], vec![1])),
        (5, (vec![], vec![4])),
        (6, (vec![3], vec![]))
    ]
    .into_iter()
    .collect();
}

// g.V(5).in().out().hasId(5).in()
fn modern_graph_filter_flatmap_test() -> ResultStream<u32> {
    let mut conf = JobConf::default();
    let num_workers = 2;
    conf.set_workers(num_workers);
    let result_stream = pegasus::run(conf, || {
        let src = if pegasus::get_current_worker().index == 0 { vec![] } else { vec![5] };
        move |input, output| {
            input
                .input_from(src)?
                .repartition(|x| Ok(*x as u64))
                .flat_map(move |x| Ok(MAP.get(&x).unwrap().1.iter().cloned()))?
                .repartition(|x| Ok(*x as u64))
                .flat_map(move |x| Ok(MAP.get(&x).unwrap().0.iter().cloned()))?
                .repartition(|x| Ok(*x as u64))
                .filter(|x| Ok(*x == 5))?
                .repartition(|x| Ok(*x as u64))
                .flat_map(|x|Ok(MAP.get(&x).unwrap().1.iter().cloned()))?
                .sink_into(output)
        }
    })
        .expect("submit job failure");
    result_stream
}

#[test]
fn modern_graph_filter_test_twice() {
    for _ in 0..2 {
        let expected = vec![4];
        let mut result = vec![];
        let mut result_stream = modern_graph_filter_flatmap_test();
        while let Some(item) = result_stream.next() {
            result.push(item.unwrap());
        }
        assert_eq!(result, expected );
    }
}

#[test]
fn modern_graph_filter_test_five_times() {
    for _ in 0..5 {
        let expected = vec![4];
        let mut result = vec![];
        let mut result_stream = modern_graph_filter_flatmap_test();
        while let Some(item) = result_stream.next() {
            result.push(item.unwrap());
        }
        assert_eq!(result, expected );
    }
}
