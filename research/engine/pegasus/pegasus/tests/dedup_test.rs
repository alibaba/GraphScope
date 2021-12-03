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

use pegasus::api::{Dedup, Map, Sink};
use pegasus::JobConf;

#[test]
fn map_dedup_flatmap_test() {
    let mut conf = JobConf::new("map_dedup_flatmap_test");
    conf.set_workers(2);
    let mut res = pegasus::run(conf, || {
        move |source, sink| {
            source
                .input_from(0..10_000u64)?
                .map(|x| Ok(x + 1))?
                .dedup()?
                .repartition(|x| Ok(*x))
                .flat_map(|x| Ok(std::iter::repeat(x).take(2)))?
                .sink_into(sink)
        }
    })
    .expect("submit job failure");

    let mut count = 0;
    while let Some(Ok(_)) = res.next() {
        count += 1;
    }
    assert_eq!(10_000 * 2, count);
}
