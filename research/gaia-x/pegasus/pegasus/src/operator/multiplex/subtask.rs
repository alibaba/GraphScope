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

use crate::api::{Binary, CorrelatedSubTask, Tumbling};
use crate::stream::{SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data, Tag};

impl<D: Data> CorrelatedSubTask<D> for Stream<D> {
    fn apply<T, F>(self, func: F) -> Result<Stream<(D, T)>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<SingleItem<T>, BuildJobError>,
    {
        let entered = self.enter()?;
        let (main, to_sub) = entered.copied()?;
        let sub = to_sub.tumble_by(1)?;
        let SingleItem { inner } = func(sub)?;
        main.binary("zip_subtask", inner, |info| {
            let mut parent_data = TidyTagMap::new(info.scope_level - 1);
            let peers = crate::worker_id::get_current_worker().total_peers();
            move |input_left, input_right, output| {
                input_left.for_each_batch(|dataset| {
                    let p_tag = dataset.tag.to_parent_uncheck();
                    let barrier = parent_data.get_mut_or_else(&p_tag, || vec![]);
                    for item in dataset.drain() {
                        barrier.push(Some(item));
                    }
                    dataset.take_end();
                    Ok(())
                })?;

                input_right.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let seq = dataset.tag.current_uncheck();
                        if seq > 0 {
                            let p_tag = dataset.tag.to_parent_uncheck();
                            if let Some(parent) = parent_data.get_mut(&p_tag) {
                                trace_worker!("join result of subtask {:?}", seq);
                                let offset = (seq / peers) as usize - 1;
                                let tag = Tag::inherit(&p_tag, 0);
                                let mut session = output.new_session(&tag)?;
                                assert_eq!(dataset.len(), 1);
                                for item in dataset.drain() {
                                    if let Some(p) = parent[offset].take() {
                                        session.give((p, item.0))?;
                                    }
                                }
                                if log_enabled!(log::Level::Trace) {
                                    // assert!(dataset.is_last());
                                    trace_worker!("all results of subtask {:?} joined;", dataset.tag);
                                }
                            } else {
                                warn_worker!("parent not found;")
                            }
                        } else {
                            // seq = 0 is not a subtask; it should be empty;
                            // but it is not empty now, may be because of some aggregation operations;
                            dataset.clear();
                        }
                    }
                    dataset.take_end();
                    Ok(())
                })
            }
        })?
        .leave()
    }
}
