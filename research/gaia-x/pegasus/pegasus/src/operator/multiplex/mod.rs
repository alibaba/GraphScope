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

use crate::api::{Tumbling, Unary};
use crate::progress::{EndSignal, Weight};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data, Tag};

impl<D: Data> Tumbling<D> for Stream<D> {
    fn tumble_by(self, length: usize) -> Result<Stream<D>, BuildJobError> {
        assert!(length > 0, "invalid length to split scope;");
        self.unary("tumbling", |info| {
            assert!(info.scope_level > 0);
            let id = crate::worker_id::get_current_worker();
            let offset = id.total_peers();
            let index = id.index + offset;
            let mut tumbling_scope = TidyTagMap::new(info.scope_level - 1);
            let mut count = 0;
            move |input, output| {
                input.for_each_batch(|dataset| {
                    assert_eq!(dataset.tag.current(), Some(0));
                    let tag = dataset.tag.to_parent_uncheck();
                    let cnt = tumbling_scope.get_mut_or_else(&tag, || (0usize, index));
                    let mut enter_tag = Tag::inherit(&tag, cnt.1);
                    for item in dataset.drain() {
                        cnt.0 += 1;
                        if cnt.0 == length {
                            cnt.0 = 0;
                            let end = EndSignal::new(enter_tag.clone(), Weight::single());
                            cnt.1 += offset;
                            enter_tag = enter_tag.advance_to(cnt.1);
                            output.push_last(item, end)?;
                            count += 1;
                        } else {
                            output.push(&enter_tag, item)?;
                        }
                    }
                    if dataset.is_last() {
                        debug_worker!("create {} scopes;", count);
                    }
                    Ok(())
                })
            }
        })
    }
}

mod subtask;
