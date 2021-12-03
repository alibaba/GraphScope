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

use ahash::AHashMap;

use crate::api::{Binary, HasKey, Join, PartitionByKey};
use crate::communication::output::OutputSession;
use crate::communication::Output;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::TidyTagMap;
use crate::stream::Stream;
use crate::{Data, Tag};

/// A type alias for the maps to process join, which has the join key as the key and
/// a vector of data `T` as value to store all items that share the same key.
type JoinMap<T> = AHashMap<<T as HasKey>::Target, MapEntry<Vec<T>>>;
/// A type alias for managing joins in different scopes
type TaggedMap<T> = TidyTagMap<MapEntry<JoinMap<T>>>;

#[derive(Clone, Copy, Debug)]
pub enum JoinType {
    LeftOuter,
    RightOuter,
    FullOuter,
    Semi,
    Anti,
}

#[derive(Default)]
struct MapEntry<C: Default> {
    /// Items, which is a collection, of the map entries
    data: C,
    /// An indicator to mark whether a condition is satisfied on the entry
    indicator: bool,
}

struct Helper<L: Data + HasKey, R: Data + HasKey> {
    /// A map to maintain the data of the left stream based on the join key
    left_map: TaggedMap<L>,
    /// A map to maintain the data of the right stream based on the join key
    right_map: TaggedMap<R>,
}

impl<L: Data + HasKey, R: Data + HasKey> Default for Helper<L, R> {
    fn default() -> Self {
        Helper { left_map: TidyTagMap::default(), right_map: TidyTagMap::default() }
    }
}

impl<L: Data + HasKey, R: Data + HasKey> Helper<L, R> {
    fn new(scope_level: u32) -> Self {
        Helper { left_map: TidyTagMap::new(scope_level), right_map: TidyTagMap::new(scope_level) }
    }

    fn get_maps_mut(&mut self, tag: &Tag) -> (&mut JoinMap<L>, &mut JoinMap<R>, bool, bool) {
        let left_map = self.left_map.get_mut_or_insert(tag);
        let right_map = self.right_map.get_mut_or_insert(tag);
        (&mut left_map.data, &mut right_map.data, !left_map.indicator, !right_map.indicator)
    }

    /// The indicator here means the join participant in the given scope (by `tag`) has completed.
    /// Thus, we can tell if the join has completed if both left and right participants have completed.
    fn is_end(&mut self, tag: &Tag) -> bool {
        self.left_map.get_mut_or_insert(tag).indicator & self.right_map.get_mut_or_insert(tag).indicator
    }

    /// Mark the left participant of the join in the given scope (by `tag`) as completed
    fn set_left_end(&mut self, tag: &Tag) {
        self.left_map.get_mut_or_insert(tag).indicator = true;
    }

    /// Mark the right participant of the join in the given scope (by `tag`) as completed
    fn set_right_end(&mut self, tag: &Tag) {
        self.right_map.get_mut_or_insert(tag).indicator = true;
    }
}

// insert data into map1, query it in map2, and return the corresponding vector of items matching data in map2
fn insert_and_query<'a, L: Data + HasKey, R: Data + HasKey<Target = L::Target>>(
    map1: &mut JoinMap<L>, map2: &'a mut JoinMap<R>, data: &L, need_insert: bool,
) -> Option<&'a Vec<R>>
where
    L::Target: Clone + Send,
{
    let k = data.get_key();
    let entry1 = map1
        .entry(k.clone())
        .or_insert_with(MapEntry::default);
    if need_insert {
        entry1.data.push(data.clone());
    }
    if let Some(entry2) = map2.get_mut(k) {
        // The indicators mark the given entries of the left/right items as been queried,
        // which can be used to indicate whether to output certain entries in the case of
        // left/right outer join and semi/anti join.
        entry1.indicator = true;
        entry2.indicator = true;
        Some(&entry2.data)
    } else {
        None
    }
}

fn try_outer_join_output<L: Data + HasKey, R: Data + HasKey>(
    helper: &mut Helper<L, R>, mut session: OutputSession<(Option<L>, Option<R>)>, output_left: bool,
    outoutput_right: bool, tag: &Tag,
) -> Result<(), JobExecError> {
    if !helper.is_end(tag) {
        return Ok(());
    }
    if let Some(MapEntry { data: map, indicator: _ }) = helper.left_map.remove(tag) {
        if output_left {
            // The case of left/full outer join, which must output a <L, None> for any left item
            // that has not been matched (`entry.indicator = false`)
            for entry in map.values().filter(|entry| !entry.indicator) {
                for item in &entry.data {
                    session.give((Some(item.clone()), None))?;
                }
            }
        }
    }
    if let Some(MapEntry { data: map, indicator: _ }) = helper.right_map.remove(tag) {
        if outoutput_right {
            // The case of right/full outer join, which must output a <None, R> for any right item
            // that has not been matched (`entry.indicator = false`)
            for entry in map.values().filter(|entry| !entry.indicator) {
                for item in &entry.data {
                    session.give((None, Some(item.clone())))?;
                }
            }
        }
    }
    Ok(())
}

fn try_semi_join_output<L: Data + HasKey, R: Data + HasKey>(
    helper: &mut Helper<L, R>, output: &Output<L>, is_anti: bool, tag: &Tag,
) -> Result<(), JobExecError> {
    if !helper.is_end(tag) {
        return Ok(());
    }
    let mut session = output.new_session(tag)?;
    if let Some(MapEntry { data: map, indicator: _ }) = helper.left_map.remove(tag) {
        for entry in map
            .values()
            // A semi join would output the items that have been matched in the join,
            // while an anti join output those that have not been matched.
            // Here, `entry.indicator ^ is_anti` does the above assertion.
            .filter(|entry| entry.indicator ^ is_anti)
        {
            for item in &entry.data {
                session.give(item.clone())?;
            }
        }
    }
    helper.right_map.remove(tag);
    Ok(())
}

fn internal_inner_join<L: Data + HasKey, R: Data + HasKey<Target = L::Target>>(
    this: Stream<L>, other: Stream<R>,
) -> Result<Stream<(L, R)>, BuildJobError>
where
    L::Target: Clone + Send,
{
    this.partition_by_key()
        .binary("inner_join", other, |info| {
            let mut helper = Helper::<L, R>::new(info.scope_level);
            move |left, right, output| {
                left.for_each_batch(|dataset| {
                    let mut session = output.new_session(&dataset.tag)?;
                    let (mut l_map, mut r_map, _, need_insert) = helper.get_maps_mut(&dataset.tag);
                    for l in dataset.drain() {
                        if let Some(arr) = insert_and_query(&mut l_map, &mut r_map, &l, need_insert) {
                            for r in arr {
                                session.give((l.clone(), r.clone()))?;
                            }
                        }
                    }
                    if dataset.is_last() {
                        helper.set_left_end(&dataset.tag);
                    }
                    Ok(())
                })?;
                right.for_each_batch(|dataset| {
                    let mut session = output.new_session(&dataset.tag)?;
                    let (mut l_map, mut r_map, need_insert, _) = helper.get_maps_mut(&dataset.tag);
                    for r in dataset.drain() {
                        if let Some(arr) = insert_and_query(&mut r_map, &mut l_map, &r, need_insert) {
                            for l in arr {
                                session.give((l.clone(), r.clone()))?;
                            }
                        }
                    }
                    if dataset.is_last() {
                        helper.set_right_end(&dataset.tag);
                    }
                    Ok(())
                })
            }
        })
}

fn internal_outer_join<L: Data + HasKey, R: Data + HasKey<Target = L::Target>>(
    this: Stream<L>, other: Stream<R>, join_type: JoinType,
) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError>
where
    L::Target: Clone + Send,
{
    let (output_left, output_right) = match join_type {
        JoinType::LeftOuter => (true, false),
        JoinType::RightOuter => (false, true),
        JoinType::FullOuter => (true, true),
        _ => return Err(BuildJobError::from("wrong join type".to_string())),
    };
    this.partition_by_key()
        .binary(format!("{:?}", join_type).as_str(), other, |info| {
            let mut helper = Helper::<L, R>::new(info.scope_level);
            move |left, right, output| {
                left.for_each_batch(|dataset| {
                    let mut session = output.new_session(&dataset.tag)?;
                    let (mut l_map, mut r_map, _, need_insert) = helper.get_maps_mut(&dataset.tag);
                    for l in dataset.drain() {
                        if let Some(arr) =
                            insert_and_query(&mut l_map, &mut r_map, &l, output_left || need_insert)
                        {
                            for r in arr {
                                session.give((Some(l.clone()), Some(r.clone())))?;
                            }
                        }
                    }
                    if dataset.is_last() {
                        helper.set_left_end(&dataset.tag);
                        try_outer_join_output(
                            &mut helper,
                            session,
                            output_left,
                            output_right,
                            &dataset.tag,
                        )?;
                    }
                    Ok(())
                })?;
                right.for_each_batch(|dataset| {
                    let mut session = output.new_session(&dataset.tag)?;
                    let (mut l_map, mut r_map, need_insert, _) = helper.get_maps_mut(&dataset.tag);
                    for r in dataset.drain() {
                        if let Some(arr) =
                            insert_and_query(&mut r_map, &mut l_map, &r, output_right || need_insert)
                        {
                            for l in arr {
                                session.give((Some(l.clone()), Some(r.clone())))?;
                            }
                        }
                    }
                    if dataset.is_last() {
                        helper.set_right_end(&dataset.tag);
                        try_outer_join_output(
                            &mut helper,
                            session,
                            output_left,
                            output_right,
                            &dataset.tag,
                        )?;
                    }
                    Ok(())
                })?;
                Ok(())
            }
        })
}

fn internal_semi_join<L: Data + HasKey, R: Data + HasKey<Target = L::Target>>(
    this: Stream<L>, other: Stream<R>, join_type: JoinType,
) -> Result<Stream<L>, BuildJobError>
where
    L::Target: Clone + Send,
{
    let is_anti = match join_type {
        JoinType::Semi => false,
        JoinType::Anti => true,
        _ => return Err(BuildJobError::from("wrong join type".to_string())),
    };
    this.partition_by_key()
        .binary(format!("{:?}", join_type).as_str(), other, |info| {
            let mut helper = Helper::<L, R>::new(info.scope_level);
            move |left, right, output| {
                left.for_each_batch(|dataset| {
                    let (mut l_map, mut r_map, _, _) = helper.get_maps_mut(&dataset.tag);
                    for l in dataset.drain() {
                        insert_and_query(&mut l_map, &mut r_map, &l, true);
                    }
                    if dataset.is_last() {
                        helper.set_left_end(&dataset.tag);
                        try_semi_join_output(&mut helper, output, is_anti, &dataset.tag)?;
                    }
                    Ok(())
                })?;
                right.for_each_batch(|dataset| {
                    let (mut l_map, mut r_map, _, _) = helper.get_maps_mut(&dataset.tag);
                    for r in dataset.drain() {
                        insert_and_query(&mut r_map, &mut l_map, &r, false);
                    }
                    if dataset.is_last() {
                        helper.set_right_end(&dataset.tag);
                        try_semi_join_output(&mut helper, output, is_anti, &dataset.tag)?;
                    }
                    Ok(())
                })?;
                Ok(())
            }
        })
}

impl<L: Data + HasKey, R: Data + HasKey<Target = L::Target>> Join<L, R> for Stream<L> {
    fn inner_join(self, other: Stream<R>) -> Result<Stream<(L, R)>, BuildJobError> {
        internal_inner_join(self, other)
    }

    fn left_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError> {
        internal_outer_join(self, other, JoinType::LeftOuter)
    }

    fn right_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError> {
        internal_outer_join(self, other, JoinType::RightOuter)
    }

    fn full_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError> {
        internal_outer_join(self, other, JoinType::FullOuter)
    }

    fn semi_join(self, other: Stream<R>) -> Result<Stream<L>, BuildJobError> {
        internal_semi_join(self, other, JoinType::Semi)
    }

    fn anti_join(self, other: Stream<R>) -> Result<Stream<L>, BuildJobError> {
        internal_semi_join(self, other, JoinType::Anti)
    }
}
