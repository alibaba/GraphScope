//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::Arc;

use graph_proxy::apis::graph::element::GraphElement;
use graph_proxy::apis::{Direction, DynDetails, QueryParams, Statement, Vertex, ID};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FilterMapFunction, FnResult};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Entry, Record};

/// An ExpandOrIntersect operator to expand neighbors
/// and intersect with the ones of the same tag found previously (if exists).
/// Notice that start_v_tag (from which tag to expand from)
/// and edge_or_end_v_tag (the alias of expanded neighbors) must be specified.
struct ExpandOrIntersect<E: Into<Entry>> {
    start_v_tag: KeyId,
    edge_or_end_v_tag: KeyId,
    stmt: Box<dyn Statement<ID, E>>,
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub struct Intersection {
    id_record_count_map: BTreeMap<ID, (Vertex, u64)>,
}

impl Intersection {
    pub fn from_iter<I: Iterator<Item = Vertex>>(iter: I) -> Intersection {
        let mut id_record_count_map = BTreeMap::new();
        for vertex in iter {
            let vertex_id = vertex.id();
            id_record_count_map
                .entry(vertex_id)
                .or_insert((vertex, 0))
                .1 += 1;
        }
        Intersection { id_record_count_map }
    }

    pub fn intersect(&mut self, seeker: &BTreeMap<ID, u64>) {
        let mut records_to_remove = Vec::with_capacity(self.id_record_count_map.len());
        for (record_id, (_, count)) in self.id_record_count_map.iter_mut() {
            if let Some(seeker_count) = seeker.get(record_id) {
                *count *= *seeker_count;
            } else {
                records_to_remove.push(*record_id);
            }
        }
        for record_to_remove in records_to_remove {
            self.id_record_count_map
                .remove(&record_to_remove);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.id_record_count_map.is_empty()
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        for (_, &(_, count)) in self.id_record_count_map.iter() {
            len += count;
        }
        len as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vertex> {
        self.id_record_count_map
            .iter()
            .flat_map(move |(_, (record, count))| std::iter::repeat(record).take(*count as usize))
    }
}

impl IntoIterator for Intersection {
    type Item = Vertex;
    type IntoIter = DynIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.id_record_count_map
                .into_iter()
                .flat_map(move |(_, (record, count))| std::iter::repeat(record).take(count as usize)),
        )
    }
}

impl Encode for Intersection {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.id_record_count_map.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Intersection {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let id_record_count_map = <BTreeMap<ID, (Vertex, u64)>>::read_from(reader)?;
        Ok(Intersection { id_record_count_map })
    }
}

impl<E: Into<Entry> + 'static> FilterMapFunction<Record, Record> for ExpandOrIntersect<E> {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(Some(self.start_v_tag))
            .ok_or(FnExecError::get_tag_error(&format!(
                "start_v_tag {:?} in ExpandOrIntersect",
                self.start_v_tag
            )))?;
        if let Some(v) = entry.as_graph_vertex() {
            let id = v.id();
            let iter = self.stmt.exec(id)?.map(|e| match e.into() {
                Entry::V(v) => v,
                Entry::E(e) => {
                    Vertex::new(e.get_other_id(), e.get_other_label().cloned(), DynDetails::default())
                }
                _ => {
                    unreachable!()
                }
            });
            if let Some(pre_entry) = input.get_column_mut(&self.edge_or_end_v_tag) {
                // the case of expansion and intersection
                match pre_entry {
                    Entry::Intersection(pre_intersection) => {
                        let mut seeker = BTreeMap::new();
                        for v in iter {
                            *seeker.entry(v.id()).or_default() += 1;
                        }
                        pre_intersection.intersect(&seeker);
                        if pre_intersection.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(input))
                        }
                    }
                    _ => Err(FnExecError::unexpected_data_error(&format!(
                        "entry {:?} is not a intersection in ExpandOrIntersect",
                        pre_entry
                    )))?,
                }
            } else {
                // the case of expansion only
                let neighbors_intersection = Intersection::from_iter(iter);
                if neighbors_intersection.is_empty() {
                    Ok(None)
                } else {
                    // append columns without changing head
                    let columns = input.get_columns_mut();
                    columns
                        .insert(self.edge_or_end_v_tag as usize, Arc::new(neighbors_intersection.into()));
                    Ok(Some(input))
                }
            }
        } else {
            Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.edge_or_end_v_tag
            )))?
        }
    }
}

impl FilterMapFuncGen for algebra_pb::EdgeExpand {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let graph = graph_proxy::apis::get_graph().ok_or(FnGenError::NullGraphError)?;
        let start_v_tag = self
            .v_tag
            .ok_or(ParsePbError::from("`EdgeExpand::v_tag` cannot be empty for intersection"))?
            .try_into()?;
        let edge_or_end_v_tag = self
            .alias
            .ok_or(ParsePbError::from("`EdgeExpand::alias` cannot be empty for intersection"))?
            .try_into()?;
        let direction_pb: algebra_pb::edge_expand::Direction =
            unsafe { ::std::mem::transmute(self.direction) };
        let direction = Direction::from(direction_pb);
        let query_params: QueryParams = self.params.try_into()?;
        debug!(
            "Runtime expand collection operator of edge with start_v_tag {:?}, edge_tag {:?}, direction {:?}, query_params {:?}",
            start_v_tag, edge_or_end_v_tag, direction, query_params
        );
        if self.expand_opt != algebra_pb::edge_expand::ExpandOpt::Vertex as i32 {
            Err(FnGenError::unsupported_error("expand edges in ExpandIntersection"))
        } else {
            if query_params.filter.is_some() {
                // Expand vertices with filters on edges.
                // This can be regarded as a combination of EdgeExpand (with expand_opt as Edge) + GetV
                let stmt = graph.prepare_explore_edge(direction, &query_params)?;
                let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            } else {
                // Expand vertices without any filters
                let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
                let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use graph_proxy::apis::graph::element::Element;
    use graph_proxy::apis::ID;

    use super::{BTreeMap, Intersection};

    #[test]
    fn intersect_test_01() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter(
            vec![1, 2, 3, 4, 5]
                .into_iter()
                .map(|id| (id, 1)),
        );
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_02() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3, 4, 5]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter(vec![3, 2, 1].into_iter().map(|id| (id, 1)));
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_03() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3, 4, 5]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter(
            vec![9, 7, 5, 3, 1]
                .into_iter()
                .map(|id| (id, 1)),
        );
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 3, 5]
        )
    }

    #[test]
    fn intersect_test_04() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3, 4, 5]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter(vec![9, 8, 7, 6].into_iter().map(|id| (id, 1)));
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            Vec::<ID>::new()
        )
    }

    #[test]
    fn intersect_test_05() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                [(1, 2), (2, 1), (3, 1), (4, 1), (5, 1)].map(|(id, count)| (id, (id.into(), count))),
            ),
        };
        let seeker = BTreeMap::from_iter(vec![1, 2, 3].into_iter().map(|id| (id, 1)));
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_06() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter([(1, 2), (2, 1), (3, 1), (4, 1), (5, 1)]);
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_07() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                [(1, 2), (2, 2), (3, 2), (4, 1), (5, 1)].map(|(id, count)| (id, (id.into(), count))),
            ),
        };
        let seeker = BTreeMap::from_iter(vec![1, 2, 3].into_iter().map(|id| (id, 1)));
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 2, 3, 3]
        )
    }

    #[test]
    fn intersect_test_08() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                vec![1, 2, 3]
                    .into_iter()
                    .map(|id| (id, (id.into(), 1))),
            ),
        };
        let seeker = BTreeMap::from_iter([(1, 2), (2, 2), (3, 2), (4, 1), (5, 1)]);
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 2, 3, 3]
        )
    }

    #[test]
    fn intersect_test_09() {
        let mut intersection = Intersection {
            id_record_count_map: BTreeMap::from_iter(
                [(1, 2), (2, 2), (3, 2)].map(|(id, count)| (id, (id.into(), count))),
            ),
        };
        let seeker = BTreeMap::from_iter([(1, 2), (2, 2), (3, 2), (4, 1), (5, 1)]);
        intersection.intersect(&seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|record| record.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]
        )
    }
}
