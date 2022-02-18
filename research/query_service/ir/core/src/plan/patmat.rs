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
//!

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::rc::Rc;

use ir_common::error::ParsePbError;
use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;

use crate::error::{IrError, IrResult};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd)]
#[repr(i32)]
pub enum BindingOpt {
    Vertex = 0,
    Edge = 1,
    Path = 2,
}

/// Define the behavior of concatenating two matching sentences
pub trait Sentence: Debug {
    /// Composite works by extending the first sentence with the second one via
    /// the **only** common tag. Composition cannot be apply when either sentence
    /// has anti-semantics.
    fn composite(&mut self, other: &BaseSentence) -> bool;
    /// Join works by joining two sentences via the common (at least one) tags
    /// and produce a `JoinSentence`
    fn join(&self, other: &JoinSentence) -> Option<JoinSentence>;
    /// Transform a `sentence` into a logical plan
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan>;
    /// Get base sentence if any
    fn get_base(&self) -> Option<&BaseSentence>;
}

/// An internal representation of `pb::Sentence`
#[derive(Clone, Debug, PartialEq)]
pub struct BaseSentence {
    // The start tag of this sentence
    start_tag: NameOrId,
    // The end tag, if any, of this sentence
    end_tag: Option<NameOrId>,
    /// The tags bound to this sentence
    tags: BTreeSet<NameOrId>,
    /// Use `pb::logical_plan::Operator` rather than `pb::patmat::binder`,
    /// to facilitate building the logical plan that may translate a tag into an `As` operator.
    operators: Vec<pb::logical_plan::Operator>,
    /// Is this a sentence with Anti(No)-semanatics
    is_anti: bool,
    /// What kind of entities this sentence binds to
    end_as: BindingOpt,
    /// The number of filters contained by the sentence
    num_filters: usize,
    /// The number of hops of edge expansions of the sentence
    num_hops: usize,
}

#[derive(Clone, Debug)]
pub struct JoinSentence {
    /// The left sentence
    left: Rc<dyn Sentence>,
    /// The right sentence
    right: Option<Rc<dyn Sentence>>,
    /// The common tags for join
    common_tags: BTreeSet<NameOrId>,
    /// The join kind, either `Inner`, or `Anti`
    join_kind: pb::join::JoinKind,
}

impl From<BaseSentence> for JoinSentence {
    fn from(base: BaseSentence) -> Self {
        let is_anti = base.is_anti;
        let tags = base.tags.clone();
        Self {
            left: Rc::new(base),
            right: None,
            common_tags: tags,
            join_kind: if is_anti {
                // use the join_kind to represent whether it is an anti-sentence
                pb::join::JoinKind::Anti
            } else {
                pb::join::JoinKind::Inner
            },
        }
    }
}

fn detect_filters(params_opt: Option<&pb::QueryParams>) -> usize {
    if let Some(params) = params_opt {
        let mut count = 0;
        // Simply count whether there is any predicate, without actually looking into
        // the number of filter clauses
        if params.predicate.is_some() {
            count += 1;
        }
        // Simply count whether there is any column (for filtering)
        if !params.columns.is_empty() {
            count += 1;
        }
        count
    } else {
        0
    }
}

impl TryFrom<pb::pattern::Sentence> for BaseSentence {
    type Error = ParsePbError;

    fn try_from(pb: pb::pattern::Sentence) -> Result<Self, Self::Error> {
        use pb::pattern::binder::Item;

        if !pb.binders.is_empty() {
            let mut operators = vec![];
            let mut num_filters = 0;
            let mut num_hops = 0;
            let start_tag: NameOrId = pb
                .start
                .clone()
                .ok_or(ParsePbError::EmptyFieldError("patmat::Sentence::start".to_string()))?
                .try_into()?;

            // Add an `As` operator to reflect the starting tag
            operators.push(pb::As { alias: pb.start.clone() }.into());

            let end_tag: Option<NameOrId> = pb
                .end
                .clone()
                .map(|tag| tag.try_into())
                .transpose()?;

            let mut tags = BTreeSet::new();
            tags.insert(start_tag.clone());
            if let Some(et) = &end_tag {
                tags.insert(et.clone());
            }

            let mut end_as = BindingOpt::Vertex;
            let size = pb.binders.len();
            for (id, binder) in pb.binders.into_iter().enumerate() {
                let opr = match binder.item {
                    Some(Item::Vertex(v)) => {
                        num_filters += detect_filters(v.params.as_ref());
                        Ok(v.into())
                    }
                    Some(Item::Edge(e)) => {
                        if id == size - 1 {
                            if e.is_edge {
                                end_as = BindingOpt::Edge;
                            }
                        }
                        num_filters += detect_filters(e.params.as_ref());
                        num_hops += 1;
                        Ok(e.into())
                    }
                    Some(Item::Path(p)) => {
                        if id == size - 1 {
                            end_as = BindingOpt::Path;
                        }
                        if let Some(range) = &p.hop_range {
                            num_hops += range.upper as usize;
                            if let Some(base) = &p.base {
                                num_filters += detect_filters(base.params.as_ref()) * range.upper as usize;
                            }
                        }
                        Ok(p.into())
                    }
                    None => Err(ParsePbError::EmptyFieldError("patmat::Binder::item".to_string())),
                }?;
                operators.push(opr);
            }
            if end_tag.is_some() {
                operators.push(pb::As { alias: pb.end.clone() }.into());
            }
            let is_anti = pb.is_anti;
            if end_as == BindingOpt::Path {
                Err(ParsePbError::Unsupported("binding a sentence to a path".to_string()))
            } else {
                Ok(Self { start_tag, end_tag, tags, operators, is_anti, end_as, num_filters, num_hops })
            }
        } else {
            Err(ParsePbError::EmptyFieldError("patmat::Sentence::start".to_string()))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct OrdSentence {
    inner: BaseSentence,
}

impl OrdSentence {
    fn get_ord_meta(&self) -> (i32, i32, i32, i32, i32) {
        (
            self.inner.num_hops as i32,       // less edge expansions is preferred
            -(self.inner.num_filters as i32), // more filters is preferred
            self.inner.end_as as i32,         // ending as vertex is preferred
            -(self.inner.tags.len() as i32),  // more tags is preferred
            self.inner.is_anti as i32,        // non anti-sentence is preferred
        )
    }
}

impl From<BaseSentence> for OrdSentence {
    fn from(inner: BaseSentence) -> Self {
        Self { inner }
    }
}

impl From<OrdSentence> for BaseSentence {
    fn from(os: OrdSentence) -> Self {
        os.inner
    }
}

impl Eq for OrdSentence {}

impl PartialOrd for OrdSentence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_ord_meta()
            .partial_cmp(&other.get_ord_meta())
    }
}

impl Ord for OrdSentence {
    fn cmp(&self, other: &Self) -> Ordering {
        if let Some(ord) = self.partial_cmp(other) {
            ord
        } else {
            if self == other {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
    }
}

impl Sentence for BaseSentence {
    fn composite(&mut self, other: &BaseSentence) -> bool {
        // cannot composite anti-sentence
        if self.is_anti || other.is_anti {
            return false;
        }
        // composition can happens if `self` sentence contains the start_tag of `other`
        if !self.tags.contains(&other.start_tag) {
            return false;
        } else {
            // Two common tags at the same time must enable a join instead of composition
            if let Some(end_tag) = &other.end_tag {
                if self.tags.contains(end_tag) {
                    return false;
                }
            }
        }
        self.end_tag = other.end_tag.clone();
        self.tags.extend(other.tags.iter().cloned());
        if self.end_tag.is_none() || self.end_tag.as_ref().unwrap() != &other.start_tag {
            let expr_str = match &other.start_tag {
                NameOrId::Str(s) => format!("@{:?}", s),
                NameOrId::Id(i) => format!("@{:?}", i),
            };
            // Projection is required to project other.start_tag as the head,
            // so that the traversal formed by the composition can continue.
            self.operators.push(
                pb::Project {
                    mappings: vec![pb::project::ExprAlias {
                        // This cannot be wrong
                        expr: str_to_expr_pb(expr_str).ok(),
                        alias: None,
                    }],
                    is_append: true,
                }
                .into(),
            );
        }
        self.operators
            .extend(other.operators.iter().cloned());
        self.end_as = other.end_as.clone();
        self.num_filters += other.num_filters;
        self.num_hops += other.num_hops;

        true
    }

    fn join(&self, other: &JoinSentence) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .tags
            .intersection(&other.common_tags)
            .cloned()
            .collect();
        if common_tags.is_empty() {
            return None;
        }
        if other.get_base().is_some() {
            // cannot join two base anti-sentences
            if self.is_anti && other.join_kind == pb::join::JoinKind::Anti {
                return None;
            }
        }
        if self.is_anti {
            Some(JoinSentence {
                left: Rc::new(other.clone()),
                // anti-sentence must be placed on the right
                right: Some(Rc::new(self.clone())),
                common_tags,
                join_kind: pb::join::JoinKind::Anti,
            })
        } else {
            Some(JoinSentence {
                left: Rc::new(self.clone()),
                right: Some(Rc::new(other.clone())),
                common_tags,
                join_kind: if other.get_base().is_some() {
                    // if other is a base sentence, follow its join kind,
                    // which indicates whether the sentence is anti.
                    other.join_kind
                } else {
                    pb::join::JoinKind::Inner
                },
            })
        }
    }

    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut plan = pb::LogicalPlan { nodes: vec![] };
        let size = self.operators.len();
        for (idx, opr) in self.operators.iter().enumerate() {
            let node = if idx != size - 1 {
                // A sentence is definitely a chain
                pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![idx as i32 + 1] }
            } else {
                pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![] }
            };
            plan.nodes.push(node);
        }

        Ok(plan)
    }

    fn get_base(&self) -> Option<&BaseSentence> {
        Some(self)
    }
}

impl Sentence for JoinSentence {
    fn composite(&mut self, _: &BaseSentence) -> bool {
        false
    }

    fn join(&self, other: &JoinSentence) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .common_tags
            .intersection(&other.common_tags)
            .cloned()
            .collect();
        if !common_tags.is_empty() {
            if let Some(base) = self.get_base() {
                base.join(other)
            } else if let Some(base) = other.get_base() {
                base.join(&self.clone().into())
            } else {
                Some(JoinSentence {
                    left: Rc::new(self.clone()),
                    right: Some(Rc::new(other.clone())),
                    common_tags,
                    join_kind: pb::join::JoinKind::Inner,
                })
            }
        } else {
            None
        }
    }

    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let (mut plan, right_plan_opt) = (
            self.left.build_logical_plan()?,
            self.right
                .as_ref()
                .map(|s| s.build_logical_plan())
                .transpose()?,
        );
        if let Some(mut right_plan) = right_plan_opt {
            let left_size = plan.nodes.len();
            let right_size = right_plan.nodes.len();
            let join_node_idx = (left_size + right_size) as i32;

            for (idx, node) in right_plan.nodes.iter_mut().enumerate() {
                for child in node.children.iter_mut() {
                    *child += left_size as i32;
                }
                if idx == right_size - 1 {
                    node.children.push(join_node_idx);
                }
            }
            if let Some(node) = plan.nodes.last_mut() {
                node.children.push(join_node_idx);
            }
            plan.nodes.extend(right_plan.nodes.into_iter());
            let keys = self
                .common_tags
                .iter()
                .cloned()
                .map(|tag| common_pb::Variable { tag: tag.try_into().ok(), property: None })
                .collect::<Vec<_>>();
            plan.nodes.push(pb::logical_plan::Node {
                opr: Some(
                    pb::Join {
                        left_keys: keys.clone(),
                        right_keys: keys,
                        kind: unsafe { std::mem::transmute(self.join_kind) },
                    }
                    .into(),
                ),
                children: vec![],
            });
        }

        Ok(plan)
    }

    fn get_base(&self) -> Option<&BaseSentence> {
        if self.right.is_none() {
            self.left.get_base()
        } else {
            None
        }
    }
}

/// A trait to abstract how to build a logical plan for `Pattern` operator.
pub trait MatchingStrategy {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan>;
}

/// A naive implementation of `MatchingStrategy`
pub struct NaiveStrategy {
    /// The matching sentence
    sentences: BinaryHeap<OrdSentence>,
}

impl TryFrom<pb::Pattern> for NaiveStrategy {
    type Error = ParsePbError;

    fn try_from(_pb: pb::Pattern) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl NaiveStrategy {
    fn do_composition(&self) -> BinaryHeap<OrdSentence> {
        let mut sentences = self.sentences.clone();
        // a temporary container
        let mut container = Vec::with_capacity(sentences.len());
        // sentences after composition remain for join
        let mut to_join = BinaryHeap::with_capacity(sentences.len());
        while !sentences.is_empty() {
            let mut first = sentences.pop().unwrap();
            let mut matched = false;
            container.clear();
            while !sentences.is_empty() {
                let second = sentences.pop().unwrap();
                if first.inner.composite(&second.inner) {
                    matched = true;
                    break;
                } else {
                    container.push(second);
                }
            }
            if !matched {
                to_join.push(first);
            } else {
                sentences.push(first);
            }
            if !container.is_empty() {
                sentences.extend(container.drain(..));
            }
        }
        to_join
    }
}

impl MatchingStrategy for NaiveStrategy {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut sentences = self.do_composition();
        let mut container = vec![];
        if !sentences.is_empty() {
            let mut first: JoinSentence = sentences.pop().unwrap().inner.into();
            loop {
                let old_count = sentences.len();
                while !sentences.is_empty() {
                    let second = sentences.pop().unwrap().inner;
                    if let Some(join) = second.join(&first) {
                        first = join;
                    } else {
                        container.push(second.into());
                    }
                }
                if !container.is_empty() {
                    if container.len() == old_count {
                        return Err(IrError::InvalidPatternMatch(
                            "the matching pattern may be disconnected".to_string(),
                        ));
                    } else {
                        sentences.extend(container.drain(..));
                    }
                } else {
                    break;
                }
            }
            first.build_logical_plan()
        } else {
            Err(IrError::InvalidPatternMatch("sentences are empty after composition".to_string()))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
