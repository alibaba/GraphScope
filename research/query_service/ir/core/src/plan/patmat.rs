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

/// A trait to abstract how to build a logical plan for `Pattern` operator.
pub trait MatchingStrategy {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan>;
}

/// Define the behavior of concatenating two matching sentences
pub trait Sentence: Debug + MatchingStrategy {
    /// Composite works by extending the first sentence with the second one via
    /// the **only** common tag. Composition cannot be applied when either sentence
    /// has anti-semantics.
    fn composite(&self, other: &BaseSentence) -> Option<BaseSentence>;
    /// Join works by joining two sentences via the common (at least one) tags
    /// and produce a `JoinSentence`
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence>;
    /// Get base sentence if any
    fn get_base(&self) -> Option<&BaseSentence>;
    /// Get tags for the sentence
    fn get_tags(&self) -> &BTreeSet<NameOrId>;
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
    /// Use `pb::logical_plan::Operator` rather than `pb::Pattern::binder`,
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
                .ok_or(ParsePbError::EmptyFieldError("Pattern::Sentence::start".to_string()))?
                .try_into()?;

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
                    None => Err(ParsePbError::EmptyFieldError("Pattern::Binder::item".to_string())),
                }?;
                operators.push(opr);
            }
            let is_anti = pb.is_anti;
            if end_as == BindingOpt::Path {
                Err(ParsePbError::Unsupported("binding a sentence to a path".to_string()))
            } else {
                Ok(Self { start_tag, end_tag, tags, operators, is_anti, end_as, num_filters, num_hops })
            }
        } else {
            Err(ParsePbError::EmptyFieldError("Pattern::Sentence::start".to_string()))
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

impl MatchingStrategy for BaseSentence {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut plan = pb::LogicalPlan { nodes: vec![] };
        let size = self.operators.len();
        if size == 0 {
            Err(IrError::InvalidPattern("empty sentence".to_string()))
        } else {
            plan.nodes.push(pb::logical_plan::Node {
                // pb::NameOrId -> NameOrId never fails.
                opr: Some(pb::As { alias: self.start_tag.clone().try_into().ok() }.into()),
                children: vec![1],
            });
            for (idx, opr) in self.operators.iter().enumerate() {
                let child_id = idx as i32 + 2;
                let node = if idx != size - 1 {
                    // A sentence is definitely a chain
                    pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![child_id] }
                } else {
                    if self.end_tag.is_some() {
                        pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![child_id] }
                    } else {
                        pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![] }
                    }
                };
                plan.nodes.push(node);
            }
            if let Some(end_tag) = self.end_tag.clone() {
                plan.nodes.push(pb::logical_plan::Node {
                    // pb::NameOrId -> NameOrId never fails.
                    opr: Some(pb::As { alias: end_tag.try_into().ok() }.into()),
                    children: vec![],
                });
            }

            Ok(plan)
        }
    }
}

impl Sentence for BaseSentence {
    fn composite(&self, other: &BaseSentence) -> Option<BaseSentence> {
        // cannot composite anti-sentence
        if self.is_anti || other.is_anti {
            return None;
        }
        // composition can happens if `self` sentence contains the start_tag of `other`
        if !self.tags.contains(&other.start_tag) {
            return None;
        } else {
            // Two common tags at the same time must enable a join instead of composition
            if let Some(end_tag) = &other.end_tag {
                if self.tags.contains(end_tag) {
                    return None;
                }
            }
        }
        let mut compo = self.clone();
        if let Some(end_tag) = self.end_tag.clone() {
            // pb::NameOrId -> NameOrId never fails.
            compo
                .operators
                .push(pb::As { alias: end_tag.try_into().ok() }.into());
        }
        if self.end_tag.is_none() || self.end_tag.as_ref() != Some(&other.start_tag) {
            let expr_str = match &other.start_tag {
                NameOrId::Str(s) => format!("@{:?}", s),
                NameOrId::Id(i) => format!("@{:?}", i),
            };
            // Projection is required to project other.start_tag as the head,
            // so that the traversal formed by the composition can continue.
            compo.operators.push(
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
        compo.end_tag = other.end_tag.clone();
        compo.tags.extend(other.tags.iter().cloned());
        compo
            .operators
            .extend(other.operators.iter().cloned());
        compo.end_as = other.end_as.clone();
        compo.num_filters += other.num_filters;
        compo.num_hops += other.num_hops;

        Some(compo)
    }

    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .tags
            .intersection(&other.get_tags())
            .cloned()
            .collect();
        if common_tags.is_empty() {
            return None;
        }
        if let Some(base) = other.get_base() {
            // cannot join two base anti-sentences
            if self.is_anti && base.is_anti {
                return None;
            }
        }
        if self.is_anti {
            Some(JoinSentence {
                left: other,
                // anti-sentence must be placed on the right
                right: Some(Rc::new(self.clone())),
                common_tags,
                join_kind: pb::join::JoinKind::Anti,
            })
        } else {
            let join_kind = if let Some(base) = other.get_base() {
                // if other is an anti sentence
                if base.is_anti {
                    pb::join::JoinKind::Anti
                } else {
                    pb::join::JoinKind::Inner
                }
            } else {
                pb::join::JoinKind::Inner
            };

            Some(JoinSentence { left: Rc::new(self.clone()), right: Some(other), common_tags, join_kind })
        }
    }

    fn get_base(&self) -> Option<&BaseSentence> {
        Some(self)
    }

    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.tags
    }
}

impl MatchingStrategy for JoinSentence {
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
}

impl Sentence for JoinSentence {
    fn composite(&self, _: &BaseSentence) -> Option<BaseSentence> {
        None
    }

    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .common_tags
            .intersection(&other.get_tags())
            .cloned()
            .collect();
        if !common_tags.is_empty() {
            if let Some(base) = self.get_base() {
                base.join(other)
            } else if let Some(base) = other.get_base() {
                base.join(Rc::new(self.clone()))
            } else {
                Some(JoinSentence {
                    left: Rc::new(self.clone()),
                    right: Some(other),
                    common_tags,
                    join_kind: pb::join::JoinKind::Inner,
                })
            }
        } else {
            None
        }
    }

    fn get_base(&self) -> Option<&BaseSentence> {
        if self.right.is_none() {
            self.left.get_base()
        } else {
            None
        }
    }

    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.common_tags
    }
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
            let first = sentences.pop().unwrap();
            let mut matched = false;
            container.clear();
            while !sentences.is_empty() {
                let second = sentences.pop().unwrap();
                if let Some(compo) = first.inner.composite(&second.inner) {
                    sentences.push(compo.into());
                    matched = true;
                    break;
                } else {
                    container.push(second);
                }
            }
            if !matched {
                to_join.push(first);
            }
            sentences.extend(container.drain(..));
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
                    if let Some(join) = second.join(Rc::new(first.clone())) {
                        first = join;
                    } else {
                        container.push(second.into());
                    }
                }
                if !container.is_empty() {
                    if container.len() == old_count {
                        return Err(IrError::InvalidPattern(
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
            Err(IrError::InvalidPattern("sentences are empty after composition".to_string()))
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[allow(dead_code)]
    fn query_params() -> pb::QueryParams {
        pb::QueryParams {
            tables: vec![],
            columns: vec![],
            is_all_columns: false,
            limit: None,
            predicate: None,
            extra: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn gen_sentence_x_out_y(x: &str, y: Option<&str>, is_edge: bool, is_anti: bool) -> BaseSentence {
        let pb = pb::pattern::Sentence {
            start: x.try_into().ok(),
            binders: vec![pb::pattern::Binder {
                item: Some(pb::pattern::binder::Item::Edge(pb::EdgeExpand {
                    v_tag: None,
                    direction: 0,
                    params: Some(query_params()),
                    is_edge,
                    alias: None,
                })),
            }],
            end: y.and_then(|s| s.try_into().ok()),
            is_anti,
        };

        pb.try_into().unwrap()
    }

    #[allow(dead_code)]
    fn get_sentence_x_inv_y(x: &str, y: Option<&str>, is_anti: bool) -> BaseSentence {
        let pb = pb::pattern::Sentence {
            start: x.try_into().ok(),
            binders: vec![pb::pattern::Binder {
                item: Some(pb::pattern::binder::Item::Vertex(pb::GetV {
                    tag: None,
                    opt: 0,
                    params: Some(query_params()),
                    alias: None,
                })),
            }],
            end: y.and_then(|s| s.try_into().ok()),
            is_anti,
        };

        pb.try_into().unwrap()
    }

    #[test]
    fn sentence_composition() {
        use ir_common::generated::algebra::logical_plan::operator::Opr;

        // case 1.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let b_out_c = gen_sentence_x_out_y("b", Some("c"), false, false);
        let a_out_b_out_c = a_out_b.composite(&b_out_c).unwrap();
        assert_eq!(
            a_out_b_out_c.tags,
            vec!["a".into(), "b".into(), "c".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(a_out_b_out_c.start_tag, "a".into());
        assert_eq!(a_out_b_out_c.end_tag.unwrap(), "c".into());
        // out(), As(b), out()
        assert_eq!(a_out_b_out_c.operators.len(), 3);

        // case 2.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        let a_out_b_out_c = a_out_b.composite(&a_out_c).unwrap();

        assert_eq!(
            a_out_b_out_c.tags,
            vec!["a".into(), "b".into(), "c".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(a_out_b_out_c.start_tag, "a".into());
        assert_eq!(a_out_b_out_c.end_tag.unwrap(), "c".into());
        // out(), As(b), Project(a), out()
        assert_eq!(a_out_b_out_c.operators.len(), 4);
        match a_out_b_out_c
            .operators
            .get(2)
            .unwrap()
            .opr
            .as_ref()
            .unwrap()
        {
            Opr::Project(_) => {}
            _ => {
                panic!("should be a `Project` operator.")
            }
        }

        // case 3.
        let a_out = gen_sentence_x_out_y("a", None, false, false);
        let a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        let a_out_out_c = a_out.composite(&a_out_c).unwrap();

        assert_eq!(a_out_out_c.start_tag, "a".into());
        assert_eq!(a_out_out_c.end_tag.unwrap(), "c".into());
        // out(), Project(a), out()
        assert_eq!(a_out_out_c.operators.len(), 3);
        match a_out_out_c
            .operators
            .get(1)
            .unwrap()
            .opr
            .as_ref()
            .unwrap()
        {
            Opr::Project(_) => {}
            _ => {
                panic!("should be a `Project` operator.")
            }
        }

        // case 4: cannot composite without sharing tags
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let c_out_d = gen_sentence_x_out_y("c", Some("d"), false, false);

        assert!(a_out_b.composite(&c_out_d).is_none());

        // case 5: cannot composite anti-sentence
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let b_out_c = gen_sentence_x_out_y("b", Some("c"), false, true);

        assert!(a_out_b.composite(&b_out_c).is_none());

        // case 6: cannot composite while sharing more than two tags
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_b2 = gen_sentence_x_out_y("a", Some("b"), false, false);

        assert!(a_out_b.composite(&a_out_b2).is_none());
    }

    #[test]
    fn sentence_into_logical_plan() {
        // case 1.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);

        // As(a), Out(), As(b)
        let plan = a_out_b.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 3);
        println!("case 1: {:#?}", plan.nodes);

        // case 2.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        let a_out_b_out_c = a_out_b.composite(&a_out_c).unwrap();
        // As(a), Out(), As(b), Project(a), out(), As(c)
        let plan = a_out_b_out_c.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 6);
        println!("case 2: {:#?}", plan.nodes);
    }

    #[test]
    fn sentence_join() {
        // case 1.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_b2 = a_out_b.clone();

        let join = a_out_b.join(Rc::new(a_out_b2)).unwrap();
        assert_eq!(
            join.common_tags,
            vec!["a".into(), "b".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(join.join_kind, pb::join::JoinKind::Inner);

        // case 2.
        let a_out_b_anti = gen_sentence_x_out_y("a", Some("b"), false, true);
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);

        let join = a_out_b_anti.join(Rc::new(a_out_b)).unwrap();
        assert!(!(join.left.get_base().unwrap().is_anti));
        // anti-sentence moved to the right
        assert!(
            join.right
                .as_ref()
                .unwrap()
                .get_base()
                .unwrap()
                .is_anti
        );
        assert_eq!(
            join.common_tags,
            vec!["a".into(), "b".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(join.join_kind, pb::join::JoinKind::Anti);

        // case 3: cannot join without sharing tags
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let c_out_d = gen_sentence_x_out_y("c", Some("d"), false, false);

        assert!(a_out_b.join(Rc::new(c_out_d)).is_none());

        // case 2.
        let a_out_b_anti = gen_sentence_x_out_y("a", Some("b"), false, true);
        let a_out_b_anti2 = a_out_b_anti.clone();

        assert!(a_out_b_anti
            .join(Rc::new(a_out_b_anti2))
            .is_none());
    }
}
