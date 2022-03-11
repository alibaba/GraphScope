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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::rc::Rc;

use ir_common::error::{ParsePbError, ParsePbResult};
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

pub trait AsBaseSentence: Debug + MatchingStrategy {
    /// Get base sentence if any
    fn get_base(&self) -> Option<&BaseSentence>;
    /// Get tags for the sentence
    fn get_tags(&self) -> &BTreeSet<NameOrId>;
}

/// Define the behavior of concatenating two matching basic sentences via `Composition`
pub trait BasicSentence: AsBaseSentence {
    /// Composite works by extending the first sentence with the second one via
    /// the **only** common tag. Composition cannot be applied when either sentence
    /// has anti-semantics.
    fn composite(&self, other: Rc<dyn BasicSentence>) -> Option<CompoSentence>;

    /// Get the start tag of the `BasicSentence`, must present
    fn get_start_tag(&self) -> &NameOrId;
    /// Get the end tag of the `BasicSentence`, which is optional
    fn get_end_tag(&self) -> Option<&NameOrId>;
    /// Get the join kind, for identifying if the basic sentence carries the
    /// anti/semi join semantics, which can not be composited
    fn get_join_kind(&self) -> pb::join::JoinKind;
}

/// Define the behavior of concatenating two matching sentences via `Join`
pub trait Sentence: AsBaseSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence>;
}

/// An internal representation of `pb::Sentence`
#[derive(Clone, Debug, PartialEq)]
pub struct BaseSentence {
    /// The start tag of this sentence
    start_tag: NameOrId,
    /// The end tag, if any, of this sentence
    end_tag: Option<NameOrId>,
    /// The tags bound to this sentence
    tags: BTreeSet<NameOrId>,
    /// Use `pb::logical_plan::Operator` rather than `pb::Pattern::binder`,
    /// to facilitate building the logical plan that may translate a tag into an `As` operator.
    operators: Vec<pb::logical_plan::Operator>,
    /// Is this a sentence with Anti(No)-semanatics
    join_kind: pb::join::JoinKind,
    /// What kind of entities this sentence binds to
    end_as: BindingOpt,
    /// The number of filters contained by the sentence
    num_filters: usize,
    /// The number of hops of edge expansions of the sentence
    num_hops: usize,
    /// While transforming into a logical plan, will add a `pb::As(start_tag)` operator
    /// to the logical plan
    has_as_opr: bool,
}

impl BaseSentence {
    pub fn set_has_as_opr(&mut self, has_as_opr: bool) {
        self.has_as_opr = has_as_opr;
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
            let join_kind = unsafe { std::mem::transmute(pb.join_kind) };
            if end_as == BindingOpt::Path {
                Err(ParsePbError::Unsupported("binding a sentence to a path".to_string()))
            } else {
                Ok(Self {
                    start_tag,
                    end_tag,
                    tags,
                    operators,
                    join_kind,
                    end_as,
                    num_filters,
                    num_hops,
                    has_as_opr: true,
                })
            }
        } else {
            Err(ParsePbError::EmptyFieldError("Pattern::Sentence::start".to_string()))
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
            let mut child_offset = 1;
            if self.has_as_opr {
                plan.nodes.push(pb::logical_plan::Node {
                    // pb::NameOrId -> NameOrId never fails.
                    opr: Some(pb::As { alias: self.start_tag.clone().try_into().ok() }.into()),
                    children: vec![1],
                });
                child_offset += 1;
            }

            for (idx, opr) in self.operators.iter().enumerate() {
                let child_id = idx as i32 + child_offset;
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

impl AsBaseSentence for BaseSentence {
    fn get_base(&self) -> Option<&BaseSentence> {
        Some(self)
    }
    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.tags
    }
}

impl BasicSentence for BaseSentence {
    fn composite(&self, other: Rc<dyn BasicSentence>) -> Option<CompoSentence> {
        // can only composite two sentences that are both inner-join
        if self.get_join_kind() != pb::join::JoinKind::Inner
            || other.get_join_kind() != pb::join::JoinKind::Inner
        {
            return None;
        }
        let mut tags = self.get_tags().clone();
        // composition can happens if `self` sentence contains the start_tag of `other`
        if !tags.contains(other.get_start_tag()) {
            return None;
        }
        if tags.intersection(other.get_tags()).count() > 1 {
            return None;
        }
        for tag in other.get_tags() {
            tags.insert(tag.clone());
        }

        Some(CompoSentence { head: Rc::new(self.clone()), tail: Some(other), tags })
    }

    fn get_start_tag(&self) -> &NameOrId {
        &self.start_tag
    }

    fn get_end_tag(&self) -> Option<&NameOrId> {
        self.end_tag.as_ref()
    }
    fn get_join_kind(&self) -> pb::join::JoinKind {
        self.join_kind
    }
}

impl Sentence for BaseSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .get_tags()
            .intersection(&other.get_tags())
            .cloned()
            .collect();

        if !common_tags.is_empty() {
            let tags: BTreeSet<NameOrId> = self
                .get_tags()
                .union(&other.get_tags())
                .cloned()
                .collect();

            if let Some(base) = other.get_base() {
                // cannot join two sentences that both have Non-inner join_type
                if self.get_join_kind() != pb::join::JoinKind::Inner
                    && base.join_kind != pb::join::JoinKind::Inner
                {
                    return None;
                }
            }
            if self.get_join_kind() != pb::join::JoinKind::Inner {
                Some(JoinSentence {
                    left: other,
                    // anti-sentence must be placed on the right
                    right: Some(Rc::new(self.clone())),
                    common_tags,
                    tags,
                    join_kind: pb::join::JoinKind::Anti,
                })
            } else {
                let join_kind = if let Some(base) = other.get_base() {
                    // if other is an non-inner sentence
                    base.join_kind
                } else {
                    pb::join::JoinKind::Inner
                };

                Some(JoinSentence {
                    left: Rc::new(self.clone()),
                    right: Some(other),
                    common_tags,
                    tags,
                    join_kind,
                })
            }
        } else {
            None
        }
    }
}

/// Defines merging an array of `BaseSentence` with the same start and end tag
#[derive(Clone, Debug)]
pub struct MergedSentence {
    /// The common start tag of the merged sentences
    start_tag: NameOrId,
    /// The common end tag, if any, of the merged sentences
    end_tag: Option<NameOrId>,
    /// The tags bound to the merged sentences
    tags: BTreeSet<NameOrId>,
    /// The merged sentences
    bases: Vec<BaseSentence>,
}

impl MergedSentence {
    pub fn set_has_as_opr(&mut self, has_as_opr: bool) {
        for base in self.bases.iter_mut() {
            base.set_has_as_opr(has_as_opr);
        }
    }
}

impl From<BaseSentence> for MergedSentence {
    fn from(base: BaseSentence) -> Self {
        Self {
            start_tag: base.start_tag.clone(),
            end_tag: base.end_tag.clone(),
            tags: base.tags.clone(),
            bases: vec![base],
        }
    }
}

impl From<MergedSentence> for CompoSentence {
    fn from(merged: MergedSentence) -> Self {
        let tags = merged.tags.clone();
        Self { head: Rc::new(merged), tail: None, tags }
    }
}

impl From<MergedSentence> for JoinSentence {
    fn from(mut merged: MergedSentence) -> Self {
        assert!(!merged.bases.is_empty());
        if merged.bases.len() == 1 {
            merged.bases.get(0).unwrap().clone().into()
        } else {
            let left_base = merged.bases.remove(0);
            let tags = merged.tags.clone();
            let join_kind = left_base.join_kind;
            Self {
                left: Rc::new(left_base),
                right: Some(Rc::new(JoinSentence::from(merged))),
                common_tags: tags.clone(),
                tags,
                join_kind,
            }
        }
    }
}

impl MergedSentence {
    pub fn new(s1: BaseSentence, s2: BaseSentence) -> Option<Self> {
        if s1.start_tag == s2.start_tag && s1.end_tag == s2.end_tag {
            Some(Self {
                start_tag: s1.start_tag.clone(),
                end_tag: s1.end_tag.clone(),
                tags: s1.tags.clone(),
                bases: vec![s1, s2],
            })
        } else {
            None
        }
    }

    pub fn merge(&mut self, base: BaseSentence) -> bool {
        if self.start_tag == base.start_tag && self.end_tag == base.end_tag {
            self.bases.push(base);
            true
        } else {
            false
        }
    }
}

impl MatchingStrategy for MergedSentence {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        JoinSentence::from(self.clone()).build_logical_plan()
    }
}

impl AsBaseSentence for MergedSentence {
    fn get_base(&self) -> Option<&BaseSentence> {
        None
    }

    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.tags
    }
}

impl BasicSentence for MergedSentence {
    fn composite(&self, other: Rc<dyn BasicSentence>) -> Option<CompoSentence> {
        // can only composite two sentences that are both inner-join
        if self.get_join_kind() != pb::join::JoinKind::Inner
            || other.get_join_kind() != pb::join::JoinKind::Inner
        {
            return None;
        }
        let mut tags = self.get_tags().clone();
        // composition can happens if `self` sentence contains the start_tag of `other`
        if !tags.contains(other.get_start_tag()) {
            return None;
        }
        if tags.intersection(other.get_tags()).count() > 1 {
            return None;
        }
        for tag in other.get_tags() {
            tags.insert(tag.clone());
        }

        Some(CompoSentence { head: Rc::new(self.clone()), tail: Some(other), tags })
    }

    fn get_start_tag(&self) -> &NameOrId {
        &self.start_tag
    }

    fn get_end_tag(&self) -> Option<&NameOrId> {
        self.end_tag.as_ref()
    }

    fn get_join_kind(&self) -> pb::join::JoinKind {
        pb::join::JoinKind::Inner
    }
}

impl Sentence for MergedSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        JoinSentence::from(self.clone()).join(other)
    }
}

/// Define the sentences after composition
#[derive(Clone, Debug)]
pub struct CompoSentence {
    /// The head sentence of the composition
    head: Rc<dyn BasicSentence>,
    /// The tail sentence of the composition
    tail: Option<Rc<dyn BasicSentence>>,
    /// The union tags of the sentences after composition
    tags: BTreeSet<NameOrId>,
}

/// To obtain the nodes in `plan` that has no parent
fn get_first_nodes(plan: &pb::LogicalPlan) -> IrResult<BTreeSet<u32>> {
    let mut nodes: BTreeSet<u32> = (0..plan.nodes.len() as u32).collect();
    for node in &plan.nodes {
        for child in &node.children {
            nodes.remove(&(*child as u32));
        }
    }
    if nodes.is_empty() {
        Err(IrError::InvalidPattern(format!("fail to obtain first nodes from plan {:?}", plan)))
    } else {
        Ok(nodes)
    }
}

impl MatchingStrategy for CompoSentence {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut plan = self.head.build_logical_plan()?;
        if let Some(tail) = &self.tail {
            let start_tag = self.get_start_tag();
            let mut last_node = plan.nodes.len() as u32 - 1;
            if start_tag == tail.get_start_tag() {
                let mut expr_str = "@".to_string();
                match start_tag {
                    NameOrId::Str(s) => expr_str.push_str(s),
                    NameOrId::Id(i) => expr_str.push_str(&i.to_string()),
                }
                let project_node = pb::logical_plan::Node {
                    opr: Some(pb::logical_plan::Operator {
                        opr: Some(pb::logical_plan::operator::Opr::Project(pb::Project {
                            mappings: vec![pb::project::ExprAlias {
                                expr: Some(str_to_expr_pb(expr_str)?),
                                alias: None,
                            }],
                            is_append: true,
                        })),
                    }),
                    children: vec![],
                };
                if let Some(n) = plan.nodes.get_mut(last_node as usize) {
                    n.children.push(last_node as i32 + 1);
                }
                // update the last node as the newly added project node
                last_node += 1;
                plan.nodes.push(project_node);
            }
            let tail_plan = tail.build_logical_plan()?;
            let tail_first_nodes: BTreeSet<u32> = get_first_nodes(&tail_plan)?;
            // set the first nodes in tail plan as the children of last node of head plan.
            if let Some(n) = plan.nodes.get_mut(last_node as usize) {
                n.children.extend(
                    tail_first_nodes
                        .into_iter()
                        .map(|id| (id + last_node + 1) as i32),
                );
            }
            // push the nodes in tail_plan into the resulted plan
            for mut node in tail_plan.nodes {
                for child in node.children.iter_mut() {
                    // update the child node's id
                    *child += last_node as i32 + 1;
                }
                plan.nodes.push(node);
            }
        }
        Ok(plan)
    }
}

impl AsBaseSentence for CompoSentence {
    fn get_base(&self) -> Option<&BaseSentence> {
        None
    }
    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.tags
    }
}

impl BasicSentence for CompoSentence {
    fn composite(&self, other: Rc<dyn BasicSentence>) -> Option<CompoSentence> {
        // can only composite two sentences that are both inner-join
        if self.get_join_kind() != pb::join::JoinKind::Inner
            || other.get_join_kind() != pb::join::JoinKind::Inner
        {
            return None;
        }
        let mut tags = self.get_tags().clone();
        // composition can happens if `self` sentence contains the start_tag of `other`
        if !tags.contains(other.get_start_tag()) {
            return None;
        }
        if tags.intersection(other.get_tags()).count() > 1 {
            return None;
        }
        for tag in other.get_tags() {
            tags.insert(tag.clone());
        }

        Some(CompoSentence { head: Rc::new(self.clone()), tail: Some(other), tags })
    }

    fn get_start_tag(&self) -> &NameOrId {
        self.head.get_start_tag()
    }

    fn get_end_tag(&self) -> Option<&NameOrId> {
        if let Some(tail) = &self.tail {
            tail.get_end_tag()
        } else {
            None
        }
    }

    fn get_join_kind(&self) -> pb::join::JoinKind {
        pb::join::JoinKind::Inner
    }
}

impl Sentence for CompoSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .get_tags()
            .intersection(&other.get_tags())
            .cloned()
            .collect();
        if !common_tags.is_empty() {
            let tags: BTreeSet<NameOrId> = self
                .get_tags()
                .union(&other.get_tags())
                .cloned()
                .collect();
            let join_kind = if let Some(base) = other.get_base() {
                base.get_join_kind()
            } else {
                pb::join::JoinKind::Inner
            };
            Some(JoinSentence {
                left: Rc::new(self.clone()),
                right: Some(other),
                common_tags,
                tags,
                join_kind,
            })
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct JoinSentence {
    /// The left sentence
    left: Rc<dyn Sentence>,
    /// The right sentence
    right: Option<Rc<dyn Sentence>>,
    /// The common tags of left and right for join
    common_tags: BTreeSet<NameOrId>,
    /// The union tags of left and right
    tags: BTreeSet<NameOrId>,
    /// The join kind, either `Inner`, or `Anti`
    join_kind: pb::join::JoinKind,
}

impl From<BaseSentence> for JoinSentence {
    fn from(base: BaseSentence) -> Self {
        let join_kind = base.join_kind;
        let tags = base.tags.clone();
        Self { left: Rc::new(base), right: None, common_tags: tags.clone(), tags, join_kind }
    }
}

impl From<CompoSentence> for JoinSentence {
    fn from(combo: CompoSentence) -> Self {
        let join_kind = pb::join::JoinKind::Inner;
        let tags = combo.tags.clone();
        Self { left: Rc::new(combo), right: None, common_tags: tags.clone(), tags, join_kind }
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

impl MatchingStrategy for JoinSentence {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut plan = self.left.build_logical_plan()?;
        let mut right_plan_opt = None;
        if let Some(right) = &self.right {
            right_plan_opt = Some(right.build_logical_plan()?);
        }
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

impl AsBaseSentence for JoinSentence {
    fn get_base(&self) -> Option<&BaseSentence> {
        if self.right.is_none() {
            self.left.get_base()
        } else {
            None
        }
    }

    fn get_tags(&self) -> &BTreeSet<NameOrId> {
        &self.tags
    }
}

impl Sentence for JoinSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        let common_tags: BTreeSet<NameOrId> = self
            .tags
            .intersection(&other.get_tags())
            .cloned()
            .collect();
        let tags: BTreeSet<NameOrId> = self
            .tags
            .union(&other.get_tags())
            .cloned()
            .collect();
        if common_tags.is_empty() {
            return None;
        }
        if let Some(base) = self.get_base() {
            base.join(other)
        } else if let Some(base) = other.get_base() {
            base.join(Rc::new(self.clone()))
        } else {
            Some(JoinSentence {
                left: Rc::new(self.clone()),
                right: Some(other),
                common_tags,
                tags,
                join_kind: pb::join::JoinKind::Inner,
            })
        }
    }
}

/// A naive implementation of `MatchingStrategy`
#[derive(Clone, Default)]
pub struct NaiveStrategy {
    /// To record the set of tags a given tag may connect to
    tag_adj_list: BTreeMap<NameOrId, VecDeque<Option<NameOrId>>>,
    /// The matching sentence
    sentences: BTreeMap<(NameOrId, Option<NameOrId>), MergedSentence>,
}

impl From<Vec<BaseSentence>> for NaiveStrategy {
    fn from(bases: Vec<BaseSentence>) -> Self {
        let mut tag_adj_set = BTreeMap::new();
        let mut sentences: BTreeMap<_, MergedSentence> = BTreeMap::new();
        for base in bases {
            let start_tag = base.start_tag.clone();
            let end_tag = base.end_tag.clone();
            let entry = sentences.entry((start_tag.clone(), end_tag.clone()));
            match entry {
                Entry::Vacant(vac) => {
                    vac.insert(base.into());
                    tag_adj_set
                        .entry(start_tag.clone())
                        .or_insert_with(BinaryHeap::new)
                        .push(end_tag.clone());
                }
                Entry::Occupied(mut occ) => {
                    occ.get_mut().merge(base);
                }
            }
        }
        let mut tag_adj_list = BTreeMap::new();
        for (k, v) in tag_adj_set {
            tag_adj_list.insert(
                k,
                v.into_sorted_vec()
                    .into_iter()
                    .collect::<VecDeque<_>>(),
            );
        }

        Self { tag_adj_list, sentences }
    }
}

// TODO(longbin) For now, the either the `start` tag or `end` tag of each sentence in
// TODO(longbin) `pb::Pattern` does not exist outside of the match operator.
impl TryFrom<pb::Pattern> for NaiveStrategy {
    type Error = ParsePbError;

    fn try_from(pb: pb::Pattern) -> Result<Self, Self::Error> {
        let bases = pb
            .sentences
            .into_iter()
            .map(|s| s.try_into())
            .collect::<ParsePbResult<Vec<BaseSentence>>>()?;
        Ok(Self::from(bases))
    }
}

impl NaiveStrategy {
    fn get_start_tag(&self) -> Option<NameOrId> {
        let mut max_cnt = 0;
        let mut start_tag = None;
        for (tag, nbrs) in &self.tag_adj_list {
            if max_cnt < nbrs.len() {
                max_cnt = nbrs.len();
                start_tag = Some(tag.clone());
            }
        }
        start_tag
    }

    fn is_empty(&self) -> bool {
        self.sentences.is_empty()
    }

    fn do_dfs(
        &mut self, start_tag: NameOrId, is_initial: bool, visited: &mut BTreeSet<NameOrId>,
    ) -> Option<CompoSentence> {
        let mut result = None;
        let mut should_remove = false;
        visited.insert(start_tag.clone());
        if let Some(nbrs) = self.tag_adj_list.get_mut(&start_tag) {
            let mut end_tag_opt = None;
            let nbr_len = nbrs.len();
            let mut count = 0;
            should_remove = nbr_len == 0;
            while !nbrs.is_empty() {
                let test_tag = nbrs.pop_front().unwrap();
                let mut is_visited = false;
                if let Some(tag) = &test_tag {
                    is_visited = visited.contains(tag);
                }
                if is_visited {
                    nbrs.push_back(test_tag);
                    count += 1;
                    if count == nbr_len {
                        // means all the neighbors have been visited
                        break;
                    }
                } else {
                    end_tag_opt = Some(test_tag);
                    break;
                }
            }

            if let Some(end_tag) = end_tag_opt {
                if let Some(mut sentence) = self
                    .sentences
                    .remove(&(start_tag.clone(), end_tag.clone()))
                {
                    if !is_initial {
                        sentence.set_has_as_opr(false);
                    }
                    if let Some(new_start_tag) = end_tag {
                        if let Some(next_sentence) = self.do_dfs(new_start_tag, false, visited) {
                            result = sentence.composite(Rc::new(next_sentence));
                        } else {
                            result = Some(sentence.into())
                        }
                    } else {
                        result = Some(sentence.into())
                    }
                }
            }
        }
        if should_remove {
            self.tag_adj_list.remove(&start_tag);
        }
        visited.remove(&start_tag);

        result
    }

    fn do_composition(&mut self) -> IrResult<VecDeque<CompoSentence>> {
        let mut results = VecDeque::new();
        while !self.is_empty() {
            let mut visited = BTreeSet::new();
            if let Some(start_tag) = self.get_start_tag() {
                if let Some(sentence) = self.do_dfs(start_tag, true, &mut visited) {
                    results.push_back(sentence);
                } else {
                    return Err(IrError::InvalidPattern("the pattern may be disconnected".to_string()));
                }
            } else {
                if !self.is_empty() {
                    return Err(IrError::InvalidPattern("the pattern may be disconnected".to_string()));
                }
            }
        }
        Ok(results)
    }
}

impl MatchingStrategy for NaiveStrategy {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut sentences = self
            .clone()
            .do_composition()?
            .into_iter()
            .map(|s| JoinSentence::from(s))
            .collect::<VecDeque<JoinSentence>>();

        if !sentences.is_empty() {
            let mut first = sentences.pop_front().unwrap();
            while !sentences.is_empty() {
                let old_count = sentences.len();
                let mut new_count = 0;
                for _ in 0..old_count {
                    let second = sentences.pop_front().unwrap();
                    if let Some(join) = first.join(Rc::new(second.clone())) {
                        first = join;
                    } else {
                        sentences.push_back(second);
                        new_count += 1;
                    }
                }
                if new_count != 0 {
                    if new_count == old_count {
                        return Err(IrError::InvalidPattern(
                            "the matching pattern may be disconnected".to_string(),
                        ));
                    }
                } else {
                    break;
                }
            }
            first.build_logical_plan()
        } else {
            Err(IrError::InvalidPattern("empty sentences".to_string()))
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
            join_kind: if is_anti { 5 } else { 0 },
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
            join_kind: if is_anti { 5 } else { 0 },
        };

        pb.try_into().unwrap()
    }

    #[test]
    fn sentence_composition() {
        // case 1.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let b_out_c = gen_sentence_x_out_y("b", Some("c"), false, false);
        let a_out_b_out_c = a_out_b.composite(Rc::new(b_out_c)).unwrap();
        assert_eq!(
            a_out_b_out_c.tags,
            vec!["a".into(), "b".into(), "c".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(a_out_b_out_c.get_start_tag().clone(), "a".into());
        assert_eq!(a_out_b_out_c.get_end_tag().unwrap().clone(), "c".into());

        // case 2.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        let a_out_b_out_c = a_out_b.composite(Rc::new(a_out_c)).unwrap();

        assert_eq!(
            a_out_b_out_c.tags,
            vec!["a".into(), "b".into(), "c".into()]
                .into_iter()
                .collect()
        );
        assert_eq!(a_out_b_out_c.get_start_tag().clone(), "a".into());
        assert_eq!(a_out_b_out_c.get_end_tag().unwrap().clone(), "c".into());

        // case 3.
        let a_out = gen_sentence_x_out_y("a", None, false, false);
        let a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        let a_out_out_c = a_out.composite(Rc::new(a_out_c)).unwrap();

        assert_eq!(a_out_out_c.get_start_tag().clone(), "a".into());
        assert_eq!(a_out_out_c.get_end_tag().unwrap().clone(), "c".into());

        // case 4: cannot composite without sharing tags
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let c_out_d = gen_sentence_x_out_y("c", Some("d"), false, false);

        assert!(a_out_b.composite(Rc::new(c_out_d)).is_none());

        // case 5: cannot composite anti-sentence
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let b_out_c = gen_sentence_x_out_y("b", Some("c"), false, true);

        assert!(a_out_b.composite(Rc::new(b_out_c)).is_none());

        // case 6: cannot composite while sharing more than two tags
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let a_out_b2 = gen_sentence_x_out_y("a", Some("b"), false, false);

        assert!(a_out_b.composite(Rc::new(a_out_b2)).is_none());
    }

    #[test]
    fn sentence_into_logical_plan() {
        // case 1.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);

        // As(a), Out(), As(b)
        let plan = a_out_b.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 3);
        println!("case 1: {:?}", plan.nodes);

        // case 2.
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let mut a_out_c = gen_sentence_x_out_y("a", Some("c"), false, false);
        a_out_c.set_has_as_opr(false);
        let a_out_b_out_c = a_out_b.composite(Rc::new(a_out_c)).unwrap();
        // As(a), Out(), As(b), Project(a), out(), As(c)
        let plan = a_out_b_out_c.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 6);
        println!("case 2: {:?}", plan.nodes);

        // case 3. merged case
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let merged = MergedSentence::new(a_out_b.clone(), a_out_b).unwrap();
        let plan = merged.build_logical_plan().unwrap();
        // As(a), Out(), As(b), As(a), out(), As(b), Join
        assert_eq!(plan.nodes.len(), 7);
        println!("case 3: {:?}", plan.nodes);

        // case 4. concatenate merged sentence
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);
        let b_out_c = gen_sentence_x_out_y("b", Some("c"), false, false);
        let mut merged = MergedSentence::new(b_out_c.clone(), b_out_c).unwrap();
        merged.set_has_as_opr(false);
        let a_out_c = a_out_b.composite(Rc::new(merged)).unwrap();

        let plan = a_out_c.build_logical_plan().unwrap();
        // As(a), Out(), As(b), Out(), As(c), Out(), As(c), Join
        assert_eq!(plan.nodes.len(), 8);
        println!("case 4: {:?}", plan.nodes);
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

        let plan = join.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 7);
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![6]);
        assert_eq!(plan.nodes.get(5).unwrap().children, vec![6]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("b".into()), property: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("b".into()), property: None }
                ],
                kind: 0
            }
            .into()
        );

        // case 2.
        let a_out_b_anti = gen_sentence_x_out_y("a", Some("b"), false, true);
        let a_out_b = gen_sentence_x_out_y("a", Some("b"), false, false);

        let join = a_out_b_anti.join(Rc::new(a_out_b)).unwrap();
        assert_ne!(join.left.get_base().unwrap().join_kind, pb::join::JoinKind::Anti);
        // anti-sentence moved to the right
        assert_eq!(
            join.right
                .as_ref()
                .unwrap()
                .get_base()
                .unwrap()
                .join_kind,
            pb::join::JoinKind::Anti
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

        // case 4
        let a_out_b_anti = gen_sentence_x_out_y("a", Some("b"), false, true);
        let a_out_b_anti2 = a_out_b_anti.clone();

        assert!(a_out_b_anti
            .join(Rc::new(a_out_b_anti2))
            .is_none());
    }

    #[test]
    fn pattern_case1_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
        ]);

        // As(a), Out(), As(b),
        // Join(id = 7) (
        //   Out() (id = 3), As(c),
        //   Out() (id = 5), As(c),
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 8);
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![3, 5]);
        assert_eq!(plan.nodes.get(4).unwrap().children, vec![7]);
        assert_eq!(plan.nodes.get(6).unwrap().children, vec![7]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None }
                ],
                kind: 0
            }
            .into()
        );
    }

    #[test]
    fn pattern_case2_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
            gen_sentence_x_out_y("a", Some("c"), false, false),
        ]);

        // Join (id = 8) (
        //    As(a), Out(), As(b), Out(), As(c) (id = 4)
        //    As(a), Out(), As(c) (id = 7)
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.get(4).unwrap().children, vec![8]);
        assert_eq!(
            plan.nodes.get(4).unwrap().opr.clone().unwrap(),
            pb::As { alias: "c".try_into().ok() }.into()
        );
        assert_eq!(plan.nodes.get(7).unwrap().children, vec![8]);
        assert_eq!(
            plan.nodes.get(7).unwrap().opr.clone().unwrap(),
            pb::As { alias: "c".try_into().ok() }.into()
        );
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None }
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case3_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
            gen_sentence_x_out_y("d", Some("c"), false, false),
            gen_sentence_x_out_y("a", Some("d"), false, false),
            gen_sentence_x_out_y("b", Some("d"), false, false),
        ]);
        // Join (opr_id = 14)
        //      As(b), Out(), as(d), (opr_id = 13),
        //      Join (opr_id = 10) (
        //          As(a), Out(), As(b), Out(), As(c) (opr_id = 4)
        //          As(a), Out(), As(d), Out(), As(c), (opr_id = 9)
        //      )
        // )
        let plan = strategy.build_logical_plan().unwrap();
        println!("{:#?}", plan.nodes);

        assert_eq!(plan.nodes.get(4).unwrap().children, vec![10]);
        assert_eq!(plan.nodes.get(9).unwrap().children, vec![10]);
        assert_eq!(
            plan.nodes.get(10).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("c".into()), property: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
        assert_eq!(plan.nodes.get(10).unwrap().children, vec![14]);
        assert_eq!(plan.nodes.get(13).unwrap().children, vec![14]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None },
                    common_pb::Variable { tag: Some("d".into()), property: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None },
                    common_pb::Variable { tag: Some("d".into()), property: None }
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case4_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("a"), false, false),
        ]);
        // Join (id = 6) (
        //    As(a), Out(), As(b) (id = 2),
        //    As(b), Out(), As(a) (id = 5),
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 7);
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![6]);
        assert_eq!(plan.nodes.get(5).unwrap().children, vec![6]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("b".into()), property: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None },
                    common_pb::Variable { tag: Some("b".into()), property: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_disconnected_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false).into(),
            gen_sentence_x_out_y("b", Some("c"), false, false).into(),
            gen_sentence_x_out_y("d", Some("e"), false, false).into(),
        ]);

        let result = strategy.build_logical_plan();
        match result.err().unwrap() {
            IrError::InvalidPattern(_) => {}
            _ => panic!("should produce invalid pattern error"),
        }
    }
}
