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
use crate::glogue::error::IrPatternResult;
use crate::glogue::pattern::Pattern;
use crate::plan::meta::PlanMeta;

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
    /// Get the reverse sentence, which not only reverse the start and end tag (must present),
    /// and the direction of all edge/path expansions if possible.
    fn reverse(&mut self) -> bool;
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
                            let expand_opt: pb::edge_expand::ExpandOpt =
                                unsafe { ::std::mem::transmute(e.expand_opt) };
                            if expand_opt.eq(&pb::edge_expand::ExpandOpt::Edge) {
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
                                num_filters += (detect_filters(
                                    base.edge_expand
                                        .as_ref()
                                        .and_then(|expand| expand.params.as_ref()),
                                ) + detect_filters(
                                    base.get_v
                                        .as_ref()
                                        .and_then(|get_v| get_v.params.as_ref()),
                                )) * range.upper as usize;
                            }
                        }
                        Ok(p.into())
                    }
                    Some(Item::Select(pred)) => Ok(pred.into()),
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
        let mut plan = pb::LogicalPlan { nodes: vec![], roots: vec![0] };
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

fn reverse_dir(dir: i32) -> i32 {
    if dir == 0 {
        1
    } else if dir == 1 {
        0
    } else {
        2
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

    fn reverse(&mut self) -> bool {
        use pb::logical_plan::operator::Opr;
        if let Some(end_tag) = self.end_tag.clone() {
            let mut result = true;
            let mut new_operators = self.operators.clone();
            for operator in new_operators.iter_mut() {
                if let Some(opr) = operator.opr.as_mut() {
                    match opr {
                        Opr::Vertex(_) => {}
                        Opr::Edge(edge) => {
                            let expand_opt: pb::edge_expand::ExpandOpt =
                                unsafe { ::std::mem::transmute(edge.expand_opt) };
                            if expand_opt.eq(&pb::edge_expand::ExpandOpt::Edge) {
                                result = false;
                                break;
                            }
                            if let Some(params) = edge.params.as_mut() {
                                if !params.columns.is_empty()
                                    || params.is_all_columns
                                    || params.predicate.is_some()
                                {
                                    result = false;
                                    break;
                                }
                            }
                            edge.direction = reverse_dir(edge.direction);
                        }
                        Opr::Path(path) => {
                            if let Some(base) = path
                                .base
                                .as_mut()
                                .and_then(|expand_base| expand_base.edge_expand.as_mut())
                            {
                                base.direction = reverse_dir(base.direction);
                            }
                        }
                        _ => {}
                    }
                }
            }
            if result {
                self.end_tag = Some(self.start_tag.clone());
                self.start_tag = end_tag;
                self.operators = new_operators;
            }

            result
        } else {
            false
        }
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

    pub fn len(&self) -> usize {
        self.bases.len()
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

impl TryFrom<MergedSentence> for JoinSentence {
    type Error = IrError;

    fn try_from(merged: MergedSentence) -> IrResult<Self> {
        if merged.len() == 0 {
            Err(IrError::InvalidPattern("empty `MergedSentence` is not allowed".to_string()))
        } else {
            let tags = merged.tags.clone();
            let mut queue = merged
                .bases
                .into_iter()
                .collect::<VecDeque<_>>();

            let first = queue.pop_front().unwrap();
            if queue.is_empty() {
                Ok(first.into())
            } else {
                let mut first: JoinSentence = first.into();
                if first.join_kind != pb::join::JoinKind::Inner {
                    return Err(IrError::InvalidPattern(
                        "the first sentence of `MergedSentence` must have InnerJoin".to_string(),
                    ));
                }

                while !queue.is_empty() {
                    let second = queue.pop_front().unwrap();
                    let join_kind = second.join_kind;
                    first = JoinSentence {
                        left: Rc::new(first),
                        right: Some(Rc::new(second)),
                        common_tags: tags.clone(),
                        tags: tags.clone(),
                        join_kind,
                    };
                }

                Ok(first)
            }
        }
    }
}

impl MergedSentence {
    pub fn new(mut s1: BaseSentence, mut s2: BaseSentence) -> Option<Self> {
        if s1.start_tag == s2.start_tag && s1.end_tag == s2.end_tag {
            if s1.join_kind != pb::join::JoinKind::Inner {
                std::mem::swap(&mut s1, &mut s2);
            }
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
            for i in self.bases.len() - 1..1 {
                if self.bases[i].join_kind == pb::join::JoinKind::Inner
                    && self.bases[i - 1].join_kind != pb::join::JoinKind::Inner
                {
                    self.bases.swap(i, i - 1);
                } else {
                    break;
                }
            }
            true
        } else {
            false
        }
    }
}

impl MatchingStrategy for MergedSentence {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        JoinSentence::try_from(self.clone())?.build_logical_plan()
    }
}

impl AsBaseSentence for MergedSentence {
    fn get_base(&self) -> Option<&BaseSentence> {
        if self.len() == 1 {
            self.bases.first()
        } else {
            None
        }
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

    fn reverse(&mut self) -> bool {
        if let Some(end_tag) = self.end_tag.clone() {
            let mut result = true;
            let mut rev_bases = self.bases.clone();
            for base in rev_bases.iter_mut() {
                if !base.reverse() {
                    result = false;
                    break;
                }
            }
            if result {
                self.end_tag = Some(self.start_tag.clone());
                self.start_tag = end_tag;
                self.bases = rev_bases;
            }

            result
        } else {
            false
        }
    }
}

impl Sentence for MergedSentence {
    fn join(&self, other: Rc<dyn Sentence>) -> Option<JoinSentence> {
        if let Ok(this) = JoinSentence::try_from(self.clone()) {
            this.join(other)
        } else {
            None
        }
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

/// Preprocess a plan such that it does not contain two root nodes. More speciically,
/// we will add a common `As(None)` operator as the parent of the two original roots
/// in the plan, which becomes the new and only root node of the plan
fn preprocess_plan(plan: &mut pb::LogicalPlan) -> IrResult<()> {
    if plan.roots.len() == 1 {
        Ok(())
    } else if plan.roots.len() == 2 {
        let new_root = pb::logical_plan::Node {
            opr: Some(pb::As { alias: None }.into()),
            children: plan.roots.iter().map(|id| *id + 1).collect(),
        };
        let mut i = plan.nodes.len();
        plan.nodes.push(plan.nodes[i - 1].clone());

        while i > 0 {
            for child in plan.nodes[i - 1].children.iter_mut() {
                *child += 1;
            }
            plan.nodes.swap(i - 1, i);
            i -= 1;
        }
        plan.nodes[0] = new_root;
        plan.roots = vec![0];

        Ok(())
    } else {
        Err(IrError::InvalidPattern(format!("a plan cannot contain more than two roots ")))
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
                            meta_data: vec![],
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
            // set the first nodes in tail plan as the children of last node of head plan.
            if let Some(n) = plan.nodes.get_mut(last_node as usize) {
                n.children.extend(
                    tail_plan
                        .roots
                        .iter()
                        .map(|id| (*id + last_node as i32 + 1)),
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
        if self.tail.is_none() {
            self.head.get_base()
        } else {
            None
        }
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

    fn reverse(&mut self) -> bool {
        false
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
            // must be preprocessed to only one single root
            preprocess_plan(&mut plan)?;
            // must be preprocessed to only one single root
            preprocess_plan(&mut right_plan)?;
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

            plan.roots
                .push(left_size as i32 + right_plan.roots[0]);
            plan.nodes.extend(right_plan.nodes.into_iter());
            let keys = self
                .common_tags
                .iter()
                .cloned()
                .map(|tag| common_pb::Variable {
                    tag: tag.try_into().ok(),
                    property: None,
                    node_type: None,
                })
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
    /// To record the set of tags a given tag may connect to (out) and be connected with (in)
    tag_adj_list: BTreeMap<NameOrId, (VecDeque<Option<NameOrId>>, VecDeque<Option<NameOrId>>)>,
    /// The matching sentence
    sentences: BTreeMap<(NameOrId, Option<NameOrId>), MergedSentence>,
    /// The start tag is the start_tag of the first matching sentence
    start_tag: NameOrId,
}

impl From<Vec<BaseSentence>> for NaiveStrategy {
    fn from(bases: Vec<BaseSentence>) -> Self {
        let mut tag_out_set = BTreeMap::new();
        let mut tag_in_set = BTreeMap::new();
        let mut sentences: BTreeMap<_, MergedSentence> = BTreeMap::new();
        let mut match_start_tag = None;
        for base in bases {
            let start_tag = base.start_tag.clone();
            if match_start_tag.is_none() {
                match_start_tag = Some(start_tag.clone());
            }
            let end_tag_opt = base.end_tag.clone();
            let entry = sentences.entry((start_tag.clone(), end_tag_opt.clone()));
            match entry {
                Entry::Vacant(vac) => {
                    vac.insert(base.into());
                    tag_out_set
                        .entry(start_tag.clone())
                        .or_insert_with(BinaryHeap::new)
                        .push(end_tag_opt.clone());
                    if let Some(end_tag) = end_tag_opt {
                        tag_in_set
                            .entry(end_tag.clone())
                            .or_insert_with(BinaryHeap::new)
                            .push(Some(start_tag.clone()));
                    }
                }
                Entry::Occupied(mut occ) => {
                    occ.get_mut().merge(base);
                }
            }
        }
        let mut tag_adj_list = BTreeMap::new();
        for (k, v) in tag_out_set {
            let out_list = v
                .into_sorted_vec()
                .into_iter()
                .collect::<VecDeque<_>>();
            tag_adj_list.insert(k, (out_list, VecDeque::new()));
        }
        for (k, v) in tag_in_set {
            (tag_adj_list.entry(k).or_default())
                .1
                .extend(v.into_sorted_vec().into_iter());
        }

        Self { tag_adj_list, sentences, start_tag: match_start_tag.unwrap_or_default() }
    }
}

// TODO(longbin) For now, neither the `start` tag nor `end` tag of each sentence in
// TODO(longbin) `pb::Pattern` can exist outside of the match operator.
impl TryFrom<pb::Pattern> for NaiveStrategy {
    type Error = ParsePbError;

    fn try_from(pb: pb::Pattern) -> Result<Self, Self::Error> {
        let bases = pb
            .sentences
            .into_iter()
            .map(|s| s.try_into())
            .collect::<ParsePbResult<Vec<BaseSentence>>>()?;
        if !bases.is_empty() {
            Ok(Self::from(bases))
        } else {
            Err(ParsePbError::EmptyFieldError("empty sentences in `Pattern`".to_string()))
        }
    }
}

fn get_next_tag(
    nbrs: &mut VecDeque<Option<NameOrId>>, visited: &BTreeSet<NameOrId>,
) -> Option<Option<NameOrId>> {
    let mut end_tag_opt = None;
    let len = nbrs.len();
    let mut count = 0;
    while !nbrs.is_empty() {
        let test_tag = nbrs.pop_front().unwrap();
        let mut is_visited = false;
        if let Some(tag) = &test_tag {
            is_visited = visited.contains(tag);
        }
        if is_visited {
            nbrs.push_back(test_tag);
            count += 1;
            if count == len {
                // means all the neighbors have been visited
                break;
            }
        } else {
            end_tag_opt = Some(test_tag);
            break;
        }
    }

    end_tag_opt
}

impl NaiveStrategy {
    fn get_start_tag(&self) -> Option<NameOrId> {
        if self.tag_adj_list.contains_key(&self.start_tag) {
            Some(self.start_tag.clone())
        } else {
            let mut max_cnt = 0;
            let mut start_tag = None;
            for (tag, (nbrs, _)) in &self.tag_adj_list {
                if max_cnt < nbrs.len() {
                    max_cnt = nbrs.len();
                    start_tag = Some(tag.clone());
                }
            }
            start_tag
        }
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
        if let Some((out_nbrs, in_nbrs)) = self.tag_adj_list.get_mut(&start_tag) {
            let mut is_in_tag = false;
            let mut next_tag_opt = get_next_tag(out_nbrs, visited);
            if next_tag_opt.is_none() {
                next_tag_opt = get_next_tag(in_nbrs, visited);
                is_in_tag = true;
            }
            should_remove = out_nbrs.is_empty() && in_nbrs.is_empty();
            if let Some(next_tag) = next_tag_opt {
                let tag_key = if is_in_tag {
                    // it is fine to unwrap as it must present
                    (next_tag.clone().unwrap(), Some(start_tag.clone()))
                } else {
                    (start_tag.clone(), next_tag.clone())
                };
                if let Some(mut sentence) = self.sentences.remove(&tag_key) {
                    sentence.set_has_as_opr(is_initial);
                    if is_in_tag {
                        if !sentence.reverse() {
                            in_nbrs.push_back(next_tag.clone());
                            self.sentences.insert(tag_key, sentence);
                            return None;
                        }
                    }
                    if let Some(next_start_tag) = next_tag {
                        if let Some(next_sentence) = self.do_dfs(next_start_tag, false, visited) {
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

pub struct ExtendStrategy {
    pattern: Pattern,
}

/// Initializer of a ExtendStrategy
impl ExtendStrategy {
    pub fn init(pb_pattern: &pb::Pattern, plan_meta: &PlanMeta) -> IrPatternResult<Self> {
        let pattern = Pattern::from_pb_pattern(pb_pattern, plan_meta)?;
        Ok(ExtendStrategy { pattern })
    }
}

/// Build pattern match Logical Plan for ExtendStrategy
impl MatchingStrategy for ExtendStrategy {
    fn build_logical_plan(&self) -> IrResult<pb::LogicalPlan> {
        // TODO: generate optimized logical plan based on catalogue
        let logical_plan = self
            .pattern
            .generate_simple_extend_match_plan()?;
        Ok(logical_plan)
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
            sample_ratio: 1.0,
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
                    alias: None,
                    expand_opt: if is_edge { 1 } else { 0 },
                    meta_data: None,
                })),
            }],
            end: y.and_then(|s| s.try_into().ok()),
            join_kind: if is_anti { 5 } else { 0 },
        };

        pb.try_into().unwrap()
    }

    #[allow(dead_code)]
    fn gen_sentence_x_out_y_all_columns(x: &str, y: Option<&str>) -> BaseSentence {
        let mut params = query_params();
        params.is_all_columns = true;
        let pb = pb::pattern::Sentence {
            start: x.try_into().ok(),
            binders: vec![pb::pattern::Binder {
                item: Some(pb::pattern::binder::Item::Edge(pb::EdgeExpand {
                    v_tag: None,
                    direction: 0,
                    params: Some(params),
                    expand_opt: 0,
                    alias: None,
                    meta_data: None,
                })),
            }],
            end: y.and_then(|s| s.try_into().ok()),
            join_kind: 0,
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
                    meta_data: None,
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
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None }
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
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None }
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
        //    As(a), Out(), As(c) (id = 2)
        //    As(a), Out(), As(b), Out(), As(c) (id = 7)
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![8]);
        assert_eq!(
            plan.nodes.get(7).unwrap().opr.clone().unwrap(),
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
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None }
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
        //      As(b), Out(), as(d), (opr_id = 2),
        //      Join (opr_id = 14) (
        //          As(), As(a), Out(), As(d), (opr_id = 6)
        //          As(a), Out(), As(b), Out(), As(c), In(), (12, this has been reversed), As(d) (opr_id = 13)
        //      )
        // )
        let plan = strategy.build_logical_plan().unwrap();
        println!("{:#?}", plan.nodes);

        assert_eq!(plan.nodes.get(3).unwrap().opr.clone().unwrap(), pb::As { alias: None }.into());
        assert_eq!(
            plan.nodes.get(12).unwrap().opr.clone().unwrap(),
            pb::EdgeExpand {
                v_tag: None,
                direction: 1, // check this has been reversed from 0 (Out) to 1 (In)
                params: Some(query_params()),
                expand_opt: 0,
                alias: None,
                meta_data: None
            }
            .into()
        );
        assert_eq!(plan.nodes.get(6).unwrap().children, vec![14]);
        assert_eq!(plan.nodes.get(13).unwrap().children, vec![14]);
        assert_eq!(
            plan.nodes.get(14).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![15]);
        assert_eq!(plan.nodes.get(14).unwrap().children, vec![15]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None }
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case3_no_reverse_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
            gen_sentence_x_out_y_all_columns("d", Some("c")),
            gen_sentence_x_out_y("a", Some("d"), false, false),
            gen_sentence_x_out_y("b", Some("d"), false, false),
        ]);
        // Join (opr_id = 15)
        //      As(b), Out(), as(d), (opr_id = 2),
        //      Join (opr_id = 14) (
        //          As(), As(a), Out(), As(b), Out(), As(c), (opr_id = 8)
        //          As(a), Out(), As(d), Out(), As(c) (opr_id = 13)
        //      )
        // )
        let plan = strategy.build_logical_plan().unwrap();
        println!("{:#?}", plan.nodes);

        assert_eq!(plan.nodes.get(3).unwrap().opr.clone().unwrap(), pb::As { alias: None }.into());
        assert_eq!(plan.nodes.get(8).unwrap().children, vec![14]);
        assert_eq!(plan.nodes.get(13).unwrap().children, vec![14]);
        assert_eq!(
            plan.nodes.get(14).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![15]);
        assert_eq!(plan.nodes.get(14).unwrap().children, vec![15]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None }
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("d".into()), property: None, node_type: None }
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
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case5_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("a", Some("b"), false, true),
        ]);
        // Join (id = 6) (
        //    As(a), Out(), As(b) (id = 2),
        //    As(a), Out(), As(b) (id = 5),
        // )
        let plan = strategy.build_logical_plan().unwrap();
        println!("{:#?}", plan.nodes);

        assert_eq!(plan.nodes.len(), 7);
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![6]);
        assert_eq!(plan.nodes.get(5).unwrap().children, vec![6]);
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("b".into()), property: None, node_type: None },
                ],
                kind: 5 // anti join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case6_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("b", Some("c"), false, false),
            gen_sentence_x_out_y("c", Some("a"), false, false),
        ]);
        // Join (id = 8) (
        //    As(a), In() (id = 1, reversed), As(c) (id = 2),
        //    As(a), Out(), As(b), Out(), As(c) (id = 7),
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 9);
        assert_eq!(plan.nodes.get(2).unwrap().children, vec![8]);
        assert_eq!(plan.nodes.get(7).unwrap().children, vec![8]);
        assert_eq!(
            plan.nodes.get(1).unwrap().opr.clone().unwrap(),
            pb::EdgeExpand {
                v_tag: None,
                direction: 1, // check this has been reversed from 0 (Out) to 1 (In)
                params: Some(query_params()),
                expand_opt: 0,
                alias: None,
                meta_data: None
            }
            .into()
        );
        assert_eq!(
            plan.nodes.last().unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None },
                ],
                right_keys: vec![
                    common_pb::Variable { tag: Some("a".into()), property: None, node_type: None },
                    common_pb::Variable { tag: Some("c".into()), property: None, node_type: None },
                ],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case7_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false),
            gen_sentence_x_out_y("a", Some("c"), false, false),
            gen_sentence_x_out_y("a", Some("d"), false, false),
        ]);
        // Join (id = 11) (
        //      As(a), Out(), As(d) (id = 2)
        //      Join (id = 10) (
        //          As(), As(a), Out(), As(b) (id = 6),
        //                As(a), Out(), As(c) (id = 9),
        //      ),
        //)
        let plan = strategy.build_logical_plan().unwrap();
        println!("{:#?}", plan.nodes);
        assert_eq!(plan.nodes.len(), 12);
        assert_eq!(plan.nodes.get(3).unwrap().opr.clone().unwrap(), pb::As { alias: None }.into());

        assert_eq!(plan.nodes.get(6).unwrap().children, vec![10]);
        assert_eq!(plan.nodes.get(9).unwrap().children, vec![10]);
        assert_eq!(
            plan.nodes.get(10).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },],
                right_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },],
                kind: 0 // inner join
            }
            .into()
        );

        assert_eq!(plan.nodes.get(2).unwrap().children, vec![11]);
        assert_eq!(plan.nodes.get(10).unwrap().children, vec![11]);
        assert_eq!(
            plan.nodes.get(11).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },],
                right_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },],
                kind: 0 // inner join
            }
            .into()
        );
    }

    #[test]
    fn pattern_case8_into_logical_plan() {
        let strategy = NaiveStrategy::from(vec![
            gen_sentence_x_out_y("a", Some("b"), false, false).into(),
            gen_sentence_x_out_y("b", Some("c"), false, false).into(),
            gen_sentence_x_out_y("c", Some("a"), false, true).into(),
        ]);

        // AntiJoin (id = 8) (
        //    As(a), Out(), As(b), Out(), As(c) (id = 4)
        //    As(a), In(), As(c) (id = 7)
        // )
        let plan = strategy.build_logical_plan().unwrap();
        assert_eq!(plan.nodes.len(), 9);

        assert_eq!(plan.nodes.get(4).unwrap().children, vec![8]);
        assert_eq!(plan.nodes.get(7).unwrap().children, vec![8]);
        assert_eq!(
            plan.nodes.get(8).unwrap().opr.clone().unwrap(),
            pb::Join {
                left_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },common_pb::Variable {
                    tag: Some("c".into()),
                    property: None,
                    node_type: None
                }],
                right_keys: vec![common_pb::Variable {
                    tag: Some("a".into()),
                    property: None,
                    node_type: None
                },common_pb::Variable {
                    tag: Some("c".into()),
                    property: None,
                    node_type: None
                }],
                kind: 5 // inner join
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
