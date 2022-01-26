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

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io;
use std::rc::Rc;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use vec_map::VecMap;

use crate::error::{IrError, IrResult};
use crate::plan::meta::{PlanMeta, StoreMeta, INVALID_META_ID, STORE_META};
use crate::JsonIO;

/// An internal representation of the pb-[`Node`].
///
/// [`Node`]: crate::generated::algebra::logical_plan::Node
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub(crate) id: u32,
    pub(crate) opr: pb::logical_plan::Operator,
    pub(crate) parents: BTreeSet<u32>,
    pub(crate) children: BTreeSet<u32>,
}

#[allow(dead_code)]
impl Node {
    pub fn new(id: u32, opr: pb::logical_plan::Operator) -> Node {
        Node { id, opr, parents: BTreeSet::new(), children: BTreeSet::new() }
    }

    pub fn add_child(&mut self, child_id: u32) {
        self.children.insert(child_id);
    }

    pub fn get_first_child(&self) -> Option<u32> {
        self.children.iter().next().cloned()
    }

    pub fn add_parent(&mut self, parent_id: u32) {
        self.parents.insert(parent_id);
    }
}

pub(crate) type NodeType = Rc<RefCell<Node>>;

/// An internal representation of the pb-[`LogicalPlan`].
///
/// [`LogicalPlan`]: crate::generated::algebra::LogicalPlan
#[derive(Default, Clone)]
pub struct LogicalPlan {
    pub(crate) nodes: VecMap<NodeType>,
    /// To record the nodes' maximum id in the logical plan. Note that the nodes
    /// **include the removed ones**
    pub(crate) max_node_id: u32,
    /// The metadata of the logical plan
    pub(crate) meta: PlanMeta,
}

impl PartialEq for LogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        if self.nodes.len() != other.nodes.len() {
            return false;
        }
        for (this_node, other_node) in self
            .nodes
            .iter()
            .map(|(_, node)| node)
            .zip(other.nodes.iter().map(|(_, node)| node))
        {
            if this_node != other_node {
                return false;
            }
        }

        true
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.nodes.iter().map(|(_, node)| node.borrow()))
            .finish()
    }
}

impl TryFrom<pb::LogicalPlan> for LogicalPlan {
    type Error = ParsePbError;

    fn try_from(pb: pb::LogicalPlan) -> Result<Self, Self::Error> {
        let nodes_pb = pb.nodes;
        let mut plan = LogicalPlan::default();
        let mut parents = HashMap::<u32, Vec<u32>>::new();
        for (id, node) in nodes_pb.iter().enumerate() {
            for &child in &node.children {
                if child <= id as i32 {
                    return Err(ParsePbError::ParseError(format!(
                        "the child node's id {:?} is not larger than parent node's id {:?}",
                        child, id
                    )));
                }
                parents
                    .entry(child as u32)
                    .or_insert_with(Vec::new)
                    .push(id as u32);
            }
        }

        for (id, node) in nodes_pb.into_iter().enumerate() {
            if let Some(opr) = node.opr {
                plan.append_operator_as_node(
                    opr,
                    parents
                        .get(&(id as u32))
                        .cloned()
                        .unwrap_or(Vec::new()),
                )
                .map_err(|err| ParsePbError::ParseError(format!("{:?}", err)))?;
            } else {
                return Err(ParsePbError::from("do not specify operator in a node"));
            }
        }

        Ok(plan)
    }
}

impl From<LogicalPlan> for pb::LogicalPlan {
    fn from(plan: LogicalPlan) -> Self {
        let mut id_map: HashMap<u32, i32> = HashMap::with_capacity(plan.len());
        // As there might be some nodes being removed, we gonna remap the nodes's ids
        for (id, node) in plan.nodes.iter().enumerate() {
            id_map.insert(node.0 as u32, id as i32);
        }
        let mut plan_pb = pb::LogicalPlan { nodes: vec![] };
        for (_, node) in &plan.nodes {
            let mut node_pb = pb::logical_plan::Node { opr: None, children: vec![] };
            node_pb.opr = Some(node.borrow().opr.clone());
            node_pb.children = node
                .borrow()
                .children
                .iter()
                .map(|old_id| *id_map.get(old_id).unwrap())
                .collect();
            plan_pb.nodes.push(node_pb);
        }

        plan_pb
    }
}

fn clone_node(node: NodeType) -> Node {
    let mut clone_node = (*node.borrow()).clone();
    clone_node.children.clear();
    clone_node.parents.clear();

    clone_node
}

// Implement some private functions
#[allow(dead_code)]
impl LogicalPlan {
    /// Get the corresponding merge node of the given branch node.
    fn get_merge_node(&self, branch_node: NodeType) -> Option<NodeType> {
        if branch_node.borrow().children.len() > 1 {
            let mut layer = 0;
            let mut curr_node_opt = Some(branch_node.clone());
            while curr_node_opt.is_some() {
                let next_node_id = curr_node_opt
                    .as_ref()
                    .unwrap()
                    .borrow()
                    .get_first_child();
                curr_node_opt = next_node_id.and_then(|id| self.get_node(id));
                if let Some(curr_node) = &curr_node_opt {
                    let curr_ref = curr_node.borrow();
                    if curr_ref.children.len() > 1 {
                        layer += 1;
                    }
                    if curr_ref.parents.len() > 1 {
                        // Every branch node must have a corresponding merge node in a valid plan
                        if layer > 0 {
                            layer -= 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            if layer == 0 {
                curr_node_opt
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[allow(dead_code)]
impl LogicalPlan {
    /// Create a new logical plan from some root.
    pub fn with_root(node: Node) -> Self {
        let mut nodes = VecMap::new();
        nodes.insert(node.id as usize, Rc::new(RefCell::new(node)));
        Self { nodes, max_node_id: 1, meta: PlanMeta::default() }
    }

    /// Get a node reference from the logical plan
    pub fn get_node(&self, id: u32) -> Option<NodeType> {
        self.nodes.get(id as usize).cloned()
    }

    pub fn get_meta(&self) -> &PlanMeta {
        &self.meta
    }

    /// Append a new node into the logical plan, with specified `parent_ids`
    /// as its parent nodes. In order to do so, all specified parents must present in the
    /// logical plan.
    ///
    /// # Return
    ///   * If succeed, the id of the newly added node
    ///   * Otherwise, `IrError::ParentNodeNotExist`
    pub fn append_node(&mut self, mut node: Node, parent_ids: Vec<u32>) -> IrResult<u32> {
        let id = node.id;
        if !self.is_empty() && !parent_ids.is_empty() {
            let mut parent_nodes = vec![];
            for parent_id in parent_ids {
                if let Some(parent_node) = self.get_node(parent_id) {
                    parent_nodes.push(parent_node);
                } else {
                    return Err(IrError::ParentNodeNotExist(parent_id));
                }
            }
            for parent_node in parent_nodes {
                node.add_parent(parent_node.borrow().id);
                parent_node.borrow_mut().add_child(id);
            }
        }
        let node_rc = Rc::new(RefCell::new(node));
        self.nodes.insert(id as usize, node_rc.clone());
        self.max_node_id = std::cmp::max(self.max_node_id, id) + 1;

        Ok(id)
    }

    /// Append an operator into the logical plan, as a new node with `self.max_node_id` as its id.
    pub fn append_operator_as_node(
        &mut self, mut opr: pb::logical_plan::Operator, parent_ids: Vec<u32>,
    ) -> IrResult<u32> {
        use common_pb::expr_opr::Item;
        use pb::logical_plan::operator::Opr;

        let old_curr_node = self.meta.get_curr_node();
        let mut is_update_curr = false;
        if let Ok(meta) = STORE_META.read() {
            match &opr.opr {
                Some(Opr::Scan(scan)) => {
                    if let Some(alias) = &scan.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.max_node_id);
                    }
                    self.meta.set_curr_node(self.max_node_id);
                }
                Some(Opr::Edge(edge)) => {
                    if let Some(alias) = &edge.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.max_node_id);
                    }
                    self.meta.set_curr_node(self.max_node_id);
                }
                Some(Opr::Vertex(getv)) => {
                    if let Some(alias) = &getv.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.max_node_id);
                    }
                    self.meta.set_curr_node(self.max_node_id);
                }
                Some(Opr::Path(path)) => {
                    if let Some(alias) = &path.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.max_node_id);
                    }
                    self.meta.set_curr_node(self.max_node_id);
                }
                Some(Opr::Project(ref proj)) => {
                    is_update_curr = true;
                    if proj.mappings.len() == 1 {
                        // change current node as the tagged node
                        if let Some(expr) = &proj.mappings[0].expr {
                            if expr.operators.len() == 1 {
                                if let common_pb::ExprOpr { item: Some(Item::Var(var)) } =
                                    expr.operators.get(0).unwrap()
                                {
                                    if var.tag.is_some() && var.property.is_none()
                                    // tag as Head
                                    {
                                        if let Some(node) = self
                                            .meta
                                            .get_tag_node(&var.tag.clone().unwrap().try_into()?)
                                        {
                                            is_update_curr = false;
                                            self.meta.set_curr_node(node);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Some(Opr::Apply(apply_opr)) => {
                    if let Some(alias) = &apply_opr.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.max_node_id);
                    }
                    is_update_curr = true
                }
                Some(Opr::As(as_opr)) => {
                    if let Some(alias) = &as_opr.alias {
                        self.meta
                            .insert_tag_node(alias.clone().try_into().unwrap(), self.meta.get_curr_node());
                    }
                }
                Some(Opr::Select(_))
                | Some(Opr::OrderBy(_))
                | Some(Opr::Dedup(_))
                | Some(Opr::Limit(_))
                | Some(Opr::Sink(_)) => {} // do not change current node
                _ => is_update_curr = true,
            }
            opr.preprocess(&meta, &mut self.meta)?;
        }

        let new_curr_node_rst = self.append_node(Node::new(self.max_node_id, opr), parent_ids);
        if let Ok(new_curr_node) = &new_curr_node_rst {
            if is_update_curr {
                self.meta.set_curr_node(*new_curr_node);
            }
        } else {
            self.meta.set_curr_node(old_curr_node);
        }

        new_curr_node_rst
    }

    /// Remove a node from the logical plan, and do the following:
    /// * For each of its parent, if present, remove this node's id reference from its `children`.
    /// * For each of its children, remove this node's id reference from its `parent`, and if
    /// the child's parent becomes empty, the child must be removed recursively.
    ///
    ///  Note that this does not decrease `self.node_max_id`, which serves as the indication
    /// of new id of the plan.
    pub fn remove_node(&mut self, id: u32) -> Option<NodeType> {
        let node = self.nodes.remove(id as usize);
        if let Some(n) = &node {
            for p in &n.borrow().parents {
                if let Some(parent_node) = self.get_node(*p) {
                    parent_node.borrow_mut().children.remove(&id);
                }
            }

            for c in &n.borrow().children {
                if let Some(child) = self.get_node(*c) {
                    child.borrow_mut().parents.remove(&id);
                    if child.borrow().parents.is_empty() {
                        // Recursively remove the child
                        let _ = self.remove_node(*c);
                    }
                }
            }
        }
        node
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn root(&self) -> Option<NodeType> {
        self.nodes
            .iter()
            .next()
            .map(|(_, node)| node.clone())
    }

    /// Append branch plans to a certain node which has **no** children in this logical plan.
    pub fn append_branch_plans(&mut self, node: NodeType, subplans: Vec<LogicalPlan>) {
        if !node.borrow().children.is_empty() {
            return;
        } else {
            for subplan in subplans {
                if let Some((_, root)) = subplan.nodes.iter().next() {
                    node.borrow_mut()
                        .children
                        .insert(root.borrow().id);
                    root.borrow_mut()
                        .parents
                        .insert(node.borrow().id);
                }
                self.nodes.extend(subplan.nodes.into_iter());
            }
        }
    }

    /// From a branch node `root`, obtain the sub-plans (branch node excluded), each representing
    /// a branch of operators, till the merge node (merge node excluded).
    ///
    /// # Return
    ///   * the merge node and sub-plans if the `brach_node` is indeed a branch node (has more
    /// than one child), and its corresponding merge_node present.
    ///   * `None` and empty sub-plans if otherwise.
    pub fn get_branch_plans(&self, branch_node: NodeType) -> (Option<NodeType>, Vec<LogicalPlan>) {
        let mut plans = vec![];
        let mut merge_node_opt = None;
        if branch_node.borrow().children.len() > 1 {
            merge_node_opt = self.get_merge_node(branch_node.clone());
            if let Some(merge_node) = &merge_node_opt {
                for &child_node_id in &branch_node.borrow().children {
                    if let Some(child_node) = self.get_node(child_node_id) {
                        if let Some(subplan) = self.subplan(child_node, merge_node.clone()) {
                            plans.push(subplan)
                        }
                    }
                }
            }
        }

        (merge_node_opt, plans)
    }

    /// To construct a subplan from every node lying between `from_node` (included) and `to_node` (excluded)
    /// in the logical plan. Thus, for the subplan to be valid, `to_node` must refer to a
    /// downstream node against `from_node` in the plan.
    ///
    /// If there are some branches between `from_node` and `to_node`, there are two cases:
    /// * 1. `to_node` lies within a sub-branch. In this case, **NO** subplan can be produced;
    /// * 2. `to_node` is a downstream node of the merge node of the branch, then all branches
    /// of nodes must be included in the subplan. For example, F is a `from_node` which has two
    /// branches, namely F -> A1 -> B1 -> M, and F -> A2 -> B2 -> M. we have `to_node` T connected
    /// to M in the logical plan, as M -> T0 -> T, the subplan from F to T, must include the operators
    /// of F, A1, B1, A2, B2, M and T0.
    ///
    /// # Return
    ///   * The subplan in case of success,
    ///   * `None` if `from_node` is `to_node`, or could not arrive at `to_node` following the
    ///  plan, or there is a branch node in between, but fail to locate the corresponding merge node.
    pub fn subplan(&self, from_node: NodeType, to_node: NodeType) -> Option<LogicalPlan> {
        if from_node == to_node {
            return None;
        }
        let mut plan = LogicalPlan::with_root(clone_node(from_node.clone()));
        plan.meta = self.meta.clone();
        let mut curr_node = from_node;
        while curr_node.borrow().id != to_node.borrow().id {
            if curr_node.borrow().children.is_empty() {
                // While still not locating to_node
                return None;
            } else if curr_node.borrow().children.len() == 1 {
                let next_node_id = curr_node.borrow().get_first_child().unwrap();
                if let Some(next_node) = self.get_node(next_node_id) {
                    if next_node.borrow().id != to_node.borrow().id {
                        plan.append_node(clone_node(next_node.clone()), vec![curr_node.borrow().id])
                            .expect("append node to subplan error");
                    }
                    curr_node = next_node;
                } else {
                    return None;
                }
            } else {
                let (merge_node_opt, subplans) = self.get_branch_plans(curr_node.clone());
                if let Some(merge_node) = merge_node_opt {
                    plan.append_branch_plans(plan.get_node(curr_node.borrow().id).unwrap(), subplans);
                    if merge_node.borrow().id != to_node.borrow().id {
                        let merge_node_parent = merge_node
                            .borrow()
                            .parents
                            .iter()
                            .map(|x| *x)
                            .collect();
                        let merge_node_clone = clone_node(merge_node.clone());
                        plan.append_node(merge_node_clone, merge_node_parent)
                            .expect("append node to subplan error");
                    }
                    curr_node = merge_node;
                } else {
                    return None;
                }
            }
        }
        Some(plan)
    }

    /// Given a node that contains a subtask, which is typically an  `Apply` operator,
    /// try to extract the subtask as a logical plan.
    ///
    /// TODO(longbin): There may be an issue when the last node of a subtask is a merge node.
    pub fn extract_subplan(&self, node: NodeType) -> Option<LogicalPlan> {
        let node_borrow = node.borrow();
        match &node_borrow.opr.opr {
            Some(pb::logical_plan::operator::Opr::Apply(apply_opr)) => {
                if let Some(from_node) = self.get_node(apply_opr.subtask as u32) {
                    let mut curr_node = from_node.clone();
                    while let Some(to_node) = curr_node
                        .clone()
                        .borrow()
                        .get_first_child()
                        .and_then(|node_id| self.get_node(node_id))
                    {
                        curr_node = to_node.clone();
                    }
                    let parent_ids = curr_node
                        .borrow()
                        .parents
                        .iter()
                        .cloned()
                        .collect();
                    let mut subplan = if from_node == curr_node {
                        let mut p = LogicalPlan::default();
                        p.meta = self.meta.clone();
                        Some(p)
                    } else {
                        self.subplan(from_node, curr_node.clone())
                    };
                    if let Some(plan) = subplan.as_mut() {
                        plan.append_node(curr_node.borrow().clone(), parent_ids)
                            .expect("append node to subplan error!");
                    }
                    subplan
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl JsonIO for LogicalPlan {
    fn into_json<W: io::Write>(self, writer: W) -> io::Result<()> {
        let plan_pb: pb::LogicalPlan = self.into();
        serde_json::to_writer_pretty(writer, &plan_pb)?;

        Ok(())
    }

    fn from_json<R: io::Read>(reader: R) -> io::Result<Self> {
        let plan_pb = serde_json::from_reader::<_, pb::LogicalPlan>(reader)?;
        Self::try_from(plan_pb).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
    }
}

pub trait AsLogical {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()>;
}

fn preprocess_var(
    var: &mut common_pb::Variable, meta: &StoreMeta, plan_meta: &mut PlanMeta,
) -> IrResult<()> {
    if let Some(property) = var.property.as_mut() {
        if let Some(key) = property.item.as_mut() {
            match key {
                common_pb::property::Item::Key(key) => {
                    if let Some(schema) = &meta.schema {
                        if plan_meta.is_preprocess() && schema.is_column_id() {
                            let new_key = schema
                                .get_column_id_from_pb(key)
                                .unwrap_or(INVALID_META_ID)
                                .into();
                            debug!("column: {:?} -> {:?}", key, new_key);
                            *key = new_key;
                        }
                    }
                    debug!("add column ({:?}) to {:?}", key, var.tag);
                    plan_meta
                        .tag_node_meta_mut(
                            var.tag
                                .clone()
                                .map(|tag| tag.try_into())
                                .transpose()?
                                .as_ref(),
                        )?
                        .insert_column(key.clone().try_into()?);
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn preprocess_const(
    val: &mut common_pb::Value, meta: &StoreMeta, plan_meta: &mut PlanMeta,
) -> IrResult<()> {
    if let Some(schema) = &meta.schema {
        // A Const needs to be preprocessed only if it is while comparing a label (table)
        if plan_meta.is_preprocess() && schema.is_table_id() {
            if let Some(item) = val.item.as_mut() {
                match item {
                    common_pb::value::Item::Str(name) => {
                        let new_item = common_pb::value::Item::I32(
                            schema
                                .get_table_id(name)
                                .ok_or(IrError::TableNotExist(name.to_string()))?,
                        );
                        debug!("table: {:?} -> {:?}", item, new_item);
                        *item = new_item;
                    }
                    common_pb::value::Item::StrArray(names) => {
                        let new_item = common_pb::value::Item::I32Array(common_pb::I32Array {
                            item: names
                                .item
                                .iter()
                                .map(|name| {
                                    schema
                                        .get_table_id(name)
                                        .ok_or(IrError::TableNotExist(name.to_string()))
                                })
                                .collect::<IrResult<Vec<i32>>>()?,
                        });
                        debug!("table: {:?} -> {:?}", item, new_item);
                        *item = new_item;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

fn preprocess_expression(
    expr: &mut common_pb::Expression, meta: &StoreMeta, plan_meta: &mut PlanMeta,
) -> IrResult<()> {
    let mut count = 0;
    for opr in expr.operators.iter_mut() {
        if let Some(item) = opr.item.as_mut() {
            match item {
                common_pb::expr_opr::Item::Var(var) => {
                    if let Some(property) = var.property.as_mut() {
                        if let Some(key) = property.item.as_mut() {
                            match key {
                                common_pb::property::Item::Key(key) => {
                                    if let Some(schema) = &meta.schema {
                                        count = 0;
                                        if plan_meta.is_preprocess() && schema.is_column_id() {
                                            let new_key = schema
                                                .get_column_id_from_pb(key)
                                                .unwrap_or(INVALID_META_ID)
                                                .into();
                                            debug!("column: {:?} -> {:?}", key, new_key);
                                            *key = new_key;
                                        }
                                    }
                                    debug!("add column ({:?}) to {:?}", key, var.tag);
                                    plan_meta
                                        .tag_node_meta_mut(
                                            var.tag
                                                .clone()
                                                .map(|tag| tag.try_into())
                                                .transpose()?
                                                .as_ref(),
                                        )?
                                        .insert_column(key.clone().try_into()?);
                                }
                                common_pb::property::Item::Label(_) => count = 1,
                                _ => count = 0,
                            }
                        }
                    }
                }
                common_pb::expr_opr::Item::Logical(l) => {
                    if count == 1 {
                        // means previous one is LabelKey
                        // The logical operator of Eq, Ne, Lt, Le, Gt, Ge
                        if *l >= 0 && *l <= 7 {
                            count = 2; // indicates LabelKey <cmp>
                        }
                    } else {
                        count = 0;
                    }
                }
                common_pb::expr_opr::Item::Const(c) => {
                    if count == 2 {
                        // indicates LabelKey <cmp> labelValue
                        preprocess_const(c, meta, plan_meta)?;
                    }
                    count = 0;
                }
                common_pb::expr_opr::Item::Vars(vars) | common_pb::expr_opr::Item::VarMap(vars) => {
                    for var in &mut vars.keys {
                        preprocess_var(var, meta, plan_meta)?;
                    }
                    count = 0;
                }
                _ => count = 0,
            }
        }
    }

    Ok(())
}

fn preprocess_params(
    params: &mut pb::QueryParams, meta: &StoreMeta, plan_meta: &mut PlanMeta,
) -> IrResult<()> {
    if let Some(pred) = &mut params.predicate {
        preprocess_expression(pred, meta, plan_meta)?;
    }
    if let Some(schema) = &meta.schema {
        if plan_meta.is_preprocess() && schema.is_table_id() {
            for table in params.table_names.iter_mut() {
                let new_table = schema
                    .get_table_id_from_pb(table)
                    .ok_or(IrError::TableNotExist(format!("{:?}", table)))?
                    .into();
                debug!("table: {:?} -> {:?}", table, new_table);
                *table = new_table;
            }
        }
    }
    for column in params.columns.iter_mut() {
        if let Some(schema) = &meta.schema {
            if plan_meta.is_preprocess() && schema.is_column_id() {
                let new_column = schema
                    .get_column_id_from_pb(column)
                    .unwrap_or(INVALID_META_ID)
                    .into();
                debug!("column: {:?} -> {:?}", column, new_column);
                *column = new_column;
            }
        }
        debug!("add column ({:?}) to HEAD", column);
        plan_meta
            .curr_node_meta_mut()
            .insert_column(column.clone().try_into()?);
    }
    Ok(())
}

impl AsLogical for pb::Project {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        for mapping in self.mappings.iter_mut() {
            if let Some(expr) = &mut mapping.expr {
                preprocess_expression(expr, meta, plan_meta)?;
            }
        }
        Ok(())
    }
}

impl AsLogical for pb::Select {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if let Some(pred) = self.predicate.as_mut() {
            preprocess_expression(pred, meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::Scan {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        plan_meta.curr_node_meta_mut().is_add_column = true;
        if let Some(params) = self.params.as_mut() {
            preprocess_params(params, meta, plan_meta)?;
        }
        if let Some(index_pred) = self.idx_predicate.as_mut() {
            index_pred.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::EdgeExpand {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        plan_meta.curr_node_meta_mut().is_add_column = false;
        if let Some(params) = self.params.as_mut() {
            preprocess_params(params, meta, plan_meta)?;
        }
        plan_meta.curr_node_meta_mut().is_add_column = true;

        Ok(())
    }
}

impl AsLogical for pb::PathExpand {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        // PathExpand would never require adding columns
        plan_meta.curr_node_meta_mut().is_add_column = false;
        if let Some(base) = self.base.as_mut() {
            base.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::GetV {
    fn preprocess(&mut self, _meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        plan_meta.curr_node_meta_mut().is_add_column = true;
        Ok(())
    }
}

impl AsLogical for pb::Dedup {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::GroupBy {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        for mapping in self.mappings.iter_mut() {
            if let Some(key) = &mut mapping.key {
                preprocess_var(key, meta, plan_meta)?;
            }
        }
        for agg_fn in self.functions.iter_mut() {
            for var in agg_fn.vars.iter_mut() {
                preprocess_var(var, meta, plan_meta)?;
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::IndexPredicate {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        for and_pred in self.or_predicates.iter_mut() {
            for pred in and_pred.predicates.iter_mut() {
                if let Some(pred_key) = &mut pred.key {
                    if let Some(key_item) = pred_key.item.as_mut() {
                        match key_item {
                            common_pb::property::Item::Key(key) => {
                                if let Some(schema) = &meta.schema {
                                    if plan_meta.is_preprocess() && schema.is_column_id() {
                                        let new_key = schema
                                            .get_column_id_from_pb(key)
                                            .unwrap_or(INVALID_META_ID)
                                            .into();
                                        debug!("column: {:?} -> {:?}", key, new_key);
                                        *key = new_key;
                                    }
                                }
                                debug!("add column ({:?}) to HEAD", key);
                                plan_meta
                                    .curr_node_meta_mut()
                                    .insert_column(key.clone().try_into()?);
                            }
                            common_pb::property::Item::Label(_) => {
                                if let Some(val) = pred.value.as_mut() {
                                    preprocess_const(val, meta, plan_meta)?;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::OrderBy {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        for pair in self.pairs.iter_mut() {
            if let Some(key) = &mut pair.key {
                preprocess_var(key, meta, plan_meta)?;
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::Limit {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::Join {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        for left_key in self.left_keys.iter_mut() {
            preprocess_var(left_key, meta, plan_meta)?
        }
        for right_key in self.right_keys.iter_mut() {
            preprocess_var(right_key, meta, plan_meta)?
        }
        Ok(())
    }
}

impl AsLogical for pb::Sink {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::Apply {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::logical_plan::Operator {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> IrResult<()> {
        use pb::logical_plan::operator::Opr;
        if let Some(opr) = self.opr.as_mut() {
            match opr {
                Opr::Project(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Select(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Scan(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Edge(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Path(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Vertex(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Dedup(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::GroupBy(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::OrderBy(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Limit(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Join(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Sink(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Apply(opr) => opr.preprocess(meta, plan_meta)?,
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra::logical_plan::operator::Opr;
    use ir_common::generated::common::property::Item;

    use super::*;
    use crate::plan::meta::set_schema_simple;

    #[test]
    fn logical_plan_construct() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::default();

        let id = plan
            .append_operator_as_node(opr.clone(), vec![])
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.max_node_id, 1);
        let node0 = plan.get_node(0).unwrap().clone();

        let id = plan
            .append_operator_as_node(opr.clone(), vec![0])
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.max_node_id, 2);
        let node1 = plan.get_node(1).unwrap().clone();

        let parents = node1
            .borrow()
            .parents
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(parents, vec![0]);

        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![1]);

        let id = plan
            .append_operator_as_node(opr.clone(), vec![0, 1])
            .unwrap();
        assert_eq!(id, 2);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan.max_node_id, 3);
        let node2 = plan.get_node(2).unwrap().clone();

        let parents = node2
            .borrow()
            .parents
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(parents, vec![0, 1]);

        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![1, 2]);

        let children = node1
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![2]);

        let node2 = plan.remove_node(2);
        assert_eq!(node2.unwrap().borrow().id, 2);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.max_node_id, 3);
        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![1]);

        let children = node1
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert!(children.is_empty());

        let _id = plan.append_operator_as_node(opr.clone(), vec![0, 2]);
        match _id.err().unwrap() {
            IrError::ParentNodeNotExist(node) => assert_eq!(node, 2),
            _ => panic!("wrong error type"),
        }
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.max_node_id, 3);
        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![1]);

        // add node2 back again for further testing recursive removal
        let _ = plan
            .append_operator_as_node(opr.clone(), vec![0, 1])
            .unwrap();
        let node3 = plan.get_node(3).unwrap();
        let _ = plan.remove_node(1);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.max_node_id, 4);
        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![3]);

        let parents = node3
            .borrow()
            .parents
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(parents, vec![0]);
    }

    #[test]
    fn logical_plan_from_pb() {
        let opr = pb::logical_plan::Operator { opr: None };
        let root_pb = pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![1, 2] };
        let node1_pb = pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![2] };
        let node2_pb = pb::logical_plan::Node { opr: Some(opr.clone()), children: vec![] };
        let plan_pb = pb::LogicalPlan { nodes: vec![root_pb, node1_pb, node2_pb] };

        let plan = LogicalPlan::try_from(plan_pb).unwrap();
        assert_eq!(plan.len(), 3);
        let node0 = plan.get_node(0).unwrap();
        let node1 = plan.get_node(1).unwrap();
        let node2 = plan.get_node(2).unwrap();

        let children = node0
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![1, 2]);

        let children = node1
            .borrow()
            .children
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(children, vec![2]);

        let parents = node1
            .borrow()
            .parents
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(parents, vec![0]);

        let parents = node2
            .borrow()
            .parents
            .iter()
            .map(|x| *x)
            .collect::<Vec<u32>>();
        assert_eq!(parents, vec![0, 1]);
    }

    #[test]
    fn logical_plan_into_pb() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::default();

        let _ = plan
            .append_operator_as_node(opr.clone(), vec![])
            .unwrap();
        let _ = plan
            .append_operator_as_node(opr.clone(), vec![0])
            .unwrap();
        let _ = plan
            .append_operator_as_node(opr.clone(), vec![0])
            .unwrap();

        let _ = plan.remove_node(1);

        let plan_pb = pb::LogicalPlan::from(plan);
        assert_eq!(plan_pb.nodes.len(), 2);

        let node0 = &plan_pb.nodes[0];
        let node1 = &plan_pb.nodes[1];
        assert_eq!(node0.opr, Some(opr.clone()));
        assert_eq!(node0.children, vec![1]);
        assert_eq!(node1.opr, Some(opr.clone()));
        assert!(node1.children.is_empty());
    }

    #[test]
    fn prep_expression() {
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_preprocess(true);
        plan_meta.insert_tag_node("a".into(), 1);
        plan_meta.insert_tag_node("b".into(), 2);
        plan_meta.curr_node_meta_mut().is_add_column = true;
        plan_meta
            .tag_node_meta_mut(Some(&"a".into()))
            .unwrap()
            .is_add_column = true;
        plan_meta
            .tag_node_meta_mut(Some(&"b".into()))
            .unwrap()
            .is_add_column = true;

        set_schema_simple(
            vec![("person".to_string(), 0), ("software".to_string(), 1)],
            vec![("knows".to_string(), 0), ("creates".to_string(), 1)],
            vec![("id".to_string(), 0), ("name".to_string(), 1), ("age".to_string(), 2)],
        );

        let mut expression = str_to_expr_pb("@.~label == \"person\"".to_string()).unwrap();
        preprocess_expression(&mut expression, &STORE_META.read().unwrap(), &mut plan_meta).unwrap();
        let opr = expression.operators.get(2).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Const(val) => match val.item.unwrap() {
                common_pb::value::Item::I32(i) => assert_eq!(i, 0),
                _ => panic!(),
            },
            _ => panic!(),
        }

        let mut expression =
            str_to_expr_pb("@.~label within [\"person\", \"software\"]".to_string()).unwrap();
        preprocess_expression(&mut expression, &STORE_META.read().unwrap(), &mut plan_meta).unwrap();
        let opr = expression.operators.get(2).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Const(val) => match val.item.unwrap() {
                common_pb::value::Item::I32Array(arr) => {
                    assert_eq!(arr.item, vec![0, 1]);
                }
                _ => panic!(),
            },
            _ => panic!(),
        }

        let mut expression =
            str_to_expr_pb("(@.name == \"person\") && @a.~label == \"knows\"".to_string()).unwrap();
        preprocess_expression(&mut expression, &STORE_META.read().unwrap(), &mut plan_meta).unwrap();

        // person should not be mapped, as name is not a label key
        let opr = expression.operators.get(3).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Const(val) => match val.item.unwrap() {
                common_pb::value::Item::Str(str) => assert_eq!(str, "person".to_string()),
                _ => panic!(),
            },
            _ => panic!(),
        }
        // "knows maps to 0"
        let opr = expression.operators.get(8).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Const(val) => match val.item.unwrap() {
                common_pb::value::Item::I32(i) => assert_eq!(i, 0),
                _ => panic!(),
            },
            _ => panic!(),
        }

        // Assert whether the columns have been updated in PlanMeta
        assert_eq!(
            plan_meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            // has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // name maps to 1
        let mut expression = str_to_expr_pb("@a.name == \"John\"".to_string()).unwrap();
        preprocess_expression(&mut expression, &STORE_META.read().unwrap(), &mut plan_meta).unwrap();
        let opr = expression.operators.get(0).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Var(var) => {
                match var.clone().property.unwrap().item.unwrap() {
                    Item::Key(key) => assert_eq!(key, 1.into()),
                    _ => panic!(),
                }
                assert_eq!(var.tag.unwrap(), "a".into());
            }
            _ => panic!(),
        }

        // Assert whether the columns have been updated in PlanMeta
        assert_eq!(
            plan_meta
                .get_node_meta(1)
                .unwrap()
                .get_columns(),
            // node1 with tag a has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let mut expression = str_to_expr_pb("{@a.name, @b.id}".to_string()).unwrap();
        preprocess_expression(&mut expression, &STORE_META.read().unwrap(), &mut plan_meta).unwrap();
        let opr = expression.operators.get(0).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::VarMap(vars) => {
                let var1 = vars.keys[0].clone();
                match var1.property.unwrap().item.unwrap() {
                    Item::Key(key) => assert_eq!(key, 1.into()),
                    _ => panic!(),
                }
                assert_eq!(var1.tag.unwrap(), "a".into());
                let var2 = vars.keys[1].clone();
                match var2.property.unwrap().item.unwrap() {
                    Item::Key(key) => assert_eq!(key, 0.into()),
                    _ => panic!(),
                }
                assert_eq!(var2.tag.unwrap(), "b".into());
            }
            _ => panic!(),
        }

        // Assert whether the columns have been updated in PlanMeta
        assert_eq!(
            plan_meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            // node1 with tag a has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            plan_meta
                .get_node_meta(2)
                .unwrap()
                .get_columns(),
            // node2 with tag b has a new column "id", which is mapped to 0
            &vec![0.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn preprocess_scan() {
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_preprocess(true);
        plan_meta.insert_tag_node("a".into(), 1);
        plan_meta.curr_node_meta_mut().is_add_column = true;
        plan_meta
            .tag_node_meta_mut(Some(&"a".into()))
            .unwrap()
            .is_add_column = true;
        set_schema_simple(
            vec![("person".to_string(), 0), ("software".to_string(), 1)],
            vec![("knows".to_string(), 0), ("creates".to_string(), 1)],
            vec![("id".to_string(), 0), ("name".to_string(), 1), ("age".to_string(), 2)],
        );
        let mut scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec!["person".into()],
                columns: vec!["age".into(), "name".into()],
                limit: None,
                predicate: Some(
                    str_to_expr_pb("@a.~label > \"person\" && @a.age == 10".to_string()).unwrap(),
                ),
                requirements: vec![],
            }),
            idx_predicate: Some(vec!["software".to_string()].into()),
        };
        scan.preprocess(&STORE_META.read().unwrap(), &mut plan_meta)
            .unwrap();
        assert_eq!(scan.clone().params.unwrap().table_names[0], 0.into());
        assert_eq!(
            scan.idx_predicate.unwrap().or_predicates[0].predicates[0]
                .value
                .clone()
                .unwrap(),
            1.into()
        );
        let operators = scan
            .params
            .clone()
            .unwrap()
            .predicate
            .unwrap()
            .operators;
        match operators.get(2).unwrap().item.as_ref().unwrap() {
            common_pb::expr_opr::Item::Const(val) => assert_eq!(val.clone(), 0.into()),
            _ => panic!(),
        }
        match operators.get(4).unwrap().item.as_ref().unwrap() {
            common_pb::expr_opr::Item::Var(var) => {
                match var
                    .property
                    .as_ref()
                    .unwrap()
                    .item
                    .clone()
                    .unwrap()
                {
                    Item::Key(key) => assert_eq!(key, 2.into()),
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
        // Assert whether the columns have been updated in PlanMeta
        assert_eq!(
            plan_meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            &vec![1.into(), 2.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            plan_meta
                .get_node_meta(1)
                .unwrap()
                .get_columns(),
            &vec![2.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn tag_maintain_case1() {
        let mut plan = LogicalPlan::default();
        // g.V().hasLabel("person").has("age", 27).valueMap("age", "name", "id")

        // g.V()
        let scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };
        plan.append_operator_as_node(scan.into(), vec![])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);

        // .hasLabel("person")
        let select = pb::Select { predicate: str_to_expr_pb("@.~label == \"person\"".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![0])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);

        // .has("age", 27)
        let select = pb::Select { predicate: str_to_expr_pb("@.age == 27".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![1])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);
        assert_eq!(
            plan.meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            &vec!["age".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .valueMap("age", "name", "id")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("{@.name, @.age, @.id}".to_string()).ok(),
                alias: None,
            }],
            is_append: false,
        };
        plan.append_operator_as_node(project.into(), vec![2])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 3);
        assert_eq!(
            plan.meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            &vec!["age".into(), "id".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn tag_maintain_case2() {
        let mut plan = LogicalPlan::default();
        // g.V().out().as("here").has("lang", "java").select("here").values("name")
        let scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };
        plan.append_operator_as_node(scan.into(), vec![])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);

        // .out().as("here")
        let expand = pb::EdgeExpand {
            v_tag: Some("a".into()),
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: false,
            alias: Some("here".into()),
        };
        plan.append_operator_as_node(expand.into(), vec![0])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);

        // .has("lang", "Java")
        let select = pb::Select { predicate: str_to_expr_pb("@.lang == \"Java\"".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![1])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);
        assert_eq!(
            plan.meta
                .get_node_meta(1)
                .unwrap()
                .get_columns(),
            &vec!["lang".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .select("here")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@here".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![2])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);

        // .values("name")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@.name".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![3])
            .unwrap();
        assert_eq!(
            plan.meta
                .get_node_meta(1)
                .unwrap()
                .get_columns(),
            &vec!["lang".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(plan.meta.get_curr_node(), 4);
    }

    #[test]
    fn tag_maintain_case3() {
        let mut plan = LogicalPlan::default();
        // g.V().outE().as("e").inV().as("v").select("e").order().by("weight").select("v").values("name").dedup()

        // g.V()
        let scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };
        plan.append_operator_as_node(scan.into(), vec![])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);

        // .outE().as("e")
        let expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: true,
            alias: Some("e".into()),
        };
        plan.append_operator_as_node(expand.into(), vec![0])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);
        assert_eq!(plan.meta.get_tag_node(&"e".into()).unwrap(), 1);

        // .inV().as("v")
        let getv = pb::GetV { tag: None, opt: 1, alias: Some("v".into()) };
        plan.append_operator_as_node(getv.into(), vec![1])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 2);
        assert_eq!(plan.meta.get_tag_node(&"v".into()).unwrap(), 2);

        // .select("e")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@e".to_string()).ok(),
                alias: Some("project_e".into()),
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![2])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);

        // .order().by("weight")
        let orderby = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(common_pb::Variable {
                    tag: None,
                    property: Some(common_pb::Property {
                        item: Some(common_pb::property::Item::Key("weight".into())),
                    }),
                }),
                order: 1,
            }],
            limit: None,
        };
        plan.append_operator_as_node(orderby.into(), vec![3])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 1);
        assert_eq!(
            plan.meta
                .curr_node_meta()
                .unwrap()
                .get_columns()
                .clone(),
            vec!["weight".into()].into_iter().collect()
        );

        // select("v")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@v".to_string()).ok(),
                alias: Some("project_v".into()),
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![4])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 2);

        // .values("name")
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@.name".to_string()).ok(),
                alias: Some("name".into()),
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![5])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 6);
        assert_eq!(
            plan.meta
                .get_node_meta(2)
                .unwrap()
                .get_columns()
                .clone(),
            vec!["name".into()].into_iter().collect()
        );
    }

    #[test]
    fn tag_maintain_case4() {
        let mut plan = LogicalPlan::default();
        // g.V("person").has("name", "John").as('a').outE("knows").as('b')
        //  .has("date", 20200101).inV().as('c').has('id', 10)
        //  .select('a').by(valueMap('age', "name"))
        //  .select('c').by(valueMap('id', "name"))

        // g.V("person")
        let scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec!["person".into()],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };
        let mut opr_id = plan
            .append_operator_as_node(scan.into(), vec![])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);

        // .has("name", "John")
        let select = pb::Select { predicate: str_to_expr_pb("@.name == \"John\"".to_string()).ok() };
        opr_id = plan
            .append_operator_as_node(select.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);
        assert_eq!(
            plan.meta
                .get_node_meta(0)
                .unwrap()
                .get_columns(),
            &vec!["name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .as('a')
        let as_opr = pb::As { alias: Some("a".into()) };
        opr_id = plan
            .append_operator_as_node(as_opr.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), 0);
        assert_eq!(plan.meta.get_tag_node(&"a".into()).unwrap(), 0);

        // outE("knows").as('b').has("date", 20200101)
        let expand = pb::EdgeExpand {
            v_tag: Some("a".into()),
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec!["knows".into()],
                columns: vec![],
                limit: None,
                predicate: str_to_expr_pb("@.date == 20200101".to_string()).ok(),
                requirements: vec![],
            }),
            is_edge: true,
            alias: Some("b".into()),
        };
        opr_id = plan
            .append_operator_as_node(expand.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), opr_id as u32);
        assert_eq!(plan.meta.get_tag_node(&"b".into()).unwrap(), opr_id as u32);

        //.inV().as('c')
        let getv = pb::GetV { tag: None, opt: 2, alias: Some("c".into()) };
        opr_id = plan
            .append_operator_as_node(getv.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), opr_id as u32);
        assert_eq!(plan.meta.get_tag_node(&"c".into()).unwrap(), opr_id as u32);

        // .has("id", 10)
        let select = pb::Select { predicate: str_to_expr_pb("@.id == 10".to_string()).ok() };
        opr_id = plan
            .append_operator_as_node(select.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), opr_id as u32 - 1);
        assert_eq!(
            plan.meta
                .get_node_meta(opr_id as u32 - 1)
                .unwrap()
                .get_columns(),
            &vec!["id".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .select('a').by(valueMap('age', "name"))
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("{@a.age, @a.name}".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        opr_id = plan
            .append_operator_as_node(project.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), opr_id as u32);
        assert_eq!(
            plan.meta
                .get_node_meta(plan.meta.get_tag_node(&"a".into()).unwrap())
                .unwrap()
                .get_columns(),
            &vec!["age".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .select('c').by(valueMap('age', "name"))
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("{@c.age, @c.name}".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        opr_id = plan
            .append_operator_as_node(project.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.meta.get_curr_node(), opr_id as u32);
        assert_eq!(
            plan.meta
                .get_node_meta(plan.meta.get_tag_node(&"c".into()).unwrap())
                .unwrap()
                .get_columns(),
            &vec!["age".into(), "id".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn extract_subplan_from_apply_case1() {
        let mut plan = LogicalPlan::default();
        // g.V().as("v").where(out().as("o").has("lang", "java")).select("v").values("name")

        // g.V("person")
        let scan = pb::Scan {
            scan_opt: 0,
            alias: Some("v".into()),
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };

        let opr_id = plan
            .append_operator_as_node(scan.into(), vec![])
            .unwrap();

        // .out().as("o")
        let expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: false,
            alias: Some("o".into()),
        };

        let root_id = plan
            .append_operator_as_node(expand.into(), vec![])
            .unwrap();

        // .has("lang", "Java")
        let select = pb::Select { predicate: str_to_expr_pb("@.lang == \"Java\"".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![root_id])
            .unwrap();

        let apply = pb::Apply { join_kind: 4, tags: vec![], subtask: root_id as i32, alias: None };
        let opr_id = plan
            .append_operator_as_node(apply.into(), vec![opr_id])
            .unwrap();

        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@v.name".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![opr_id])
            .unwrap();

        let subplan = plan
            .extract_subplan(plan.get_node(opr_id).unwrap())
            .unwrap();
        assert_eq!(subplan.len(), 2);
        for (id, node) in &subplan.nodes {
            let node_ref = node.borrow();
            if id == root_id as usize {
                match node_ref.opr.opr.as_ref().unwrap() {
                    Opr::Edge(_) => {}
                    _ => panic!("should be edge expand"),
                }
                assert_eq!(subplan.meta.get_tag_node(&"o".into()).unwrap(), root_id);
                assert_eq!(
                    subplan
                        .meta
                        .get_node_meta(root_id)
                        .unwrap()
                        .get_columns()
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                    vec!["lang".into()]
                )
            } else {
                match node_ref.opr.opr.as_ref().unwrap() {
                    Opr::Select(_) => {}
                    _ => panic!("should be select"),
                }
            }
        }
    }

    #[test]
    fn extract_subplan_from_apply_case2() {
        let mut plan = LogicalPlan::default();
        // g.V().where(not(out("created"))).values("name")
        let scan = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        };

        plan.append_operator_as_node(scan.into(), vec![])
            .unwrap();

        let expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: false,
            alias: None,
        };
        let root_id = plan
            .append_operator_as_node(expand.into(), vec![])
            .unwrap();

        let apply = pb::Apply { join_kind: 5, tags: vec![], subtask: root_id as i32, alias: None };
        plan.append_operator_as_node(apply.into(), vec![0])
            .unwrap();

        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@.name".to_string()).ok(),
                alias: Some("name".into()),
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.into(), vec![2])
            .unwrap();

        let sink = pb::Sink {
            tags: vec![common_pb::NameOrIdKey { key: Some("name".into()) }],
            id_name_mappings: vec![],
        };
        plan.append_operator_as_node(sink.into(), vec![3])
            .unwrap();

        let subplan = plan
            .extract_subplan(plan.get_node(2).unwrap())
            .unwrap();
        assert_eq!(subplan.len(), 1);
        match subplan
            .get_node(1)
            .unwrap()
            .borrow()
            .opr
            .opr
            .as_ref()
            .unwrap()
        {
            Opr::Edge(_) => {}
            _ => panic!("wrong operator: should be `EdgeExpand`"),
        }
    }

    // The plan looks like:
    //       root
    //       / \
    //      1    2
    //     / \   |
    //    3   4  |
    //    \   /  |
    //      5    |
    //       \  /
    //         6
    //         |
    //         7
    fn create_logical_plan() -> LogicalPlan {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(opr.clone(), vec![])
            .unwrap(); // root
        plan.append_operator_as_node(opr.clone(), vec![0])
            .unwrap(); // node 1
        plan.append_operator_as_node(opr.clone(), vec![0])
            .unwrap(); // node 2
        plan.append_operator_as_node(opr.clone(), vec![1])
            .unwrap(); // node 3
        plan.append_operator_as_node(opr.clone(), vec![1])
            .unwrap(); // node 4
        plan.append_operator_as_node(opr.clone(), vec![3, 4])
            .unwrap(); // node 5
        plan.append_operator_as_node(opr.clone(), vec![2, 5])
            .unwrap(); // node 6
        plan.append_operator_as_node(opr.clone(), vec![6])
            .unwrap(); // node 7

        plan
    }

    #[test]
    fn get_merge_node() {
        let plan = create_logical_plan();
        let merge_node = plan.get_merge_node(plan.get_node(1).unwrap());
        assert_eq!(merge_node, plan.get_node(5));
        let merge_node = plan.get_merge_node(plan.get_node(0).unwrap());
        assert_eq!(merge_node, plan.get_node(6));
        // Not a branch node
        let merge_node = plan.get_merge_node(plan.get_node(2).unwrap());
        assert!(merge_node.is_none());
    }

    #[test]
    fn merge_branch_plans() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::with_root(Node::new(0, opr.clone()));

        let mut subplan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        subplan1
            .append_node(Node::new(3, opr.clone()), vec![1])
            .unwrap();
        subplan1
            .append_node(Node::new(4, opr.clone()), vec![1])
            .unwrap();
        subplan1
            .append_node(Node::new(5, opr.clone()), vec![3, 4])
            .unwrap();

        let subplan2 = LogicalPlan::with_root(Node::new(2, opr.clone()));

        plan.append_branch_plans(plan.get_node(0).unwrap(), vec![subplan1, subplan2]);
        let mut expected_plan = create_logical_plan();
        expected_plan.remove_node(6);

        plan.append_node(Node::new(6, opr.clone()), vec![2, 5])
            .unwrap();
        plan.append_node(Node::new(7, opr.clone()), vec![6])
            .unwrap();

        assert_eq!(plan, create_logical_plan());
    }

    #[test]
    fn subplan() {
        let plan = create_logical_plan();
        let opr = pb::logical_plan::Operator { opr: None };
        let subplan = plan.subplan(plan.get_node(2).unwrap(), plan.get_node(7).unwrap());
        let mut expected_plan = LogicalPlan::with_root(Node::new(2, opr.clone()));
        expected_plan
            .append_node(Node::new(6, opr.clone()), vec![2])
            .unwrap();
        assert_eq!(subplan.unwrap(), expected_plan);

        // The node 3 is at one of the branches, which is incomplete and hence invalid subplan
        let subplan = plan.subplan(plan.get_node(1).unwrap(), plan.get_node(3).unwrap());
        assert!(subplan.is_none());

        let subplan = plan.subplan(plan.get_node(1).unwrap(), plan.get_node(6).unwrap());
        let mut expected_plan = LogicalPlan::with_root(Node::new(1, opr.clone()));
        expected_plan
            .append_node(Node::new(3, opr.clone()), vec![1])
            .unwrap();
        expected_plan
            .append_node(Node::new(4, opr.clone()), vec![1])
            .unwrap();
        expected_plan
            .append_node(Node::new(5, opr.clone()), vec![3, 4])
            .unwrap();

        assert_eq!(subplan.unwrap(), expected_plan);
    }

    #[test]
    fn get_branch_plans() {
        let plan = create_logical_plan();
        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(1).unwrap());
        let opr = pb::logical_plan::Operator { opr: None };

        let plan1 = LogicalPlan::with_root(Node::new(3, opr.clone()));
        let plan2 = LogicalPlan::with_root(Node::new(4, opr.clone()));

        assert_eq!(merge_node, plan.get_node(5));
        assert_eq!(subplans, vec![plan1, plan2]);

        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(0).unwrap());
        let mut plan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        plan1
            .append_node(Node::new(3, opr.clone()), vec![1])
            .unwrap();
        plan1
            .append_node(Node::new(4, opr.clone()), vec![1])
            .unwrap();
        plan1
            .append_node(Node::new(5, opr.clone()), vec![3, 4])
            .unwrap();
        let plan2 = LogicalPlan::with_root(Node::new(2, opr.clone()));

        assert_eq!(merge_node, plan.get_node(6));
        assert_eq!(subplans, vec![plan1, plan2]);

        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(opr.clone(), vec![])
            .unwrap(); // root
        plan.append_operator_as_node(opr.clone(), vec![0])
            .unwrap(); // node 1
        plan.append_operator_as_node(opr.clone(), vec![0])
            .unwrap(); // node 2
        plan.append_operator_as_node(opr.clone(), vec![0])
            .unwrap(); // node 3
        plan.append_operator_as_node(opr.clone(), vec![1, 2, 3])
            .unwrap(); // node 4

        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(0).unwrap());
        let plan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        let plan2 = LogicalPlan::with_root(Node::new(2, opr.clone()));
        let plan3 = LogicalPlan::with_root(Node::new(3, opr.clone()));

        assert_eq!(merge_node, plan.get_node(4));
        assert_eq!(subplans, vec![plan1, plan2, plan3]);
    }
}
