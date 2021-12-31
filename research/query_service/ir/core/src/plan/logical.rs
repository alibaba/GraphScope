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
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io;
use std::iter::FromIterator;
use std::rc::Rc;

use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;
use vec_map::VecMap;

use crate::plan::meta::{StoreMeta, STORE_META};
use crate::JsonIO;

/// Record any error while transforming ir to a pegasus physical plan
#[derive(Debug, Clone)]
pub enum LogicalError {
    TableNotExist(String),
    ColumnNotExist(String),
    TagNotExist(NameOrId),
    ParsePbError(ParsePbError),
    Unsupported,
}

pub type LogicalResult<T> = Result<T, LogicalError>;

impl fmt::Display for LogicalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogicalError::TableNotExist(s) => write!(f, "the given table(label): {:?} does not exist", s),
            LogicalError::ColumnNotExist(s) => write!(f, "the given column: {:?} does not exist", s),
            LogicalError::TagNotExist(tag) => write!(f, "the given tag: {:?} does not exist", tag),
            LogicalError::ParsePbError(err) => write!(f, "parse pb error: {:?}", err),
            LogicalError::Unsupported => write!(f, "the function has not been supported"),
        }
    }
}

impl std::error::Error for LogicalError {}

impl From<ParsePbError> for LogicalError {
    fn from(err: ParsePbError) -> Self {
        Self::ParsePbError(err)
    }
}

/// An internal representation of the pb-[`Node`].
///
/// [`Node`]: crate::generated::algebra::logical_plan::Node
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Node {
    pub id: u32,
    pub opr: pb::logical_plan::Operator,
    pub parents: BTreeSet<u32>,
    pub children: BTreeSet<u32>,
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
pub(crate) struct LogicalPlan {
    pub nodes: VecMap<NodeType>,
    /// To record the total number of operators ever created in the logical plan,
    /// **ignorant of the removed nodes**
    pub total_size: usize,
    /// The tag data of the logical plan
    pub plan_meta: PlanMeta,
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

fn parse_pb_node(node_pbs: &Vec<pb::logical_plan::Node>, nodes: &Vec<NodeType>) -> ParsePbResult<()> {
    for (node_pb, node) in node_pbs.iter().zip(nodes.iter()) {
        for child_id in &node_pb.children {
            node.borrow_mut().add_child(*child_id as u32);
            if let Some(child_node) = nodes.get(*child_id as usize) {
                child_node
                    .borrow_mut()
                    .add_parent(node.borrow().id as u32);
            } else {
                return Err(ParsePbError::from("the child id is out of index".to_string()));
            }
        }
    }

    Ok(())
}

impl TryFrom<pb::LogicalPlan> for LogicalPlan {
    type Error = ParsePbError;

    fn try_from(pb: pb::LogicalPlan) -> Result<Self, Self::Error> {
        let nodes_pb = pb.nodes;
        let mut nodes = Vec::<NodeType>::with_capacity(nodes_pb.len());

        for (id, node) in nodes_pb.iter().enumerate() {
            if let Some(opr) = &node.opr {
                nodes.push(Rc::new(RefCell::new(Node::new(id as u32, opr.clone()))));
            } else {
                return Err(ParsePbError::from("do not specify operator in a node"));
            }
        }

        parse_pb_node(&nodes_pb, &nodes)?;

        let plan = LogicalPlan {
            total_size: nodes.len(),
            nodes: VecMap::from_iter(nodes.into_iter().enumerate()),
            plan_meta: PlanMeta::default(),
        };

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
        Self { nodes, total_size: 1, plan_meta: PlanMeta::default() }
    }

    /// Get a node reference from the logical plan
    pub fn get_node(&self, id: u32) -> Option<NodeType> {
        self.nodes.get(id as usize).cloned()
    }

    /// Append a new node into the logical plan, with specified `parent_ids`
    /// as its parent node. In order to do so, all specified parents must present in the
    /// logical plan.
    ///
    /// # Return
    ///   * If succeed, the id of the newly added node
    ///   * Otherwise, -1
    pub fn append_node(&mut self, mut node: Node, parent_ids: Vec<u32>) -> i32 {
        let id = node.id;
        if !self.is_empty() && !parent_ids.is_empty() {
            let mut parent_nodes = vec![];
            for parent_id in parent_ids {
                if let Some(parent_node) = self.get_node(parent_id) {
                    parent_nodes.push(parent_node);
                } else {
                    return -1;
                }
            }
            for parent_node in parent_nodes {
                node.add_parent(parent_node.borrow().id);
                parent_node.borrow_mut().add_child(id);
            }
        }
        let node_rc = Rc::new(RefCell::new(node));
        self.nodes.insert(id as usize, node_rc.clone());

        match &node_rc.borrow().opr.opr {
            Some(pb::logical_plan::operator::Opr::Scan(scan)) => {
                if let Some(alias) = &scan.alias {
                    self.plan_meta
                        .add_tag_node(alias.clone().try_into().unwrap(), id);
                }
            }
            Some(pb::logical_plan::operator::Opr::Vertex(vertex)) => {
                if let Some(alias) = &vertex.alias {
                    self.plan_meta
                        .add_tag_node(alias.clone().try_into().unwrap(), id);
                }
            }
            Some(pb::logical_plan::operator::Opr::Edge(edge)) => {
                if let Some(alias) = &edge.alias {
                    self.plan_meta
                        .add_tag_node(alias.clone().try_into().unwrap(), id);
                }
            }
            Some(pb::logical_plan::operator::Opr::Path(path)) => {
                if let Some(alias) = &path.alias {
                    self.plan_meta
                        .add_tag_node(alias.clone().try_into().unwrap(), id);
                }
            }
            Some(pb::logical_plan::operator::Opr::As(as_opr)) => {
                if let Some(alias_) = &as_opr.alias {
                    if let Some(alias) = &alias_.alias {
                        self.plan_meta
                            .add_tag_node(alias.clone().try_into().unwrap(), self.plan_meta.curr_node);
                    }
                }
            }
            _ => {}
        }
        self.total_size = id as usize + 1;

        id as i32
    }

    /// Append an operator into the logical plan, as a new node with `self.total_size` as its id.
    pub fn append_operator_as_node(
        &mut self, mut opr: pb::logical_plan::Operator, parent_ids: Vec<u32>,
    ) -> LogicalResult<i32> {
        let old_curr_node = self.plan_meta.curr_node;
        let mut is_update_curr = false;
        if let Ok(meta) = STORE_META.read() {
            match opr.opr {
                Some(pb::logical_plan::operator::Opr::Edge(_)) => {
                    self.plan_meta
                        .update_curr_node(self.total_size as u32);
                }
                Some(pb::logical_plan::operator::Opr::As(_))
                | Some(pb::logical_plan::operator::Opr::Select(_)) => {}
                _ => is_update_curr = true,
            }
            opr.preprocess(&meta, &mut self.plan_meta)?;
        }
        let new_curr_node = self.append_node(Node::new(self.total_size as u32, opr), parent_ids);
        if new_curr_node < 0 {
            self.plan_meta.update_curr_node(old_curr_node);
        } else {
            if is_update_curr {
                self.plan_meta
                    .update_curr_node(new_curr_node as u32);
            }
        }

        Ok(new_curr_node)
    }

    /// Remove a node from the logical plan, and do the following:
    /// * For each of its parent, if present, remove this node's id reference from its `children`.
    /// * For each of its children, remove this node's id reference from its `parent`, and if
    /// the child's parent becomes empty, the child must be removed recursively.
    ///
    ///  Note that this does not decrease `self.total_size`, which serves as the indication
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
        let mut curr_node = from_node;
        while curr_node.borrow().id != to_node.borrow().id {
            if curr_node.borrow().children.is_empty() {
                // While still not locating to_node
                return None;
            } else if curr_node.borrow().children.len() == 1 {
                let next_node_id = curr_node.borrow().get_first_child().unwrap();
                if let Some(next_node) = self.get_node(next_node_id) {
                    if next_node.borrow().id != to_node.borrow().id {
                        plan.append_node(clone_node(next_node.clone()), vec![curr_node.borrow().id]);
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
                        plan.append_node(merge_node_clone, merge_node_parent);
                    }
                    curr_node = merge_node;
                } else {
                    return None;
                }
            }
        }
        Some(plan)
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

/// To record any tag-related data while processing the logical plan
#[derive(Default, Clone, Debug)]
pub struct PlanMeta {
    /// To record all possible columns of a node, which is typically referred from a tag
    /// while processing projection, selection, groupby, orderby, and etc. For example, when
    /// select the record via an expression "a.name == \"John\"", the tag "a" must refer to
    /// some node in the logical plan, and the node requires the column of \"John\". Such
    /// information is critical in distributed processing, as the computation may not align
    /// with the storage to access the required column. Thus, such information can help
    /// the computation route and fetch columns.
    node_cols: HashMap<u32, BTreeSet<NameOrId>>,
    /// The tag must refer to a valid node in the plan.
    tag_nodes: HashMap<NameOrId, u32>,
    /// To ease the processing, tag may be transformed to an internal id.
    /// This maintains the mappings
    tag_ids: HashMap<NameOrId, NameOrId>,
    /// To record the current node's id in the logical plan. Note that nodes that have operators that
    /// of `As` or `Selection` does not alter curr_node.
    curr_node: u32,
    /// The maximal tag id that has been assigned, for mapping tag ids.
    max_tag_id: i32,
    /// Whether to preprocess the operator.
    is_preprocess: bool,
}

impl PlanMeta {
    pub fn new(node_id: u32) -> Self {
        let mut plan_meta = PlanMeta::default();
        plan_meta.curr_node = node_id;
        plan_meta.node_cols.entry(node_id).or_default();
        plan_meta
    }

    pub fn insert_tag_columns(
        &mut self, tag_opt: Option<NameOrId>, column: NameOrId,
    ) -> LogicalResult<u32> {
        if let Some(tag) = tag_opt {
            if let Some(&node_id) = self.tag_nodes.get(&tag) {
                self.node_cols
                    .entry(node_id)
                    .or_default()
                    .insert(column);

                Ok(node_id)
            } else {
                Err(LogicalError::TagNotExist(tag))
            }
        } else {
            let curr_node = self.curr_node;
            self.node_cols
                .entry(curr_node)
                .or_default()
                .insert(column);

            Ok(curr_node)
        }
    }

    pub fn get_node_columns(&self, id: u32) -> Option<&BTreeSet<NameOrId>> {
        self.node_cols.get(&id)
    }

    pub fn get_curr_node_columns(&self) -> Option<&BTreeSet<NameOrId>> {
        self.node_cols.get(&self.curr_node)
    }

    pub fn add_tag_node(&mut self, tag: NameOrId, node: u32) {
        self.tag_nodes.entry(tag).or_insert(node);
    }

    pub fn get_or_set_tag_id(&mut self, tag: NameOrId) -> NameOrId {
        let entry = self.tag_ids.entry(tag);
        match entry {
            Entry::Occupied(o) => o.into_mut().clone(),
            Entry::Vacant(v) => {
                let new_tag_id: NameOrId = self.max_tag_id.into();
                self.max_tag_id += 1;
                v.insert(new_tag_id).clone()
            }
        }
    }

    pub fn update_curr_node(&mut self, curr_node: u32) {
        self.curr_node = curr_node;
    }

    pub fn set_preprocess(&mut self, is_preprocess: bool) {
        self.is_preprocess = is_preprocess;
    }
}

pub trait AsLogical {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()>;
}

impl AsLogical for common_pb::Property {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(prop_key) = &mut self.item {
            match prop_key {
                common_pb::property::Item::Key(key) => {
                    if let Some(schema) = &meta.schema {
                        if plan_meta.is_preprocess && schema.is_column_id() {
                            *key = schema
                                .get_column_id_from_pb(key)
                                .ok_or(LogicalError::ColumnNotExist(format!("{:?}", key)))?
                                .into();
                        }
                    }
                    plan_meta.insert_tag_columns(None, key.clone().try_into()?)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl AsLogical for common_pb::Variable {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(property) = self.property.as_mut() {
            if let Some(key) = property.item.as_mut() {
                match key {
                    common_pb::property::Item::Key(key) => {
                        if let Some(schema) = &meta.schema {
                            if plan_meta.is_preprocess && schema.is_column_id() {
                                *key = schema
                                    .get_column_id_from_pb(key)
                                    .ok_or(LogicalError::ColumnNotExist(format!("{:?}", key)))?
                                    .into();
                            }
                        }
                        plan_meta.insert_tag_columns(
                            self.tag
                                .clone()
                                .map(|tag| tag.try_into())
                                .transpose()?,
                            key.clone().try_into()?,
                        )?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

impl AsLogical for common_pb::Value {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(schema) = &meta.schema {
            // A Const needs to be preprocessed only if it is while comparing a label (table)
            if plan_meta.is_preprocess && schema.is_table_id() {
                if let Some(item) = self.item.as_mut() {
                    match item {
                        common_pb::value::Item::Str(name) => {
                            *item = common_pb::value::Item::I32(
                                schema
                                    .get_table_id(name)
                                    .ok_or(LogicalError::TableNotExist(name.to_string()))?,
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}

impl AsLogical for common_pb::Expression {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        let mut count = 0;
        for opr in self.operators.iter_mut() {
            if let Some(item) = opr.item.as_mut() {
                match item {
                    common_pb::expr_opr::Item::Var(var) => {
                        if let Some(property) = var.property.as_mut() {
                            if let Some(key) = property.item.as_mut() {
                                match key {
                                    common_pb::property::Item::Key(key) => {
                                        if let Some(schema) = &meta.schema {
                                            count = 0;
                                            if plan_meta.is_preprocess && schema.is_column_id() {
                                                *key = schema
                                                    .get_column_id_from_pb(key)
                                                    .ok_or(LogicalError::ColumnNotExist(format!(
                                                        "{:?}",
                                                        key
                                                    )))?
                                                    .into();
                                            }
                                        }
                                        plan_meta.insert_tag_columns(
                                            var.tag
                                                .clone()
                                                .map(|tag| tag.try_into())
                                                .transpose()?,
                                            key.clone().try_into()?,
                                        )?;
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
                            if *l >= 0 && *l <= 5 {
                                count = 2; // indicates LabelKey <cmp>
                            }
                        } else {
                            count = 0;
                        }
                    }
                    common_pb::expr_opr::Item::Const(c) => {
                        if count == 2 {
                            // indicates LabelKey <cmp> labelValue
                            c.preprocess(meta, plan_meta)?;
                        }
                        count = 0;
                    }
                    common_pb::expr_opr::Item::Vars(vars) | common_pb::expr_opr::Item::VarMap(vars) => {
                        for var in &mut vars.keys {
                            var.preprocess(meta, plan_meta)?;
                        }
                        count = 0;
                    }
                    _ => count = 0,
                }
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::QueryParams {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(pred) = &mut self.predicate {
            pred.preprocess(meta, plan_meta)?;
        }
        if let Some(schema) = &meta.schema {
            if plan_meta.is_preprocess && schema.is_table_id() {
                for table in self.table_names.iter_mut() {
                    *table = schema
                        .get_table_id_from_pb(table)
                        .ok_or(LogicalError::TableNotExist(format!("{:?}", table)))?
                        .into();
                }
            }
        }
        for column in self.columns.iter_mut() {
            if let Some(schema) = &meta.schema {
                if plan_meta.is_preprocess && schema.is_column_id() {
                    *column = schema
                        .get_column_id_from_pb(column)
                        .ok_or(LogicalError::ColumnNotExist(format!("{:?}", column)))?
                        .into();
                }
            }
            plan_meta.insert_tag_columns(None, column.clone().try_into()?)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::Project {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        for mapping in self.mappings.iter_mut() {
            if let Some(expr) = &mut mapping.expr {
                expr.preprocess(meta, plan_meta)?;
            }
        }
        Ok(())
    }
}

impl AsLogical for pb::Select {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(pred) = self.predicate.as_mut() {
            pred.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::Scan {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(params) = self.params.as_mut() {
            params.preprocess(meta, plan_meta)?;
        }
        if let Some(index_pred) = self.idx_predicate.as_mut() {
            index_pred.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::ExpandBase {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(params) = self.params.as_mut() {
            params.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::EdgeExpand {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        if let Some(expand) = self.base.as_mut() {
            expand.preprocess(meta, plan_meta)?;
        }
        Ok(())
    }
}

impl AsLogical for pb::GetV {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::Dedup {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::GroupBy {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        for mapping in self.mappings.iter_mut() {
            if let Some(key) = &mut mapping.key {
                key.preprocess(meta, plan_meta)?;
            }
        }
        for agg_fn in self.functions.iter_mut() {
            for var in agg_fn.vars.iter_mut() {
                var.preprocess(meta, plan_meta)?;
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::IndexPredicate {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        for and_pred in self.or_predicates.iter_mut() {
            for pred in and_pred.predicates.iter_mut() {
                if let Some(pred_key) = &mut pred.key {
                    if let Some(key_item) = pred_key.item.as_mut() {
                        match key_item {
                            common_pb::property::Item::Key(key) => {
                                if let Some(schema) = &meta.schema {
                                    if plan_meta.is_preprocess && schema.is_column_id() {
                                        *key = schema
                                            .get_column_id_from_pb(key)
                                            .ok_or(LogicalError::ColumnNotExist(format!("{:?}", key)))?
                                            .into();
                                    }
                                }
                                plan_meta.insert_tag_columns(None, key.clone().try_into()?)?;
                            }
                            common_pb::property::Item::Label(_) => {
                                if let Some(val) = pred.value.as_mut() {
                                    val.preprocess(meta, plan_meta)?;
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
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        for pair in self.pairs.iter_mut() {
            if let Some(key) = &mut pair.key {
                key.preprocess(meta, plan_meta)?;
            }
        }

        Ok(())
    }
}

impl AsLogical for pb::Limit {
    fn preprocess(&mut self, _meta: &StoreMeta, _plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        Ok(())
    }
}

impl AsLogical for pb::Join {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        for left_key in self.left_keys.iter_mut() {
            left_key.preprocess(meta, plan_meta)?
        }
        for right_key in self.right_keys.iter_mut() {
            right_key.preprocess(meta, plan_meta)?
        }
        Ok(())
    }
}

impl AsLogical for pb::logical_plan::Operator {
    fn preprocess(&mut self, meta: &StoreMeta, plan_meta: &mut PlanMeta) -> LogicalResult<()> {
        use pb::logical_plan::operator::Opr;
        if let Some(opr) = self.opr.as_mut() {
            match opr {
                Opr::Project(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Select(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Scan(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Edge(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Vertex(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Dedup(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::GroupBy(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::OrderBy(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Limit(opr) => opr.preprocess(meta, plan_meta)?,
                Opr::Join(opr) => opr.preprocess(meta, plan_meta)?,
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::common::property::Item;

    use super::*;
    use crate::plan::meta::set_schema_simple;

    #[test]
    fn test_logical_plan() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::default();

        let id = plan
            .append_operator_as_node(opr.clone(), vec![])
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.total_size, 1);
        let node0 = plan.get_node(0).unwrap().clone();

        let id = plan
            .append_operator_as_node(opr.clone(), vec![0])
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.total_size, 2);
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
        assert_eq!(plan.total_size, 3);
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
        assert_eq!(plan.total_size, 3);
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

        let _id = plan
            .append_operator_as_node(opr.clone(), vec![0, 2])
            .unwrap();
        assert_eq!(_id, -1);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.total_size, 3);
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
        assert_eq!(plan.total_size, 4);
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
    fn test_logical_plan_from_pb() {
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
    fn test_logical_plan_into_pb() {
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
    fn test_preprocess_expression() {
        let mut plan_meta = PlanMeta::default();
        plan_meta.is_preprocess = true;
        plan_meta.tag_nodes.insert("a".into(), 1);
        plan_meta.tag_nodes.insert("b".into(), 2);

        set_schema_simple(
            vec![("person".to_string(), 0), ("software".to_string(), 1)],
            vec![("knows".to_string(), 0), ("creates".to_string(), 1)],
            vec![("id".to_string(), 0), ("name".to_string(), 1), ("age".to_string(), 2)],
        );
        let mut expression = str_to_expr_pb("@.~label == \"person\"".to_string()).unwrap();
        expression
            .preprocess(&STORE_META.read().unwrap(), &mut plan_meta)
            .unwrap();
        let opr = expression.operators.get(2).unwrap().clone();
        match opr.item.unwrap() {
            common_pb::expr_opr::Item::Const(val) => match val.item.unwrap() {
                common_pb::value::Item::I32(i) => assert_eq!(i, 0),
                _ => panic!(),
            },
            _ => panic!(),
        }

        let mut expression =
            str_to_expr_pb("(@.name == \"person\") && @a.~label == \"knows\"".to_string()).unwrap();
        expression
            .preprocess(&STORE_META.read().unwrap(), &mut plan_meta)
            .unwrap();
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
            plan_meta.node_cols.get(&0).unwrap(),
            // has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // name maps to 1
        let mut expression = str_to_expr_pb("@a.name == \"John\"".to_string()).unwrap();
        expression
            .preprocess(&STORE_META.read().unwrap(), &mut plan_meta)
            .unwrap();
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
            plan_meta.node_cols.get(&1).unwrap(),
            // node1 with tag a has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let mut expression = str_to_expr_pb("{@a.name, @b.id}".to_string()).unwrap();
        expression
            .preprocess(&STORE_META.read().unwrap(), &mut plan_meta)
            .unwrap();
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
            plan_meta.node_cols.get(&0).unwrap(),
            // node1 with tag a has a new column "name", which is mapped to 1
            &vec![1.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            plan_meta.node_cols.get(&2).unwrap(),
            // node2 with tag b has a new column "id", which is mapped to 0
            &vec![0.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_preprocess_scan() {
        let mut plan_meta = PlanMeta::default();
        plan_meta.is_preprocess = true;
        plan_meta.tag_nodes.insert("a".into(), 1);
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
            plan_meta.node_cols.get(&0).unwrap(),
            &vec![1.into(), 2.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            plan_meta.node_cols.get(&1).unwrap(),
            &vec![2.into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_tag_maintain_simple() {
        let mut plan = LogicalPlan::default();
        plan.plan_meta.is_preprocess = false;
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
        assert_eq!(plan.plan_meta.curr_node, 0);

        // .hasLabel("person")
        let select = pb::Select { predicate: str_to_expr_pb("@.~label == \"person\"".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![0])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, 0);

        // .has("age", 27)
        let select = pb::Select { predicate: str_to_expr_pb("@.age == 27".to_string()).ok() };
        plan.append_operator_as_node(select.into(), vec![1])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, 0);
        assert_eq!(
            plan.plan_meta.node_cols.get(&0).unwrap(),
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
        assert_eq!(plan.plan_meta.curr_node, 3);
        assert_eq!(
            plan.plan_meta.node_cols.get(&0).unwrap(),
            &vec!["age".into(), "id".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_tag_maintain_complex() {
        let mut plan = LogicalPlan::default();
        plan.plan_meta.is_preprocess = false;

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
        assert_eq!(plan.plan_meta.curr_node, 0);

        // .has("name", "John")
        let select = pb::Select { predicate: str_to_expr_pb("@.name == \"John\"".to_string()).ok() };
        opr_id = plan
            .append_operator_as_node(select.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, 0);
        assert_eq!(
            plan.plan_meta.node_cols.get(&0).unwrap(),
            &vec!["name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        // .as('a')
        let as_opr = pb::As { alias: Some(pb::Alias { alias: Some("a".into()), is_query_given: false }) };
        opr_id = plan
            .append_operator_as_node(as_opr.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, 0);
        assert_eq!(
            *plan
                .plan_meta
                .tag_nodes
                .get(&"a".into())
                .unwrap(),
            0
        );

        // outE("knows").as('b').has("date", 20200101)
        let expand = pb::EdgeExpand {
            base: Some(pb::ExpandBase {
                v_tag: Some("a".into()),
                direction: 0,
                params: Some(pb::QueryParams {
                    table_names: vec!["knows".into()],
                    columns: vec![],
                    limit: None,
                    predicate: str_to_expr_pb("@.date == 20200101".to_string()).ok(),
                    requirements: vec![],
                }),
            }),
            is_edge: true,
            alias: Some("b".into()),
        };
        opr_id = plan
            .append_operator_as_node(expand.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, opr_id as u32);
        assert_eq!(
            plan.plan_meta
                .node_cols
                .get(&(opr_id as u32))
                .unwrap(),
            &vec!["date".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            *plan
                .plan_meta
                .tag_nodes
                .get(&"b".into())
                .unwrap(),
            opr_id as u32
        );

        //.inV().as('c')
        let getv = pb::GetV { tag: None, opt: 2, alias: Some("c".into()) };
        opr_id = plan
            .append_operator_as_node(getv.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, opr_id as u32);
        assert_eq!(
            *plan
                .plan_meta
                .tag_nodes
                .get(&"c".into())
                .unwrap(),
            opr_id as u32
        );

        // .has("id", 10)
        let select = pb::Select { predicate: str_to_expr_pb("@.id == 10".to_string()).ok() };
        opr_id = plan
            .append_operator_as_node(select.into(), vec![opr_id as u32])
            .unwrap();
        assert_eq!(plan.plan_meta.curr_node, opr_id as u32 - 1);
        assert_eq!(
            plan.plan_meta
                .node_cols
                .get(&(opr_id as u32 - 1))
                .unwrap(),
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
        assert_eq!(plan.plan_meta.curr_node, opr_id as u32);
        assert_eq!(
            plan.plan_meta
                .node_cols
                .get(
                    plan.plan_meta
                        .tag_nodes
                        .get(&"a".into())
                        .unwrap()
                )
                .unwrap(),
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
        assert_eq!(plan.plan_meta.curr_node, opr_id as u32);
        assert_eq!(
            plan.plan_meta
                .node_cols
                .get(
                    plan.plan_meta
                        .tag_nodes
                        .get(&"c".into())
                        .unwrap()
                )
                .unwrap(),
            &vec!["age".into(), "id".into(), "name".into()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
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
    fn test_get_merge_node() {
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
    fn test_merge_branch_plans() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::with_root(Node::new(0, opr.clone()));

        let mut subplan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        subplan1.append_node(Node::new(3, opr.clone()), vec![1]);
        subplan1.append_node(Node::new(4, opr.clone()), vec![1]);
        subplan1.append_node(Node::new(5, opr.clone()), vec![3, 4]);

        let subplan2 = LogicalPlan::with_root(Node::new(2, opr.clone()));

        plan.append_branch_plans(plan.get_node(0).unwrap(), vec![subplan1, subplan2]);
        let mut expected_plan = create_logical_plan();
        expected_plan.remove_node(6);

        plan.append_node(Node::new(6, opr.clone()), vec![2, 5]);
        plan.append_node(Node::new(7, opr.clone()), vec![6]);

        assert_eq!(plan, create_logical_plan());
    }

    #[test]
    fn test_subplan() {
        let plan = create_logical_plan();
        let opr = pb::logical_plan::Operator { opr: None };
        let subplan = plan.subplan(plan.get_node(2).unwrap(), plan.get_node(7).unwrap());
        let mut expected_plan = LogicalPlan::with_root(Node::new(2, opr.clone()));
        expected_plan.append_node(Node::new(6, opr.clone()), vec![2]);
        assert_eq!(subplan.unwrap(), expected_plan);

        // The node 3 is at one of the branches, which is incomplete and hence invalid subplan
        let subplan = plan.subplan(plan.get_node(1).unwrap(), plan.get_node(3).unwrap());
        assert!(subplan.is_none());

        let subplan = plan.subplan(plan.get_node(1).unwrap(), plan.get_node(6).unwrap());
        let mut expected_plan = LogicalPlan::with_root(Node::new(1, opr.clone()));
        expected_plan.append_node(Node::new(3, opr.clone()), vec![1]);
        expected_plan.append_node(Node::new(4, opr.clone()), vec![1]);
        expected_plan.append_node(Node::new(5, opr.clone()), vec![3, 4]);

        assert_eq!(subplan.unwrap(), expected_plan);
    }

    #[test]
    fn test_get_branch_plans() {
        let plan = create_logical_plan();
        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(1).unwrap());
        let opr = pb::logical_plan::Operator { opr: None };

        let plan1 = LogicalPlan::with_root(Node::new(3, opr.clone()));
        let plan2 = LogicalPlan::with_root(Node::new(4, opr.clone()));

        assert_eq!(merge_node, plan.get_node(5));
        assert_eq!(subplans, vec![plan1, plan2]);

        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(0).unwrap());
        let mut plan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        plan1.append_node(Node::new(3, opr.clone()), vec![1]);
        plan1.append_node(Node::new(4, opr.clone()), vec![1]);
        plan1.append_node(Node::new(5, opr.clone()), vec![3, 4]);
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
