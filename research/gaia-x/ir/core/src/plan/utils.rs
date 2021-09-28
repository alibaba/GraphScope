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
extern crate vec_map;

use crate::error::{ParsePbError, ParsePbResult};
use crate::expr::to_suffix_expr_pb;
use crate::expr::token::tokenize;
use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::convert::TryFrom;
use std::ffi::CStr;
use std::iter::FromIterator;
use std::os::raw::c_char;
use std::rc::Rc;
use vec_map::VecMap;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResultCode {
    Success = 0,
    /// Parse an expression error
    ParseExprError = 1,
    /// Query an object that does not exist
    NotExistError = 2,
    /// The error while transforming from C-like string, aka char*
    CStringError = 3,
    /// Negative index
    NegativeIndexError = 4,
}

impl std::fmt::Display for ResultCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResultCode::Success => write!(f, "success"),
            ResultCode::ParseExprError => write!(f, "parse expression error"),
            ResultCode::NotExistError => write!(f, "access to non-existed element"),
            ResultCode::CStringError => write!(f, "convert from c-like string error"),
            ResultCode::NegativeIndexError => write!(f, "try passing a negative index"),
        }
    }
}

impl std::error::Error for ResultCode {}

pub(crate) type FfiResult<T> = Result<T, ResultCode>;

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
        Node {
            id,
            opr,
            parents: BTreeSet::new(),
            children: BTreeSet::new(),
        }
    }

    pub fn add_child(&mut self, child_id: u32) {
        self.children.insert(child_id);
    }

    pub fn add_parent(&mut self, parent_id: u32) {
        self.parents.insert(parent_id);
    }
}

type NodeType = Rc<RefCell<Node>>;

/// An internal representation of the pb-[`LogicalPlan`].
///
/// [`Node`]: crate::generated::algebra::LogicalPlan
#[derive(Default, Debug)]
pub(crate) struct LogicalPlan {
    pub nodes: VecMap<NodeType>,
    /// To record the total number of operators ever created in the logical plan,
    /// **ignorant of the removed nodes**
    pub total_size: usize,
}

fn parse_pb_node(
    node_pbs: &Vec<pb::logical_plan::Node>,
    nodes: &Vec<NodeType>,
) -> ParsePbResult<()> {
    for (node_pb, node) in node_pbs.iter().zip(nodes.iter()) {
        for child_id in &node_pb.children {
            &(*node).borrow_mut().add_child(*child_id as u32);
            if let Some(child_node) = nodes.get(*child_id as usize) {
                &(*child_node)
                    .borrow_mut()
                    .add_parent(node.borrow().id as u32);
            } else {
                return Err(ParsePbError::InvalidPb(
                    "the child id is out of index".to_string(),
                ));
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
                nodes.push(Rc::new(RefCell::new(Node::new(
                    id as u32,
                    opr.to_owned().clone(),
                ))));
            } else {
                return Err(ParsePbError::InvalidPb(
                    "do not specify operator in a node".to_string(),
                ));
            }
        }

        parse_pb_node(&nodes_pb, &nodes)?;

        let plan = LogicalPlan {
            total_size: nodes.len(),
            nodes: VecMap::from_iter(nodes.into_iter().enumerate()),
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
            let mut node_pb = pb::logical_plan::Node {
                opr: None,
                children: vec![],
            };
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

#[allow(dead_code)]
impl LogicalPlan {
    pub fn root(&self) -> Option<NodeType> {
        self.nodes.get(0).cloned()
    }

    pub fn get_node(&self, id: u32) -> Option<NodeType> {
        self.nodes.get(id as usize).cloned()
    }

    /// Append an operator into the logical plan, as a new node, with specified `parent_ids`
    /// as its parent node. In order to do so, all specified parents must present in the
    /// logical plan.
    ///
    /// # Return
    ///   * If succeed, the id of the newly added node
    ///   * Otherwise, a `ResultCode` indicating any error, mostly the parent node does not present
    pub fn append_node(
        &mut self,
        opr: pb::logical_plan::Operator,
        parent_ids: Vec<u32>,
    ) -> FfiResult<u32> {
        let id = self.total_size as u32;
        let mut node = Node::new(id, opr);
        if !self.is_empty() {
            let mut parent_nodes = vec![];
            for parent_id in parent_ids {
                if let Some(parent_node) = self.get_node(parent_id) {
                    parent_nodes.push(parent_node);
                } else {
                    return Err(ResultCode::NotExistError);
                }
            }
            for parent_node in parent_nodes {
                node.add_parent(parent_node.borrow().id);
                &(*parent_node).borrow_mut().add_child(id);
            }
        }
        self.nodes.insert(id as usize, Rc::new(RefCell::new(node)));
        self.total_size += 1;
        Ok(id)
    }

    /// Remove a node from the logical plan, and do the following:
    /// * For each of its parent, if present, shall remove node's id reference from the `children`.
    /// * For each of its children, shall remove the node's id reference from the `parent`, and if
    /// the child's parent becomes empty, the child must be removed recursively.
    ///
    ///  Note that this does not decrease `self.total_size`, which serves as the indication
    /// of new id of the plan.
    pub fn remove_node(&mut self, id: u32) -> Option<NodeType> {
        let node = self.nodes.remove(id as usize);
        if let Some(n) = &node {
            for p in &n.borrow().parents {
                if let Some(parent_node) = self.get_node(*p) {
                    (&*parent_node).borrow_mut().children.remove(&id);
                }
            }

            for c in &n.borrow().children {
                if let Some(child) = self.get_node(*c) {
                    (&*child).borrow_mut().parents.remove(&id);
                    if (&*child).borrow_mut().parents.is_empty() {
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
}

pub(crate) fn cstr_to_string(c_str: *const c_char) -> FfiResult<String> {
    let str_result = unsafe { CStr::from_ptr(c_str) }.to_str();
    if let Ok(str) = str_result {
        Ok(str.to_string())
    } else {
        Err(ResultCode::CStringError)
    }
}

impl TryFrom<String> for common_pb::SuffixExpr {
    type Error = ResultCode;

    fn try_from(expr_str: String) -> FfiResult<Self> {
        let tokens_result = tokenize(&expr_str);
        if let Ok(tokens) = tokens_result {
            if let Ok(expr) = to_suffix_expr_pb(tokens) {
                return Ok(expr);
            }
        }
        Err(ResultCode::ParseExprError)
    }
}

impl From<pb::Project> for pb::logical_plan::Operator {
    fn from(opr: pb::Project) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Project(opr)),
        }
    }
}

impl From<pb::Select> for pb::logical_plan::Operator {
    fn from(opr: pb::Select) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Select(opr)),
        }
    }
}

impl From<pb::Join> for pb::logical_plan::Operator {
    fn from(opr: pb::Join) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Join(opr)),
        }
    }
}

impl From<pb::Union> for pb::logical_plan::Operator {
    fn from(opr: pb::Union) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Union(opr)),
        }
    }
}

impl From<pb::GroupBy> for pb::logical_plan::Operator {
    fn from(opr: pb::GroupBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::GroupBy(opr)),
        }
    }
}

impl From<pb::OrderBy> for pb::logical_plan::Operator {
    fn from(opr: pb::OrderBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::OrderBy(opr)),
        }
    }
}

impl From<pb::Dedup> for pb::logical_plan::Operator {
    fn from(opr: pb::Dedup) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Dedup(opr)),
        }
    }
}

impl From<pb::Unfold> for pb::logical_plan::Operator {
    fn from(opr: pb::Unfold) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Unfold(opr)),
        }
    }
}

impl From<pb::Apply> for pb::logical_plan::Operator {
    fn from(opr: pb::Apply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Apply(opr)),
        }
    }
}

impl From<pb::SegmentApply> for pb::logical_plan::Operator {
    fn from(opr: pb::SegmentApply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::SegApply(opr)),
        }
    }
}

impl From<pb::Source> for pb::logical_plan::Operator {
    fn from(opr: pb::Source) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Source(opr)),
        }
    }
}

impl From<pb::EdgeExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::EdgeExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Edge(opr)),
        }
    }
}

impl From<pb::PathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::PathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Path(opr)),
        }
    }
}

impl From<pb::ShortestPathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::ShortestPathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::ShortestPath(opr)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_logical_plan() {
        let opr = pb::logical_plan::Operator { opr: None };
        let mut plan = LogicalPlan::default();

        let id = plan.append_node(opr.clone(), vec![]).unwrap();
        assert_eq!(id, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.total_size, 1);
        let node0 = plan.get_node(0).unwrap().clone();

        let id = plan.append_node(opr.clone(), vec![0]).unwrap();
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

        let id = plan.append_node(opr.clone(), vec![0, 1]).unwrap();
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

        let _id = plan.append_node(opr.clone(), vec![0, 2]);
        assert!(_id.is_err());
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
        let _ = plan.append_node(opr.clone(), vec![0, 1]).unwrap();
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

        let root_pb = pb::logical_plan::Node {
            opr: Some(opr.clone()),
            children: vec![1, 2],
        };

        let node1_pb = pb::logical_plan::Node {
            opr: Some(opr.clone()),
            children: vec![2],
        };

        let node2_pb = pb::logical_plan::Node {
            opr: Some(opr.clone()),
            children: vec![],
        };

        let plan_pb = pb::LogicalPlan {
            nodes: vec![root_pb, node1_pb, node2_pb],
        };

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

        let _ = plan.append_node(opr.clone(), vec![]).unwrap();
        let _ = plan.append_node(opr.clone(), vec![0]).unwrap();
        let _ = plan.append_node(opr.clone(), vec![0]).unwrap();

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
}
