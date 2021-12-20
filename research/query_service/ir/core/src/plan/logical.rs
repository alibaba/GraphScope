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
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::iter::FromIterator;
use std::rc::Rc;

use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::algebra as pb;
use ir_common::generated::algebra::logical_plan::operator::Opr;
use vec_map::VecMap;

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

fn clone_pure_node(node: NodeType) -> Node {
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
        Self { nodes, total_size: 1 }
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
        self.nodes
            .insert(id as usize, Rc::new(RefCell::new(node)));
        self.total_size = id as usize + 1;

        id as i32
    }

    /// Append an operator into the logical plan, as a new node with `self.total_size` as its id.
    pub fn append_operator_as_node(
        &mut self, opr: pb::logical_plan::Operator, parent_ids: Vec<u32>,
    ) -> i32 {
        self.append_node(Node::new(self.total_size as u32, opr), parent_ids)
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

    /// Determine whether the logical plan is valid while checking the follows:
    /// * The root operator must be a Scan operator
    pub fn sanity_check(&self) -> bool {
        if let Some(root) = self.root() {
            let root_ref = root.borrow();
            if let Some(root_opr) = &root_ref.opr.opr {
                match root_opr {
                    Opr::Scan(_) => true,
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
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
        let mut plan = LogicalPlan::with_root(clone_pure_node(from_node.clone()));
        let mut curr_node = from_node;
        while curr_node.borrow().id != to_node.borrow().id {
            if curr_node.borrow().children.is_empty() {
                // While still not locating to_node
                return None;
            } else if curr_node.borrow().children.len() == 1 {
                let next_node_id = curr_node.borrow().get_first_child().unwrap();
                if let Some(next_node) = self.get_node(next_node_id) {
                    if next_node.borrow().id != to_node.borrow().id {
                        plan.append_node(clone_pure_node(next_node.clone()), vec![curr_node.borrow().id]);
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
                        let merge_node_clone = clone_pure_node(merge_node.clone());
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

    /// Write the logical plan to a json via the given `writer`.
    pub fn into_json<W: io::Write>(self, writer: W) -> io::Result<()> {
        let plan_pb: pb::LogicalPlan = self.into();
        serde_json::to_writer_pretty(writer, &plan_pb)?;

        Ok(())
    }

    /// Read the logical plan from a json via the given `reader`
    pub fn from_json<R: io::Read>(reader: R) -> ParsePbResult<Self> {
        let serde_result = serde_json::from_reader::<_, pb::LogicalPlan>(reader);
        if let Ok(plan_pb) = serde_result {
            Self::try_from(plan_pb)
        } else {
            Err(ParsePbError::SerdeError(format!("{:?}", serde_result.err().unwrap())))
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

        let id = plan.append_operator_as_node(opr.clone(), vec![]);
        assert_eq!(id, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.total_size, 1);
        let node0 = plan.get_node(0).unwrap().clone();

        let id = plan.append_operator_as_node(opr.clone(), vec![0]);
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

        let id = plan.append_operator_as_node(opr.clone(), vec![0, 1]);
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

        let _id = plan.append_operator_as_node(opr.clone(), vec![0, 2]);
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
        let _ = plan.append_operator_as_node(opr.clone(), vec![0, 1]);
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

        let _ = plan.append_operator_as_node(opr.clone(), vec![]);
        let _ = plan.append_operator_as_node(opr.clone(), vec![0]);
        let _ = plan.append_operator_as_node(opr.clone(), vec![0]);

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
        plan.append_operator_as_node(opr.clone(), vec![]); // root
        plan.append_operator_as_node(opr.clone(), vec![0]); // node 1
        plan.append_operator_as_node(opr.clone(), vec![0]); // node 2
        plan.append_operator_as_node(opr.clone(), vec![1]); // node 3
        plan.append_operator_as_node(opr.clone(), vec![1]); // node 4
        plan.append_operator_as_node(opr.clone(), vec![3, 4]); // node 5
        plan.append_operator_as_node(opr.clone(), vec![2, 5]); // node 6
        plan.append_operator_as_node(opr.clone(), vec![6]); // node 7

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
        plan.append_operator_as_node(opr.clone(), vec![]); // root
        plan.append_operator_as_node(opr.clone(), vec![0]); // node 1
        plan.append_operator_as_node(opr.clone(), vec![0]); // node 2
        plan.append_operator_as_node(opr.clone(), vec![0]); // node 3
        plan.append_operator_as_node(opr.clone(), vec![1, 2, 3]); // node 4

        let (merge_node, subplans) = plan.get_branch_plans(plan.get_node(0).unwrap());
        let plan1 = LogicalPlan::with_root(Node::new(1, opr.clone()));
        let plan2 = LogicalPlan::with_root(Node::new(2, opr.clone()));
        let plan3 = LogicalPlan::with_root(Node::new(3, opr.clone()));

        assert_eq!(merge_node, plan.get_node(4));
        assert_eq!(subplans, vec![plan1, plan2, plan3]);
    }
}
