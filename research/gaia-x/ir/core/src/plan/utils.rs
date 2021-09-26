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

use crate::expr::to_suffix_expr_pb;
use crate::expr::token::tokenize;
use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use std::convert::TryFrom;
use std::ffi::CStr;
use std::os::raw::c_char;

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
    pub min_child_id: u32,
    pub max_child_id: u32,
    pub children: Vec<Node>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            id: 0,
            min_child_id: u32::MAX,
            max_child_id: 0,
            children: vec![],
        }
    }
}

#[allow(dead_code)]
impl Node {
    pub fn new(id: u32) -> Node {
        let mut node = Self::default();
        node.id = id;
        node
    }

    pub fn append_child(&mut self, node: Node) {
        // The child must have larger id
        assert!(self.id < node.id);
        if node.id < self.min_child_id {
            self.min_child_id = node.id;
        }
        if node.id > self.max_child_id {
            self.max_child_id = node.id;
        }
        self.children.push(node);
    }

    pub fn get_mut(&mut self, id: u32) -> Option<&mut Node> {
        if self.id == id {
            Some(self)
        } else {
            let mut find = None;
            if id >= self.min_child_id && id <= self.max_child_id {
                for node in &mut self.children {
                    if node.id == id {
                        find = Some(node);
                        break;
                    }
                }
            } else if id > self.max_child_id {
                for node in &mut self.children {
                    find = node.get_mut(id);
                    if find.is_some() {
                        break;
                    }
                }
            }
            find
        }
    }

    pub fn get(&self, id: u32) -> Option<&Node> {
        if self.id == id {
            Some(self)
        } else {
            let mut find = None;
            if id >= self.min_child_id && id <= self.max_child_id {
                for node in &self.children {
                    if node.id == id {
                        find = Some(node);
                        break;
                    }
                }
            } else if id > self.max_child_id {
                for node in &self.children {
                    find = node.get(id);
                    if find.is_some() {
                        break;
                    }
                }
            }
            find
        }
    }

    pub fn has_child(&self) -> bool {
        !self.children.is_empty()
    }
}

/// An internal representation of the pb-[`LogicalPlan`].
///
/// [`Node`]: crate::generated::algebra::LogicalPlan
#[derive(Default, Debug)]
pub(crate) struct LogicalPlan {
    pub operators: Vec<pb::logical_plan::Operator>,
    pub root: Option<Node>,
}

#[allow(dead_code)]
impl LogicalPlan {
    pub fn append_operator(
        &mut self,
        opr: pb::logical_plan::Operator,
        parent_id: u32,
    ) -> FfiResult<u32> {
        let id = self.new_id();
        let node = Node::new(id);
        if let Some(root) = self.root.as_mut() {
            if let Some(parent) = root.get_mut(parent_id) {
                parent.append_child(node);
            } else {
                return Err(ResultCode::NotExistError);
            }
        } else {
            self.root = Some(node);
        }
        self.operators.push(opr);
        Ok(id)
    }

    pub fn new_id(&self) -> u32 {
        self.len() as u32
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
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

impl From<pb::GetV> for pb::logical_plan::Operator {
    fn from(opr: pb::GetV) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::GetV(opr)),
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
    fn get_node_test() {
        let mut root = Node::new(0);
        let mut node1 = Node::new(1);
        let mut node2 = Node::new(2);
        let node3 = Node::new(3);
        let node4 = Node::new(4);

        node1.append_child(node3.clone());
        node2.append_child(node4.clone());
        root.append_child(node1.clone());
        root.append_child(node2.clone());

        let node = root.get(0);
        assert_eq!(node, Some(&root));

        let node = root.get(1);
        assert_eq!(node, Some(&node1));

        let node = root.get(2);
        assert_eq!(node, Some(&node2));

        let node = root.get(3);
        assert_eq!(node, Some(&node3));

        let node = root.get(4);
        assert_eq!(node, Some(&node4));

        let node = root.get(5);
        assert!(node.is_none());

        let node = node1.get(3);
        assert_eq!(node, Some(&node3));

        let node = node1.get(4);
        assert!(node.is_none());
    }

    #[test]
    fn get_node_test2() {
        let mut root = Node::new(0);
        let mut node1 = Node::new(1);
        let mut node2 = Node::new(2);
        let mut node3 = Node::new(3);
        let mut node4 = Node::new(4);
        let mut node5 = Node::new(5);
        let node20 = Node::new(20);

        node4.append_child(node5.clone());
        node4.append_child(node20.clone());
        node2.append_child(node4.clone());
        root.append_child(node1.clone());
        root.append_child(node2.clone());
        root.append_child(node3.clone());

        let node = root.get(0);
        assert_eq!(node, Some(&root));

        let node = root.get(1);
        assert_eq!(node, Some(&node1));

        let node = root.get(2);
        assert_eq!(node, Some(&node2));

        let node = root.get(3);
        assert_eq!(node, Some(&node3));

        let node = root.get(4);
        assert_eq!(node, Some(&node4));

        let node = root.get(5);
        assert_eq!(node, Some(&node5));

        let node = root.get(20);
        assert_eq!(node, Some(&node20));
    }
}
