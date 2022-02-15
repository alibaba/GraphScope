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

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::algebra as pb;
use ir_common::generated::algebra::Patmat;
use ir_common::NameOrId;

use crate::error::IrResult;

#[derive(Clone, Debug, PartialEq)]
pub enum Binder {
    Edge(pb::EdgeExpand),
    Path(pb::PathExpand),
    Vertex(pb::GetV),
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i32)]
pub enum BindingOpt {
    Vertex = 0,
    Edge = 1,
    Path = 2,
}

impl TryFrom<pb::patmat::Binder> for Binder {
    type Error = ParsePbError;

    fn try_from(pb: pb::patmat::Binder) -> Result<Self, Self::Error> {
        if let Some(item) = pb.item {
            Ok(match item {
                pb::patmat::binder::Item::Vertex(v) => Self::Vertex(v),
                pb::patmat::binder::Item::Edge(e) => Self::Edge(e),
                pb::patmat::binder::Item::Path(p) => Self::Path(p),
            })
        } else {
            Err(ParsePbError::EmptyFieldError("Binder::item".to_string()))
        }
    }
}

impl Binder {
    /// Return whether this binder connects to a vertex
    pub fn get_binding_opt(&self) -> BindingOpt {
        match self {
            Binder::Vertex(_) => BindingOpt::Vertex,
            Binder::Edge(edge) => {
                if edge.is_edge {
                    BindingOpt::Edge
                } else {
                    BindingOpt::Vertex
                }
            }
            Binder::Path(_) => BindingOpt::Path,
        }
    }
}

/// A match sentence defines how to connect a `start_tag` (required) to an `end_tag` (optional) with an ordered
/// list of binders, each of which is either an `EdgeExpand`, a `PathExpand` or a `GetV`
/// operator. The rules of their concatenations are:
///   * Both `EdgeExpand` and `PathExpand` must be followed by an operator that returns a vertex,
///   * `GetV` must be followed by an operator of either `EdgeExpand` or `PathExpand`. For the
/// former case, `EdgeExpand` must return an edge, namely `EdgeExpand::is_edge` is set to `true`.
#[derive(Clone, Debug, PartialEq)]
pub struct Sentence {
    /// The start tag of the sentence is mandatory
    start: NameOrId,
    /// The binders
    binders: Vec<Binder>,
    /// The end tag is not necessary
    end: Option<NameOrId>,
    /// Is this a sentence with Anti(No)-semanatics
    is_anti: bool,
    /// What kind of entities this sentence binds to
    end_as: BindingOpt,
}

impl TryFrom<pb::patmat::Sentence> for Sentence {
    type Error = ParsePbError;

    fn try_from(pb: pb::patmat::Sentence) -> Result<Self, Self::Error> {
        if !pb.binders.is_empty() {
            let start = pb
                .start
                .clone()
                .ok_or(ParsePbError::EmptyFieldError("Sentence::start".to_string()))?
                .try_into()?;
            let binders = pb
                .binders
                .into_iter()
                .map(|binder| binder.try_into())
                .collect::<ParsePbResult<Vec<Binder>>>()?;
            let end = pb
                .end
                .clone()
                .map(|tag| tag.try_into())
                .transpose()?;
            let is_anti = pb.is_anti;
            let end_as = binders.last().unwrap().get_binding_opt();

            Ok(Self { start, binders, end, is_anti, end_as })
        } else {
            Err(ParsePbError::EmptyFieldError("Sentence::start".to_string()))
        }
    }
}

/// A trait to abstract how to build a logical plan for `Patmat` operator.
pub trait MatchingStrategy {
    fn build_plan(&self) -> IrResult<pb::LogicalPlan>;
}

/// A naive implementation of `MatchingStrategy`
pub struct NaiveStrategy {
    /// Maintaining the frequencies each tag presents in the pattern matching sentences
    tag_freq: HashMap<NameOrId, usize>,
    /// Derive a tag to start the pattern matching. For now, it is the `start_tag` among
    /// all sentences with the largest frequency.
    entry_tag: Option<NameOrId>,
}

impl TryFrom<pb::Patmat> for NaiveStrategy {
    type Error = ParsePbError;

    fn try_from(_pb: Patmat) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl MatchingStrategy for NaiveStrategy {
    fn build_plan(&self) -> IrResult<pb::LogicalPlan> {
        todo!()
    }
}
