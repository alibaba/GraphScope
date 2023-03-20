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

use std::cmp::Ordering;
use std::collections::HashMap;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;

use crate::glogue::error::IrPatternResult;

pub type PatternId = usize;
pub type PatternLabelId = ir_common::LabelId;
pub type DynIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub mod error;
pub mod extend_step;
pub mod pattern;

pub type PatternDirection = pb::edge_expand::Direction;

pub(crate) fn query_params(
    tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    predicate: Option<common_pb::Expression>,
) -> pb::QueryParams {
    pb::QueryParams {
        tables,
        columns,
        is_all_columns: false,
        limit: None,
        predicate,
        sample_ratio: 1.0,
        extra: HashMap::new(),
    }
}

pub(crate) fn query_params_to_get_v(
    params: Option<pb::QueryParams>, alias: Option<KeyId>, opt: i32,
) -> pb::GetV {
    pb::GetV { tag: None, opt, params, alias: alias.map(|id| id.into()), meta_data: None }
}

pub(crate) fn connect_query_params(params1: pb::QueryParams, params2: pb::QueryParams) -> pb::QueryParams {
    let mut params = params1;
    params.tables.extend(params2.tables);
    params.columns.extend(params2.columns);
    params.is_all_columns &= params2.is_all_columns;
    params.limit = {
        let limit1 = params.limit;
        let limit2 = params2.limit;
        limit1.and_then(|range1| {
            limit2.map(|range2| pb::Range {
                lower: std::cmp::max(range1.lower, range2.lower),
                upper: std::cmp::min(range1.upper, range2.upper),
            })
        })
    };
    params.predicate = {
        let predicate1 = params.predicate;
        let predicate2 = params2.predicate;
        predicate1.and_then(|expr1| predicate2.map(|expr2| connect_exprs(expr1, expr2)))
    };
    if params2.sample_ratio < params.sample_ratio {
        params.sample_ratio = params2.sample_ratio
    }
    params.extra.extend(params2.extra);
    params
}

fn connect_exprs(expr1: common_pb::Expression, expr2: common_pb::Expression) -> common_pb::Expression {
    let left_brace = common_pb::ExprOpr {
        node_type: None,
        item: Some(common_pb::expr_opr::Item::Brace(common_pb::expr_opr::Brace::LeftBrace as i32)),
    };
    let right_brace = common_pb::ExprOpr {
        node_type: None,
        item: Some(common_pb::expr_opr::Item::Brace(common_pb::expr_opr::Brace::RightBrace as i32)),
    };
    let and_opr = common_pb::ExprOpr {
        node_type: None,
        item: Some(common_pb::expr_opr::Item::Logical(common_pb::Logical::And as i32)),
    };
    // (expr1) and (expr2)
    let mut expr_oprs = vec![left_brace.clone()];
    expr_oprs.extend(expr1.operators);
    expr_oprs.push(right_brace.clone());
    expr_oprs.push(and_opr);
    expr_oprs.push(left_brace);
    expr_oprs.extend(expr2.operators);
    expr_oprs.push(right_brace);
    common_pb::Expression { operators: expr_oprs }
}

pub trait PatternOrderTrait<D> {
    fn compare(&self, left: &D, right: &D) -> IrPatternResult<Ordering>;
}

pub trait PatternWeightTrait<W: PartialOrd> {
    fn get_vertex_weight(&self, vid: PatternId) -> IrPatternResult<W>;
    fn get_adjacencies_weight(&self, vid: PatternId) -> IrPatternResult<W>;
}
