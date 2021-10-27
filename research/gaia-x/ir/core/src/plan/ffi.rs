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
//! The ffi module gives the C-like apis for the Gaia client to build the plan from the
//! query semantics, and to connect with the distributed service of Gaia.
//!
//! We instruct how to use these apis as follows.
//!
//! First of all, call `cbindgen` to generate the header of apis for C-binded caller, as:
//! `cbindgen --crate ir_core --output /path/to/c-caller/ir_core.h`
//!
//! Secondly, build the dynamic ir_core library, as: `cargo build --release`,
//! which will generate the `libir_core.dylib` under `./target/release`.
//! Copy it to `/path/to/c-caller`.
//!
//! Thirdly, write the C-code for building the ir plan, as:
//!
//! # Example
//!
//! # #include<ir_core.h>
//! # using namespace std;
//! # int main(int argc, char** argv) {
//! #    const void* ptr_plan = init_logical_plan();
//! #    const void* ptr_project = init_project_operator();
//! #    add_project_mapping(ptr_project, "@name", int_as_name_or_id(0));
//! #    int opr_id = 0;
//! #    append_project_operator(ptr_plan, ptr_project, 0, &opr_id);
//! #    cout << "the id is: " << opr_id << endl;
//!
//! #    const void* ptr_select = init_select_operator();
//! #    set_select_predicate(ptr_select, "@age > 20 && @name == \"John\"");
//! #    append_select_operator(ptr_plan, ptr_select, opr_id, &opr_id);
//! #    cout << "the id is: " << opr_id << endl;
//!
//! #    debug_plan(ptr_plan);
//! #    destroy_logical_plan(ptr_plan);
//! # }
//!
//! Save the codes as </path/to/c-caller/test.cc>, and build like:
//! `g++ -o test test.cc -std=c++11 -L. -lir_core`

use crate::plan::logical::LogicalPlan;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use runtime::expr::to_suffix_expr_pb;
use runtime::expr::token::tokenize;
use std::convert::{TryFrom, TryInto};
use std::ffi::{c_void, CStr};
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
    /// The provided data type is unknown
    UnknownTypeError = 4,
    /// The provided range is invalid
    InvalidRangeError = 5,
    /// The given index is negative
    NegativeIndexError = 6,
}

impl std::fmt::Display for ResultCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResultCode::Success => write!(f, "success"),
            ResultCode::ParseExprError => write!(f, "parse expression error"),
            ResultCode::NotExistError => write!(f, "access to non-existed element"),
            ResultCode::CStringError => write!(f, "convert from c-like string error"),
            ResultCode::UnknownTypeError => write!(f, "unknown data type"),
            ResultCode::InvalidRangeError => write!(f, "the range is invalid"),
            ResultCode::NegativeIndexError => write!(f, "the given index is negative"),
        }
    }
}

impl std::error::Error for ResultCode {}

pub(crate) type FfiResult<T> = Result<T, ResultCode>;

pub(crate) fn cstr_to_string(cstr: *const c_char) -> FfiResult<String> {
    let str_result = unsafe { CStr::from_ptr(cstr) }.to_str();
    if let Ok(str) = str_result {
        Ok(str.to_string())
    } else {
        Err(ResultCode::CStringError)
    }
}

pub(crate) fn str_to_expr(expr_str: String) -> FfiResult<common_pb::SuffixExpr> {
    let tokens_result = tokenize(&expr_str);
    if let Ok(tokens) = tokens_result {
        if let Ok(expr) = to_suffix_expr_pb(tokens) {
            return Ok(expr);
        }
    }
    Err(ResultCode::ParseExprError)
}

pub(crate) fn cstr_to_suffix_expr_pb(cstr: *const c_char) -> FfiResult<common_pb::SuffixExpr> {
    let str = cstr_to_string(cstr);
    if str.is_err() {
        Err(str.err().unwrap())
    } else {
        str_to_expr(str.unwrap())
    }
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FfiNameIdOpt {
    None = 0,
    Name = 1,
    Id = 2,
}

impl Default for FfiNameIdOpt {
    fn default() -> Self {
        Self::None
    }
}

#[repr(C)]
pub struct FfiNameOrId {
    opt: FfiNameIdOpt,
    name: *const c_char,
    name_id: i32,
}

impl Default for FfiNameOrId {
    fn default() -> Self {
        Self {
            opt: FfiNameIdOpt::default(),
            name: std::ptr::null() as *const c_char,
            name_id: 0,
        }
    }
}

impl TryFrom<FfiNameOrId> for Option<common_pb::NameOrId> {
    type Error = ResultCode;

    fn try_from(ffi: FfiNameOrId) -> FfiResult<Self> {
        match &ffi.opt {
            FfiNameIdOpt::None => Ok(None),
            FfiNameIdOpt::Name => Ok(Some(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::Name(cstr_to_string(ffi.name)?)),
            })),
            FfiNameIdOpt::Id => Ok(Some(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::Id(ffi.name_id)),
            })),
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone)]
pub enum FfiPropertyOpt {
    None = 0,
    Id = 1,
    Label = 2,
    Key = 3,
}

impl Default for FfiPropertyOpt {
    fn default() -> Self {
        Self::None
    }
}

#[repr(C)]
#[derive(Default)]
pub struct FfiProperty {
    opt: FfiPropertyOpt,
    key: FfiNameOrId,
}

impl TryFrom<FfiProperty> for Option<common_pb::Property> {
    type Error = ResultCode;

    fn try_from(ffi: FfiProperty) -> FfiResult<Self> {
        let result = match &ffi.opt {
            FfiPropertyOpt::None => None,
            FfiPropertyOpt::Id => Some(common_pb::Property {
                item: Some(common_pb::property::Item::Id(common_pb::IdKey {})),
            }),
            FfiPropertyOpt::Label => Some(common_pb::Property {
                item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})),
            }),
            FfiPropertyOpt::Key => {
                if let Some(key) = ffi.key.try_into()? {
                    Some(common_pb::Property {
                        item: Some(common_pb::property::Item::Key(key)),
                    })
                } else {
                    None
                }
            }
        };

        Ok(result)
    }
}

#[repr(C)]
pub struct FfiVariable {
    tag: FfiNameOrId,
    property: FfiProperty,
}

impl TryFrom<FfiVariable> for common_pb::Variable {
    type Error = ResultCode;

    fn try_from(ffi: FfiVariable) -> Result<Self, Self::Error> {
        let (tag, property) = (ffi.tag.try_into()?, ffi.property.try_into()?);
        Ok(Self { tag, property })
    }
}

/// Build a none-`NameOrId`
#[no_mangle]
pub extern "C" fn none_name_or_id() -> FfiNameOrId {
    FfiNameOrId {
        opt: FfiNameIdOpt::None,
        name: std::ptr::null(),
        name_id: 0,
    }
}

/// Transform a c-like string into `NameOrId`
#[no_mangle]
pub extern "C" fn cstr_as_name_or_id(cstr: *const c_char) -> FfiNameOrId {
    FfiNameOrId {
        opt: FfiNameIdOpt::Name,
        name: cstr,
        name_id: 0,
    }
}

/// Transform an integer into `NameOrId`.
#[no_mangle]
pub extern "C" fn int_as_name_or_id(integer: i32) -> FfiNameOrId {
    FfiNameOrId {
        opt: FfiNameIdOpt::Id,
        name: std::ptr::null(),
        name_id: integer,
    }
}

/// Build an id property
#[no_mangle]
pub extern "C" fn as_id_key() -> FfiProperty {
    FfiProperty {
        opt: FfiPropertyOpt::Id,
        key: FfiNameOrId::default(),
    }
}

/// Build a label property
#[no_mangle]
pub extern "C" fn as_label_key() -> FfiProperty {
    FfiProperty {
        opt: FfiPropertyOpt::Label,
        key: FfiNameOrId::default(),
    }
}

/// Build a keyed property from a given key
#[no_mangle]
pub extern "C" fn as_property_key(key: FfiNameOrId) -> FfiProperty {
    FfiProperty {
        opt: FfiPropertyOpt::Key,
        key,
    }
}

/// Build a variable
#[no_mangle]
pub extern "C" fn as_var(tag: FfiNameOrId) -> FfiVariable {
    FfiVariable {
        tag,
        property: FfiProperty::default(),
    }
}

/// Build variable with property
#[no_mangle]
pub extern "C" fn as_var_ppt(tag: FfiNameOrId, property: FfiProperty) -> FfiVariable {
    FfiVariable { tag, property }
}

fn destroy_ptr<M>(ptr: *const c_void) {
    unsafe {
        let _ = Box::from_raw(ptr as *mut M);
    }
}

/// Initialize a logical plan, which expose a pointer for c-like program to access the
/// entry of the logical plan. This pointer, however, is owned by Rust, and the caller
/// **must not** process any operation, which includes but not limited to deallocate it.
/// We have provided  the [`destroy_logical_plan`] api for deallocating the pointer of the logical plan.
#[no_mangle]
pub extern "C" fn init_logical_plan() -> *const c_void {
    let plan = Box::new(LogicalPlan::default());
    Box::into_raw(plan) as *const c_void
}

/// To destroy a logical plan.
#[no_mangle]
pub extern "C" fn destroy_logical_plan(ptr_plan: *const c_void) {
    destroy_ptr::<LogicalPlan>(ptr_plan)
}

fn append_operator(
    ptr_plan: *const c_void,
    operator: pb::logical_plan::Operator,
    parent_ids: Vec<i32>,
    id: *mut i32,
) -> ResultCode {
    let mut plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };
    let opr_id = plan.append_operator_as_node(
        operator,
        parent_ids
            .into_iter()
            .filter_map(|x| if x >= 0 { Some(x as u32) } else { None })
            .collect(),
    );
    // Do not let rust drop the pointer before explicitly calling `destroy_logical_plan`
    std::mem::forget(plan);
    if opr_id >= 0 {
        unsafe {
            *id = opr_id;
        }
        ResultCode::Success
    } else {
        // This must due to the query parent does not present
        ResultCode::NotExistError
    }
}

#[no_mangle]
pub extern "C" fn debug_plan(ptr_plan: *const c_void) {
    let plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };

    println!("{:#?}", plan);
    std::mem::forget(plan);
}

#[allow(dead_code)]
#[derive(PartialEq)]
enum Opr {
    Select,
    Scan,
    GetV,
    ExpandBase,
    EdgeExpand,
    PathExpand,
    Limit,
    OrderBy,
    Apply,
}

/// Set the size range limitation for certain operators
fn set_range(ptr: *const c_void, lower: i32, upper: i32, opr: Opr) -> ResultCode {
    if lower < 0 || upper < 0 || upper < lower {
        ResultCode::InvalidRangeError
    } else {
        match opr {
            Opr::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                getv.params.as_mut().unwrap().limit = Some(pb::Range { lower, upper });
                std::mem::forget(getv);
            }
            Opr::ExpandBase => {
                let mut base = unsafe { Box::from_raw(ptr as *mut pb::ExpandBase) };
                base.params.as_mut().unwrap().limit = Some(pb::Range { lower, upper });
                std::mem::forget(base);
            }
            Opr::PathExpand => {
                let mut pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                pathxpd.hop_range = Some(pb::Range { lower, upper });
                std::mem::forget(pathxpd);
            }
            Opr::Scan => {
                let mut scan = unsafe { Box::from_raw(ptr as *mut pb::Scan) };
                scan.params.as_mut().unwrap().limit = Some(pb::Range { lower, upper });
                std::mem::forget(scan);
            }
            Opr::Limit => {
                let mut limit = unsafe { Box::from_raw(ptr as *mut pb::Limit) };
                limit.range = Some(pb::Range { lower, upper });
                std::mem::forget(limit);
            }
            Opr::OrderBy => {
                let mut orderby = unsafe { Box::from_raw(ptr as *mut pb::OrderBy) };
                orderby.limit = Some(pb::Range { lower, upper });
                std::mem::forget(orderby);
            }
            _ => unreachable!(),
        }

        ResultCode::Success
    }
}

fn set_alias(ptr: *const c_void, alias: FfiNameOrId, is_query_given: bool, opr: Opr) -> ResultCode {
    let mut return_code = ResultCode::Success;
    let alias_pb: FfiResult<Option<common_pb::NameOrId>> = alias.try_into();
    if alias_pb.is_err() {
        return_code = alias_pb.err().unwrap()
    } else {
        match &opr {
            Opr::Scan => {
                let mut scan = unsafe { Box::from_raw(ptr as *mut pb::Scan) };
                scan.alias = alias_pb.unwrap();
                std::mem::forget(scan);
            }
            Opr::EdgeExpand => {
                let mut edgexpd = unsafe { Box::from_raw(ptr as *mut pb::EdgeExpand) };
                edgexpd.alias = alias_pb.unwrap();
                std::mem::forget(edgexpd);
            }
            Opr::PathExpand => {
                let mut pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                pathxpd.alias = alias_pb.unwrap();
                std::mem::forget(pathxpd);
            }
            Opr::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                getv.alias = alias_pb.unwrap();
                std::mem::forget(getv);
            }
            Opr::Apply => {
                let mut apply = unsafe { Box::from_raw(ptr as *mut pb::Apply) };
                apply.subtask.as_mut().unwrap().alias = Some(pb::project::Alias {
                    alias: alias_pb.unwrap(),
                    is_query_given,
                });
                std::mem::forget(apply);
            }
            _ => unreachable!(),
        }
    }

    return_code
}

/// To set an operator's predicate.
fn set_predicate(ptr: *const c_void, cstr_predicate: *const c_char, opr: Opr) -> ResultCode {
    let mut return_code = ResultCode::Success;
    let predicate_pb = cstr_to_suffix_expr_pb(cstr_predicate);
    if predicate_pb.is_err() {
        return_code = predicate_pb.err().unwrap()
    } else {
        match opr {
            Opr::Select => {
                let mut select = unsafe { Box::from_raw(ptr as *mut pb::Select) };
                select.predicate = predicate_pb.ok();
                std::mem::forget(select);
            }
            Opr::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                getv.params.as_mut().unwrap().predicate = predicate_pb.ok();
                std::mem::forget(getv);
            }
            Opr::ExpandBase => {
                let mut expand = unsafe { Box::from_raw(ptr as *mut pb::ExpandBase) };
                expand.params.as_mut().unwrap().predicate = predicate_pb.ok();
                std::mem::forget(expand);
            }
            Opr::Scan => {
                let mut scan = unsafe { Box::from_raw(ptr as *mut pb::Scan) };
                scan.params.as_mut().unwrap().predicate = predicate_pb.ok();
                std::mem::forget(scan);
            }
            _ => unreachable!(),
        }
    }

    return_code
}

#[derive(PartialEq)]
enum ParamsKey {
    Tag,
    Table,
    Column,
}

fn process_params(ptr: *const c_void, key: ParamsKey, val: FfiNameOrId, opr: Opr) -> ResultCode {
    let mut return_code = ResultCode::Success;
    let pb: FfiResult<Option<common_pb::NameOrId>> = val.try_into();
    if pb.is_ok() {
        match opr {
            Opr::ExpandBase => {
                let mut expand = unsafe { Box::from_raw(ptr as *mut pb::ExpandBase) };
                match key {
                    ParamsKey::Tag => expand.v_tag = pb.unwrap(),
                    ParamsKey::Table => {
                        if let Some(label) = pb.unwrap() {
                            expand.params.as_mut().unwrap().table_names.push(label)
                        }
                    }
                    ParamsKey::Column => {
                        if let Some(ppt) = pb.unwrap() {
                            expand.params.as_mut().unwrap().columns.push(ppt)
                        }
                    }
                }
                std::mem::forget(expand);
            }
            Opr::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                match key {
                    ParamsKey::Tag => getv.tag = pb.unwrap(),
                    ParamsKey::Table => {
                        if let Some(label) = pb.unwrap() {
                            getv.params.as_mut().unwrap().table_names.push(label)
                        }
                    }
                    ParamsKey::Column => {
                        if let Some(ppt) = pb.unwrap() {
                            getv.params.as_mut().unwrap().columns.push(ppt)
                        }
                    }
                }
                std::mem::forget(getv);
            }
            Opr::Scan => {
                let mut scan = unsafe { Box::from_raw(ptr as *mut pb::Scan) };
                match key {
                    ParamsKey::Table => {
                        if let Some(table) = pb.unwrap() {
                            scan.params.as_mut().unwrap().table_names.push(table)
                        }
                    }
                    ParamsKey::Column => {
                        if let Some(col) = pb.unwrap() {
                            scan.params.as_mut().unwrap().columns.push(col)
                        }
                    }
                    _ => unreachable!(),
                }
                std::mem::forget(scan);
            }
            _ => unreachable!(),
        }
    } else {
        return_code = pb.err().unwrap();
    }

    return_code
}

mod project {
    use super::*;
    /// To initialize a project operator.
    #[no_mangle]
    pub extern "C" fn init_project_operator(is_append: bool) -> *const c_void {
        let project = Box::new(pb::Project {
            mappings: vec![],
            is_append,
        });
        Box::into_raw(project) as *const c_void
    }

    /// To add a mapping for the project operator, which maps a c-like string to represent an
    /// expression, to a `NameOrId` parameter that represents an alias.
    #[no_mangle]
    pub extern "C" fn add_project_mapping(
        ptr_project: *const c_void,
        cstr_expr: *const c_char,
        alias: FfiNameOrId,
        is_query_given: bool,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut project = unsafe { Box::from_raw(ptr_project as *mut pb::Project) };
        let expr_pb = cstr_to_suffix_expr_pb(cstr_expr);
        let alias_pb = Option::<common_pb::NameOrId>::try_from(alias);

        if !expr_pb.is_ok() || !alias_pb.is_ok() {
            return_code = expr_pb.err().unwrap();
        } else {
            let attribute = pb::project::ExprAlias {
                expr: expr_pb.ok(),
                alias: Some(pb::project::Alias {
                    alias: alias_pb.unwrap(),
                    is_query_given,
                }),
            };
            project.mappings.push(attribute);
        }
        std::mem::forget(project);

        return_code
    }

    /// Append a project operator to the logical plan. To do so, one specifies the following arguments:
    /// * `ptr_plan`: A rust-owned pointer created by `init_logical_plan()`.
    /// * `ptr_project`: A rust-owned pointer created by `init_project_operator()`.
    /// * `parent_id`: The unique parent operator's index in the logical plan, which must be present
    /// except when a negative id is provided to bypass the setting of parent operator.
    /// * `id`: An index pointer that gonna hold the index of this operator.
    ///
    /// If it is successful to be appended to the logical plan, the `ptr_project` will be
    /// automatically released by the rust program. Therefore, the caller needs not to deallocate
    /// the pointer, and must **not** use it thereafter.
    ///
    /// Otherwise, user can manually call [`destroy_project_operator()`] to release the pointer.
    ///
    /// # Return
    /// * Returning [`ResultCode`] to capture any error.
    ///
    /// **Note**: All following `append_xx_operator()` apis have the same usage as this one.
    ///
    #[no_mangle]
    pub extern "C" fn append_project_operator(
        ptr_plan: *const c_void,
        ptr_project: *const c_void,
        parent_id: i32,
        id: *mut i32,
    ) -> ResultCode {
        let project = unsafe { Box::from_raw(ptr_project as *mut pb::Project) };
        append_operator(
            ptr_plan,
            project.as_ref().clone().into(),
            vec![parent_id],
            id,
        )
    }

    #[no_mangle]
    pub extern "C" fn destroy_project_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Project>(ptr)
    }
}

mod select {
    use super::*;

    /// To initialize a select operator
    #[no_mangle]
    pub extern "C" fn init_select_operator() -> *const c_void {
        let select = Box::new(pb::Select { predicate: None });
        Box::into_raw(select) as *const c_void
    }

    /// To set a select operator's metadata, which is a predicate represented as a c-string.
    #[no_mangle]
    pub extern "C" fn set_select_predicate(
        ptr_select: *const c_void,
        cstr_predicate: *const c_char,
    ) -> ResultCode {
        set_predicate(ptr_select, cstr_predicate, Opr::Select)
    }

    /// Append a select operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_select_operator(
        ptr_plan: *const c_void,
        ptr_select: *const c_void,
        parent_id: i32,
        id: *mut i32,
    ) -> ResultCode {
        let select = unsafe { Box::from_raw(ptr_select as *mut pb::Select) };
        append_operator(
            ptr_plan,
            select.as_ref().clone().into(),
            vec![parent_id],
            id,
        )
    }

    #[no_mangle]
    pub extern "C" fn destroy_select_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Select>(ptr)
    }
}

mod join {
    use super::*;

    #[allow(dead_code)]
    #[repr(i32)]
    #[derive(Copy, Clone, Debug)]
    pub enum FfiJoinKind {
        /// Inner join
        Inner = 0,
        /// Left outer join
        LeftOuter = 1,
        /// Right outer join
        RightOuter = 2,
        /// Full outer join
        FullOuter = 3,
        /// Left semi-join, right alternative can be naturally adapted
        Semi = 4,
        /// Left anti-join, right alternative can be naturally adapted
        Anti = 5,
        /// aka. Cartesian product
        Times = 6,
    }

    /// To initialize a join operator
    #[no_mangle]
    pub extern "C" fn init_join_operator(join_kind: FfiJoinKind) -> *const c_void {
        let kind = match join_kind {
            FfiJoinKind::Inner => 0,
            FfiJoinKind::LeftOuter => 1,
            FfiJoinKind::RightOuter => 2,
            FfiJoinKind::FullOuter => 3,
            FfiJoinKind::Semi => 4,
            FfiJoinKind::Anti => 5,
            FfiJoinKind::Times => 6,
        };
        let join = Box::new(pb::Join {
            left_keys: vec![],
            right_keys: vec![],
            kind,
        });
        Box::into_raw(join) as *const c_void
    }

    /// To add a join operator's metadata, which is a pair of left and right keys.
    /// In the join processing, a pair of data will be output if the corresponding fields
    /// regarding left and right keys are **equivalent**.
    #[no_mangle]
    pub extern "C" fn add_join_key_pair(
        ptr_join: *const c_void,
        left_key: FfiVariable,
        right_key: FfiVariable,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut join = unsafe { Box::from_raw(ptr_join as *mut pb::Join) };
        let left_key_pb: FfiResult<common_pb::Variable> = left_key.try_into();
        let right_key_pb: FfiResult<common_pb::Variable> = right_key.try_into();
        if left_key_pb.is_err() {
            return_code = left_key_pb.err().unwrap();
        } else if right_key_pb.is_err() {
            return_code = right_key_pb.err().unwrap();
        } else {
            join.left_keys.push(left_key_pb.unwrap());
            join.right_keys.push(right_key_pb.unwrap());
        }
        std::mem::forget(join);

        return_code
    }

    /// Append a join operator to the logical plan. Note that both left and right parent ids
    /// for join must be non-negative, and they must refer some nodes in the logical plan
    #[no_mangle]
    pub extern "C" fn append_join_operator(
        ptr_plan: *const c_void,
        ptr_join: *const c_void,
        parent_left: i32,
        parent_right: i32,
        id: *mut i32,
    ) -> ResultCode {
        if parent_left < 0 || parent_right < 0 {
            ResultCode::NegativeIndexError
        } else {
            let join = unsafe { Box::from_raw(ptr_join as *mut pb::Join) };
            append_operator(
                ptr_plan,
                join.as_ref().clone().into(),
                vec![parent_left, parent_right],
                id,
            )
        }
    }

    #[no_mangle]
    pub extern "C" fn destroy_join_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Join>(ptr)
    }
}

mod union {
    use super::*;

    /// To initialize a union operator
    #[no_mangle]
    pub extern "C" fn init_union_operator() -> *const c_void {
        let union = Box::new(pb::Union {});
        Box::into_raw(union) as *const c_void
    }

    /// Append a union operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_union_operator(
        ptr_plan: *const c_void,
        ptr_union: *const c_void,
        parent_left: i32,
        parent_right: i32,
        id: *mut i32,
    ) -> ResultCode {
        let union = unsafe { Box::from_raw(ptr_union as *mut pb::Union) };
        append_operator(
            ptr_plan,
            union.as_ref().clone().into(),
            vec![parent_left, parent_right],
            id,
        )
    }
}

mod groupby {
    use super::*;

    /// To initialize a groupby operator
    #[no_mangle]
    pub extern "C" fn init_groupby_operator() -> *const c_void {
        let group = Box::new(pb::GroupBy {
            keys: vec![],
            functions: vec![],
        });
        Box::into_raw(group) as *const c_void
    }

    #[allow(dead_code)]
    #[repr(i32)]
    #[derive(Clone, Copy)]
    pub enum FfiAggOpt {
        Sum = 0,
        Min = 1,
        Max = 2,
        Count = 3,
        CountDistinct = 4,
        ToList = 5,
        ToSet = 6,
        Avg = 7,
    }

    #[repr(C)]
    pub struct FfiAggFn {
        vars: *const FfiVariable,
        aggregate: FfiAggOpt,
        alias: FfiNameOrId,
    }

    impl TryFrom<FfiAggFn> for pb::group_by::AggFunc {
        type Error = ResultCode;

        fn try_from(value: FfiAggFn) -> Result<Self, Self::Error> {
            let mut agg_fn_pb = pb::group_by::AggFunc {
                vars: vec![],
                aggregate: unsafe { std::mem::transmute::<FfiAggOpt, i32>(value.aggregate) },
                alias: None,
            };
            let (vars, alias) = (value.vars as *mut Vec<FfiVariable>, value.alias);
            let vars: Box<Vec<FfiVariable>> = unsafe { Box::from_raw(vars) };
            for var in vars.into_iter() {
                agg_fn_pb.vars.push(var.try_into()?)
            }
            agg_fn_pb.alias = alias.try_into()?;

            Ok(agg_fn_pb)
        }
    }

    /// The group function actually requires a collection of variables. Right now we
    /// provide the support of just one variable cause it suits for most cases already.
    /// TODO(longbin) Will provide the support for multiple grouping variables
    #[no_mangle]
    pub extern "C" fn build_agg_fn(
        agg_var: FfiVariable,
        aggregate: FfiAggOpt,
        alias: FfiNameOrId,
    ) -> FfiAggFn {
        let vars: Box<Vec<FfiVariable>> = Box::new(vec![agg_var]);
        FfiAggFn {
            vars: Box::into_raw(vars) as *const FfiVariable,
            aggregate,
            alias,
        }
    }

    /// Add the key according to which the grouping is conducted
    #[no_mangle]
    pub extern "C" fn add_groupby_key(ptr_groupby: *const c_void, key: FfiVariable) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut group = unsafe { Box::from_raw(ptr_groupby as *mut pb::GroupBy) };
        let key_pb: FfiResult<common_pb::Variable> = key.try_into();
        if key_pb.is_ok() {
            group.keys.push(key_pb.unwrap());
        } else {
            return_code = key_pb.err().unwrap();
        }
        std::mem::forget(group);

        return_code
    }

    /// Add the aggregate function for each group.
    #[no_mangle]
    pub extern "C" fn add_groupby_agg_fn(
        ptr_groupby: *const c_void,
        agg_fn: FfiAggFn,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut group = unsafe { Box::from_raw(ptr_groupby as *mut pb::GroupBy) };
        let agg_fn_pb: FfiResult<pb::group_by::AggFunc> = agg_fn.try_into();

        if agg_fn_pb.is_ok() {
            group.as_mut().functions.push(agg_fn_pb.unwrap());
        } else {
            return_code = agg_fn_pb.err().unwrap();
        }
        std::mem::forget(group);

        return_code
    }

    /// Append a groupby operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_groupby_operator(
        ptr_plan: *const c_void,
        ptr_groupby: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let group = unsafe { Box::from_raw(ptr_groupby as *mut pb::GroupBy) };
        append_operator(ptr_plan, group.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_groupby_operator(ptr: *const c_void) {
        destroy_ptr::<pb::GroupBy>(ptr)
    }
}

mod orderby {
    use super::*;

    #[allow(dead_code)]
    #[repr(i32)]
    #[derive(Clone, Copy)]
    pub enum FfiOrderOpt {
        Shuffle = 0,
        Asc = 1,
        Desc = 2,
    }

    /// To initialize an orderby operator
    #[no_mangle]
    pub extern "C" fn init_orderby_operator() -> *const c_void {
        let order = Box::new(pb::OrderBy {
            pairs: vec![],
            limit: None,
        });
        Box::into_raw(order) as *const c_void
    }

    /// Add the pair for conducting ordering.
    #[no_mangle]
    pub extern "C" fn add_orderby_pair(
        ptr_orderby: *const c_void,
        var: FfiVariable,
        order_opt: FfiOrderOpt,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut orderby = unsafe { Box::from_raw(ptr_orderby as *mut pb::OrderBy) };
        let key_result: FfiResult<common_pb::Variable> = var.try_into();
        if key_result.is_ok() {
            let order = match order_opt {
                FfiOrderOpt::Shuffle => 0,
                FfiOrderOpt::Asc => 1,
                FfiOrderOpt::Desc => 2,
            };
            orderby.pairs.push(pb::order_by::OrderingPair {
                key: key_result.ok(),
                order,
            });
        } else {
            return_code = key_result.err().unwrap();
        }
        std::mem::forget(orderby);

        return_code
    }

    /// Set the size limit of the orderby operator, which will turn it into topk
    #[no_mangle]
    pub extern "C" fn set_orderby_limit(
        ptr_orderby: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_orderby, lower, upper, Opr::OrderBy)
    }

    /// Append an orderby operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_orderby_operator(
        ptr_plan: *const c_void,
        ptr_orderby: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let orderby = unsafe { Box::from_raw(ptr_orderby as *mut pb::OrderBy) };
        append_operator(ptr_plan, orderby.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_orderby_operator(ptr: *const c_void) {
        destroy_ptr::<pb::OrderBy>(ptr)
    }
}

mod dedup {
    use super::*;

    /// To initialize a dedup operator
    #[no_mangle]
    pub extern "C" fn init_dedup_operator() -> *const c_void {
        let dedup = Box::new(pb::Dedup { keys: vec![] });
        Box::into_raw(dedup) as *const c_void
    }

    /// Add a key for de-duplicating.
    #[no_mangle]
    pub extern "C" fn add_dedup_key(ptr_dedup: *const c_void, var: FfiVariable) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut dedup = unsafe { Box::from_raw(ptr_dedup as *mut pb::Dedup) };
        let key_result: FfiResult<common_pb::Variable> = var.try_into();
        if key_result.is_ok() {
            dedup.keys.push(key_result.unwrap());
        } else {
            return_code = key_result.err().unwrap();
        }
        std::mem::forget(dedup);

        return_code
    }

    /// Append a dedup operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_dedup_operator(
        ptr_plan: *const c_void,
        ptr_dedup: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let dedup = unsafe { Box::from_raw(ptr_dedup as *mut pb::Dedup) };
        append_operator(ptr_plan, dedup.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_dedup_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Dedup>(ptr)
    }
}

mod unfold {
    use super::*;

    /// To initialize an unfold operator
    #[no_mangle]
    pub extern "C" fn init_unfold_operator() -> *const c_void {
        let unfold = Box::new(pb::Unfold {
            tag: None,
            alias: None,
        });
        Box::into_raw(unfold) as *const c_void
    }

    /// Set the argument pair for unfold, which are:
    /// * a tag points to a collection-type data field for unfolding,
    /// * an alias for referencing to each element of the collection.
    #[no_mangle]
    pub extern "C" fn set_unfold_pair(
        ptr_unfold: *const c_void,
        tag: FfiNameOrId,
        alias: FfiNameOrId,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut unfold = unsafe { Box::from_raw(ptr_unfold as *mut pb::Unfold) };
        let tag_result: FfiResult<Option<common_pb::NameOrId>> = tag.try_into();
        let alias_result: FfiResult<Option<common_pb::NameOrId>> = alias.try_into();

        if tag_result.is_ok() && alias_result.is_ok() {
            unfold.tag = tag_result.unwrap();
            unfold.alias = alias_result.unwrap();
        } else {
            return_code = if tag_result.is_err() {
                tag_result.err().unwrap()
            } else {
                alias_result.err().unwrap()
            };
        }
        std::mem::forget(unfold);

        return_code
    }

    /// Append an unfold operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_unfold_operator(
        ptr_plan: *const c_void,
        ptr_unfold: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let unfold = unsafe { Box::from_raw(ptr_unfold as *mut pb::Unfold) };
        append_operator(ptr_plan, unfold.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_unfold_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Unfold>(ptr)
    }
}

mod scan {
    use super::*;

    #[allow(dead_code)]
    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(i32)]
    pub enum FfiScanOpt {
        Vertex = 0,
        Edge = 1,
        Table = 2,
    }

    /// To initialize a scan operator
    #[no_mangle]
    pub extern "C" fn init_scan_operator(scan_opt: FfiScanOpt) -> *const c_void {
        let scan = Box::new(pb::Scan {
            scan_opt: unsafe { std::mem::transmute::<FfiScanOpt, i32>(scan_opt) },
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
        });
        Box::into_raw(scan) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn set_scan_limit(
        ptr_scan: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_scan, lower, upper, Opr::Scan)
    }

    #[no_mangle]
    pub extern "C" fn add_scan_table_name(
        ptr_scan: *const c_void,
        table_name: FfiNameOrId,
    ) -> ResultCode {
        process_params(ptr_scan, ParamsKey::Table, table_name, Opr::Scan)
    }

    /// Add a data field to be scanned from the data source (vertex, edge, or a relational table)
    #[no_mangle]
    pub extern "C" fn add_scan_column(ptr_scan: *const c_void, column: FfiNameOrId) -> ResultCode {
        process_params(ptr_scan, ParamsKey::Column, column, Opr::Scan)
    }

    /// Set an alias for the data if it is a vertex/edge
    #[no_mangle]
    pub extern "C" fn set_scan_alias(ptr_scan: *const c_void, alias: FfiNameOrId) -> ResultCode {
        set_alias(ptr_scan, alias, true, Opr::Scan)
    }

    /// Append a scan operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_scan_operator(
        ptr_plan: *const c_void,
        ptr_scan: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
        append_operator(ptr_plan, scan.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_scan_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Scan>(ptr)
    }
}

mod idxscan {
    use super::*;
    use ir_common::generated::algebra::indexed_scan::{KvEquivPair, KvEquivPairs};

    /// To initialize an indexed-scan operator from a scan operator
    #[no_mangle]
    pub extern "C" fn init_idxscan_operator(ptr_scan: *const c_void) -> *const c_void {
        let scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
        let indexed_scan = Box::new(pb::IndexedScan {
            scan: Some(scan.as_ref().clone()),
            or_kv_equiv_pairs: vec![],
        });
        Box::into_raw(indexed_scan) as *const c_void
    }

    #[derive(Clone, Copy)]
    #[repr(i32)]
    pub enum FfiDataType {
        Unknown = 0,
        Boolean = 1,
        I32 = 2,
        I64 = 3,
        F64 = 4,
        Str = 5,
        // TODO(longbin) More data type will be defined
    }

    #[derive(Clone)]
    #[repr(C)]
    pub struct FfiConst {
        data_type: FfiDataType,
        boolean: bool,
        int32: i32,
        int64: i64,
        float64: f64,
        cstr: *const c_char,
        raw: *const c_void,
    }

    impl Default for FfiConst {
        fn default() -> Self {
            FfiConst {
                data_type: FfiDataType::Unknown,
                boolean: false,
                int32: 0,
                int64: 0,
                float64: 0.0,
                cstr: std::ptr::null::<c_char>(),
                raw: std::ptr::null::<c_void>(),
            }
        }
    }

    impl TryFrom<FfiConst> for common_pb::Const {
        type Error = ResultCode;

        fn try_from(ffi: FfiConst) -> Result<Self, Self::Error> {
            match &ffi.data_type {
                FfiDataType::Unknown => Err(ResultCode::UnknownTypeError),
                FfiDataType::Boolean => Ok(common_pb::Const {
                    value: Some(common_pb::Value::from(ffi.boolean)),
                }),
                FfiDataType::I32 => Ok(common_pb::Const {
                    value: Some(common_pb::Value::from(ffi.int32)),
                }),
                FfiDataType::I64 => Ok(common_pb::Const {
                    value: Some(common_pb::Value::from(ffi.int64)),
                }),
                FfiDataType::F64 => Ok(common_pb::Const {
                    value: Some(common_pb::Value::from(ffi.float64)),
                }),
                FfiDataType::Str => {
                    let str = cstr_to_string(ffi.cstr);
                    if str.is_ok() {
                        Ok(common_pb::Const {
                            value: str.ok().map(|s| common_pb::Value::from(s)),
                        })
                    } else {
                        Err(str.err().unwrap())
                    }
                }
            }
        }
    }

    #[no_mangle]
    pub extern "C" fn boolean_as_const(boolean: bool) -> FfiConst {
        let mut ffi = FfiConst::default();
        ffi.data_type = FfiDataType::Boolean;
        ffi.boolean = boolean;
        ffi
    }

    #[no_mangle]
    pub extern "C" fn int32_as_const(int32: i32) -> FfiConst {
        let mut ffi = FfiConst::default();
        ffi.data_type = FfiDataType::I32;
        ffi.int32 = int32;
        ffi
    }

    #[no_mangle]
    pub extern "C" fn int64_as_const(int64: i64) -> FfiConst {
        let mut ffi = FfiConst::default();
        ffi.data_type = FfiDataType::I64;
        ffi.int64 = int64;
        ffi
    }

    #[no_mangle]
    pub extern "C" fn f64_as_const(float64: f64) -> FfiConst {
        let mut ffi = FfiConst::default();
        ffi.data_type = FfiDataType::F64;
        ffi.float64 = float64;
        ffi
    }

    #[no_mangle]
    pub extern "C" fn cstr_as_const(cstr: *const c_char) -> FfiConst {
        let mut ffi = FfiConst::default();
        ffi.data_type = FfiDataType::Str;
        ffi.cstr = cstr;
        ffi
    }

    #[no_mangle]
    pub extern "C" fn init_kv_equiv_pairs() -> *const c_void {
        let pairs: Box<Vec<KvEquivPair>> = Box::new(vec![]);
        Box::into_raw(pairs) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn and_kv_equiv_pair(
        ptr_pairs: *const c_void,
        key: FfiProperty,
        value: FfiConst,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let key_pb: FfiResult<Option<common_pb::Property>> = key.try_into();
        let value_pb: FfiResult<common_pb::Const> = value.try_into();
        if key_pb.is_err() {
            return_code = key_pb.err().unwrap();
        } else if value_pb.is_err() {
            return_code = value_pb.err().unwrap();
        } else {
            let mut kv_equiv_pairs = unsafe { Box::from_raw(ptr_pairs as *mut Vec<KvEquivPair>) };
            kv_equiv_pairs.push(KvEquivPair {
                key: key_pb.unwrap(),
                value: value_pb.ok(),
            });
            std::mem::forget(kv_equiv_pairs)
        }

        return_code
    }

    #[no_mangle]
    pub extern "C" fn add_idxscan_kv_equiv_pairs(
        ptr_idxscan: *const c_void,
        ptr_pairs: *const c_void,
    ) -> ResultCode {
        let mut idxscan = unsafe { Box::from_raw(ptr_idxscan as *mut pb::IndexedScan) };
        let kv_equiv_pairs = unsafe { Box::from_raw(ptr_pairs as *mut Vec<KvEquivPair>) };
        idxscan.or_kv_equiv_pairs.push(KvEquivPairs {
            pairs: kv_equiv_pairs.as_ref().clone(),
        });
        std::mem::forget(idxscan);

        ResultCode::Success
    }

    /// Append an indexed scan operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_idxscan_operator(
        ptr_plan: *const c_void,
        ptr_idxscan: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let idxscan = unsafe { Box::from_raw(ptr_idxscan as *mut pb::IndexedScan) };
        append_operator(ptr_plan, idxscan.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_idxscan_operator(ptr: *const c_void) {
        destroy_ptr::<pb::IndexedScan>(ptr)
    }
}

mod limit {
    use super::*;

    #[no_mangle]
    pub extern "C" fn init_limit_operator() -> *const c_void {
        let limit: Box<pb::Limit> = Box::new(pb::Limit { range: None });
        Box::into_raw(limit) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn set_limit_range(
        ptr_limit: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_limit, lower, upper, Opr::Limit)
    }

    /// Append an indexed scan operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_limit_operator(
        ptr_plan: *const c_void,
        ptr_limit: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let limit = unsafe { Box::from_raw(ptr_limit as *mut pb::Limit) };
        append_operator(ptr_plan, limit.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_limit_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Limit>(ptr)
    }
}

mod graph {
    use super::*;

    #[allow(dead_code)]
    #[derive(Copy, Clone)]
    #[repr(i32)]
    pub enum FfiDirection {
        Out = 0,
        In = 1,
        Both = 2,
    }

    /// To initialize an expansion base
    #[no_mangle]
    pub extern "C" fn init_expand_base(direction: FfiDirection) -> *const c_void {
        let expand = Box::new(pb::ExpandBase {
            v_tag: None,
            direction: unsafe { std::mem::transmute::<FfiDirection, i32>(direction) },
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
        });
        Box::into_raw(expand) as *const c_void
    }

    /// Set the start-vertex's tag to conduct this expansion
    #[no_mangle]
    pub extern "C" fn set_expand_vtag(ptr_expand: *const c_void, v_tag: FfiNameOrId) -> ResultCode {
        process_params(ptr_expand, ParamsKey::Tag, v_tag, Opr::ExpandBase)
    }

    /// Add a label of the edge that this expansion must satisfy
    #[no_mangle]
    pub extern "C" fn add_expand_label(
        ptr_expand: *const c_void,
        label: FfiNameOrId,
    ) -> ResultCode {
        process_params(ptr_expand, ParamsKey::Table, label, Opr::ExpandBase)
    }

    /// Add a property that this edge expansion must carry
    #[no_mangle]
    pub extern "C" fn add_expand_property(
        ptr_expand: *const c_void,
        property: FfiNameOrId,
    ) -> ResultCode {
        process_params(ptr_expand, ParamsKey::Column, property, Opr::ExpandBase)
    }

    /// Set the size range limitation of this expansion
    #[no_mangle]
    pub extern "C" fn set_expand_limit(
        ptr_expand: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_expand, lower, upper, Opr::ExpandBase)
    }

    /// Set the edge predicate of this expansion
    #[no_mangle]
    pub extern "C" fn set_expand_predicate(
        ptr_expand: *const c_void,
        cstr_predicate: *const c_char,
    ) -> ResultCode {
        set_predicate(ptr_expand, cstr_predicate, Opr::ExpandBase)
    }

    /// To initialize an edge expand operator from an expand base
    #[no_mangle]
    pub extern "C" fn init_edgexpd_operator(
        ptr_expand: *const c_void,
        is_edge: bool,
    ) -> *const c_void {
        let expand = unsafe { Box::from_raw(ptr_expand as *mut pb::ExpandBase) };
        let edgexpd = Box::new(pb::EdgeExpand {
            base: Some(expand.as_ref().clone()),
            is_edge,
            alias: None,
        });

        Box::into_raw(edgexpd) as *const c_void
    }

    /// Set edge alias of this edge expansion
    #[no_mangle]
    pub extern "C" fn set_edgexpd_alias(
        ptr_edgexpd: *const c_void,
        alias: FfiNameOrId,
    ) -> ResultCode {
        set_alias(ptr_edgexpd, alias, true, Opr::EdgeExpand)
    }

    /// Append an edge expand operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_edgexpd_operator(
        ptr_plan: *const c_void,
        ptr_edgexpd: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let edgexpd = unsafe { Box::from_raw(ptr_edgexpd as *mut pb::EdgeExpand) };
        append_operator(ptr_plan, edgexpd.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_edgexpd_operator(ptr: *const c_void) {
        destroy_ptr::<pb::EdgeExpand>(ptr)
    }

    #[allow(dead_code)]
    #[repr(i32)]
    pub enum FfiVOpt {
        Start = 0,
        End = 1,
        Other = 2,
        This = 3,
    }

    /// To initialize an expansion base
    #[no_mangle]
    pub extern "C" fn init_getv_operator(opt: FfiVOpt) -> *const c_void {
        let getv = Box::new(pb::GetV {
            tag: None,
            opt: unsafe { std::mem::transmute::<FfiVOpt, i32>(opt) },
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            alias: None,
        });
        Box::into_raw(getv) as *const c_void
    }

    /// Set the tag of edge/path to get its end vertex
    #[no_mangle]
    pub extern "C" fn set_getv_tag(ptr_getv: *const c_void, tag: FfiNameOrId) -> ResultCode {
        process_params(ptr_getv, ParamsKey::Tag, tag, Opr::GetV)
    }

    /// Set vertex alias of this getting vertex
    #[no_mangle]
    pub extern "C" fn set_getv_alias(ptr_getv: *const c_void, alias: FfiNameOrId) -> ResultCode {
        set_alias(ptr_getv, alias, true, Opr::GetV)
    }

    /// Add a label of the vertex that this getv must satisfy
    #[no_mangle]
    pub extern "C" fn add_getv_label(ptr_getv: *const c_void, label: FfiNameOrId) -> ResultCode {
        process_params(ptr_getv, ParamsKey::Table, label, Opr::GetV)
    }

    /// Add a property that this vertex must carry
    #[no_mangle]
    pub extern "C" fn add_getv_property(
        ptr_getv: *const c_void,
        property: FfiNameOrId,
    ) -> ResultCode {
        process_params(ptr_getv, ParamsKey::Column, property, Opr::GetV)
    }

    /// Set the size range limitation of getting vertices
    #[no_mangle]
    pub extern "C" fn set_getv_limit(
        ptr_getv: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_getv, lower, upper, Opr::GetV)
    }

    /// Set the predicate of getting vertices
    #[no_mangle]
    pub extern "C" fn set_getv_predicate(
        ptr_getv: *const c_void,
        cstr_predicate: *const c_char,
    ) -> ResultCode {
        set_predicate(ptr_getv, cstr_predicate, Opr::GetV)
    }

    /// Append an edge expand operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_getv_operator(
        ptr_plan: *const c_void,
        ptr_getv: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let getv = unsafe { Box::from_raw(ptr_getv as *mut pb::GetV) };
        append_operator(ptr_plan, getv.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_getv_operator(ptr: *const c_void) {
        destroy_ptr::<pb::GetV>(ptr)
    }

    /// To initialize an path expand operator from an expand base
    #[no_mangle]
    pub extern "C" fn init_pathxpd_operator(
        ptr_expand: *const c_void,
        is_path: bool,
    ) -> *const c_void {
        let expand = unsafe { Box::from_raw(ptr_expand as *mut pb::ExpandBase) };
        let edgexpd = Box::new(pb::PathExpand {
            base: Some(expand.as_ref().clone()),
            is_path,
            alias: None,
            hop_range: None,
        });

        Box::into_raw(edgexpd) as *const c_void
    }

    /// Set path alias of this path expansion
    #[no_mangle]
    pub extern "C" fn set_pathxpd_alias(
        ptr_pathxpd: *const c_void,
        alias: FfiNameOrId,
    ) -> ResultCode {
        set_alias(ptr_pathxpd, alias, true, Opr::PathExpand)
    }

    /// Set the hop-range limitation of expanding path
    #[no_mangle]
    pub extern "C" fn set_pathxpd_hops(
        ptr_pathxpd: *const c_void,
        lower: i32,
        upper: i32,
    ) -> ResultCode {
        set_range(ptr_pathxpd, lower, upper, Opr::PathExpand)
    }

    /// Append an path-expand operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_pathxpd_operator(
        ptr_plan: *const c_void,
        ptr_pathxpd: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let pathxpd = unsafe { Box::from_raw(ptr_pathxpd as *mut pb::PathExpand) };
        append_operator(ptr_plan, pathxpd.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_pathxpd_operator(ptr: *const c_void) {
        destroy_ptr::<pb::PathExpand>(ptr)
    }
}

mod subtask {
    use super::*;
    use crate::plan::ffi::join::FfiJoinKind;

    /// To initialize an apply operator from a root node (id) of the subtask. Before initializing
    /// the subtask, one need to first prepare the subtask and append the operators within to the
    /// logical plan.
    #[no_mangle]
    pub extern "C" fn init_apply_operator(
        subtask_root: i32,
        join_kind: FfiJoinKind,
    ) -> *const c_void {
        let apply = Box::new(pb::Apply {
            join_kind: unsafe { std::mem::transmute::<FfiJoinKind, i32>(join_kind) },
            subtask: Some(pb::apply::Subtask {
                tags: vec![],
                subtask: subtask_root,
                alias: None,
            }),
        });

        Box::into_raw(apply) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn add_apply_tag(ptr_apply: *const c_void, ffi_tag: FfiNameOrId) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let tag_pb: FfiResult<Option<common_pb::NameOrId>> = ffi_tag.try_into();
        if tag_pb.is_ok() {
            if let Some(tag) = tag_pb.unwrap() {
                let mut apply = unsafe { Box::from_raw(ptr_apply as *mut pb::Apply) };
                apply.subtask.as_mut().unwrap().tags.push(tag);
                std::mem::forget(apply);
            }
        } else {
            return_code = tag_pb.err().unwrap();
        }

        return_code
    }

    #[no_mangle]
    pub extern "C" fn set_apply_alias(
        ptr_apply: *const c_void,
        alias: FfiNameOrId,
        is_query_given: bool,
    ) -> ResultCode {
        set_alias(ptr_apply, alias, is_query_given, Opr::Apply)
    }

    /// Append an apply operator to the logical plan.
    /// If the apply is used alone (other than segment apply), the parent node must set and present
    /// in the logical plan.
    #[no_mangle]
    pub extern "C" fn append_apply_operator(
        ptr_plan: *const c_void,
        ptr_apply: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        let apply = unsafe { Box::from_raw(ptr_apply as *mut pb::Apply) };
        append_operator(ptr_plan, apply.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_apply_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Apply>(ptr)
    }

    /// To initialize a segment apply operator from an apply operator.
    #[no_mangle]
    pub extern "C" fn init_segapply_operator(ptr_apply: *const c_void) -> *const c_void {
        let apply = unsafe { Box::from_raw(ptr_apply as *mut pb::Apply) };
        let segapply = Box::new(pb::SegmentApply {
            keys: vec![],
            apply_subtask: Some(apply.as_ref().clone()),
        });

        Box::into_raw(segapply) as *const c_void
    }

    /// To add the key for grouping on which the segment apply can be conducted.
    #[no_mangle]
    pub extern "C" fn add_segapply_key(
        ptr_segapply: *const c_void,
        ffi_key: FfiNameOrId,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let key_pb: FfiResult<Option<common_pb::NameOrId>> = ffi_key.try_into();
        if key_pb.is_ok() {
            if let Some(key) = key_pb.unwrap() {
                let mut segapply = unsafe { Box::from_raw(ptr_segapply as *mut pb::SegmentApply) };
                segapply.keys.push(key);
                std::mem::forget(segapply);
            }
        } else {
            return_code = key_pb.err().unwrap();
        }

        return_code
    }

    /// Append an apply operator to the logical plan. The parent node id for appending a segment apply operator
    /// must not be negative and must present in the logical plan.
    #[no_mangle]
    pub extern "C" fn append_segapply_operator(
        ptr_plan: *const c_void,
        ptr_segapply: *const c_void,
        parent: i32,
        id: *mut i32,
    ) -> ResultCode {
        if parent < 0 {
            ResultCode::NegativeIndexError
        } else {
            let segapply = unsafe { Box::from_raw(ptr_segapply as *mut pb::SegmentApply) };
            append_operator(ptr_plan, segapply.as_ref().clone().into(), vec![parent], id)
        }
    }

    #[no_mangle]
    pub extern "C" fn destroy_segapply_operator(ptr: *const c_void) {
        destroy_ptr::<pb::SegmentApply>(ptr)
    }
}
