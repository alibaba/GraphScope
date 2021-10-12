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
//!
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

use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use crate::plan::{cstr_to_string, cstr_to_suffix_expr_pb, FfiResult, LogicalPlan, ResultCode};
use std::convert::{TryFrom, TryInto};
use std::ffi::c_void;
use std::os::raw::c_char;

#[repr(i32)]
#[derive(Copy, Clone, Debug)]
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

impl TryFrom<FfiNameOrId> for common_pb::NameOrId {
    type Error = ResultCode;

    fn try_from(ffi: FfiNameOrId) -> FfiResult<Self> {
        match &ffi.opt {
            FfiNameIdOpt::None => Err(ResultCode::NotExistError),
            FfiNameIdOpt::Name => Ok(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::Name(cstr_to_string(ffi.name)?)),
            }),
            FfiNameIdOpt::Id => Ok(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::NameId(ffi.name_id)),
            }),
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
            FfiPropertyOpt::Key => Some(common_pb::Property {
                item: Some(common_pb::property::Item::Key(ffi.key.try_into()?)),
            }),
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
        Ok(Self {
            tag: Some(tag),
            property,
        })
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
    let result = plan.append_node(operator, parent_ids.into_iter().map(|x| x as u32).collect());
    // Do not let rust drop the pointer before explicitly calling `destroy_logical_plan`
    std::mem::forget(plan);
    if let Ok(opr_id) = result {
        unsafe {
            *id = opr_id as i32;
        }
        ResultCode::Success
    } else {
        result.err().unwrap()
    }
}

#[no_mangle]
pub extern "C" fn debug_plan(ptr_plan: *const c_void) {
    let plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };

    println!("{:#?}", plan);
    std::mem::forget(plan);
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
        let alias_pb = common_pb::NameOrId::try_from(alias);

        if !expr_pb.is_ok() || !alias_pb.is_ok() {
            return_code = expr_pb.err().unwrap();
        } else {
            let attribute = pb::project::ExprAlias {
                expr: expr_pb.ok(),
                alias: alias_pb.ok(),
                is_query_given,
            };
            project.mappings.push(attribute);
        }
        std::mem::forget(project);

        return_code
    }

    /// Append a project operator to the logical plan. To do so, one specifies the following arguments:
    /// * `ptr_plan`: A rust-owned pointer created by `init_logical_plan()`.
    /// * `ptr_project`: A rust-owned pointer created by `init_project_operator()`.
    /// * `parent_id`: The unique parent operator's index in the logical plan.
    /// * `id`: An index pointer that gonna hold the index for this operator.
    ///
    /// If it is successful to be appended to the logical plan, the `ptr_project` will be
    /// automatically released by by the rust program. Therefore, the caller needs not to deallocate
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
    pub extern "C" fn add_select_predicate(
        ptr_select: *const c_void,
        cstr_predicate: *const c_char,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let predicate_pb = cstr_to_suffix_expr_pb(cstr_predicate);
        if predicate_pb.is_err() {
            return_code = predicate_pb.err().unwrap()
        } else {
            let mut select = unsafe { Box::from_raw(ptr_select as *mut pb::Select) };
            select.predicate = predicate_pb.ok();
            std::mem::forget(select);
        }

        return_code
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

    /// Append a join operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_join_operator(
        ptr_plan: *const c_void,
        ptr_join: *const c_void,
        parent_left: i32,
        parent_right: i32,
        id: *mut i32,
    ) -> ResultCode {
        let join = unsafe { Box::from_raw(ptr_join as *mut pb::Join) };
        append_operator(
            ptr_plan,
            join.as_ref().clone().into(),
            vec![parent_left, parent_right],
            id,
        )
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
            agg_fn_pb.alias = Some(alias.try_into()?);

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
        let order = Box::new(pb::OrderBy { pairs: vec![] });
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

    /// Set the arguments for unfold, which are:
    /// * a tag points to a collection-type data field for unfolding,
    /// * an alias for referencing to each element of the collection.
    #[no_mangle]
    pub extern "C" fn set_unfold_args(
        ptr_unfold: *const c_void,
        tag: FfiNameOrId,
        alias: FfiNameOrId,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let mut unfold = unsafe { Box::from_raw(ptr_unfold as *mut pb::Unfold) };
        let tag_result: FfiResult<common_pb::NameOrId> = tag.try_into();
        let alias_result: FfiResult<common_pb::NameOrId> = alias.try_into();

        if tag_result.is_ok() && alias_result.is_ok() {
            unfold.tag = tag_result.ok();
            unfold.alias = alias_result.ok();
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

    /// To initialize a scan operator
    #[no_mangle]
    pub extern "C" fn init_scan_operator() -> *const c_void {
        let scan = Box::new(pb::Scan {
            schema_name: "".to_string(),
            mappings: vec![],
        });
        Box::into_raw(scan) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn set_scan_schema_name(
        ptr_scan: *const c_void,
        cstr_name: *const c_char,
    ) -> ResultCode {
        let mut return_code = ResultCode::Success;
        let schema_name = cstr_to_string(cstr_name);
        if schema_name.is_err() {
            return_code = schema_name.err().unwrap()
        } else {
            let mut scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
            scan.schema_name = schema_name.unwrap();
            std::mem::forget(scan);
        }

        return_code
    }

    /// Append an unfold operator to the logical plan
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
