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
//!  #include<ir_core.h>
//!  using namespace std;
//!  int main(int argc, char** argv) {
//!     const void* ptr_plan = init_logical_plan();`
//!     const void* ptr_project = init_project_operator();
//!     add_project_meta(ptr_project, "@name", as_tag_id(0));
//!     int opr_id = 0;
//!     append_project_operator(ptr_plan, ptr_project, 0, &opr_id);
//!     cout << "the id is: " << opr_id << endl;
//!
//!     const void* ptr_select = init_select_operator();
//!     set_select_meta(ptr_select, "@age > 20 && @name == \"John\"");
//!     append_select_operator(ptr_plan, ptr_select, opr_id, &opr_id);
//!     cout << "the id is: " << opr_id << endl;
//!
//!     debug_plan(ptr_plan);
//!     destroy_logical_plan(ptr_plan);
//! }
//!
//! Save the codes as </path/to/c-caller/test.cc>, and build like:
//! `g++ -o test test.cc -std=c++11 -L. -lir_core`

use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use crate::plan::utils::{cstr_to_string, FfiResult, LogicalPlan, ResultCode};
use std::convert::TryFrom;
use std::ffi::c_void;
use std::os::raw::c_char;

#[repr(i32)]
#[derive(Copy, Clone, Debug)]
pub enum NameIdOpt {
    None = 0,
    Name = 1,
    Id = 2,
}

impl Default for NameIdOpt {
    fn default() -> Self {
        Self::None
    }
}

#[repr(C)]
pub struct FfiNameOrId {
    opt: NameIdOpt,
    name: *const c_char,
    name_id: i32,
}

impl Default for FfiNameOrId {
    fn default() -> Self {
        Self {
            opt: NameIdOpt::default(),
            name: std::ptr::null() as *const c_char,
            name_id: 0,
        }
    }
}

impl TryFrom<FfiNameOrId> for common_pb::NameOrId {
    type Error = ResultCode;

    fn try_from(ffi: FfiNameOrId) -> FfiResult<Self> {
        match &ffi.opt {
            NameIdOpt::None => Err(ResultCode::NotExistError),
            NameIdOpt::Name => Ok(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::Name(cstr_to_string(ffi.name)?)),
            }),
            NameIdOpt::Id => Ok(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::NameId(ffi.name_id)),
            }),
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone)]
pub enum PropertyOpt {
    None = 0,
    Id = 1,
    Label = 2,
    Key = 3,
}

impl Default for PropertyOpt {
    fn default() -> Self {
        Self::None
    }
}

#[repr(C)]
#[derive(Default)]
pub struct FfiProperty {
    opt: PropertyOpt,
    key: FfiNameOrId,
}

#[repr(C)]
pub struct FfiVariable {
    tag: FfiNameOrId,
    property: FfiProperty,
}

/// Transform a c-like string into `NameOrId`.
#[no_mangle]
pub extern "C" fn as_tag_name(name: *const c_char) -> FfiNameOrId {
    FfiNameOrId {
        opt: NameIdOpt::Name,
        name,
        name_id: 0,
    }
}

/// Transform an integer into `NameOrId`.
#[no_mangle]
pub extern "C" fn as_tag_id(name_id: i32) -> FfiNameOrId {
    FfiNameOrId {
        opt: NameIdOpt::Id,
        name: std::ptr::null(),
        name_id,
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
    unsafe {
        let ptr = Box::from_raw(ptr_plan as *mut LogicalPlan);
        drop(ptr);
    }
}

fn append_operator(
    ptr_plan: *const c_void,
    operator: pb::logical_plan::Operator,
    parent_id: u32,
    id: *mut i32,
) -> ResultCode {
    let mut plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };
    let result = plan.append_operator(operator, parent_id as u32);
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
    println!("{:?}", plan);
    std::mem::forget(plan);
}

mod project {
    use super::*;
    /// To initialize a project operator.
    #[no_mangle]
    pub extern "C" fn init_project_operator() -> *const c_void {
        let args = Box::new(Vec::<pb::project::ExprAlias>::new());
        Box::into_raw(args) as *const c_void
    }

    /// To add a meta data for the project operator, which is a c-like string to represent an
    /// expression, together with a `NameOrId` parameter that represents an alias.
    #[no_mangle]
    pub extern "C" fn add_project_meta(
        ptr_project: *const c_void,
        expr: *const c_char,
        alias: FfiNameOrId,
    ) -> ResultCode {
        let mut args = unsafe { Box::from_raw(ptr_project as *mut Vec<pb::project::ExprAlias>) };
        let expr_str = cstr_to_string(expr);
        let alias_pb = common_pb::NameOrId::try_from(alias);

        if !expr_str.is_ok() || !alias_pb.is_ok() {
            return ResultCode::CStringError;
        }

        let expr_rst = common_pb::SuffixExpr::try_from(expr_str.unwrap());
        if expr_rst.is_ok() {
            let arg = pb::project::ExprAlias {
                expr: expr_rst.ok(),
                alias: alias_pb.ok(),
            };
            args.push(arg);
            std::mem::forget(args);

            ResultCode::Success
        } else {
            expr_rst.err().unwrap()
        }
    }

    /// Append a project operator to the logical plan. To do so, one specifies the following arguments:
    /// * `ptr_plan`: A rust-owned pointer created by `init_logical_plan()`.
    /// * `ptr_project`: A rust-owned pointer created by `init_project_operator()`.
    /// * `parent_id`: The unique parent operator's index in the logical plan.
    /// * `id`: An index pointer that gonna hold the index for this operator.
    ///
    /// After successfully appending to the logical plan, the `ptr_project` shall be released by
    /// by the rust program. Therefore, the caller needs not to deallocate the pointer.
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
        if parent_id >= 0 {
            let expr_alias_vec =
                unsafe { Box::from_raw(ptr_project as *mut Vec<pb::project::ExprAlias>) };
            let mut attributes: Vec<pb::project::ExprAlias> = Vec::new();
            for expr_alias in expr_alias_vec.into_iter() {
                attributes.push(expr_alias);
            }
            let project_pb = pb::Project { attributes };
            append_operator(ptr_plan, project_pb.into(), parent_id as u32, id)
        } else {
            ResultCode::NegativeIndexError
        }
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

    /// To set a select operator's metadata, which is a C-string predicate
    #[no_mangle]
    pub extern "C" fn set_select_meta(
        ptr_select: *const c_void,
        ptr_predicate: *const c_char,
    ) -> ResultCode {
        let predicate_str = cstr_to_string(ptr_predicate);
        if predicate_str.is_err() {
            predicate_str.err().unwrap()
        } else {
            let predicate_pb = common_pb::SuffixExpr::try_from(predicate_str.unwrap());
            if predicate_pb.is_ok() {
                let mut select = unsafe { Box::from_raw(ptr_select as *mut pb::Select) };
                select.predicate = predicate_pb.ok();
                std::mem::forget(select);
                ResultCode::Success
            } else {
                ResultCode::ParseExprError
            }
        }
    }

    /// Append a select operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_select_operator(
        ptr_plan: *const c_void,
        ptr_select: *const c_void,
        parent_id: i32,
        id: *mut i32,
    ) -> ResultCode {
        if parent_id >= 0 {
            let select = unsafe { Box::from_raw(ptr_select as *mut pb::Select) };
            append_operator(
                ptr_plan,
                select.as_ref().clone().into(),
                parent_id as u32,
                id,
            )
        } else {
            ResultCode::NegativeIndexError
        }
    }
}
