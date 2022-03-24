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
//! #    write_plan_to_json(ptr_plan, "./plan.json");
//! #    destroy_logical_plan(ptr_plan);
//! # }
//!
//! Save the codes as </path/to/c-caller/test.cc>, and build like:
//! `g++ -o test test.cc -std=c++11 -L. -lir_core`

use std::convert::{TryFrom, TryInto};
use std::ffi::{c_void, CStr};
use std::fs::File;
use std::os::raw::c_char;

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use pegasus::BuildJobError;
use pegasus_client::builder::JobBuilder;
use prost::Message;

use crate::error::IrError;
use crate::plan::logical::LogicalPlan;
use crate::plan::meta::set_schema_from_json;
use crate::plan::physical::AsPhysical;
use crate::JsonIO;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResultCode {
    Success = 0,
    /// Parse an expression error
    ParseExprError = 1,
    /// Missing necessary data
    MissingDataError = 2,
    /// The error while transforming from C-like string, aka char*
    CStringError = 3,
    /// The provided data type is unknown
    UnknownTypeError = 4,
    /// The provided range is invalid
    InvalidRangeError = 5,
    /// The given index is negative
    NegativeIndexError = 6,
    /// Build Physical Plan Error
    BuildJobError = 7,
    /// Parse protobuf error
    ParsePbError = 8,
    /// The parent of an operator cannot be found
    ParentNotFoundError = 9,
    /// A column (property) does not exist in the store
    ColumnNotExistError = 10,
    /// A table (label) does not exist in the store
    TableNotExistError = 11,
    /// A queried tag has not been specified
    TagNotExistError = 12,
    UnSupported = 13,
    Others = 16,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FfiError {
    code: ResultCode,
    msg: *const c_char,
}

impl FfiError {
    pub fn new(code: ResultCode, msg: String) -> Self {
        let result = string_to_cstr(msg);
        if let Ok(msg) = result {
            Self { code, msg }
        } else {
            result.err().unwrap()
        }
    }

    pub fn success() -> Self {
        Self { code: ResultCode::Success, msg: std::ptr::null::<c_char>() }
    }
}

impl std::fmt::Display for FfiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FfiError")
            .field("code", &self.code)
            .field("msg", &cstr_to_string(self.msg))
            .finish()
    }
}

impl std::error::Error for FfiError {}

impl From<IrError> for FfiError {
    fn from(err: IrError) -> Self {
        match err {
            IrError::TableNotExist(t) => {
                FfiError::new(ResultCode::TableNotExistError, format!("table {:?} does not exist", t))
            }
            IrError::ColumnNotExist(col) => {
                FfiError::new(ResultCode::ColumnNotExistError, format!("column {:?} does not exist", col))
            }
            IrError::ParentNodeNotExist(p) => FfiError::new(
                ResultCode::ParentNotFoundError,
                format!("parent node {:?} does not exist", p),
            ),
            IrError::TagNotExist(t) => FfiError::new(
                ResultCode::TagNotExistError,
                format!("the queried tag {:?} is not specified", t),
            ),
            IrError::ParsePbError(err) => FfiError::new(ResultCode::ParsePbError, err.to_string()),
            IrError::ParseExprError(err) => FfiError::new(ResultCode::ParseExprError, err.to_string()),
            IrError::InvalidPattern(s) => FfiError::new(ResultCode::Others, s),
            IrError::PbEncodeError(err) => FfiError::new(ResultCode::Others, err.to_string()),
            IrError::MissingData(d) => {
                FfiError::new(ResultCode::MissingDataError, format!("required data {:?} is missing", d))
            }
            IrError::InvalidRange(l, u) => FfiError::new(
                ResultCode::InvalidRangeError,
                format!("the range ({:?}, {:?}) is invalid", l, u),
            ),
            IrError::Unsupported(err) => FfiError::new(ResultCode::UnSupported, err.to_string()),
        }
    }
}

impl From<BuildJobError> for FfiError {
    fn from(err: BuildJobError) -> Self {
        Self::new(ResultCode::BuildJobError, err.to_string())
    }
}

#[repr(C)]
pub struct FfiData {
    /// The pointer to the raw data returned from Ffi functions
    ptr: *mut u8,
    /// The length of the data (in bytes) allocated for `Self::ptr`
    len: usize,
    /// To recode any error captured from building logical/physical plan
    error: FfiError,
}

impl From<IrError> for FfiData {
    fn from(error: IrError) -> Self {
        FfiData { ptr: std::ptr::null_mut(), len: 0, error: error.into() }
    }
}

impl From<BuildJobError> for FfiData {
    fn from(error: BuildJobError) -> Self {
        FfiData { ptr: std::ptr::null_mut(), len: 0, error: error.into() }
    }
}

impl From<FfiError> for FfiData {
    fn from(error: FfiError) -> Self {
        FfiData { ptr: std::ptr::null_mut(), len: 0, error }
    }
}

pub(crate) fn cstr_to_string(cstr: *const c_char) -> Result<String, FfiError> {
    if !cstr.is_null() {
        let str_result = unsafe { CStr::from_ptr(cstr) }.to_str();
        if let Ok(str) = str_result {
            Ok(str.to_string())
        } else {
            Err(FfiError::new(
                ResultCode::CStringError,
                "error parsing C string into Rust string".to_string(),
            ))
        }
    } else {
        Ok("".to_string())
    }
}

pub(crate) fn string_to_cstr(str: String) -> Result<*const c_char, FfiError> {
    let str_result = std::ffi::CString::new(str);
    if let Ok(str) = str_result {
        let str_ptr = str.as_ptr();
        // **NOTE** must release the string pointer on the C side.
        std::mem::forget(str);

        Ok(str_ptr)
    } else {
        Err(FfiError::new(ResultCode::CStringError, "error parsing Rust string into C string".to_string()))
    }
}

pub(crate) fn cstr_to_expr_pb(cstr: *const c_char) -> Result<common_pb::Expression, FfiError> {
    let str = cstr_to_string(cstr);
    if str.is_err() {
        Err(str.err().unwrap())
    } else {
        let expr = str_to_expr_pb(str.unwrap()).map_err(|err| IrError::from(err))?;
        Ok(expr)
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
        Self { opt: FfiNameIdOpt::default(), name: std::ptr::null() as *const c_char, name_id: 0 }
    }
}

impl TryFrom<FfiNameOrId> for Option<common_pb::NameOrId> {
    type Error = FfiError;

    fn try_from(ffi: FfiNameOrId) -> Result<Self, FfiError> {
        match &ffi.opt {
            FfiNameIdOpt::None => Ok(None),
            FfiNameIdOpt::Name => Ok(Some(common_pb::NameOrId {
                item: Some(common_pb::name_or_id::Item::Name(cstr_to_string(ffi.name)?)),
            })),
            FfiNameIdOpt::Id => {
                Ok(Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(ffi.name_id)) }))
            }
        }
    }
}

impl TryFrom<FfiNameOrId> for common_pb::NameOrIdKey {
    type Error = FfiError;

    fn try_from(ffi: FfiNameOrId) -> Result<Self, FfiError> {
        match &ffi.opt {
            FfiNameIdOpt::None => Ok(common_pb::NameOrIdKey { key: None }),
            FfiNameIdOpt::Name => Ok(common_pb::NameOrIdKey {
                key: Some(common_pb::NameOrId {
                    item: Some(common_pb::name_or_id::Item::Name(cstr_to_string(ffi.name)?)),
                }),
            }),
            FfiNameIdOpt::Id => Ok(common_pb::NameOrIdKey {
                key: Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(ffi.name_id)) }),
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
    Len = 3,
    Key = 4,
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
    type Error = FfiError;

    fn try_from(ffi: FfiProperty) -> Result<Self, FfiError> {
        let result = match &ffi.opt {
            FfiPropertyOpt::None => None,
            FfiPropertyOpt::Id => {
                Some(common_pb::Property { item: Some(common_pb::property::Item::Id(common_pb::IdKey {})) })
            }
            FfiPropertyOpt::Label => Some(common_pb::Property {
                item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})),
            }),
            FfiPropertyOpt::Len => Some(common_pb::Property {
                item: Some(common_pb::property::Item::Len(common_pb::LengthKey {})),
            }),
            FfiPropertyOpt::Key => {
                if let Some(key) = ffi.key.try_into()? {
                    Some(common_pb::Property { item: Some(common_pb::property::Item::Key(key)) })
                } else {
                    None
                }
            }
        };

        Ok(result)
    }
}

#[repr(C)]
#[derive(Default)]
pub struct FfiVariable {
    tag: FfiNameOrId,
    property: FfiProperty,
}

impl TryFrom<FfiVariable> for common_pb::Variable {
    type Error = FfiError;

    fn try_from(ffi: FfiVariable) -> Result<Self, Self::Error> {
        let (tag, property) = (ffi.tag.try_into()?, ffi.property.try_into()?);
        Ok(Self { tag, property })
    }
}

#[repr(C)]
#[derive(Default)]
pub struct FfiAlias {
    alias: FfiNameOrId,
    is_query_given: bool,
}

impl TryFrom<FfiAlias> for Option<common_pb::NameOrId> {
    type Error = FfiError;

    fn try_from(ffi: FfiAlias) -> Result<Self, Self::Error> {
        Self::try_from(ffi.alias)
    }
}

/// Build a none-`NameOrId`
#[no_mangle]
pub extern "C" fn none_name_or_id() -> FfiNameOrId {
    FfiNameOrId { opt: FfiNameIdOpt::None, name: std::ptr::null(), name_id: 0 }
}

/// Transform a c-like string into `NameOrId`
#[no_mangle]
pub extern "C" fn cstr_as_name_or_id(cstr: *const c_char) -> FfiNameOrId {
    FfiNameOrId { opt: FfiNameIdOpt::Name, name: cstr, name_id: 0 }
}

/// Transform an integer into `NameOrId`.
#[no_mangle]
pub extern "C" fn int_as_name_or_id(integer: i32) -> FfiNameOrId {
    FfiNameOrId { opt: FfiNameIdOpt::Id, name: std::ptr::null(), name_id: integer }
}

/// Build an `None` property
#[no_mangle]
pub extern "C" fn as_none_key() -> FfiProperty {
    FfiProperty::default()
}

/// Build an id property
#[no_mangle]
pub extern "C" fn as_id_key() -> FfiProperty {
    FfiProperty { opt: FfiPropertyOpt::Id, key: FfiNameOrId::default() }
}

/// Build a label property
#[no_mangle]
pub extern "C" fn as_label_key() -> FfiProperty {
    FfiProperty { opt: FfiPropertyOpt::Label, key: FfiNameOrId::default() }
}

/// Build a length property
#[no_mangle]
pub extern "C" fn as_len_key() -> FfiProperty {
    FfiProperty { opt: FfiPropertyOpt::Len, key: FfiNameOrId::default() }
}

/// Build a keyed property from a given key
#[no_mangle]
pub extern "C" fn as_property_key(key: FfiNameOrId) -> FfiProperty {
    FfiProperty { opt: FfiPropertyOpt::Key, key }
}

/// Build a variable with tag only
#[no_mangle]
pub extern "C" fn as_var_tag_only(tag: FfiNameOrId) -> FfiVariable {
    FfiVariable { tag, property: FfiProperty::default() }
}

/// Build a variable with property only
#[no_mangle]
pub extern "C" fn as_var_property_only(property: FfiProperty) -> FfiVariable {
    FfiVariable { tag: FfiNameOrId::default(), property }
}

/// Build a variable with tag and property
#[no_mangle]
pub extern "C" fn as_var(tag: FfiNameOrId, property: FfiProperty) -> FfiVariable {
    FfiVariable { tag, property }
}

/// Build a default variable with `None` tag and property
#[no_mangle]
pub extern "C" fn as_none_var() -> FfiVariable {
    FfiVariable::default()
}

fn destroy_ptr<M>(ptr: *const c_void) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr as *mut M);
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum FfiDataType {
    Unknown = 0,
    Boolean = 1,
    I32 = 2,
    I64 = 3,
    F64 = 4,
    Str = 5,
    I32Array = 6,
    I64Array = 7,
    F64Array = 8,
    StrArray = 9,
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

impl TryFrom<FfiConst> for common_pb::Value {
    type Error = FfiError;

    fn try_from(ffi: FfiConst) -> Result<Self, Self::Error> {
        match &ffi.data_type {
            FfiDataType::Boolean => Ok(common_pb::Value::from(ffi.boolean)),
            FfiDataType::I32 => Ok(common_pb::Value::from(ffi.int32)),
            FfiDataType::I64 => Ok(common_pb::Value::from(ffi.int64)),
            FfiDataType::F64 => Ok(common_pb::Value::from(ffi.float64)),
            FfiDataType::Str => {
                let str = cstr_to_string(ffi.cstr);
                if str.is_ok() {
                    Ok(common_pb::Value::from(str.unwrap()))
                } else {
                    Err(str.err().unwrap())
                }
            }
            // TODO(longbin) add support for other type
            _ => Err(FfiError::new(
                ResultCode::UnknownTypeError,
                format!("unknown data type {:?}", ffi.data_type),
            )),
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

/// Set schema via a json-formatted cstring.
#[no_mangle]
pub extern "C" fn set_schema(cstr_json: *const c_char) -> FfiError {
    let result = cstr_to_string(cstr_json);
    if let Ok(json) = result {
        set_schema_from_json(json.as_bytes());

        FfiError::success()
    } else {
        result.err().unwrap()
    }
}

/// Initialize a logical plan, which expose a pointer for c-like program to access the
/// entry of the logical plan. This pointer, however, is owned by Rust, and the caller
/// **must not** process any operation, which includes but not limited to deallocate it.
/// We have provided  the [`destroy_logical_plan`] api for deallocating the pointer of the logical plan.
#[no_mangle]
pub extern "C" fn init_logical_plan() -> *const c_void {
    use super::meta::STORE_META;
    let mut plan = Box::new(LogicalPlan::default());
    if let Ok(meta) = STORE_META.read() {
        if let Some(schema) = &meta.schema {
            plan.meta = plan
                .meta
                .with_store_conf(schema.is_table_id(), schema.is_column_id());
        }
    }
    Box::into_raw(plan) as *const c_void
}

/// To destroy a logical plan.
#[no_mangle]
pub extern "C" fn destroy_logical_plan(ptr_plan: *const c_void) {
    destroy_ptr::<LogicalPlan>(ptr_plan)
}

/// To release a FfiError
#[no_mangle]
pub extern "C" fn destroy_ffi_error(error: FfiError) {
    if !error.msg.is_null() {
        let _ = unsafe { std::ffi::CString::from_raw(error.msg as *mut c_char) };
    }
}

/// To release a FfiData
#[no_mangle]
pub extern "C" fn destroy_ffi_data(data: FfiData) {
    if !data.ptr.is_null() {
        let _ = unsafe { Vec::from_raw_parts(data.ptr, data.len, data.len) };
    }
    if !data.error.msg.is_null() {
        let _ = unsafe { std::ffi::CString::from_raw(data.error.msg as *mut c_char) };
    }
}

/// To build a physical plan from the logical plan.
#[no_mangle]
pub extern "C" fn build_physical_plan(
    ptr_plan: *const c_void, num_workers: u32, num_servers: u32,
) -> FfiData {
    let mut plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };
    if num_workers > 1 || num_servers > 1 {
        plan.meta = plan.meta.with_partition();
    }
    let mut plan_meta = plan.meta.clone();
    let mut builder = JobBuilder::default();
    let build_result = plan.add_job_builder(&mut builder, &mut plan_meta);
    let result = if build_result.is_ok() {
        let req_result = builder.build();
        if let Ok(req) = req_result {
            let mut req_bytes = req.encode_to_vec().into_boxed_slice();
            let data =
                FfiData { ptr: req_bytes.as_mut_ptr(), len: req_bytes.len(), error: FfiError::success() };
            std::mem::forget(req_bytes);

            data
        } else {
            req_result.err().unwrap().into()
        }
    } else {
        build_result.err().unwrap().into()
    };

    std::mem::forget(plan);

    result
}

fn append_operator(
    ptr_plan: *const c_void, operator: pb::logical_plan::Operator, parent_ids: Vec<i32>, id: *mut i32,
) -> FfiError {
    let mut plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };
    let result = plan.append_operator_as_node(
        operator,
        parent_ids
            .into_iter()
            .filter_map(|x| if x >= 0 { Some(x as u32) } else { None })
            .collect(),
    );
    if result.is_err() {
        std::mem::forget(plan);
        result.err().unwrap().into()
    } else {
        // Do not let rust drop the pointer before explicitly calling `destroy_logical_plan`
        std::mem::forget(plan);
        unsafe { *id = result.unwrap() as i32 };
        FfiError::success()
    }
}

#[no_mangle]
pub extern "C" fn write_plan_to_json(ptr_plan: *const c_void, cstr_file: *const c_char) {
    let box_plan = unsafe { Box::from_raw(ptr_plan as *mut LogicalPlan) };
    let plan = box_plan.as_ref().clone();
    let file = cstr_to_string(cstr_file).expect("C String to Rust String error!");
    plan.into_json(File::create(&file).expect(&format!("Create json file: {:?} error", file)))
        .expect("Write to json error");

    std::mem::forget(box_plan);
}

/// Define the target operator/parameter while setting certain parameters
#[allow(dead_code)]
#[derive(PartialEq, Copy, Clone)]
enum Target {
    Select,
    Scan,
    GetV,
    EdgeExpand,
    PathExpand,
    Limit,
    As,
    OrderBy,
    Apply,
    Sink,
    Params,
}

/// Set the size range limitation for certain operators
fn set_range(ptr: *const c_void, lower: i32, upper: i32, target: Target) -> FfiError {
    if lower < 0 || upper < 0 || upper < lower {
        FfiError::new(
            ResultCode::InvalidRangeError,
            format!("the range ({:?}, {:?}) is invalid", lower, upper),
        )
    } else {
        match target {
            Target::Limit => {
                let mut limit = unsafe { Box::from_raw(ptr as *mut pb::Limit) };
                limit.range = Some(pb::Range { lower, upper });
                std::mem::forget(limit);
            }
            Target::OrderBy => {
                let mut orderby = unsafe { Box::from_raw(ptr as *mut pb::OrderBy) };
                orderby.limit = Some(pb::Range { lower, upper });
                std::mem::forget(orderby);
            }
            Target::Params => {
                let mut params = unsafe { Box::from_raw(ptr as *mut pb::QueryParams) };
                params.limit = Some(pb::Range { lower, upper });
                std::mem::forget(params);
            }
            Target::PathExpand => {
                let mut pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                pathxpd.hop_range = Some(pb::Range { lower, upper });
                std::mem::forget(pathxpd);
            }
            _ => unreachable!(),
        }

        FfiError::success()
    }
}

fn set_alias(ptr: *const c_void, alias: FfiAlias, target: Target) -> FfiError {
    let alias_pb = alias.try_into();
    if alias_pb.is_ok() {
        match target {
            Target::Scan => {
                let mut scan = unsafe { Box::from_raw(ptr as *mut pb::Scan) };
                scan.alias = alias_pb.unwrap();
                std::mem::forget(scan);
            }
            Target::EdgeExpand => {
                let mut edgexpd = unsafe { Box::from_raw(ptr as *mut pb::EdgeExpand) };
                edgexpd.alias = alias_pb.unwrap();
                std::mem::forget(edgexpd);
            }
            Target::PathExpand => {
                let mut pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                pathxpd.alias = alias_pb.unwrap();
                std::mem::forget(pathxpd);
            }
            Target::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                getv.alias = alias_pb.unwrap();
                std::mem::forget(getv);
            }
            Target::Apply => {
                let mut apply = unsafe { Box::from_raw(ptr as *mut pb::Apply) };
                apply.alias = alias_pb.unwrap();
                std::mem::forget(apply);
            }
            Target::As => {
                let mut as_opr = unsafe { Box::from_raw(ptr as *mut pb::As) };
                as_opr.alias = alias_pb.unwrap();
                std::mem::forget(as_opr);
            }
            _ => unreachable!(),
        }
        FfiError::success()
    } else {
        alias_pb.err().unwrap()
    }
}

/// To set an operator's predicate.
fn set_predicate(ptr: *const c_void, cstr_predicate: *const c_char, target: Target) -> FfiError {
    let predicate_pb = cstr_to_expr_pb(cstr_predicate);
    if predicate_pb.is_ok() {
        match target {
            Target::Select => {
                let mut select = unsafe { Box::from_raw(ptr as *mut pb::Select) };
                select.predicate = predicate_pb.ok();
                std::mem::forget(select);
            }
            Target::Params => {
                let mut params = unsafe { Box::from_raw(ptr as *mut pb::QueryParams) };
                params.predicate = predicate_pb.ok();
                std::mem::forget(params);
            }
            _ => unreachable!(),
        }
        FfiError::success()
    } else {
        predicate_pb.err().unwrap()
    }
}

fn set_tag(ptr: *const c_void, tag: FfiNameOrId, target: Target) -> FfiError {
    let pb = tag.try_into();
    if pb.is_ok() {
        match target {
            Target::EdgeExpand => {
                let mut expand = unsafe { Box::from_raw(ptr as *mut pb::EdgeExpand) };
                expand.v_tag = pb.unwrap();
                std::mem::forget(expand);
            }
            Target::GetV => {
                let mut getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                getv.tag = pb.unwrap();
                std::mem::forget(getv);
            }
            Target::PathExpand => {
                let mut pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                pathxpd.start_tag = pb.unwrap();
                std::mem::forget(pathxpd);
            }
            _ => unreachable!(),
        }
        FfiError::success()
    } else {
        pb.err().unwrap()
    }
}

mod params {
    use std::collections::HashMap;

    use super::*;

    /// To initialize a query parameters
    #[no_mangle]
    pub extern "C" fn init_query_params() -> *const c_void {
        let query_params = Box::new(pb::QueryParams {
            tables: vec![],
            columns: vec![],
            is_all_columns: false,
            limit: None,
            predicate: None,
            extra: HashMap::new(),
        });

        Box::into_raw(query_params) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn add_params_table(ptr_params: *const c_void, table: FfiNameOrId) -> FfiError {
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        let pb = table.try_into();
        if pb.is_ok() {
            if let Some(table) = pb.unwrap() {
                params.tables.push(table)
            }
            std::mem::forget(params);

            FfiError::success()
        } else {
            pb.err().unwrap()
        }
    }

    #[no_mangle]
    pub extern "C" fn add_params_column(ptr_params: *const c_void, col: FfiNameOrId) -> FfiError {
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        let pb = col.try_into();
        if pb.is_ok() {
            if let Some(col) = pb.unwrap() {
                params.columns.push(col)
            }
            std::mem::forget(params);

            FfiError::success()
        } else {
            pb.err().unwrap()
        }
    }

    #[no_mangle]
    pub extern "C" fn set_params_range(ptr_params: *const c_void, lower: i32, upper: i32) -> FfiError {
        set_range(ptr_params, lower, upper, Target::Params)
    }

    #[no_mangle]
    pub extern "C" fn set_params_predicate(
        ptr_params: *const c_void, cstr_pred: *const c_char,
    ) -> FfiError {
        set_predicate(ptr_params, cstr_pred, Target::Params)
    }

    /// Set getting all columns
    #[no_mangle]
    pub extern "C" fn set_params_is_all_columns(ptr_params: *const c_void) -> FfiError {
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        params.is_all_columns = true;
        std::mem::forget(params);

        FfiError::success()
    }
}

mod project {
    use super::*;
    /// To initialize a project operator.
    #[no_mangle]
    pub extern "C" fn init_project_operator(is_append: bool) -> *const c_void {
        let project = Box::new(pb::Project { mappings: vec![], is_append });
        Box::into_raw(project) as *const c_void
    }

    /// To add a mapping for the project operator, which maps a c-like string to represent an
    /// expression, to a `NameOrId` parameter that represents an alias.
    #[no_mangle]
    pub extern "C" fn add_project_expr_alias(
        ptr_project: *const c_void, cstr_expr: *const c_char, alias: FfiAlias,
    ) -> FfiError {
        let mut result = FfiError::success();
        let mut project = unsafe { Box::from_raw(ptr_project as *mut pb::Project) };
        let expr_pb = cstr_to_expr_pb(cstr_expr);
        let alias_pb = Option::<common_pb::NameOrId>::try_from(alias);

        if !expr_pb.is_ok() {
            result = expr_pb.err().unwrap();
        } else if !alias_pb.is_ok() {
            result = alias_pb.err().unwrap();
        } else {
            let attribute = pb::project::ExprAlias { expr: expr_pb.ok(), alias: alias_pb.unwrap() };
            project.mappings.push(attribute);
        }
        std::mem::forget(project);

        result
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
    /// * Returning [`FfiError`] to capture any error.
    ///
    /// **Note**: All following `append_xx_operator()` apis have the same usage as this one.
    ///
    #[no_mangle]
    pub extern "C" fn append_project_operator(
        ptr_plan: *const c_void, ptr_project: *const c_void, parent_id: i32, id: *mut i32,
    ) -> FfiError {
        let project = unsafe { Box::from_raw(ptr_project as *mut pb::Project) };
        append_operator(ptr_plan, project.as_ref().clone().into(), vec![parent_id], id)
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
        ptr_select: *const c_void, cstr_predicate: *const c_char,
    ) -> FfiError {
        set_predicate(ptr_select, cstr_predicate, Target::Select)
    }

    /// Append a select operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_select_operator(
        ptr_plan: *const c_void, ptr_select: *const c_void, parent_id: i32, id: *mut i32,
    ) -> FfiError {
        let select = unsafe { Box::from_raw(ptr_select as *mut pb::Select) };
        append_operator(ptr_plan, select.as_ref().clone().into(), vec![parent_id], id)
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
        let kind = unsafe { std::mem::transmute(join_kind) };
        let join = Box::new(pb::Join { left_keys: vec![], right_keys: vec![], kind });
        Box::into_raw(join) as *const c_void
    }

    /// To add a join operator's metadata, which is a pair of left and right keys.
    /// In the join processing, a pair of data will be output if the corresponding fields
    /// regarding left and right keys are **equivalent**.
    #[no_mangle]
    pub extern "C" fn add_join_key_pair(
        ptr_join: *const c_void, left_key: FfiVariable, right_key: FfiVariable,
    ) -> FfiError {
        let mut result = FfiError::success();
        let mut join = unsafe { Box::from_raw(ptr_join as *mut pb::Join) };
        let left_key_pb = left_key.try_into();
        let right_key_pb = right_key.try_into();
        if left_key_pb.is_err() {
            result = left_key_pb.err().unwrap();
        } else if right_key_pb.is_err() {
            result = right_key_pb.err().unwrap();
        } else {
            join.left_keys.push(left_key_pb.unwrap());
            join.right_keys.push(right_key_pb.unwrap());
        }
        std::mem::forget(join);

        result
    }

    /// Append a join operator to the logical plan. Note that both left and right parent ids
    /// for join must be non-negative, and they must refer some nodes in the logical plan
    #[no_mangle]
    pub extern "C" fn append_join_operator(
        ptr_plan: *const c_void, ptr_join: *const c_void, parent_left: i32, parent_right: i32, id: *mut i32,
    ) -> FfiError {
        if parent_left < 0 || parent_right < 0 {
            FfiError::new(
                ResultCode::NegativeIndexError,
                format!("invalid left parent {:?}, or right {:?}", parent_left, parent_right),
            )
        } else {
            let join = unsafe { Box::from_raw(ptr_join as *mut pb::Join) };
            append_operator(ptr_plan, join.as_ref().clone().into(), vec![parent_left, parent_right], id)
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
        let union = Box::new(pb::Union { parents: vec![] });
        Box::into_raw(union) as *const c_void
    }

    /// Add the subtask parent id to Union
    #[no_mangle]
    pub extern "C" fn add_union_parent(ptr_union: *const c_void, parent_id: i32) -> FfiError {
        let mut union = unsafe { Box::from_raw(ptr_union as *mut pb::Union) };
        union.parents.push(parent_id);
        std::mem::forget(union);

        FfiError::success()
    }

    /// Append a Union operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_union_operator(
        ptr_plan: *const c_void, ptr_union: *const c_void, id: *mut i32,
    ) -> FfiError {
        let union_opr = unsafe { Box::from_raw(ptr_union as *mut pb::Union) };
        append_operator(ptr_plan, union_opr.as_ref().clone().into(), union_opr.parents, id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_union_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Union>(ptr)
    }
}

mod groupby {
    use super::*;

    /// To initialize a groupby operator
    #[no_mangle]
    pub extern "C" fn init_groupby_operator() -> *const c_void {
        let group = Box::new(pb::GroupBy { mappings: vec![], functions: vec![] });
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
        alias: FfiAlias,
    }

    impl TryFrom<FfiAggFn> for pb::group_by::AggFunc {
        type Error = FfiError;

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

    /// Initialize an aggregate function with empty value to aggregate.
    /// To add value to aggregate, call `add_agg_value()`
    #[no_mangle]
    pub extern "C" fn init_agg_fn(aggregate: FfiAggOpt, alias: FfiAlias) -> FfiAggFn {
        let vars: Box<Vec<FfiVariable>> = Box::new(vec![]);
        FfiAggFn { vars: Box::into_raw(vars) as *const FfiVariable, aggregate, alias }
    }

    #[no_mangle]
    pub extern "C" fn add_agg_value(agg_fn: &mut FfiAggFn, agg_var: FfiVariable) {
        let mut vars = unsafe { Box::from_raw(agg_fn.vars as *mut Vec<FfiVariable>) };
        vars.push(agg_var);
        std::mem::forget(vars);
    }

    /// Add the key (and its alias if any) according to which the grouping is conducted
    #[no_mangle]
    pub extern "C" fn add_groupby_key_alias(
        ptr_groupby: *const c_void, key: FfiVariable, alias: FfiAlias,
    ) -> FfiError {
        let mut result = FfiError::success();
        let mut group = unsafe { Box::from_raw(ptr_groupby as *mut pb::GroupBy) };
        let key_pb = key.try_into();
        let alias_pb = alias.try_into();

        if key_pb.is_ok() && alias_pb.is_ok() {
            group
                .mappings
                .push(pb::group_by::KeyAlias { key: key_pb.ok(), alias: alias_pb.unwrap() });
        } else {
            result = key_pb.err().unwrap();
        }
        std::mem::forget(group);

        result
    }

    /// Add the aggregate function for each group.
    #[no_mangle]
    pub extern "C" fn add_groupby_agg_fn(ptr_groupby: *const c_void, agg_fn: FfiAggFn) -> FfiError {
        let mut result = FfiError::success();
        let mut group = unsafe { Box::from_raw(ptr_groupby as *mut pb::GroupBy) };
        let agg_fn_pb = agg_fn.try_into();

        if agg_fn_pb.is_ok() {
            group
                .as_mut()
                .functions
                .push(agg_fn_pb.unwrap());
        } else {
            result = agg_fn_pb.err().unwrap();
        }
        std::mem::forget(group);

        result
    }

    /// Append a groupby operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_groupby_operator(
        ptr_plan: *const c_void, ptr_groupby: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
        let order = Box::new(pb::OrderBy { pairs: vec![], limit: None });
        Box::into_raw(order) as *const c_void
    }

    /// Add the pair for conducting ordering.
    #[no_mangle]
    pub extern "C" fn add_orderby_pair(
        ptr_orderby: *const c_void, var: FfiVariable, order_opt: FfiOrderOpt,
    ) -> FfiError {
        let mut result = FfiError::success();
        let mut orderby = unsafe { Box::from_raw(ptr_orderby as *mut pb::OrderBy) };
        let key_result = var.try_into();
        if key_result.is_ok() {
            let order = match order_opt {
                FfiOrderOpt::Shuffle => 0,
                FfiOrderOpt::Asc => 1,
                FfiOrderOpt::Desc => 2,
            };
            orderby
                .pairs
                .push(pb::order_by::OrderingPair { key: key_result.ok(), order });
        } else {
            result = key_result.err().unwrap();
        }
        std::mem::forget(orderby);

        result
    }

    /// Set the size limit of the orderby operator, which will turn it into topk
    #[no_mangle]
    pub extern "C" fn set_orderby_limit(ptr_orderby: *const c_void, lower: i32, upper: i32) -> FfiError {
        set_range(ptr_orderby, lower, upper, Target::OrderBy)
    }

    /// Append an orderby operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_orderby_operator(
        ptr_plan: *const c_void, ptr_orderby: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
    pub extern "C" fn add_dedup_key(ptr_dedup: *const c_void, var: FfiVariable) -> FfiError {
        let mut result = FfiError::success();
        let mut dedup = unsafe { Box::from_raw(ptr_dedup as *mut pb::Dedup) };
        let key_result = var.try_into();
        if key_result.is_ok() {
            dedup.keys.push(key_result.unwrap());
        } else {
            result = key_result.err().unwrap();
        }
        std::mem::forget(dedup);

        result
    }

    /// Append a dedup operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_dedup_operator(
        ptr_plan: *const c_void, ptr_dedup: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
        let unfold = Box::new(pb::Unfold { tag: None, alias: None });
        Box::into_raw(unfold) as *const c_void
    }

    /// Set the argument pair for unfold, which are:
    /// * a tag points to a collection-type data field for unfolding,
    /// * an alias for referencing to each element of the collection.
    #[no_mangle]
    pub extern "C" fn set_unfold_pair(
        ptr_unfold: *const c_void, tag: FfiNameOrId, alias: FfiNameOrId,
    ) -> FfiError {
        let mut result = FfiError::success();
        let mut unfold = unsafe { Box::from_raw(ptr_unfold as *mut pb::Unfold) };
        let tag_result = tag.try_into();
        let alias_result = alias.try_into();

        if tag_result.is_ok() && alias_result.is_ok() {
            unfold.tag = tag_result.unwrap();
            unfold.alias = alias_result.unwrap();
        } else {
            result =
                if tag_result.is_err() { tag_result.err().unwrap() } else { alias_result.err().unwrap() };
        }
        std::mem::forget(unfold);

        result
    }

    /// Append an unfold operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_unfold_operator(
        ptr_plan: *const c_void, ptr_unfold: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let unfold = unsafe { Box::from_raw(ptr_unfold as *mut pb::Unfold) };
        append_operator(ptr_plan, unfold.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_unfold_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Unfold>(ptr)
    }
}

mod scan {
    use std::collections::HashMap;

    use super::*;

    #[allow(dead_code)]
    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(i32)]
    pub enum FfiScanOpt {
        Entity = 0,
        Relation = 1,
    }

    /// To initialize a scan operator
    #[no_mangle]
    pub extern "C" fn init_scan_operator(scan_opt: FfiScanOpt) -> *const c_void {
        let scan = Box::new(pb::Scan {
            scan_opt: unsafe { std::mem::transmute::<FfiScanOpt, i32>(scan_opt) },
            alias: None,
            params: Some(pb::QueryParams {
                tables: vec![],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: None,
                extra: HashMap::new(),
            }),
            idx_predicate: None,
        });
        Box::into_raw(scan) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn init_index_predicate() -> *const c_void {
        let predicate: Box<pb::IndexPredicate> = Box::new(pb::IndexPredicate { or_predicates: vec![] });
        Box::into_raw(predicate) as *const c_void
    }

    fn parse_equiv_predicate(
        key: FfiProperty, value: FfiConst,
    ) -> Result<pb::index_predicate::Triplet, FfiError> {
        Ok(pb::index_predicate::Triplet { key: key.try_into()?, value: Some(value.try_into()?), cmp: None })
    }

    #[no_mangle]
    pub extern "C" fn and_equiv_predicate(
        ptr_predicate: *const c_void, key: FfiProperty, value: FfiConst,
    ) -> FfiError {
        let equiv_pred_result = parse_equiv_predicate(key, value);
        if let Ok(equiv_pred) = equiv_pred_result {
            let mut predicate = unsafe { Box::from_raw(ptr_predicate as *mut pb::IndexPredicate) };
            if predicate.or_predicates.is_empty() {
                predicate
                    .or_predicates
                    .push(pb::index_predicate::AndPredicate { predicates: vec![equiv_pred] });
            } else {
                predicate
                    .or_predicates
                    .last_mut()
                    .unwrap()
                    .predicates
                    .push(equiv_pred)
            }
            std::mem::forget(predicate);

            FfiError::success()
        } else {
            equiv_pred_result.err().unwrap()
        }
    }

    #[no_mangle]
    pub extern "C" fn or_equiv_predicate(
        ptr_predicate: *const c_void, key: FfiProperty, value: FfiConst,
    ) -> FfiError {
        let equiv_pred_result = parse_equiv_predicate(key, value);
        if let Ok(equiv_pred) = equiv_pred_result {
            let mut predicate = unsafe { Box::from_raw(ptr_predicate as *mut pb::IndexPredicate) };
            predicate
                .or_predicates
                .push(pb::index_predicate::AndPredicate { predicates: vec![equiv_pred] });
            std::mem::forget(predicate);

            FfiError::success()
        } else {
            equiv_pred_result.err().unwrap()
        }
    }

    #[no_mangle]
    pub extern "C" fn add_scan_index_predicate(
        ptr_scan: *const c_void, ptr_predicate: *const c_void,
    ) -> FfiError {
        let mut scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
        let predicate = unsafe { Box::from_raw(ptr_predicate as *mut pb::IndexPredicate) };
        scan.idx_predicate = Some(predicate.as_ref().clone());
        std::mem::forget(scan);

        FfiError::success()
    }

    #[no_mangle]
    pub extern "C" fn set_scan_params(ptr_scan: *const c_void, ptr_params: *const c_void) -> FfiError {
        let mut scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        std::mem::swap(scan.params.as_mut().unwrap(), params.as_mut());
        std::mem::forget(scan);

        FfiError::success()
    }

    /// Set an alias for the data if it is a vertex/edge
    #[no_mangle]
    pub extern "C" fn set_scan_alias(ptr_scan: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_scan, alias, Target::Scan)
    }

    /// Append a scan operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_scan_operator(
        ptr_plan: *const c_void, ptr_scan: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let scan = unsafe { Box::from_raw(ptr_scan as *mut pb::Scan) };
        append_operator(ptr_plan, scan.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_scan_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Scan>(ptr)
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
    pub extern "C" fn set_limit_range(ptr_limit: *const c_void, lower: i32, upper: i32) -> FfiError {
        set_range(ptr_limit, lower, upper, Target::Limit)
    }

    /// Append an indexed scan operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_limit_operator(
        ptr_plan: *const c_void, ptr_limit: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let limit = unsafe { Box::from_raw(ptr_limit as *mut pb::Limit) };
        append_operator(ptr_plan, limit.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_limit_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Limit>(ptr)
    }
}

mod as_opr {
    use super::*;

    /// To initialize an As operator
    #[no_mangle]
    pub extern "C" fn init_as_operator() -> *const c_void {
        let as_opr = Box::new(pb::As { alias: None });

        Box::into_raw(as_opr) as *const c_void
    }

    /// Set the alias of the entity to As
    #[no_mangle]
    pub extern "C" fn set_as_alias(ptr_as: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_as, alias, Target::As)
    }

    /// Append an As operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_as_operator(
        ptr_plan: *const c_void, ptr_as: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let as_opr = unsafe { Box::from_raw(ptr_as as *mut pb::As) };
        append_operator(ptr_plan, as_opr.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_as_operator(ptr: *const c_void) {
        destroy_ptr::<pb::As>(ptr)
    }
}

mod sink {
    use super::*;

    /// To initialize an Sink operator
    #[no_mangle]
    pub extern "C" fn init_sink_operator() -> *const c_void {
        let sink_opr = Box::new(pb::Sink { tags: vec![], id_name_mappings: vec![] });
        Box::into_raw(sink_opr) as *const c_void
    }

    /// Add the tag of column to output to Sink
    #[no_mangle]
    pub extern "C" fn add_sink_column(ptr_sink: *const c_void, ffi_tag: FfiNameOrId) -> FfiError {
        let mut result = FfiError::success();
        let tag_pb = ffi_tag.try_into();
        if tag_pb.is_ok() {
            let mut sink = unsafe { Box::from_raw(ptr_sink as *mut pb::Sink) };
            sink.tags.push(tag_pb.unwrap());
            std::mem::forget(sink);
        } else {
            result = tag_pb.err().unwrap();
        }

        result
    }

    /// Append an Sink operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_sink_operator(
        ptr_plan: *const c_void, ptr_sink: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let sink_opr = unsafe { Box::from_raw(ptr_sink as *mut pb::Sink) };
        append_operator(ptr_plan, sink_opr.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_sink_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Sink>(ptr)
    }
}

mod graph {
    use std::collections::HashMap;

    use super::*;
    use crate::plan::ffi::join::FfiJoinKind;

    #[allow(dead_code)]
    #[derive(Copy, Clone)]
    #[repr(i32)]
    pub enum FfiDirection {
        Out = 0,
        In = 1,
        Both = 2,
    }

    /// To initialize an edge expand operator from an expand base
    #[no_mangle]
    pub extern "C" fn init_edgexpd_operator(is_edge: bool, dir: FfiDirection) -> *const c_void {
        let edgexpd = Box::new(pb::EdgeExpand {
            v_tag: None,
            direction: unsafe { std::mem::transmute::<FfiDirection, i32>(dir) },
            params: Some(pb::QueryParams {
                tables: vec![],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: None,
                extra: HashMap::new(),
            }),
            is_edge,
            alias: None,
        });

        Box::into_raw(edgexpd) as *const c_void
    }

    /// Set the start-vertex's tag to conduct this expansion
    #[no_mangle]
    pub extern "C" fn set_edgexpd_vtag(ptr_edgexpd: *const c_void, v_tag: FfiNameOrId) -> FfiError {
        set_tag(ptr_edgexpd, v_tag, Target::EdgeExpand)
    }

    #[no_mangle]
    pub extern "C" fn set_edgexpd_params(
        ptr_edgexpd: *const c_void, ptr_params: *const c_void,
    ) -> FfiError {
        let mut edgexpd = unsafe { Box::from_raw(ptr_edgexpd as *mut pb::EdgeExpand) };
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        std::mem::swap(edgexpd.params.as_mut().unwrap(), params.as_mut());
        std::mem::forget(edgexpd);

        FfiError::success()
    }

    /// Set edge alias of this edge expansion
    #[no_mangle]
    pub extern "C" fn set_edgexpd_alias(ptr_edgexpd: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_edgexpd, alias, Target::EdgeExpand)
    }

    /// Append an edge expand operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_edgexpd_operator(
        ptr_plan: *const c_void, ptr_edgexpd: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
    }

    /// To initialize an expansion base
    #[no_mangle]
    pub extern "C" fn init_getv_operator(opt: FfiVOpt) -> *const c_void {
        let getv = Box::new(pb::GetV {
            tag: None,
            opt: unsafe { std::mem::transmute::<FfiVOpt, i32>(opt) },
            params: Some(pb::QueryParams {
                tables: vec![],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: None,
                extra: HashMap::new(),
            }),
            alias: None,
        });
        Box::into_raw(getv) as *const c_void
    }

    /// Set the tag of edge/path to get the vertex
    #[no_mangle]
    pub extern "C" fn set_getv_tag(ptr_getv: *const c_void, tag: FfiNameOrId) -> FfiError {
        set_tag(ptr_getv, tag, Target::GetV)
    }

    #[no_mangle]
    pub extern "C" fn set_getv_params(ptr_getv: *const c_void, ptr_params: *const c_void) -> FfiError {
        let mut getv = unsafe { Box::from_raw(ptr_getv as *mut pb::GetV) };
        let mut params = unsafe { Box::from_raw(ptr_params as *mut pb::QueryParams) };
        std::mem::swap(getv.params.as_mut().unwrap(), params.as_mut());
        std::mem::forget(getv);

        FfiError::success()
    }

    /// Set vertex alias of this getting vertex
    #[no_mangle]
    pub extern "C" fn set_getv_alias(ptr_getv: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_getv, alias, Target::GetV)
    }

    /// Append the operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_getv_operator(
        ptr_plan: *const c_void, ptr_getv: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
        ptr_expand: *const c_void, is_whole_path: bool,
    ) -> *const c_void {
        let expand = unsafe { Box::from_raw(ptr_expand as *mut pb::EdgeExpand) };
        let edgexpd = Box::new(pb::PathExpand {
            base: Some(expand.as_ref().clone()),
            start_tag: None,
            is_whole_path,
            alias: None,
            hop_range: None,
        });

        Box::into_raw(edgexpd) as *const c_void
    }

    /// Set path alias of this path expansion
    #[no_mangle]
    pub extern "C" fn set_pathxpd_tag(ptr_pathxpd: *const c_void, tag: FfiNameOrId) -> FfiError {
        set_tag(ptr_pathxpd, tag, Target::PathExpand)
    }

    /// Set path alias of this path expansion
    #[no_mangle]
    pub extern "C" fn set_pathxpd_alias(ptr_pathxpd: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_pathxpd, alias, Target::PathExpand)
    }

    /// Set the hop-range limitation of expanding path
    #[no_mangle]
    pub extern "C" fn set_pathxpd_hops(ptr_pathxpd: *const c_void, lower: i32, upper: i32) -> FfiError {
        set_range(ptr_pathxpd, lower, upper, Target::PathExpand)
    }

    /// Append an path-expand operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_pathxpd_operator(
        ptr_plan: *const c_void, ptr_pathxpd: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let pathxpd = unsafe { Box::from_raw(ptr_pathxpd as *mut pb::PathExpand) };
        append_operator(ptr_plan, pathxpd.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_pathxpd_operator(ptr: *const c_void) {
        destroy_ptr::<pb::PathExpand>(ptr)
    }

    #[no_mangle]
    pub extern "C" fn init_pattern_operator() -> *const c_void {
        let pattern = Box::new(pb::Pattern { sentences: vec![] });

        Box::into_raw(pattern) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn add_pattern_sentence(
        ptr_pattern: *const c_void, ptr_sentence: *const c_void,
    ) -> FfiError {
        let mut pattern = unsafe { Box::from_raw(ptr_pattern as *mut pb::Pattern) };
        let sentence = unsafe { Box::from_raw(ptr_sentence as *mut pb::pattern::Sentence) };
        pattern
            .sentences
            .push(sentence.as_ref().clone());
        std::mem::forget(pattern);

        FfiError::success()
    }

    #[no_mangle]
    pub extern "C" fn init_pattern_sentence(join_kind: FfiJoinKind) -> *const c_void {
        let sentence = Box::new(pb::pattern::Sentence {
            start: None,
            binders: vec![],
            end: None,
            join_kind: unsafe { std::mem::transmute(join_kind) },
        });

        Box::into_raw(sentence) as *const c_void
    }

    fn set_sentence_tag(ptr_sentence: *const c_void, tag: FfiNameOrId, is_start: bool) -> FfiError {
        let mut sentence = unsafe { Box::from_raw(ptr_sentence as *mut pb::pattern::Sentence) };
        let pb = tag.try_into();
        let result_code = if pb.is_ok() {
            if is_start {
                sentence.start = pb.unwrap();
            } else {
                sentence.end = pb.unwrap();
            }
            FfiError::success()
        } else {
            pb.err().unwrap()
        };
        std::mem::forget(sentence);

        result_code
    }

    #[no_mangle]
    pub extern "C" fn set_sentence_start(ptr_sentence: *const c_void, tag: FfiNameOrId) -> FfiError {
        set_sentence_tag(ptr_sentence, tag, true)
    }

    #[no_mangle]
    pub extern "C" fn set_sentence_end(ptr_sentence: *const c_void, tag: FfiNameOrId) -> FfiError {
        set_sentence_tag(ptr_sentence, tag, false)
    }

    #[derive(Copy, Clone, Debug)]
    #[repr(i32)]
    #[allow(dead_code)]
    pub enum FfiBinderOpt {
        Edge = 0,
        Path = 1,
        Vertex = 2,
    }

    #[no_mangle]
    pub extern "C" fn add_sentence_binder(
        ptr_sentence: *const c_void, ptr: *const c_void, binder: FfiBinderOpt,
    ) -> FfiError {
        let mut sentence = unsafe { Box::from_raw(ptr_sentence as *mut pb::pattern::Sentence) };
        match binder {
            FfiBinderOpt::Edge => {
                let edgexpd = unsafe { Box::from_raw(ptr as *mut pb::EdgeExpand) };
                sentence.binders.push(pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(edgexpd.as_ref().clone())),
                });
            }
            FfiBinderOpt::Path => {
                let pathxpd = unsafe { Box::from_raw(ptr as *mut pb::PathExpand) };
                sentence.binders.push(pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Path(pathxpd.as_ref().clone())),
                });
            }
            FfiBinderOpt::Vertex => {
                let getv = unsafe { Box::from_raw(ptr as *mut pb::GetV) };
                sentence.binders.push(pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Vertex(getv.as_ref().clone())),
                });
            }
        }
        std::mem::forget(sentence);

        FfiError::success()
    }

    /// Append a pattern operator to the logical plan
    #[no_mangle]
    pub extern "C" fn append_pattern_operator(
        ptr_plan: *const c_void, ptr_pattern: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        let pattern = unsafe { Box::from_raw(ptr_pattern as *mut pb::Pattern) };
        append_operator(ptr_plan, pattern.as_ref().clone().into(), vec![parent], id)
    }

    #[no_mangle]
    pub extern "C" fn destroy_pattern_operator(ptr: *const c_void) {
        destroy_ptr::<pb::Pattern>(ptr)
    }
}

mod subtask {
    use super::*;
    use crate::plan::ffi::join::FfiJoinKind;

    /// To initialize an apply operator from a root node (id) of the subtask. Before initializing
    /// the subtask, one need to first prepare the subtask and append the operators within to the
    /// logical plan.
    #[no_mangle]
    pub extern "C" fn init_apply_operator(subtask_root: i32, join_kind: FfiJoinKind) -> *const c_void {
        let apply = Box::new(pb::Apply {
            join_kind: unsafe { std::mem::transmute::<FfiJoinKind, i32>(join_kind) },
            tags: vec![],
            subtask: subtask_root,
            alias: None,
        });

        Box::into_raw(apply) as *const c_void
    }

    #[no_mangle]
    pub extern "C" fn add_apply_tag(ptr_apply: *const c_void, ffi_tag: FfiNameOrId) -> FfiError {
        let mut result = FfiError::success();
        let tag_pb: Result<Option<common_pb::NameOrId>, FfiError> = ffi_tag.try_into();
        if tag_pb.is_ok() {
            if let Some(tag) = tag_pb.unwrap() {
                let mut apply = unsafe { Box::from_raw(ptr_apply as *mut pb::Apply) };
                apply.tags.push(tag);
                std::mem::forget(apply);
            }
        } else {
            result = tag_pb.err().unwrap();
        }

        result
    }

    #[no_mangle]
    pub extern "C" fn set_apply_alias(ptr_apply: *const c_void, alias: FfiAlias) -> FfiError {
        set_alias(ptr_apply, alias, Target::Apply)
    }

    /// Append an apply operator to the logical plan.
    /// If the apply is used alone (other than segment apply), the parent node must set and present
    /// in the logical plan.
    #[no_mangle]
    pub extern "C" fn append_apply_operator(
        ptr_plan: *const c_void, ptr_apply: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
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
        let segapply =
            Box::new(pb::SegmentApply { keys: vec![], apply_subtask: Some(apply.as_ref().clone()) });

        Box::into_raw(segapply) as *const c_void
    }

    /// To add the key for grouping on which the segment apply can be conducted.
    #[no_mangle]
    pub extern "C" fn add_segapply_key(ptr_segapply: *const c_void, ffi_key: FfiNameOrId) -> FfiError {
        let mut result = FfiError::success();
        let key_pb: Result<Option<common_pb::NameOrId>, FfiError> = ffi_key.try_into();
        if key_pb.is_ok() {
            if let Some(key) = key_pb.unwrap() {
                let mut segapply = unsafe { Box::from_raw(ptr_segapply as *mut pb::SegmentApply) };
                segapply.keys.push(key);
                std::mem::forget(segapply);
            }
        } else {
            result = key_pb.err().unwrap();
        }

        result
    }

    /// Append an apply operator to the logical plan. The parent node id for appending a segment apply operator
    /// must not be negative and must present in the logical plan.
    #[no_mangle]
    pub extern "C" fn append_segapply_operator(
        ptr_plan: *const c_void, ptr_segapply: *const c_void, parent: i32, id: *mut i32,
    ) -> FfiError {
        if parent < 0 {
            FfiError::new(ResultCode::NegativeIndexError, format!("invalid parent id {:?}", parent))
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
