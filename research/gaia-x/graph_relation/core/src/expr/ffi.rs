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

use crate::expr::to_suffix_expr_pb;
use crate::expr::token::tokenize;
use prost::Message;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

#[repr(C)]
pub struct FfiExpr {
    data: *const c_void,
    len: usize,
}

/// This is to offer an interface for "C" to convert a C-string-represented
/// expression into a suffix expression tree that is encoded via protobuf.
/// The following codes show a C++ example of using this api:
/// Produce a header file <ir_core.h> for the C program using cbindgen, which looks like:
///
/// <ir_core.h>:
/// extern "C" {
///
/// typedef struct FfiExpr {
///   const void *data;
///   size_t len;
/// } FfiExpr;
///
/// FfiExpr cstr_to_suffix_expr(const char *cstr);
/// }
///
/// Use the api in a C++ program.
/// <test.cc>:
/// #include "ir_core.h"
/// #include "expr.pb.h"
/// #include "google/protobuf/message_lite.h"
/// #include <stdio.h>
///
/// using namespace std;
///
/// int main(int argc, char** argv) {
///     const char* expr = "1 + 2 * 4";
///     // Accept a c_str as input, and call the api to process and build into a suffix tree
///     // encoded as `FfiExpr`
///     FfiExpr expr = cstr_to_suffix_expr(expr);
///     common::SuffixExpr expr_pb;
///     // To convert an `FfiExpr` back to a protobuf structure
///     expr_pb.ParseFromArray(expr.data, expr.len);
///     cout << expr.len << endl;
///     for (auto opr: expr_pb.operators()) {
///         cout << opr.DebugString() << endl;
///     }
///     return 0;
/// }
#[no_mangle]
pub extern "C" fn cstr_to_suffix_expr(cstr: *const c_char) -> FfiExpr {
    let str = unsafe { CStr::from_ptr(cstr) }
        .to_str()
        .expect("transform from `CStr` error");
    let tokens = tokenize(str).expect("the expression may contain invalid tokens");
    let rst = to_suffix_expr_pb(tokens);
    if let Ok(suffix_expr) = rst {
        let mut buf: Vec<u8> = Vec::new();
        let len = suffix_expr.encoded_len();
        buf.reserve(len);
        suffix_expr
            .encode(&mut buf)
            .expect("Encode the expression tree pb error");

        let data = buf.as_ptr() as *const c_void;
        std::mem::forget(buf);
        FfiExpr { data, len }
    } else {
        panic!(
            "Parsing tokens into suffix tree encounters error: {:?}",
            rst.err()
        )
    }
}
