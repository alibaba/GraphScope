//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::ffi::CString;
use std::os::raw::c_char;

#[repr(C)]
#[allow(non_snake_case)]
pub struct EnginePortsResponse {
    success: bool,
    errMsg: *const c_char,
    enginePort: i32,
    rpcPort: i32,
}

impl EnginePortsResponse {
    pub fn new(engine_port: i32, rpc_port: i32) -> Box<EnginePortsResponse> {
        Box::new(EnginePortsResponse {
            success: true,
            errMsg: std::ptr::null(),
            enginePort: engine_port,
            rpcPort: rpc_port,
        })
    }

    pub fn new_with_error(err_msg: &str) -> Box<EnginePortsResponse> {
        let msg = CString::new(err_msg).unwrap();
        let response =
            EnginePortsResponse { success: false, errMsg: msg.as_ptr(), enginePort: 0, rpcPort: 0 };
        ::std::mem::forget(msg);
        Box::new(response)
    }
}

impl Drop for EnginePortsResponse {
    fn drop(&mut self) {
        unsafe {
            if !self.errMsg.is_null() {
                CString::from_raw(self.errMsg as *mut c_char);
            }
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn dropEnginePortsResponse(_: Box<EnginePortsResponse>) {}
