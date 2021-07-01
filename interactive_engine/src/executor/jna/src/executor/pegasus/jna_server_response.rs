use std::os::raw::c_char;
use std::ffi::CString;

/// engine server started address
#[repr(C)]
#[allow(non_snake_case)]
pub struct JnaEngineServerResponse {
    errCode: i32,
    errMsg: *const c_char,
    address: *const c_char,
}

impl JnaEngineServerResponse {
    #[inline]
    pub fn new_success(address: String) -> Box<JnaEngineServerResponse> {
        let host_port = CString::new(address).unwrap();
        let response = JnaEngineServerResponse {
            errCode: 0,
            errMsg: std::ptr::null(),
            address: host_port.as_ptr(),
        };
        ::std::mem::forget(host_port);
        Box::new(response)
    }

    #[inline]
    pub fn new_fail(error_message: String) -> Box<JnaEngineServerResponse> {
        let err_msg = CString::new(error_message).unwrap();
        let response = JnaEngineServerResponse {
            errCode: -1,
            errMsg: err_msg.as_ptr(),
            address: std::ptr::null(),
        };
        ::std::mem::forget(err_msg);
        Box::new(response)
    }
}

impl Drop for JnaEngineServerResponse {
    fn drop(&mut self) {
        unsafe {
            if !self.errMsg.is_null() {
                CString::from_raw(self.errMsg as *mut c_char);
            }
            if !self.address.is_null() {
                CString::from_raw(self.address as *mut c_char);
            }
        }
    }
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct JnaRpcServerPortResponse {
    storeQueryPort: i32,
    queryExecutePort: i32,
    queryManagePort: i32,
}

impl JnaRpcServerPortResponse {
    pub fn new(store_query_port: i32,
               query_execute_port: i32,
               query_manage_port: i32) -> Self {
        JnaRpcServerPortResponse {
            storeQueryPort: store_query_port,
            queryExecutePort: query_execute_port,
            queryManagePort: query_manage_port,
        }
    }
}

impl Drop for JnaRpcServerPortResponse {
    fn drop(&mut self) {}
}