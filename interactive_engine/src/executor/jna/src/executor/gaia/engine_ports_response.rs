use std::os::raw::c_char;
use std::ffi::CString;

#[repr(C)]
#[allow(non_snake_case)]
pub struct EnginePortsResponse {
    success: bool,
    errMsg: *const c_char,
    engine_port: i32,
    server_port: i32,
}

impl EnginePortsResponse {
    pub fn new_success(engine_port: i32, server_port: i32) -> Box<EnginePortsResponse> {
        Box::new(EnginePortsResponse {
            success: true,
            errMsg: std::ptr::null(),
            engine_port,
            server_port,
        })
    }

    pub fn new_error(err_msg: &str) -> Box<EnginePortsResponse> {
        let msg = CString::new(err_msg).unwrap();
        let response = EnginePortsResponse {
            success: false,
            errMsg: msg.as_ptr(),
            engine_port: 0,
            server_port: 0,
        };
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
pub extern fn dropEnginePortsResponse(_: Box<EnginePortsResponse>) {}
