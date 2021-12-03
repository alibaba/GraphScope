use std::os::raw::{c_char, c_void};
use std::ffi::CString;
use maxgraph_store::db::api::{GraphResult, GraphError, GraphErrorCode};
use std::fmt;
use std::fmt::Formatter;

#[repr(C)]
#[allow(non_snake_case)]
pub struct JnaResponse {
    success: i32,
    hasDdl: i32,
    errMsg: *const c_char,
    data: *const c_void,
    len: i32,
}

impl JnaResponse {
    pub fn default() -> Self {
        JnaResponse {
            success: 1,
            hasDdl: 0,
            errMsg: ::std::ptr::null(),
            data: ::std::ptr::null(),
            len: 0,
        }
    }

    #[inline]
    pub fn new_success() -> Box<JnaResponse> {
        let resp = JnaResponse::default();
        Box::new(resp)
    }

    #[inline]
    pub fn new_error(msg: &str) -> Box<JnaResponse> {
        let mut resp = JnaResponse::default();
        resp.success(false);
        resp.has_ddl(false);
        resp.err_msg(msg);
        Box::new(resp)
    }

    pub fn success(&mut self, success: bool) {
        self.success = if success {1} else {0};
    }

    pub fn has_ddl(&mut self, has_ddl: bool) {
        self.hasDdl = if has_ddl {1} else {0};
    }

    pub fn err_msg(&mut self, err_msg: &str) {
        let msg = CString::new(err_msg).unwrap();
        self.errMsg = msg.as_ptr();
        ::std::mem::forget(msg);
    }

    pub fn data(&mut self, data: Vec<u8>) -> GraphResult<()> {
        if data.len() != data.capacity() {
            let msg = format!("data len {} must eq capacity {}", data.len(), data.capacity());
            return Err(GraphError::new(GraphErrorCode::InvalidData, msg));
        }
        self.data = data.as_ptr() as *const c_void;
        self.len = data.len() as i32;
        ::std::mem::forget(data);
        Ok(())
    }
}

impl fmt::Display for JnaResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "success: {}, ddl: {}, errmsg_isnull: {}, datalen: {}", self.success, self.hasDdl, self.errMsg.is_null(), self.len)
    }
}

impl Drop for JnaResponse {
    fn drop(&mut self) {
        unsafe {
            if !self.errMsg.is_null() {
                CString::from_raw(self.errMsg as *mut c_char);
            }
            if self.len > 0 {
                Vec::from_raw_parts(self.data as *mut u8, self.len as usize, self.len as usize);
            }
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern fn dropJnaResponse(_: Box<JnaResponse>) {}
