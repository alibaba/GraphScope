#![allow(dead_code)]
use std::marker::PhantomData;
use protobuf::Message;
use crate::db::api::{GraphResult, GraphError};
use crate::db::api::GraphErrorCode::InvalidData;

/// This reader won't check whether the offset is overflow when read bytes.
/// It's for performance purpose. Be careful to use it.
#[derive(Clone)]
pub struct UnsafeBytesReader<'a> {
    buf: *const u8,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> UnsafeBytesReader<'a> {
    pub fn new(buf: &[u8]) -> Self {
        UnsafeBytesReader {
            buf: buf.as_ptr(),
            _phantom: Default::default(),
        }
    }

    pub fn read_u8(&self, offset: usize) -> u8 { unsafe_read_func!(self, u8, offset) }
    pub fn read_u16(&self, offset: usize) -> u16 { unsafe_read_func!(self, u16, offset) }
    pub fn read_u32(&self, offset: usize) -> u32 { unsafe_read_func!(self, u32, offset) }
    pub fn read_u64(&self, offset: usize) -> u64 { unsafe_read_func!(self, u64, offset) }
    pub fn read_i8(&self, offset: usize) -> i8 { unsafe_read_func!(self, i8, offset) }
    pub fn read_i16(&self, offset: usize) -> i16 { unsafe_read_func!(self, i16, offset) }
    pub fn read_i32(&self, offset: usize) -> i32 { unsafe_read_func!(self, i32, offset) }
    pub fn read_i64(&self, offset: usize) -> i64 { unsafe_read_func!(self, i64, offset) }
    pub fn read_f32(&self, offset: usize) -> f32 { unsafe_read_func!(self, f32, offset) }
    pub fn read_f64(&self, offset: usize) -> f64 { unsafe_read_func!(self, f64, offset) }
    pub fn read_ref<T>(&self, offset: usize) -> &'a T {
        unsafe {
            &*(self.buf.offset(offset as isize) as *const T)
        }
    }
    pub fn read_bytes(&self, offset: usize, len: usize) -> &'a [u8] {
        unsafe {
            ::std::slice::from_raw_parts(self.buf.offset(offset as isize), len)
        }
    }
}

/// This writer won't check whether the offset is overflow when write bytes.
/// It's for performance purpose. Be careful to use it.
pub struct UnsafeBytesWriter {
    buf: *const u8,
}

impl UnsafeBytesWriter {
    pub fn new(buf: &mut [u8]) -> Self {
        UnsafeBytesWriter {
            buf: buf.as_ptr(),
        }
    }
    pub fn write_u8(&mut self, offset: usize, data: u8) { unsafe_write_func!(self, u8, offset, data) }
    pub fn write_u16(&mut self, offset: usize, data: u16) { unsafe_write_func!(self, u16, offset, data) }
    pub fn write_u32(&mut self, offset: usize, data: u32) { unsafe_write_func!(self, u32, offset, data) }
    pub fn write_u64(&mut self, offset: usize, data: u64) { unsafe_write_func!(self, u64, offset, data) }
    pub fn write_i8(&mut self, offset: usize, data: i8) { unsafe_write_func!(self, i8, offset, data) }
    pub fn write_i16(&mut self, offset: usize, data: i16) { unsafe_write_func!(self, i16, offset, data) }
    pub fn write_i32(&mut self, offset: usize, data: i32) { unsafe_write_func!(self, i32, offset, data) }
    pub fn write_i64(&mut self, offset: usize, data: i64) { unsafe_write_func!(self, i64, offset, data) }
    pub fn write_f32(&mut self, offset: usize, data: f32) { unsafe_write_func!(self, f32, offset, data) }
    pub fn write_f64(&mut self, offset: usize, data: f64) { unsafe_write_func!(self, f64, offset, data) }
    pub fn write_bytes(&mut self, offset: usize, data: &[u8]) {
        unsafe {
            let src = data.as_ptr();
            let dst = self.buf.offset(offset as isize) as *mut u8;
            ::std::intrinsics::copy_nonoverlapping(src, dst, data.len());
        }
    }
}

pub fn parse_pb<M: Message>(buf: &[u8]) -> GraphResult<M> {
    protobuf::parse_from_bytes::<M>(buf)
        .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_unsafe_rw {
        ($r_func:ident, $w_func:ident, $ty:ty) => {
            let mut buf = Vec::with_capacity(100);
            let mut writer = UnsafeBytesWriter::new(&mut buf);
            writer.$w_func(20, 1.0 as $ty);
            let reader = UnsafeBytesReader::new(&buf);
            assert_eq!(reader.$r_func(20), 1.0 as $ty);
        };
    }

    #[test]
    fn test_unsafe_bytes_rw() {
        test_unsafe_rw!(read_u8, write_u8, u8);
        test_unsafe_rw!(read_u16, write_u16, u16);
        test_unsafe_rw!(read_u32, write_u32, u32);
        test_unsafe_rw!(read_u64, write_u64, u64);
        test_unsafe_rw!(read_i8, write_i8, i8);
        test_unsafe_rw!(read_i16, write_i16, i16);
        test_unsafe_rw!(read_i32, write_i32, i32);
        test_unsafe_rw!(read_i64, write_i64, i64);
        test_unsafe_rw!(read_f32, write_f32, f32);
        test_unsafe_rw!(read_f64, write_f64, f64);
    }
}
