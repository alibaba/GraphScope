use crate::db::api::*;

use super::util::{UnsafeBytesReader, UnsafeBytesWriter};

pub fn i16_to_vec(x: i16) -> Vec<u8> {
    int_to_vec!(x, i16)
}

pub fn i32_to_vec(x: i32) -> Vec<u8> {
    int_to_vec!(x, i32)
}

pub fn u32_to_vec(x: u32) -> Vec<u8> {
    int_to_vec!(x, u32)
}

pub fn i64_to_vec(x: i64) -> Vec<u8> {
    int_to_vec!(x, i64)
}

pub fn u64_to_vec(x: u64) -> Vec<u8> {
    int_to_vec!(x, u64)
}

pub fn bytes_to_i64(buf: &[u8]) -> GraphResult<i64> {
    if buf.len() == std::mem::size_of::<i64>() {
        let reader = UnsafeBytesReader::new(buf);
        Ok(reader.read_i64(0))
    } else {
        let msg = format!("bytes with length {} cannot transform to i64", buf.len());
        let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, bytes_to_i64);
        Err(err)
    }
}

pub fn i64_to_arr(x: i64) -> [u8; 8] {
    let mut ret = [0; 8];
    let mut writer = UnsafeBytesWriter::new(&mut ret);
    writer.write_i64(0, x);
    ret
}

pub fn bytes_to_str(buf: &[u8]) -> GraphResult<&str> {
    std::str::from_utf8(buf).map_err(|e| {
        let msg = format!("{:?}", e);
        gen_graph_err!(GraphErrorCode::InvalidData, msg, bytes_to_str)
    })
}
