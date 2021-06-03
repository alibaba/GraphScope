#![allow(dead_code)]
use crate::db::api::*;

pub fn str_to_i64(s: &str) -> GraphResult<i64> {
    s.parse().map_err(|e| {
        let msg = format!("{:?}", e);
        gen_graph_err!(GraphErrorCode::InvalidData, msg, str_to_i64, s)
    })
}

pub fn str_to_i32(s: &str) -> GraphResult<i32> {
    s.parse().map_err(|e| {
        let msg = format!("{:?}", e);
        gen_graph_err!(GraphErrorCode::InvalidData, msg, str_to_i64, s)
    })
}

pub fn parse_str<T: std::str::FromStr>(s: &str) -> GraphResult<T> {
    s.parse().map_err(|_| {
        let msg = format!("parse str failed");
        gen_graph_err!(GraphErrorCode::InvalidData, msg, parse_str, s)
    })
}