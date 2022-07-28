use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;


use crate::graph::storage::clickhouse::pb_gen::clickhouse_grpc::click_house_client::ClickHouseClient;
use crate::graph::storage::clickhouse::pb_gen::clickhouse_grpc::{Compression, QueryInfo};
use crate::graph::storage::PropsStore;
use crate::graph::{Value };

#[cfg(not(feature = "gcip"))]
mod pb_gen {
    pub mod clickhouse_grpc {
        tonic::include_proto!("clickhouse.grpc");
    }
}

#[rustfmt::skip]
#[cfg(feature = "gcip")]
mod pb_gen {
    #[path = "clickhouse.grpc.rs"]
    pub mod clickhouse_grpc;
}

/// A raw Clickhouse type.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,

    /// Not supported
    Decimal32(usize),
    /// Not supported
    Decimal64(usize),
    /// Not supported
    Decimal128(usize),
    /// Not supported
    Decimal256(usize),

    String,
    FixedString(usize),
    /// Not supported
    Uuid,

    Date,
    /// Not supported
    DateTime,
    /// Not supported
    DateTime64,
    /// Not supported
    Ipv4,
    /// Not supported
    Ipv6,

    /// Not supported
    Enum8(Vec<(String, u8)>),
    /// Not supported
    Enum16(Vec<(String, u16)>),
    /// Not supported
    LowCardinality(Box<Type>),
    /// Not supported
    Array(Box<Type>),

    // unused (server never sends this)
    // Nested(IndexMap<String, Type>),
    Tuple(Vec<Type>),

    Nullable(Box<Type>),
    /// Not supported
    Map(Box<Type>, Box<Type>),
}

// we assume complete identifier normalization and type resolution from clickhouse
fn eat_identifier(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_numeric()) {
            continue;
        } else {
            return (&input[..i], &input[i..]);
        }
    }
    (input, "")
}

impl FromStr for Type {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ident, following) = eat_identifier(s);
        if ident.is_empty() {
            return Err(format!("invalid empty identifier for type: '{}'", s));
        }

        let following = following.trim();
        if !following.is_empty() {
            return Err(format!("unsupported type : '{}'", s));
        }

        match ident {
            "Int8" => Ok(Type::Int8),
            "Int16" => Ok(Type::Int16),
            "Int32" => Ok(Type::Int32),
            "Int64" => Ok(Type::Int64),
            "Int128" => Ok(Type::Int128),
            "Int256" => Ok(Type::Int256),
            "UInt8" => Ok(Type::UInt8),
            "UInt16" => Ok(Type::UInt16),
            "UInt32" => Ok(Type::UInt32),
            "UInt64" => Ok(Type::UInt64),
            "UInt128" => Ok(Type::UInt128),
            "UInt256" => Ok(Type::UInt256),
            "Float32" => Ok(Type::Float32),
            "Float64" => Ok(Type::Float64),
            "String" => Ok(Type::String),
            "UUID" => Err("unsupported type UUID".to_owned()),
            "Date" => Ok(Type::Date),
            "DateTime" => Err("unsupported type DateTime".to_owned()),
            "IPv4" => Err("unsupported type IPv4".to_owned()),
            "IPv6" => Err("unsupported type IPv6".to_owned()),
            _ => Err(format!("invalid type name: '{}'", ident)),
        }
    }
}

impl Type {
    fn read_value<R: Read>(&self, _reader: &mut R) -> std::io::Result<Value> {
        todo!()
    }
}

pub struct ClickHouseStore {
    query_req: QueryInfo,
    conn: RefCell<ClickHouseClient<tonic::transport::Channel>>,
}

#[allow(dead_code)]
impl ClickHouseStore {
    pub fn new(url: &str, db: String) -> Self {
        let mut conn = futures::executor::block_on(ClickHouseClient::connect(url.to_owned()))
            .expect("connect database fail");
        conn = conn.accept_gzip();
        let compression = Compression { algorithm: 1, level: 3 };
        let query_req = QueryInfo {
            query: "".to_string(),
            query_id: "0".to_string(),
            settings: HashMap::default(),
            database: db,
            input_data: Default::default(),
            input_data_delimiter: Default::default(),
            output_format: "Native".to_string(),
            external_tables: vec![],
            user_name: "".to_string(),
            password: "".to_string(),
            quota: "".to_string(),
            session_id: "".to_string(),
            session_check: false,
            session_timeout: 0,
            cancel: false,
            next_query_info: false,
            result_compression: Some(compression),
            compression_type: "".to_string(),
            compression_level: -1,
        };
        ClickHouseStore { query_req, conn: RefCell::new(conn) }
    }

    fn execute(&self, query: String) -> Vec<(String, Vec<Value>)> {
        let mut req = self.query_req.clone();
        req.query = query.clone();
        match futures::executor::block_on(self.conn.borrow_mut().execute_query(req)) {
            Ok(res) => {
                let mut result = res.into_inner();
                if let Some(err) = result.exception.take() {
                    error!(
                        "error query: code {}, name {}: {}, stack: {}",
                        err.code, err.name, err.display_text, err.stack_trace
                    );
                    panic!(
                        "error query: code {}, name {}: {}, stack: {}",
                        err.code, err.name, err.display_text, err.stack_trace
                    );
                }

                if !result.output.is_empty() {
                    debug!("output: bytes(len={})", result.output.len());
                    let mut bytes = result.output.as_slice();
                    let columns = read_var_uint(&mut bytes).expect("read uint data fail;");
                    let rows = read_var_uint(&mut bytes).expect("read uint data fail");
                    debug!("fetched {} columns * {} rows ", columns, rows);
                    let mut column_vec = Vec::with_capacity(columns as usize);
                    for _ in 0..columns {
                        let mut row_vec = Vec::with_capacity(rows as usize);
                        let name = read_string(&mut bytes).expect("read string data fail;");
                        let type_name = read_string(&mut bytes).expect("read string data fail");
                        debug!("parse column {} of type {}", name, type_name);
                        let type_ = Type::from_str(&*type_name).unwrap();
                        for _ in 0..rows {
                            let v = type_
                                .read_value(&mut bytes)
                                .expect("read row value fail;");
                            row_vec.push(v);
                        }
                        column_vec.push((name, row_vec));
                    }
                    column_vec
                } else {
                    vec![]
                }
            }
            Err(e) => {
                error!("execute query: {} failed: {:?}", query, e);
                panic!("execute query: {} failed: {:?}", query, e);
            }
        }
    }
}

impl PropsStore for ClickHouseStore {
    fn get_vertices(&self, v_type: &str, ids: &[u64]) -> Vec<(u64, HashMap<String, Value>)> {
        let query = format!("select * from {} where id in {:?}", v_type, ids);
        let result = self.execute(query);
        decode_row( result)
    }

    fn select_vertices<F: ToString>(&self, v_type: &str, ids: &[u64], filter: F) -> Vec<u64> {
        let query = format!("select id from {} where id in {:?} and {}", v_type, ids, filter.to_string());
        let mut result = self.execute(query);
        if result.is_empty() {
            vec![]
        } else {
            let mut matched = Vec::with_capacity(result[0].1.len());
            for i in result[0].1.drain(..) {
                match i {
                    Value::Int(v) => {
                        matched.push(v);
                    }
                    _ => panic!("invalid data"),
                }
            }
            matched
        }
    }
}

fn decode_row( _columns: Vec<(String, Vec<Value>)>) -> Vec<(u64, HashMap<String, Value>)> {
   todo!()
}

#[inline]
fn read_var_uint<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut out = 0u64;
    for i in 0..9u64 {
        let mut octet = [0u8];
        reader.read_exact(&mut octet[..])?;
        out |= ((octet[0] & 0x7F) as u64) << (7 * i);
        if (octet[0] & 0x80) == 0 {
            break;
        }
    }
    Ok(out)
}

pub const MAX_STRING_SIZE: usize = 1 << 30;

#[inline]
fn read_string<R: Read>(reader: &mut R) -> std::io::Result<String> {
    let len = read_var_uint(reader)?;
    if len as usize > MAX_STRING_SIZE {
        panic!("string too large");
    }
    let mut buf = Vec::with_capacity(len as usize);
    unsafe { buf.set_len(len as usize) };

    reader.read_exact(&mut buf[..])?;

    Ok(String::from_utf8(buf).unwrap())
}
