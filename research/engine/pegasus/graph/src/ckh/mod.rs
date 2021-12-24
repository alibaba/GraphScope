#[cfg(not(feature = "gcip"))]
mod codegen {
    pub mod clickhouse_grpc {
        tonic::include_proto!("clickhouse.grpc");
    }
}

#[rustfmt::skip]
#[cfg(feature = "gcip")]
mod codegen {
    #[path = "clickhouse.grpc.rs"]
    pub mod clickhouse_grpc;
}

pub use codegen::clickhouse_grpc::click_house_client::ClickHouseClient;
pub use codegen::clickhouse_grpc::{QueryInfo, Compression};

// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
// pub enum CKHType {
//     Int8,
//     Int16,
//     Int32,
//     Int64,
//     Int128,
//     Int256,
//
//     UInt8,
//     UInt16,
//     UInt32,
//     UInt64,
//     UInt128,
//     UInt256,
//
//     Float32,
//     Float64,
//
//     Decimal32(usize),
//     Decimal64(usize),
//     Decimal128(usize),
//     Decimal256(usize),
//
//     String,
//     FixedString(usize),
//
//     Uuid,
//
//     Date,
//     DateTime(Tz),
//     DateTime64(usize, Tz),
//
//     Ipv4,
//     Ipv6,
//
//     /// Not supported
//     Enum8(Vec<(String, u8)>),
//     /// Not supported
//     Enum16(Vec<(String, u16)>),
//
//     LowCardinality(Box<CKHType>),
//
//     Array(Box<CKHType>),
//
//     // unused (server never sends this)
//     // Nested(IndexMap<String, Type>),
//     Tuple(Vec<CKHType>),
//
//     Nullable(Box<CKHType>),
//
//     Map(Box<CKHType>, Box<CKHType>),
// }
