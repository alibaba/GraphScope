//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#![allow(bare_trait_objects)]
pub mod cluster;
pub mod common;
pub mod coordinator;
pub mod coordinator_grpc;
pub mod data;
pub mod data_grpc;
pub mod gremlin_query;
pub mod gremlin_query_grpc;
pub mod gremlin_service;
pub mod gremlin_service_grpc;
pub mod hb;
pub mod message;
pub mod query_flow;
pub mod scheduler_monitor;
pub mod scheduler_monitor_grpc;
pub mod schema;
pub mod store_api;
pub mod store_api_grpc;
pub mod lambda_service;
pub mod lambda_service_grpc;
pub mod remote_api;
pub mod remote_api_grpc;
pub mod meta_service;


use serde::{Serialize, Serializer};
use protobuf::Message;
use serde::{Deserialize, Deserializer};
use serde::de::Visitor;
use std::fmt;
use serde::de::Error;

struct CountMapVisitor;

impl<'a> Visitor<'a> for CountMapVisitor {
    type Value = message::CountMap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("CountMap message")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
    {
        ::protobuf::parse_from_bytes(bytes).map_err(|_e| Error::missing_field("CountMap"))
    }
}

impl Serialize for message::CountMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer, {
        serializer.serialize_bytes(&self.write_to_bytes().expect("countmap ser"))
    }
}

impl<'de> Deserialize<'de> for message::CountMap {
    fn deserialize<D>(deserializer: D) -> Result<message::CountMap, D::Error>
        where D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(CountMapVisitor)
    }
}

struct QueryInputVisitor;

impl<'a> Visitor<'a> for QueryInputVisitor {
    type Value = query_flow::QueryInput;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("QueryInput message")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
    {
        ::protobuf::parse_from_bytes(bytes).map_err(|_e| Error::missing_field("QueryInput"))
    }
}

impl Serialize for query_flow::QueryInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer, {
        serializer.serialize_bytes(&self.write_to_bytes().expect("query input ser"))
    }
}

impl<'de> Deserialize<'de> for query_flow::QueryInput {
    fn deserialize<D>(deserializer: D) -> Result<query_flow::QueryInput, D::Error>
        where D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(QueryInputVisitor)
    }
}
