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

use std::path::PathBuf;
use std::fs::File;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use maxgraph_common::proto::message::*;
use dataflow::message::{RawMessage, RawMessageType};

use protobuf::{RepeatedField, Message, CodedOutputStream, CodedInputStream, ProtobufResult, parse_from_bytes};
use maxgraph_common::proto::schema::SchemaProto;
use maxgraph_common::proto::lambda_service::LambdaData;
use maxgraph_common::proto::lambda_service::LambdaBase;
use maxgraph_common::proto::lambda_service::LambdaResult;
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
use grpcio::{EnvBuilder, ChannelBuilder};
use maxgraph_store::api::graph_schema::Schema;

#[derive(Debug)]
pub enum LambdaType {
    FILTER,
    MAP,
    FLATMAP,
}

pub struct LambdaManager {
    // pub lambda_type: LambdaType,
    // pub schema: Arc<Schema>,
    pub query_id: String,
    // pub script: String,
    pub lambda_service_client: Option<Arc<LambdaServiceClient>>,
    // lambda_base_timeout_ms: u64,
}

impl LambdaManager {

    pub fn new(query_id: &str,
               lambda_service_client: Option<Arc<LambdaServiceClient>>,
               ) -> Self{

        // let lambda_type = LambdaType::Whatever;
        LambdaManager{
            // lambda_type,
            // schema,
            query_id: String::from(query_id),
            // script,
            lambda_service_client,
            // lambda_base_timeout_ms
        }
    }

    pub fn send_lambda_base_with_script(&self, script: &str, timeout_ms: u64, schema: Arc<Schema>) {
        info!("rust start to send LambdaBase...");

        let mut lambda_base = LambdaBase::new();
        lambda_base.set_query_id(self.query_id.clone());
        lambda_base.set_script(String::from(script));
        lambda_base.set_timeout_ms(timeout_ms);
        let schema_bytes = schema.to_proto();
        let schema_proto = parse_from_bytes::<SchemaProto>(schema_bytes.as_slice()).unwrap();
        lambda_base.set_schema(schema_proto);

        if let Ok(_) = self.lambda_service_client.as_ref().unwrap().prepare(&lambda_base) {
            info!("rust send LambdaBase successfully!!!");
        }
    }

    pub fn send_lambda_base_with_bytecode(&self, bytecode: Vec<u8>, timeout_ms: u64, schema: Arc<Schema>) {
        info!("rust start to send LambdaBase...");

        let mut lambda_base = LambdaBase::new();
        lambda_base.set_query_id(self.query_id.clone());
        // lambda_base.set_script(String::from(script));
        lambda_base.set_bytecode(bytecode);
        lambda_base.set_timeout_ms(timeout_ms);
        let schema_bytes = schema.to_proto();
        let schema_proto = parse_from_bytes::<SchemaProto>(schema_bytes.as_slice()).unwrap();
        lambda_base.set_schema(schema_proto);

        if let Ok(_) =  self.lambda_service_client.as_ref().unwrap().prepare(&lambda_base) {
            info!("rust send LambdaBase successfully!!!");
        }
    }

    pub fn send_lambda_filter_query(&self, message_list: ListProto, lambda_index: &str) -> HashSet<i64> {
        let mut message = LambdaData::new();
        message.set_query_id(self.query_id.clone());
        message.set_lambda_index(lambda_index.to_string());
        message.set_message_list(message_list);

        let response = self.lambda_service_client.as_ref().unwrap().filter(&message);

        response.unwrap().take_result_id_list().take_value().into_iter().collect()
    }

    pub fn send_lambda_map_query(&self, message_list: ListProto, lambda_index: &str) -> ListProto {
        let mut message = LambdaData::new();
        message.set_query_id(self.query_id.clone());
        message.set_lambda_index(lambda_index.to_string());
        message.set_message_list(message_list);

        let mut response = self.lambda_service_client.as_ref().unwrap().map(&message).unwrap();

        response.take_message_list()
    }

    pub fn send_lambda_flatmap_query(&self, message_list:ListProto, lambda_index: &str) -> (ListProto, ListLong) {
        let mut message = LambdaData::new();
        message.set_query_id(self.query_id.clone());
        message.set_lambda_index(lambda_index.to_string());
        message.set_message_list(message_list);

        let mut response = self.lambda_service_client.as_ref().unwrap().flatmap(&message).unwrap();

        (response.take_message_list(), response.take_result_id_list())
    }

    // send through named pipes
    // pub fn send_batch_data(&self, data_pipe: &PathBuf, message: LambdaData) {
    //     info!("rust start to send batch data...");
    //     let mut file = File::create(data_pipe).unwrap();
    //     let mut cos = CodedOutputStream::new(&mut file);
    //
    //     message.write_to(&mut cos);
    //     cos.flush();
    //     info!("rust send batch data successfully!!!");
    // }

//     pub fn send_lambda_base(&self, base_pipe: &PathBuf) -> ::std::io::Result<()> {
//         info!("rust start to send LambdaBase...");
// //        ::std::fs::write(self.script_pipe.as_ref().unwrap(), self.script)?;
//         let mut lambda_base = LambdaBase::new();
//         lambda_base.set_query_id(String::from(&self.query_id));
//         lambda_base.set_script(String::from(&self.script));
//         let schema_bytes = self.schema.to_proto();
//         let schema_proto = parse_from_bytes::<SchemaProto>(schema_bytes.as_slice()).unwrap();
//         lambda_base.set_schema(schema_proto);
//
//         let mut file = File::create(base_pipe).unwrap();
//         let mut cos  = CodedOutputStream::new(&mut file);
//         lambda_base.write_to(&mut cos);
//         cos.flush();
//
//         info!("rust send LambdaBase successfully!!!");
//         Ok(())
//     }

    // pub fn send_data(&self, message: RawMessage) {
    //     info!("rust start to send data...");
    //     let mut file = File::create(self.data_pipe.as_ref().unwrap()).unwrap();
    //     let mut cos = CodedOutputStream::new(&mut file);
    //
    //     message.to_proto().write_to(&mut cos);
    //     cos.flush();
    //     info!("rust send data successfully!!!");
    // }


    // pub fn wait_for_result(&self, result_pipe: &PathBuf) -> Vec<RawMessage> {
    //     info!("rust start to wait for result...");
    //     let mut result = vec![];
    //
    //     let mut file = File::open(result_pipe).unwrap();
    //     let mut ins = CodedInputStream::new(&mut file);
    //     let mut lambda_result = LambdaData::new();
    //     lambda_result.merge_from(&mut ins);
    //
    //     info!("!!!lambda_data!!!: {:?}", lambda_result.clone());
    //
    //     for mut message in lambda_result.take_message_list().take_value().into_vec() {
    //         result.push(RawMessage::from_proto(&mut message));
    //     }
    //     info!("rust get the result successfully!");
    //
    //     result
    // }
}

