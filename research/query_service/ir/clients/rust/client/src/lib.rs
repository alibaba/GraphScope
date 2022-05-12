//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::error::Error;

use pegasus_server::pb as pegasus_pb;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

pub mod builder;

pub struct JobRpcClient {
    stub: pegasus_pb::job_service_client::JobServiceClient<Channel>,
}

impl JobRpcClient {
    pub async fn from_str(addr: &'static str) -> Result<Self, Box<dyn Error>> {
        let stub = pegasus_pb::job_service_client::JobServiceClient::connect(addr).await?;
        Ok(JobRpcClient { stub })
    }

    pub fn new(ch: Channel) -> Self {
        let stub = pegasus_pb::job_service_client::JobServiceClient::new(ch);
        JobRpcClient { stub }
    }

    pub async fn submit(
        &mut self, job_req: pegasus_pb::JobRequest,
    ) -> Result<Streaming<pegasus_pb::JobResponse>, Box<dyn Error>> {
        Ok(self
            .stub
            .submit(Request::new(job_req))
            .await?
            .into_inner())
    }
}

#[allow(dead_code)]
pub struct JobClient {
    stubs: Vec<JobRpcClient>,
}
