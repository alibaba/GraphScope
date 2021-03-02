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

use crate::desc::operator::OperatorDesc;
use crate::desc::SharedResource;
use pegasus::JobConf;
use std::sync::Arc;

#[derive(Clone)]
pub struct JobDesc {
    pub conf: JobConf,
    pub source: SharedResource,
    pub plan: Arc<Vec<OperatorDesc>>,
}

impl JobDesc {
    pub fn new(conf: JobConf, source: SharedResource, plan: Vec<OperatorDesc>) -> Self {
        JobDesc { conf, source, plan: Arc::new(plan) }
    }

    #[inline]
    pub fn job_id(&self) -> u64 {
        self.conf.job_id
    }

    pub fn get_plan_mut(&mut self) -> Option<&mut [OperatorDesc]> {
        Arc::get_mut(&mut self.plan).map(|v| v.as_mut_slice())
    }
}
