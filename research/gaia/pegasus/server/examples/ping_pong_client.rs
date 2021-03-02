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

use pegasus::JobConf;
use pegasus_server::client::builder::JobBuilder;
use pegasus_server::client::JobRpcClient;
use pegasus_server::BinaryResource;
use pegasus_server::JobResult;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let mut client = JobRpcClient::from_str("http://127.0.0.1:1234").await?;
    let mut builder = JobBuilder::new(JobConf::new(1, "ping_pong_example", 2));
    builder
        .add_source(get_seed(0))
        .repeat(3, |start| {
            start.exchange(get_route()).map(add(1)).flat_map(copy_n(8));
        })
        .sink(get_sink());

    let req = builder.build().unwrap();

    let mut result = client.submit(req).await?;
    while let Some(next) = result.message().await? {
        let job_id = next.job_id;
        println!("get response of job {}", job_id);
        match next.result {
            None => {}
            Some(JobResult::Data(v)) => {
                let data = from_bytes(v);
                println!("job[{}] get {} results {:?}", job_id, data.len(), data);
            }
            Some(JobResult::Err(e)) => {
                println!("get error {}", e.err_msg);
            }
        }
    }

    Ok(())
}

fn get_route() -> BinaryResource {
    vec![]
}

#[inline]
fn get_seed(a: u64) -> BinaryResource {
    a.to_le_bytes().to_vec()
}

#[inline]
fn add(a: u64) -> BinaryResource {
    a.to_le_bytes().to_vec()
}

#[inline]
fn copy_n(a: u64) -> BinaryResource {
    a.to_le_bytes().to_vec()
}

fn get_sink() -> BinaryResource {
    vec![]
}

fn from_bytes(bytes: Vec<u8>) -> Vec<u64> {
    let size = std::mem::size_of::<u64>();
    let count = bytes.len() / size;
    assert_eq!(bytes.len() % size, 0);
    let mut data = Vec::with_capacity(count);
    let mut start = 0;
    let mut end = size;
    for _ in 0..count {
        let mut buf = [0u8; std::mem::size_of::<u64>()];
        buf.copy_from_slice(&bytes[start..end]);
        data.push(u64::from_le_bytes(buf));
        start += size;
        end += size;
    }

    data
}
