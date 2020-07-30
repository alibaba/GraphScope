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

use structopt::StructOpt;
use std::time::Duration;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "sends", default_value = "1")]
    pub sends: usize,
    #[structopt(short = "r", long = "recvs", default_value = "1")]
    pub recvs: usize,
}

struct Message(Vec<u64>);

fn main() {
    let config: Config = Config::from_args();
    println!("config : {:?}", config);
    ::std::thread::sleep(Duration::from_secs(5));
    println!("start ...");
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut tx_guards = Vec::new();
    for i in 0..config.sends {
        let tx = tx.clone();
        let tx_g = ::std::thread::Builder::new().name(format!("send-{}", i))
            .spawn(move || {
                loop {
                    tx.send(Message(vec![0])).unwrap();
                }
            }).unwrap();
        tx_guards.push(tx_g);
    }

    let mut rx_guards = Vec::new();
    for i in 0..config.recvs {
        let rx = rx.clone();
        let rx_g = ::std::thread::Builder::new().name(format!("recv-{}", i))
            .spawn(move || {
                let mut count = 0;
                while let Ok(_msg) = rx.recv() {
                    count += 1;
                }
                println!("recv {} get {} messges;", i, count);
            }).unwrap();
        rx_guards.push(rx_g);
    }

    for guard in tx_guards {
        guard.join().unwrap();
    }
}
