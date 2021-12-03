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

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use pegasus_common::codec::{AsBytes, Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use structopt::StructOpt;

pub const CONTENT_LENGTH: usize = 256;

struct EchoMessage {
    timestamp: SystemTime,
    content: Vec<u8>,
}

impl EchoMessage {
    pub fn new() -> Self {
        EchoMessage { timestamp: SystemTime::now(), content: vec![9; CONTENT_LENGTH] }
    }
}

impl Encode for EchoMessage {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        let timestamp = self.timestamp.as_bytes();
        writer.write_u64(timestamp.len() as u64)?;
        writer.write_all(timestamp)?;
        writer.write_all(&self.content)
    }
}

impl Decode for EchoMessage {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u64()? as usize;
        let mut time_bytes = vec![0u8; len];
        reader.read_exact(&mut time_bytes[0..])?;
        let timestamp = SystemTime::from_bytes(&time_bytes[0..]).clone();
        let mut content = vec![0u8; CONTENT_LENGTH];
        reader.read_exact(&mut content)?;
        Ok(EchoMessage { timestamp, content })
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "echo-benchmark", about = "The benchmark of IPC network lib;")]
struct EchoBenchConfig {
    #[structopt(parse(from_os_str))]
    network_config_file: PathBuf,
    #[structopt(short = "n", default_value = "1024")]
    message_size: usize,
    #[structopt(short = "r")]
    report: bool,
}

fn main() {
    pegasus_common::logs::init_log();
    let echo_config: EchoBenchConfig = EchoBenchConfig::from_args();
    println!("echo-benchmark config : {:?}", echo_config);
    let config = pegasus_network::config::read_from(echo_config.network_config_file.as_path()).unwrap();
    let addr = config.local_addr().unwrap();
    let params = config.get_connection_param();
    println!("connection parameters: {:?}", params);
    let peers = config.get_servers().unwrap();
    if peers.is_none() {
        eprintln!("no network peers found;");
        return;
    }

    let peers = peers.unwrap();
    let mut remotes = vec![];
    for peer in peers.iter() {
        if peer.id != config.server_id {
            remotes.push(peer.id);
        }
    }

    let addr = pegasus_network::start_up(config.server_id, params, addr, peers).unwrap();
    println!("echo-benchmark: server {} start on {:?};", config.server_id, addr);

    while !pegasus_network::check_connect(config.server_id, &remotes) {
        std::thread::sleep(Duration::from_secs(1));
    }

    let (mut sends, recv) = pegasus_network::ipc_channel::<EchoMessage>(1, config.server_id, &remotes)
        .unwrap()
        .take();

    // send the first messages to ensure targets has setup channel;
    let sync = EchoMessage::new();
    for tx in sends.iter_mut() {
        tx.send(&sync).unwrap();
    }

    let barrier = Arc::new(AtomicBool::new(false));
    let barrier_r = barrier.clone();
    let report = echo_config.report;
    let guard_recv = std::thread::spawn(move || {
        let mut start = Instant::now();
        let mut total = 0u128;
        let mut count = 0;
        loop {
            match recv.recv() {
                Ok(Some(msg)) => {
                    let elapsed = SystemTime::now().duration_since(msg.timestamp);
                    if let Ok(elapsed) = elapsed {
                        if count > 0 {
                            let cost = elapsed.as_micros();
                            // ignore the first message;
                            total += cost;
                            if count == 1 || report {
                                println!("{:?} receive message rt = {} us", start.elapsed(), cost);
                            }
                        } else {
                            barrier.store(true, Ordering::Relaxed);
                            start = Instant::now();
                        }
                        count += 1;
                    }
                }
                Ok(None) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                    break;
                }
                Err(e) => panic!("get error {:?}", e),
            }
        }
        let cost = start.elapsed();
        let avg_rt = (total as f64) / (count - 1) as f64;
        println!("get {} messages, cost {:?}, avg rt {:.2} us", count - 1, cost, avg_rt);
    });

    let message_size = echo_config.message_size;
    let guard_send = std::thread::spawn(move || {
        while !barrier_r.load(Ordering::Relaxed) {
            // wait barrier;
        }
        println!(
            "{} start to send message; ",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros()
        );
        for _ in 0..message_size {
            let msg = EchoMessage::new();
            for tx in sends.iter_mut() {
                tx.send(&msg).unwrap();
            }
        }
        for tx in sends.iter_mut() {
            println!("close IPCSender of channel {} to {:?}", tx.channel_id, tx.target);
            tx.close().unwrap();
        }
    });

    guard_send.join().unwrap();
    guard_recv.join().unwrap();
    pegasus_network::shutdown(config.server_id);
    pegasus_network::await_termination(config.server_id);
}
