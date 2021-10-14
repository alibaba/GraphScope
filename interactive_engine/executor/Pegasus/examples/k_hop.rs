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

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
#[macro_use]
extern crate serde_derive;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, Duration};
use std::io;
use std::io::BufRead;
use std::net::SocketAddr;
use std::convert::AsRef;
use std::path::Path;

use structopt::StructOpt;
use log::*;
use pegasus::{try_init_logger, ConfigArgs, Pegasus};
use pegasus::memory::MemMonitor;
use pegasus::graph::topology::{GraphTopology, Vertex};
use pegasus::operator::IntoStream;
use pegasus::operator::advanced::iterate::Iteration;
use pegasus::operator::advanced::exchange::Exchange;
use pegasus::operator::advanced::map::Map;
use pegasus::operator::advanced::inspect::Inspect;
use pegasus::operator::advanced::aggregate::Count;
use pegasus::operator::unary::Unary;
use pegasus::communication::Pipeline;
use pegasus::worker::{DefaultStrategy, Worker};
use pegasus::strategy::{MemResourceManager, ResourceBoundStrategy};
use pegasus::server::{Service, TaskGenerator, TaskRequest, Sink, BinaryMessage, TaskResponseHeader, TaskResponse};
use pegasus::operator::sink::{SinkBinary, SinkStream};
use pegasus::common::{BytesSlab, Bytes};
use pegasus::serialize::{CPSerialize, CPDeserialize};
use pegasus::server::clients::client::ClientService;

use crossbeam_queue::SegQueue;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    /// config to run in service;
    #[structopt(long = "server")]
    pub server: bool,
    /// numbers of threads created in advance;
    #[structopt(short = "t", long = "thread", default_value = "0")]
    pub threads: usize,
    /// if the graph source file is in binary format;
    #[structopt(short = "b", long = "binary")]
    pub binary: bool,
    /// graph source file
    #[structopt(short = "f", long = "file")]
    pub file: String,
    /// limit memory usage in mb
    #[structopt(short = "r", long = "resource", default_value = "0")]
    pub resource: u32,
    /// used to configure distribute environment, default is single machine;
    #[structopt(short = "d", long = "distribute host file", default_value = "")]
    pub distribute: String,
    #[structopt(short = "i", long = "index", default_value = "0")]
    /// current node's index
    pub index: usize,
    /// number of total processes
    #[structopt(short = "p", long = "process", default_value = "1")]
    pub processes: usize,
    /// how much generators used to submit tasks in parallel;
    #[structopt(short = "g", long = "generators", default_value = "1")]
    pub generators: usize,
    /// config to run client
    #[structopt(long = "client")]
    pub client: bool,
    /// server address file
    #[structopt(long = "addr", default_value = "")]
    pub address: String,
    /// query id file
    #[structopt(short = "q", long = "queries", default_value = "")]
    pub queries: String,
    /// enable client submit task one by one;
    #[structopt(long = "block", default_value = "0")]
    pub block: u64,
    /// enable memory tracing;
    #[structopt(short = "m")]
    pub memory: bool,
}

pub struct KHop {
    resources: Option<MemResourceManager>,
    graph: Arc<GraphTopology>,
}

impl KHop {
    pub fn new(resources: Option<MemResourceManager>, graph: &Arc<GraphTopology>) -> Self {
        KHop {
            resources,
            graph: graph.clone()
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct KHopConfig {
    pub workers: u32,
    pub processes: u32,
    pub src: u64,
    pub k: u32,
    pub capacity: u32,
    pub mem: u32,
    pub share: bool,
}

impl KHopConfig {
    pub fn from_str(line: String) -> Option<Self> {
        let split = line.split(',').collect::<Vec<_>>();
        if split.len() < 1 {
            None
        } else {
            let src = split[0].parse::<u64>().unwrap();
            let k = split.get(1).map(|s| s.parse::<u32>().unwrap()).unwrap_or(3);
            let workers = split.get(2).map(|s| s.parse::<u32>().unwrap()).unwrap_or(2);
            let processes = split.get(3).map(|s| s.parse::<u32>().unwrap()).unwrap_or(1);
            let capacity = split.get(4).map(|s| s.parse::<u32>().unwrap()).unwrap_or(1);
            let mem = split.get(5).map(|s| s.parse::<u32>().unwrap()).unwrap_or(0);
            let share = split.get(6).map(|s| s.parse::<bool>().unwrap()).unwrap_or(false);
            Some(KHopConfig {
                workers,
                processes,
                src,
                k,
                capacity,
                mem,
                share
            })
        }
    }
}

impl CPSerialize for KHopConfig {
    fn serialize_len(&self) -> usize {
        32
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        write.write_u32(self.workers)?;
        write.write_u32(self.processes)?;
        write.write_u64(self.src)?;
        write.write_u32(self.k)?;
        write.write_u32(self.capacity)?;
        write.write_u32(self.mem)?;
        if self.share {
            write.write_u32(1)
        } else {
            write.write_u32(0)
        }
    }
}

impl CPDeserialize for KHopConfig {
    fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
        let workers = bytes.read_u32()?;
        let processes = bytes.read_u32()?;
        let src = bytes.read_u64()?;
        let k = bytes.read_u32()?;
        let capacity = bytes.read_u32()?;
        let mem = bytes.read_u32()?;
        let share = if bytes.read_u32()? == 0 { false } else { true };
        Ok(KHopConfig {
            workers,
            processes,
            src,
            k,
            capacity,
            mem,
            share
        })
    }
}

#[derive(Debug, Copy, Clone, Abomonation, Serialize, Deserialize)]
pub struct KHopResponse {
    pub result: u64,
    pub cost: u64
}

impl KHopResponse {
    pub fn new(result: u64, cost: u64) -> Self {
        KHopResponse {
            result,
            cost
        }
    }
}

impl CPSerialize for KHopResponse {
    fn serialize_len(&self) -> usize {
        16
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        write.write_u64(self.result)?;
        write.write_u64(self.cost)
    }
}

impl CPDeserialize for KHopResponse {
    fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
        let result = bytes.read_u64()?;
        let cost = bytes.read_u64()?;
        Ok(KHopResponse {
            result,
            cost
        })
    }
}

impl TaskGenerator for KHop {
    fn create_task(&self, task: TaskRequest<Option<Bytes>>, runtime: &Pegasus, sink: &mut Sink) -> Result<(), String> {
        let header = *task.header();
        if let Some(body) = task.take_body() {
            let config = KHopConfig::read_from(body).map_err(|e| {
                format!("decode k hop config failure, {:?}, error : {:?}", header, e)
            })?;

            debug!("get task {:?}, {:?}", header, config);
            let task_id = header.task_id as usize;
            let mut workers = runtime.create_workers(task_id, header.workers as usize, header.processes as usize).unwrap();
            let mrm = if config.share {
                self.resources.clone()
            } else if config.mem > 0 {
                Some(MemResourceManager::new(config.mem as usize))
            } else {
                None
            };

            for worker in workers.iter_mut() {
                create_k_hop(worker,  config, self.graph.clone(),&mrm, Some(sink.clone()));
            }
            runtime.run_workers(workers)
                .map_err(|err| format!("create k-hop: {} failure, caused by {:?}", task_id, err))
        } else {
            Ok(())
        }
    }
}

pub fn create_k_hop(worker: &mut Worker, config: KHopConfig, graph: Arc<GraphTopology>,
                    resources: &Option<MemResourceManager>, sink: Option<Sink>) {

    if let Some(mrm) = resources.as_ref() {
        worker.set_schedule_strategy(ResourceBoundStrategy::new(1024, config.capacity as usize, mrm));
    } else {
        worker.set_schedule_strategy(DefaultStrategy::with_capacity(config.capacity as usize * 1024));
    }

    let index = worker.id.1;
    let v_size= ::std::mem::size_of::<Vertex>();
    let workers = (config.workers * config.processes) as usize;
    let k = config.k;
    let mem = config.mem > 0 || config.share;
    let src = config.src;
    worker.dataflow_opt("k-hop", 1024, true, move  |builder| {
        let stream = vec![src].into_iter().filter(move |i| (*i as usize) % workers == index)
            .map(|id| Vertex::new(id))
            .into_stream(builder)
            .iterate(k, move |start| {
                let graph = graph.clone();
                let exchange = start.exchange(|v| v.id as u64);
                if v_size < 64 && !mem {
                    exchange.unary("flat_map", Pipeline, move |info| {
                        info.set_expand();
                        move |input, output| {
                            input.for_each_batch(|dataset| {
                                let mut session = output.session(&dataset);
                                for v in dataset.data() {
                                    if let Some(result) = graph.get_neighbors(&v.id) {
                                        session.give_entire_iterator(result)?;
                                    }
                                }
                                Ok(session.has_capacity())
                            })?;
                            Ok(())
                        }
                    })
                } else {
                    exchange.flat_map(move |v| {
                        graph.get_neighbors(&v.id)
                    })
                }
            })
            .count(Pipeline)
            .inspect(move |count| debug!("worker[{}] get {} neighbors;", index, count))
            .aggregate_to(0)
            .unary_state("sum", Pipeline, |info| {
                info.set_clip();
                (
                    |input, _output, sum| {
                        input.for_each_batch(|dataset| {
                            let (t, d) = dataset.take();
                            let sum = sum.entry(t).or_insert(0);
                            for r in d {
                                *sum += r;
                            }
                            Ok(true)
                        })?;
                        Ok(())
                    },
                    |output, sums| {
                        for (t, s) in sums {
                            output.session(&t).give(s)?;
                        }
                        Ok(())
                    }
                )
            });

        if let Some(sink) = sink {
            let start = Instant::now();
            stream.sink_bytes(sink, move |info| {
                let task_id = info.worker.task_id() as u64;
                move |_, sink_stream| {
                    match sink_stream {
                        SinkStream::Data(r) => {
                            let dur = start.elapsed();
                            let cost = dur.as_millis() as u64;
                            let result = KHopResponse::new(r[0] as u64, cost);
                            info!("worker[{}] get result {:?}, cost {:?}", index, r, cost);
                            let res = TaskResponse::ok(task_id, result);
                            Some(res)
                        },
                        _ => None
                    }
                }
            });
        } else {
            stream.inspect(move |count|
                info!("worker[{}] {} has {} {}-hop neighbors;", index, src, count, k));
        }
        Ok(())
    }).unwrap();
}


fn read_queries_from_file<P: AsRef<Path>>(file: P) -> SegQueue<KHopConfig> {
    let reader = ::std::io::BufReader::new(::std::fs::File::open(file).unwrap());
    let queue = SegQueue::new();
    for query in reader.lines() {
        if let Ok(query) = query {
            if let Some(khop) = KHopConfig::from_str(query) {
                queue.push(khop);
            }
        }
    }
    queue
}

fn main() {
    try_init_logger().ok();
    let config: Config = Config::from_args();
    info!("Config: {:?}", config);
    let Config {
        server,
        threads,
        binary,
        file,
        resource,
        distribute,
        index,
        processes,
        generators,
        client,
        address,
        queries,
        block,
        memory,
    } = config;

    if !client {
        // run server service;
        let load_start = Instant::now();
        info!("start to load graph ...");
        let graph = if binary {
            GraphTopology::load_bin(0, 1, true, file)
        } else {
            GraphTopology::load(0, 1, true, ' ', file)
        };

        let graph = Arc::new(graph);
        println!("finished load graph with {} edges, cost {:?}", graph.count_edges(), load_start.elapsed());
        let mut mon = if memory {
            let mon = MemMonitor::spawn("./mem_k_hop", 10);
            mon.print_memory_use();
            Some(mon)
        } else { None };
        ::std::thread::sleep(Duration::from_millis(100));

        let rm = if resource > 0 {
            Some(MemResourceManager::new(resource as usize))
        } else {
            None
        };

        let runtime = if distribute.is_empty() || processes <= 1 {
            ConfigArgs::singleton(threads).build()
        } else {
            ConfigArgs::distribute(index, threads, processes, distribute).build()
        };

        if server {
            // run as server service;
            let k_hop = KHop::new(rm, &graph);
            let mut service = Service::new(runtime, k_hop);
            let addr = service.bind().unwrap();
            println!("start service on {:?}", addr);
            service.start(generators);
        } else {
            let queries = Arc::new(read_queries_from_file(queries));
            let start = Instant::now();
            let mut generators = Vec::new();
            let count = Arc::new(AtomicUsize::default());
            for i in 0..config.generators {
                let queries = queries.clone();
                let rm = rm.clone();
                let count = count.clone();
                let runtime = runtime.clone();
                let graph = graph.clone();
                let guard = ::std::thread::Builder::new().name(format!("generator-{}", i))
                   .spawn(move || {
                       while let Ok(q) = queries.pop() {
                           let mrm = if q.share {
                               rm.clone()
                           } else if q.mem > 0 {
                               Some(MemResourceManager::new(q.mem as usize))
                           } else {
                               None
                           };

                           let id = count.fetch_add(1, Ordering::SeqCst);
                           if let Some(mut workers) = runtime.create_workers(id, q.workers as usize, q.processes as usize) {
                               for w in workers.iter_mut() {
                                   create_k_hop(w, q, graph.clone(), &mrm, None);
                               }
                               runtime.run_workers(workers).unwrap();
                           }
                       }
                   }).unwrap();
                generators.push(guard);
            }

            for g in generators {
                g.join().unwrap();
            }
            runtime.shutdown().unwrap();
            let count = count.load(Ordering::SeqCst);
            println!("{} task finished, cost {:?}", count, start.elapsed());
        }
        mon.as_mut().map(|m| m.shutdown());
    } else {
        let mut client_service = ClientService::new();
        if !address.is_empty() {
            let reader = ::std::io::BufReader::new(::std::fs::File::open(address).unwrap());
            let mut addresses = Vec::new();
            for x in reader.lines() {
                let addr = x.unwrap().parse::<SocketAddr>().unwrap();
                addresses.push(addr);
            }

            client_service.start_service(&addresses).unwrap();
            let client = client_service.new_async_client();
            let queries = read_queries_from_file(queries);
            let start = Instant::now();
            let mut task_id = 0;
            let (tx, rx) = crossbeam_channel::unbounded();
            let mut total_result_count = 0;
            let mut total_cost = 0;
            let mut count = 0;
            while let Ok(query) = queries.pop() {
                let tx = tx.clone();
                let query_start = Instant::now();
                client.new_task(task_id, query.workers, query.processes, query, move |mut res: BinaryMessage<TaskResponseHeader>| {
                    if res.header.is_ok() {
                        if let Some(body) = res.take_body() {
                            let r = KHopResponse::read_from(body)
                                .map_err(|err| {
                                    error!("Decode response failure : {:?}", err);
                                }).ok().unwrap();
                            let elapsed = query_start.elapsed();
                            info!("Get response of {}: {}, cost {}/{:?}", res.header.task_id, r.result, r.cost, elapsed);
                            tx.send((r.result, elapsed.as_millis() as u64)).expect("send result failure");
                        } else {
                            error!("response body is null;");
                        }
                    } else {
                        error!("Task fail: {:?}", res.header);
                    }
                    true
                }).map_err(|err| {
                    error!("New task failure: {}, error : {:?}", task_id, err);
                }).ok();
                task_id += 1;
                count += 1;
                if block > 0 && count == block {
                    while count > 0 {
                        if let Ok((count, cost)) = rx.recv() {
                            total_result_count += count;
                            total_cost += cost;
                        } else {
                            panic!("response is null");
                        }
                        count -= 1;
                    }
                }
            }
            while client.running_tasks() > 0 {
                if let Ok((count, cost)) = rx.recv() {
                    total_result_count += count;
                    total_cost += cost;
                } else {
                    panic!("response is null");
                }
            }

            let count_avg = total_result_count as f64 / task_id as f64;
            let latency_avg = total_cost as f64 / task_id as f64;
            println!("all {} tasks finished, cost {:?}, results count in average {}, latency in average {}", task_id, start.elapsed(), count_avg, latency_avg);
        }
    }
}

