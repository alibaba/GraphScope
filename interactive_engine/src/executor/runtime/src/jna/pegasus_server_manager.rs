use server::manager::{ServerManager, ManagerGuards};
use std::sync::{Arc, RwLock};
use maxgraph_store::config::StoreConfig;
use pegasus::{Pegasus, ConfigArgs};
use store::task_partition_manager::TaskPartitionManager;
use std::net::{TcpListener, TcpStream};
use server::allocate::register_tcp_listener;
use std::{thread, time};
use pegasus::network::reconnect;
use tokio::io::Error;
use pegasus::network_connection::Connection;
use std::sync::atomic::AtomicBool;
use maxgraph_store::db::api::{GraphResult, GraphError};
use maxgraph_store::db::api::GraphErrorCode::InvalidOperation;

pub struct PegasusServerManager {
    pegasus_runtime: Arc<Option<Pegasus>>,
    node_id: usize,
}

impl PegasusServerManager {
    pub fn new(node_id: usize, thread_pool_size: usize, worker_num: usize) -> Self {
        let pegasus_runtime = Arc::new(Some(ConfigArgs::distribute(
            node_id,
            thread_pool_size,
            worker_num,
            "".to_string())
            .build()));
        PegasusServerManager {
            pegasus_runtime,
            node_id,
        }
    }

    #[inline]
    pub fn get_task_partition_manager(&self) -> Arc<RwLock<Option<TaskPartitionManager>>> {
        unimplemented!()
    }

    #[inline]
    pub fn get_server(&self) -> Arc<Option<Pegasus>> {
        self.pegasus_runtime.clone()
    }

    pub fn register_listener(&self) -> TcpListener {
        register_tcp_listener()
    }

    pub fn start_server(&self, listener: TcpListener, ip_list: Vec<String>, retry_times: u64) -> GraphResult<ManagerGuards<()>> {
        let group_id = 0;
        let node_id = self.node_id;
        let runtime = self.pegasus_runtime.clone();
        let handle = thread::Builder::new().name(format!("Pegasus Server Manager {}", group_id)).spawn(move || {
            let mut start_addresses = Vec::new();
            let mut await_addresses = Vec::new();

            for (index, ip) in ip_list.iter().enumerate() {
                if index == (node_id as usize) {
                    continue;
                } else if index < (node_id as usize) {
                    start_addresses.push((index, ip.clone()));
                } else {
                    await_addresses.push((index, ip.clone()));
                }
            }

            match reconnect(node_id, listener, start_addresses, await_addresses, retry_times) {
                Ok(result) => {
                    for (index, address, tcp_stream) in result.into_iter() {
                        let conn = Connection::new(index, address, tcp_stream);
                        runtime.as_ref().as_ref().unwrap().reset_single_network(conn);
                    }
                },
                Err(e) => {
                    error!("group {} worker {} reset network failed, caused by {:?}", node_id, group_id, e);
                },
            }
            thread::sleep(time::Duration::from_millis(u64::max_value()));
        });
        match handle {
            Ok(handle) => {
                Ok(ManagerGuards::new(handle, Arc::new(AtomicBool::new(true))))
            }
            Err(e) => Err(GraphError::new(InvalidOperation, format!("Build server manager thread fail: {:?}", e))),
        }
    }
}
