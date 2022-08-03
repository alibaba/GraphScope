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

use std::io::Read;
use std::path::PathBuf;

use crate::rpc::RPCServerConfig;

pub fn load_configs<P>(config_dir: P) -> std::io::Result<(pegasus::Configuration, RPCServerConfig)>
where
    P: Into<PathBuf>,
{
    let dir = config_dir.into();
    let server_config = {
        let mut server_config_file = dir.clone();
        server_config_file.push("server_config.toml");
        let mut f = std::fs::File::open(server_config_file.as_path())?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        pegasus::Configuration::parse(buf.as_str())?
    };

    let rpc_config: RPCServerConfig = {
        let mut rpc_config_file = dir.clone();
        rpc_config_file.push("rpc_config.toml");
        let mut f = std::fs::File::open(rpc_config_file.as_path())?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        toml::from_str(buf.as_str())?
    };

    Ok((server_config, rpc_config))
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::load_configs;

    #[test]
    fn parse_config_test() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        if !path.ends_with("server") {
            path.push("server");
        }
        path.push("config");
        path.push("tests");
        path.push("standalone2");
        let (server_conf, rpc_conf) = load_configs(path).unwrap();
        assert_eq!(server_conf.max_pool_size, Some(8));
        assert_eq!(server_conf.server_id(), 0);
        assert_eq!(server_conf.servers_size(), 2);
        if let Some(net_conf) = server_conf.network_config() {
            let servers = net_conf.get_servers().unwrap().unwrap();
            assert_eq!(servers.len(), 2);
            assert_eq!(servers[0].id, 0);
            assert_eq!(servers[1].id, 1);
            assert_eq!(servers[0].addr, "192.168.1.1:8080".parse().unwrap());
            assert_eq!(servers[1].addr, "192.168.1.2:8080".parse().unwrap());
            let params = net_conf.get_connection_param();
            assert!(!params.is_nonblocking);
            assert_eq!(params.get_read_params().slab_size, 65535);
            assert_eq!(
                params
                    .get_read_params()
                    .mode
                    .get_block_timeout_ms(),
                1
            );
            assert!(params.get_write_params().nodelay);
            assert_eq!(params.get_write_params().heartbeat, 5);
            assert_eq!(params.get_write_params().buffer, 4096);
            assert_eq!(
                params
                    .get_write_params()
                    .mode
                    .get_block_timeout_ms(),
                1
            );
        } else {
            panic!("Network configuration should not be None;")
        }

        assert_eq!(rpc_conf.rpc_host.unwrap(), "0.0.0.0");
        assert_eq!(rpc_conf.rpc_port.unwrap(), 5000);
    }
}
