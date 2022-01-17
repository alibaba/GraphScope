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
        server_config_file.push("/server_config.toml");
        let mut f = std::fs::File::open(server_config_file.as_path())?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        pegasus::Configuration::parse(buf.as_str())?
    };

    let rpc_config: RPCServerConfig = {
        let mut rpc_config_file = dir.clone();
        rpc_config_file.push("/rpc_config.toml");
        let mut f = std::fs::File::open(rpc_config_file.as_path())?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        toml::from_str(buf.as_str())?
    };

    Ok((server_config, rpc_config))
}
