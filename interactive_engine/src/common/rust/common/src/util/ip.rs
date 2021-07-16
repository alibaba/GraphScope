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

use std::process::Command;

pub fn get_local_ip() -> String {
    if cfg!(target_os = "linux") {
        let output = match Command::new("hostname").args(&["-I"]).output() {
            Ok(ok) => ok,
            Err(_) => {
                return String::from("127.0.0.1");
            }
        };

        let stdout = match String::from_utf8(output.stdout) {
            Ok(ok) => ok,
            Err(_) => {
                return String::from("127.0.0.1");
            }
        };

        let ips: Vec<&str> = stdout.trim().split(" ").collect::<Vec<&str>>();
        let first = ips.first();
        return first.map(|s| s.to_string()).unwrap_or("127.0.0.1".to_string());
    } else {
        // Mac can only deploy on local.
        return String::from("127.0.0.1");
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_local_ip() {
        assert!(!get_local_ip().is_empty());
    }
}
