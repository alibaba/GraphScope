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

use pegasus_common::io::{ReadExt, WriteExt};

use crate::config::*;

pub(crate) mod block;
mod nonblock;

pub const PASS_PHRASE: u32 = 9;

pub fn get_handshake(server_id: u64, hb: u32) -> u128 {
    let mut value = (PASS_PHRASE as u128) << 96;
    let server_id = server_id as u128;
    value |= server_id << 32;
    value |= hb as u128;
    value
}

#[inline]
fn check_handshake(value: u128) -> Option<(u64, u32)> {
    if (value >> 96) as u32 == PASS_PHRASE {
        let mask = (1u128 << 96) - 1;
        let server_id = ((value & mask) >> 32) as u64;
        let mask = (1u128 << 32) - 1;
        let hb = (value & mask) as u32;
        Some((server_id, hb))
    } else {
        None
    }
}

#[inline]
fn check_connection<R: ReadExt>(conn: &mut R) -> std::io::Result<Option<(u64, u32)>> {
    let handshake = conn.read_u128()?;
    Ok(check_handshake(handshake))
}

#[inline]
fn setup_connection<W: WriteExt>(server_id: u64, hb_sec: u32, conn: &mut W) -> std::io::Result<()> {
    let handshake = get_handshake(server_id, hb_sec);
    conn.write_u128(handshake)
}

#[cfg(test)]
mod test {
    use super::*;

    fn handshake(server_id: u64) {
        let value = get_handshake(server_id, 5);
        assert_eq!(Some((server_id, 5)), check_handshake(value), "error handshake on {}", server_id);
    }

    #[test]
    fn hand_shake_rw_test() {
        for i in 0u64..65536 {
            handshake(i);
        }
        handshake(!0);
    }
}
