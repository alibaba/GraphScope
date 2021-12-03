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

#![allow(non_snake_case)]

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};
use std::{str, mem};
use maxgraph_store::db::graph::store::{GraphStore, GraphBackupEngine};
use crate::store::graph::GraphHandle;
use crate::store::jna_response::JnaResponse;
use maxgraph_store::db::api::multi_version_graph::{MultiVersionGraph, GraphBackup};

pub type GraphBackupHandle = *const c_void;

#[no_mangle]
pub extern fn openGraphBackupEngine(handle: GraphHandle, backup_path: *const c_char) -> GraphBackupHandle {
    unsafe {
        let graph_store = &*(handle as *const GraphStore);
        let slice = CStr::from_ptr(backup_path).to_bytes();
        let backup_path_str = str::from_utf8(slice).unwrap();
        let graph_be = graph_store.open_backup_engine(backup_path_str).unwrap();
        Box::into_raw(graph_be) as GraphBackupHandle
    }
}

#[no_mangle]
pub extern fn closeGraphBackupEngine(handle: GraphBackupHandle) {
    let ptr = handle as *mut GraphBackupEngine;
    unsafe {
        Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern fn createNewBackup(handle: GraphBackupHandle) -> Box<JnaResponse> {
    unsafe {
        let graph_be = &mut *(handle as *mut GraphBackupEngine);
        match graph_be.create_new_backup() {
            Ok(id) => {
                let mut response = JnaResponse::new_success();
                if let Err(e) = response.data(id.to_ne_bytes().to_vec()) {
                    response.success(false);
                    let msg = format!("{:?}", e);
                    response.err_msg(&msg);
                }
                response
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            }
        }
    }
}

#[no_mangle]
pub extern fn deleteBackup(handle: GraphBackupHandle, backup_id: i32) -> Box<JnaResponse> {
    unsafe {
        let graph_be = &mut *(handle as *mut GraphBackupEngine);
        match graph_be.delete_backup(backup_id) {
            Ok(_) => {
                JnaResponse::new_success()
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            }
        }
    }
}

#[no_mangle]
pub extern fn restoreFromBackup(handle: GraphBackupHandle, restore_path: *const c_char, backup_id: i32) -> Box<JnaResponse> {
    unsafe {
        let graph_be = &mut *(handle as *mut GraphBackupEngine);
        let slice = CStr::from_ptr(restore_path).to_bytes();
        let restore_path_str = str::from_utf8(slice).unwrap();
        match graph_be.restore_from_backup(restore_path_str, backup_id) {
            Ok(_) => {
                JnaResponse::new_success()
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            }
        }
    }
}

#[no_mangle]
pub extern fn verifyBackup(handle: GraphBackupHandle, backup_id: i32) -> Box<JnaResponse> {
    unsafe {
        let graph_be = &*(handle as *const GraphBackupEngine);
        match graph_be.verify_backup(backup_id) {
            Ok(_) => {
                JnaResponse::new_success()
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            }
        }
    }
}

#[no_mangle]
pub extern fn getBackupList(handle: GraphBackupHandle) -> Box<JnaResponse> {
    unsafe {
        let graph_be = &*(handle as *const GraphBackupEngine);
        let mut backup_id_list = graph_be.get_backup_list();
        let ratio = mem::size_of::<u32>() / mem::size_of::<u8>();
        let length = backup_id_list.len() * ratio;
        let capacity = backup_id_list.capacity() * ratio;
        let ptr = backup_id_list.as_mut_ptr() as *mut u8;
        mem::forget(backup_id_list);
        let mut response = JnaResponse::new_success();
        if let Err(e) = response.data(Vec::from_raw_parts(ptr, length, capacity)) {
            response.success(false);
            let msg = format!("{:?}", e);
            response.err_msg(&msg);
        }
        response
    }
}