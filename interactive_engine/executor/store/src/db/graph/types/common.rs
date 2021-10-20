use std::sync::Arc;

use crate::db::common::concurrency::volatile::Volatile;
use crate::db::api::{SnapshotId, GraphResult};
use super::super::codec::*;
use super::super::table_manager::*;

pub const INFINITE_SI: SnapshotId = SnapshotId::max_value();

#[derive(Clone)]
pub struct TypeCommon {
    table_manager: Arc<TableManager>,
    codec_manager: Arc<CodecManager>,
}

impl TypeCommon {
    pub fn new() -> Self {
        TypeCommon {
            table_manager: Arc::new(TableManager::new()),
            codec_manager: Arc::new(CodecManager::new()),
        }
    }

    pub fn init_with_codec_manager(codec_manager: Arc<CodecManager>) -> Self {
        TypeCommon {
            table_manager: Arc::new(TableManager::new()),
            codec_manager,
        }
    }

    pub fn get_decoder(&self, si: SnapshotId, version: CodecVersion) -> GraphResult<Decoder> {
        self.codec_manager.get_decoder(si, version)
    }

    pub fn get_encoder(&self, si: SnapshotId) -> GraphResult<Encoder> {
        self.codec_manager.get_encoder(si)
    }

    pub fn get_table(&self, si: SnapshotId) -> Option<Table> {
        self.table_manager.get(si)
    }

    pub fn online_table(&self, table: Table) -> GraphResult<()> {
        res_unwrap!(self.table_manager.add(table.start_si, table.id), online_table, table)
    }

    pub fn update_codec(&self, si: SnapshotId, codec: Codec) -> GraphResult<()> {
        self.codec_manager.add_codec(si, codec)
    }

    pub fn gc(&self, _si: SnapshotId) {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct LifeTime {
    start_si: Volatile<SnapshotId>,
    end_si: Volatile<SnapshotId>,
}

#[allow(dead_code)]
impl LifeTime {
    pub fn new(start_si: SnapshotId) -> Self {
        LifeTime {
            start_si: Volatile::new(start_si),
            end_si: Volatile::new(INFINITE_SI),
        }
    }

    pub fn is_alive_at(&self, si: SnapshotId) -> bool {
        self.start_si.get() <= si && self.end_si.get() > si
    }

    pub fn get_start(&self) -> SnapshotId {
        self.start_si.get()
    }

    pub fn get_end(&self) -> SnapshotId {
        self.end_si.get()
    }

    pub fn set_end(&self, si: SnapshotId) {
        if self.end_si.get() > si {
            self.end_si.set(si);
        }
    }
}