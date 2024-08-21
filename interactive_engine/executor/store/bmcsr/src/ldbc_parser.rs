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

use std::marker::PhantomData;
use std::str::FromStr;

use abomonation_derive::Abomonation;
use csv::StringRecord;

use crate::graph::IndexType;
use crate::types::*;

/// The vertex's meta data including global_id and label_id
#[derive(Abomonation, PartialEq, Clone, Debug)]
pub struct VertexMeta<G> {
    pub global_id: G,
    pub label: LabelId,
}

/// The edge's meta data after parsing from the csv file.
#[derive(Abomonation, PartialEq, Clone, Debug)]
pub struct EdgeMeta<G> {
    pub src_global_id: G,
    pub src_label_id: LabelId,
    pub dst_global_id: G,
    pub dst_label_id: LabelId,
    pub label_id: LabelId,
}

/// Define parsing a LDBC vertex
#[derive(Clone)]
pub struct LDBCVertexParser<G = DefaultId> {
    vertex_type: LabelId,
    id_index: usize,
    ph: PhantomData<G>,
}

pub const LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

impl<G: IndexType> LDBCVertexParser<G> {
    pub fn to_global_id(ldbc_id: usize, label_id: LabelId) -> G {
        let global_id: usize = ((label_id as usize) << LABEL_SHIFT_BITS) | ldbc_id;
        G::new(global_id)
    }

    pub fn encode_local_id(local_id: usize, label_id: LabelId) -> G {
        let encode_id: usize = ((label_id as usize) << LABEL_SHIFT_BITS) | local_id;
        G::new(encode_id)
    }

    pub fn decode_local_id(encoded_id: usize) -> (LabelId, G) {
        let label_id = (encoded_id >> LABEL_SHIFT_BITS) as LabelId;
        let local_id: usize = ((1_usize << LABEL_SHIFT_BITS) - 1) & encoded_id.index();
        return (label_id, G::new(local_id));
    }

    pub fn get_label_id(global_id: G) -> LabelId {
        (global_id.index() >> LABEL_SHIFT_BITS) as LabelId
    }

    pub fn get_original_id(global_id: G) -> G {
        let mask = (1_usize << LABEL_SHIFT_BITS) - 1;
        G::new(global_id.index() & mask)
    }
}

impl<G: FromStr + PartialEq + Default + IndexType> LDBCVertexParser<G> {
    pub fn new(vertex_type: LabelId, id_index: usize) -> Self {
        Self { vertex_type, id_index, ph: PhantomData }
    }

    pub fn parse_vertex_meta(&self, record: &StringRecord) -> VertexMeta<G> {
        let global_id = Self::to_global_id(
            record
                .get(self.id_index)
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            self.vertex_type,
        );
        VertexMeta { global_id, label: self.vertex_type }
    }
}

/// Define parsing a LDBC edge
#[derive(Clone)]
pub struct LDBCEdgeParser<G = DefaultId> {
    src_vertex_type: LabelId,
    dst_vertex_type: LabelId,
    edge_type: LabelId,
    src_col_id: usize,
    dst_col_id: usize,
    ph: PhantomData<G>,
}

impl<G: FromStr + PartialEq + Default + IndexType> LDBCEdgeParser<G> {
    pub fn new(src_vertex_type: LabelId, dst_vertex_type: LabelId, edge_type: LabelId) -> Self {
        Self { src_vertex_type, dst_vertex_type, edge_type, src_col_id: 0, dst_col_id: 1, ph: PhantomData }
    }

    pub fn with_endpoint_col_id(&mut self, src_col_id: usize, dst_col_id: usize) {
        self.src_col_id = src_col_id;
        self.dst_col_id = dst_col_id;
    }

    pub fn parse_edge_meta(&self, record: &StringRecord) -> EdgeMeta<G> {
        let src_global_id = LDBCVertexParser::to_global_id(
            record
                .get(self.src_col_id)
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            self.src_vertex_type,
        );
        let dst_global_id = LDBCVertexParser::to_global_id(
            record
                .get(self.dst_col_id)
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            self.dst_vertex_type,
        );
        EdgeMeta {
            src_global_id,
            src_label_id: self.src_vertex_type,
            dst_global_id,
            dst_label_id: self.dst_vertex_type,
            label_id: self.edge_type,
        }
    }
}
