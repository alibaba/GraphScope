use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use bmcsr::bmscsr::BatchMutableSingleCsr;
use bmcsr::col_table::ColTable;
use bmcsr::columns::{DataType, Item};
use bmcsr::csr::CsrTrait;
use bmcsr::graph::{Direction, IndexType};
use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::GraphModifier;
use bmcsr::schema::{CsrGraphSchema, InputSchema, Schema};
use bmcsr::types::{DefaultId, LabelId};
use graph_index::types::{ColumnData, ColumnDataRef, ColumnMappings, DataSource, Input};
use graph_index::GraphIndex;
use itertools::max;
use num::complex::ComplexFloat;

fn properties_to_items<G, I>(properties: Vec<ColumnData>) -> Vec<Vec<Item>>
where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let properties_len = if properties.len() > 0 { properties[0].len() } else { 0 };
    let mut properties_items = Vec::with_capacity(properties_len);
    for i in 0..properties_len {
        let mut property = vec![];
        for data in properties.iter() {
            property.push(data.get_item(i));
        }
        properties_items.push(property);
    }
    properties_items
}

pub fn delete_vertices_by_ids<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, global_ids: &Vec<G>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = HashSet::new();
    for v in global_ids.iter() {
        if v.index() as u64 == u64::MAX {
            continue;
        }
        if let Some(internal_id) = graph.vertex_map.get_internal_id(*v) {
            lids.insert(internal_id.1);
        }
    }
    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for e_label_i in 0..edge_label_num {
        for src_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(src_label_i as LabelId, e_label_i as LabelId, vertex_label as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                src_label_i as LabelId,
                vertex_label as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut ie_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(ie_list) = ie_csr.get_edges(*v) {
                    for e in ie_list {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
            ie_csr.delete_vertices(&lids);
            if let Some(table) = oe_prop.as_mut() {
                oe_csr.parallel_delete_edges_with_props(&ie_to_delete, false, table, parallel);
            } else {
                oe_csr.parallel_delete_edges(&ie_to_delete, false, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
        for dst_label_i in 0..vertex_label_num {
            if graph
                .graph_schema
                .get_edge_header(vertex_label as LabelId, e_label_i as LabelId, dst_label_i as LabelId)
                .is_none()
            {
                continue;
            }
            let index = graph.edge_label_to_index(
                vertex_label as LabelId,
                dst_label_i as LabelId,
                e_label_i as LabelId,
                Direction::Outgoing,
            );
            let mut ie_csr =
                std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
            let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
            let mut oe_csr =
                std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
            let mut oe_prop = graph.oe_edge_prop_table.remove(&index);
            let mut oe_to_delete = Vec::new();
            for v in lids.iter() {
                if let Some(oe_list) = oe_csr.get_edges(*v) {
                    for e in oe_list {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
            oe_csr.delete_vertices(&lids);
            if let Some(table) = ie_prop.as_mut() {
                ie_csr.parallel_delete_edges_with_props(&oe_to_delete, true, table, parallel);
            } else {
                ie_csr.parallel_delete_edges(&oe_to_delete, true, parallel);
            }
            graph.ie[index] = ie_csr;
            if let Some(table) = ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = oe_csr;
            if let Some(table) = oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    // delete vertices
    for v in lids.iter() {
        graph.vertex_map.remove_vertex(vertex_label, v);
    }
}

pub fn delete_edges_by_ids<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    global_ids: Vec<(G, G)>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut lids = vec![];
    for (src_gid, dst_gid) in global_ids.iter() {
        lids.push((graph.get_internal_id(*src_gid), graph.get_internal_id(*dst_gid)));
    }
    let index = graph.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
    let mut ie_csr = std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new()));
    let mut ie_prop = graph.ie_edge_prop_table.remove(&index);
    let mut oe_csr = std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new()));
    let mut oe_prop = graph.oe_edge_prop_table.remove(&index);

    // delete edges
    if let Some(table) = oe_prop.as_mut() {
        oe_csr.parallel_delete_edges_with_props(&lids, false, table, parallel);
    }
    if let Some(table) = ie_prop.as_mut() {
        ie_csr.parallel_delete_edges_with_props(&lids, false, table, parallel);
    }

    // apply delete edges
    graph.ie[index] = ie_csr;
    if let Some(table) = ie_prop {
        graph.ie_edge_prop_table.insert(index, table);
    }
    graph.oe[index] = oe_csr;
    if let Some(table) = oe_prop {
        graph.oe_edge_prop_table.insert(index, table);
    }
}

pub fn insert_vertices<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, input: &Input, column_mappings: &Vec<ColumnMappings>,
    parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(vertex_header) = graph
                    .graph_schema
                    .get_vertex_header(vertex_label)
                {
                    for (i, (property_name, data_type)) in vertex_header.iter().enumerate() {
                        if let Some((column_index, column_data_type)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("vertex label {} not found", vertex_label)
                }
                modifier
                    .apply_vertices_insert_with_filename(graph, vertex_label, &filenames, id_col, &mappings)
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn insert_edges<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input: &Input,
    src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut column_map = HashMap::new();
    let mut max_col = 0;
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
        if column_index >= max_col {
            max_col = column_index + 1;
        }
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                let mut mappings = vec![-1; max_col as usize];
                if let Some(edge_header) = graph
                    .graph_schema
                    .get_edge_header(src_label, edge_label, dst_label)
                {
                    for (i, (property_name, _)) in edge_header.iter().enumerate() {
                        if let Some((column_index, _)) = column_map.get(property_name) {
                            mappings[*column_index as usize] = i as i32;
                        }
                    }
                } else {
                    panic!("edge label {}_{}_{} not found", src_label, edge_label, dst_label)
                }
                modifier
                    .apply_edges_insert_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                        &mappings,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn delete_vertices(
    graph: &mut GraphDB<usize, usize>, vertex_label: LabelId, input: &Input,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);
                modifier
                    .apply_vertices_delete_with_filename(graph, vertex_label, &filenames, id_col)
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                let data = memory_data.columns();
                let vertex_id_column = data
                    .get(id_col as usize)
                    .expect("Failed to get id column");
                if let ColumnDataRef::UInt64Array(data) = vertex_id_column.data().as_ref() {
                    let data = data.iter().map(|&x| x as usize).collect();
                    delete_vertices_by_ids(graph, vertex_label, &data, parallel);
                }
            }
        }
    }
}

pub fn delete_edges(
    graph: &mut GraphDB<usize, usize>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    input: &Input, src_vertex_mappings: &Vec<ColumnMappings>, dst_vertex_mappings: &Vec<ColumnMappings>,
    column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in src_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("src_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in dst_vertex_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        if property_name == "id" {
            column_map.insert("dst_id".to_string(), (column_index, data_type));
        }
    }
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut src_id_col = -1;
    let mut dst_id_col = -1;
    if let Some((column_index, _)) = column_map.get("src_id") {
        src_id_col = *column_index;
    }
    if let Some((column_index, _)) = column_map.get("dst_id") {
        dst_id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            if let Some(file_input) = input.file_input() {
                let file_location = &file_input.location;
                let path = Path::new(file_location);
                let input_dir = path
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_str()
                    .unwrap()
                    .to_string();
                let filename = path
                    .file_name()
                    .expect("Can not find filename")
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                let filenames = vec![filename];
                let mut modifier = GraphModifier::new(input_dir);
                if file_input.header_row {
                    modifier.skip_header();
                }
                modifier.parallel(parallel);

                modifier
                    .apply_edges_delete_with_filename(
                        graph, src_label, edge_label, dst_label, &filenames, src_id_col, dst_id_col,
                    )
                    .unwrap();
            }
        }
        DataSource::Memory => {
            if let Some(memory_data) = input.memory_data() {
                todo!()
            }
        }
    }
}

pub fn insert_edges_by_schema<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    input_dir: String, filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32, mappings: &Vec<i32>,
    parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut modifier = GraphModifier::new(input_dir);
    modifier.skip_header();
    modifier.parallel(parallel);
    modifier
        .apply_edges_insert_with_filename(
            graph, src_label, edge_label, dst_label, filenames, src_id_col, dst_id_col, mappings,
        )
        .unwrap();
}

pub fn delete_vertices_by_schema<G, I>(
    graph: &mut GraphDB<G, I>, vertex_label: LabelId, input_dir: String, filenames: &Vec<String>,
    id_col: i32, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut modifier = GraphModifier::new(input_dir);
    modifier.skip_header();
    modifier.parallel(parallel);
    modifier
        .apply_vertices_delete_with_filename(graph, vertex_label, filenames, id_col)
        .unwrap();
}

pub fn delete_edges_by_schema<G, I>(
    graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    input_dir: String, filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32, parallel: u32,
) where
    I: Send + Sync + IndexType,
    G: FromStr + Send + Sync + IndexType + Eq,
{
    let mut modifier = GraphModifier::new(input_dir);
    modifier.skip_header();
    modifier.parallel(parallel);
    modifier
        .apply_edges_delete_with_filename(
            graph, src_label, edge_label, dst_label, filenames, src_id_col, dst_id_col,
        )
        .unwrap();
}

pub fn set_vertices(
    graph: &mut GraphDB<usize, usize>, graph_index: &mut GraphIndex, vertex_label: LabelId,
    mut input: Input, column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let property_size = graph.get_vertices_num(vertex_label);
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    let mut id_col = -1;
    if let Some((column_index, _)) = column_map.get("id") {
        id_col = *column_index;
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                let id_column = column_data
                    .get_mut(id_col as usize)
                    .expect("Failed to find id column");
                let global_ids = match id_column.take_data() {
                    ColumnData::VertexIdArray(data) => data,
                    ColumnData::UInt64Array(mut data) => {
                        let mut lid = vec![];
                        for i in data.drain(..) {
                            lid.push(graph.get_internal_id(i as usize));
                        }
                        lid
                    }
                    _ => panic!("DataType of id col is not VertexId"),
                };
                for (k, v) in column_map.iter() {
                    if k == "id" {
                        continue;
                    }
                    let column_index = v.0;
                    let column_data_type = v.1;
                    if !graph_index.has_vertex_index(k.clone(), vertex_label, property_size) {
                        graph_index.init_vertex_index(
                            k.clone(),
                            vertex_label,
                            column_data_type,
                            Some(property_size),
                            Some(graph_index::types::Item::Int32(0)),
                        );
                    }
                    let column = column_data
                        .get(column_index as usize)
                        .expect("Failed to find column");
                    graph_index
                        .add_vertex_index_batch(vertex_label, k, &global_ids, column.data().as_ref())
                        .unwrap();
                }
            }
        }
    }
}

pub fn set_edges(
    graph: &mut GraphDB<usize, usize>, graph_index: &mut GraphIndex, src_label: LabelId,
    edge_label: LabelId, dst_label: LabelId, mut input: Input, src_vertex_mappings: &Vec<ColumnMappings>,
    dst_vertex_mappings: &Vec<ColumnMappings>, column_mappings: &Vec<ColumnMappings>, parallel: u32,
) {
    let mut column_map = HashMap::new();
    for column_mapping in column_mappings {
        let column = column_mapping.column();
        let column_index = column.index();
        let data_type = column.data_type();
        let property_name = column_mapping.property_name();
        column_map.insert(property_name.clone(), (column_index, data_type));
    }
    match input.data_source() {
        DataSource::File => {
            todo!()
        }
        DataSource::Memory => {
            if let Some(mut memory_data) = input.take_memory_data() {
                let mut column_data = memory_data.take_columns();
                if !src_vertex_mappings.is_empty() {
                    let offset_col_id = src_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let offsets = match offset_column.take_data() {
                        ColumnData::VertexIdArray(mut data) => data,
                        _ => panic!("DataType of id col is not VertexId"),
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        let oe_property_size = graph.get_max_edge_offset(
                            src_label,
                            edge_label,
                            dst_label,
                            Direction::Outgoing,
                        );
                        if !graph_index.has_outgoing_edge_index(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            oe_property_size,
                        ) {
                            graph_index
                                .init_outgoing_edge_index(
                                    k.clone(),
                                    src_label,
                                    dst_label,
                                    edge_label,
                                    column_data_type,
                                    Some(oe_property_size),
                                    Some(graph_index::types::Item::Int32(0)),
                                )
                                .unwrap();
                        }
                        let column = column_data
                            .get(column_index as usize)
                            .expect("Failed to find column");
                        graph_index
                            .add_outgoing_edge_index_batch(
                                src_label,
                                edge_label,
                                dst_label,
                                k,
                                &offsets,
                                column.data().as_ref(),
                            )
                            .unwrap();
                    }
                }
                if !dst_vertex_mappings.is_empty() {
                    let offset_col_id = dst_vertex_mappings[0].column().index();
                    let offset_column = column_data
                        .get_mut(offset_col_id as usize)
                        .expect("Failed to find id column");
                    let offsets = match offset_column.take_data() {
                        ColumnData::VertexIdArray(mut data) => data,
                        _ => panic!("DataType of id col is not VertexId"),
                    };
                    for (k, v) in column_map.iter() {
                        let column_index = v.0;
                        let column_data_type = v.1;
                        let ie_property_size = graph.get_max_edge_offset(
                            src_label,
                            edge_label,
                            dst_label,
                            Direction::Incoming,
                        );
                        if !graph_index.has_incoming_edge_index(
                            k.clone(),
                            src_label,
                            edge_label,
                            dst_label,
                            ie_property_size,
                        ) {
                            graph_index
                                .init_incoming_edge_index(
                                    k.clone(),
                                    src_label,
                                    dst_label,
                                    edge_label,
                                    column_data_type,
                                    Some(ie_property_size),
                                    Some(graph_index::types::Item::Int32(0)),
                                )
                                .unwrap();
                        }
                        let column = column_data
                            .get(column_index as usize)
                            .expect("Failed to find column");
                        graph_index
                            .add_incoming_edge_index_batch(
                                src_label,
                                edge_label,
                                dst_label,
                                k,
                                &offsets,
                                column.data().as_ref(),
                            )
                            .unwrap();
                    }
                }
            }
        }
    }
}
