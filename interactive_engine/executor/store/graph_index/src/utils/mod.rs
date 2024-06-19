use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use bmcsr::columns::{DataType, Item};
use bmcsr::error::GDBResult;
use bmcsr::graph_loader::get_files_list;
use bmcsr::ldbc_parser::LDBCVertexParser;
use csv::{ReaderBuilder, StringRecord};
use rust_htslib::bgzf::Reader as GzReader;

use crate::types::{ColumnData, LabelId};

pub fn get_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}

pub fn get_2d_partition(id_hi: u64, id_low: u64, workers: usize, num_servers: usize) -> u64 {
    let server_id = id_hi % num_servers as u64;
    let worker_id = id_low % workers as u64;
    server_id * workers as u64 + worker_id
}

fn create_array_data(headers: &[(String, DataType)]) -> Vec<ColumnData> {
    let mut array_datas = vec![];
    for (_, data_type) in headers {
        match data_type {
            DataType::Int32 => array_datas.push(ColumnData::Int32Array(vec![])),
            DataType::UInt64 => array_datas.push(ColumnData::UInt64Array(vec![])),
            _ => panic!("Unsupport data type"),
        }
    }
    array_datas
}

pub fn parse_vertex_id_from_file(
    vertex_label: LabelId, id_col: i32, file_locations: Vec<String>, skip_header: bool, delim: u8, id: u32,
    parallel: u32,
) -> Vec<u64> {
    let mut id_list = vec![];
    for file_location in file_locations {
        let path = Path::new(&file_location);
        let input_dir = path
            .parent()
            .unwrap_or(Path::new(""))
            .to_path_buf();
        let filename = vec![path
            .file_name()
            .expect("Can not find filename")
            .to_str()
            .unwrap_or("")
            .to_string()];
        let parser = LDBCVertexParser::<usize>::new(vertex_label, id_col as usize);
        let files = get_files_list(&input_dir, &filename).unwrap();
        for file in files.iter() {
            if let Some(path_str) = file.clone().to_str() {
                if path_str.ends_with(".csv.gz") {
                    if let Ok(gz_reader) = GzReader::from_path(&path_str) {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(skip_header)
                            .from_reader(gz_reader);
                        for (index, result) in rdr.records().enumerate() {
                            if index % parallel as usize == id as usize {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    id_list.push(vertex_meta.global_id as u64);
                                }
                            }
                        }
                    }
                } else if file.ends_with(".csv") {
                    if let Ok(file) = File::open(&file) {
                        let reader = BufReader::new(file);
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(skip_header)
                            .from_reader(reader);
                        for (index, result) in rdr.records().enumerate() {
                            if index % parallel as usize == id as usize {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    id_list.push(vertex_meta.global_id as u64);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    id_list
}

pub fn parse_properties(
    record: &StringRecord, header: &[(String, DataType)], selected: &[i32],
) -> GDBResult<Vec<Item>> {
    let mut properties = Vec::new();
    for (index, val) in record.iter().enumerate() {
        if selected[index] > 0 {
            match header[index].1 {
                DataType::Int32 => {
                    properties.push(Item::Int32(val.parse::<i32>()?));
                }
                DataType::UInt32 => {
                    properties.push(Item::UInt32(val.parse::<u32>()?));
                }
                DataType::Int64 => {
                    properties.push(Item::Int64(val.parse::<i64>()?));
                }
                DataType::UInt64 => {
                    properties.push(Item::UInt64(val.parse::<u64>()?));
                }
                DataType::String => {
                    properties.push(Item::String(val.to_string()));
                }
                DataType::Date => {
                    properties.push(Item::Date(bmcsr::date::parse_date(val)?));
                }
                DataType::DateTime => {
                    properties.push(Item::DateTime(bmcsr::date_time::parse_datetime(val)));
                }
                DataType::Double => {
                    properties.push(Item::Double(val.parse::<f64>()?));
                }
                DataType::NULL => {
                    error!("Unexpected field type");
                }
                DataType::ID => {}
                DataType::LCString => {
                    properties.push(Item::String(val.to_string()));
                }
            }
        }
    }
    Ok(properties)
}
