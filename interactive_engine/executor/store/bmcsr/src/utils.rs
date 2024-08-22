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

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use csv::{ReaderBuilder, StringRecord};
use rust_htslib::bgzf::Reader as GzReader;

use crate::columns::{DataType, Item};
use crate::error::GDBResult;
use crate::graph::IndexType;
use crate::graph_loader::get_files_list;
use crate::ldbc_parser::LDBCVertexParser;
use crate::types::LabelId;

pub struct Iter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a + Send>,
}

impl<'a, T> Iter<'a, T> {
    pub fn from_iter<I: Iterator<Item = T> + 'a + Send>(iter: I) -> Self {
        Iter { inner: Box::new(iter) }
    }

    pub fn from_iter_box(iter: Box<dyn Iterator<Item = T> + 'a + Send>) -> Self {
        Iter { inner: iter }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.inner.count()
    }
}

unsafe impl<'a, T> Send for Iter<'a, T> {}

pub struct Range<I: IndexType> {
    begin: I,
    end: I,
}

pub struct RangeIterator<I: IndexType> {
    cur: I,
    end: I,
}

impl<I: IndexType> Iterator for RangeIterator<I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == self.end {
            None
        } else {
            let ret = self.cur.clone();
            self.cur += I::new(1);
            Some(ret)
        }
    }
}

impl<I: IndexType> Range<I> {
    pub fn new(begin: I, end: I) -> Self {
        Range { begin, end }
    }

    pub fn into_iter(self) -> RangeIterator<I> {
        RangeIterator { cur: self.begin.clone(), end: self.end.clone() }
    }
}

pub struct LabeledIterator<L: Copy + Send, I: Iterator + Send> {
    labels: Vec<L>,
    iterators: Vec<I>,
    cur: usize,
}

impl<L: Copy + Send, I: Iterator + Send> LabeledIterator<L, I> {
    pub fn new(labels: Vec<L>, iterators: Vec<I>) -> Self {
        Self { labels, iterators, cur: 0 }
    }
}

impl<L: Copy + Send, I: Iterator + Send> Iterator for LabeledIterator<L, I> {
    type Item = (L, I::Item);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur == self.labels.len() {
                return None;
            }
            if let Some(item) = self.iterators[self.cur].next() {
                return Some((self.labels[self.cur], item));
            } else {
                self.cur += 1;
            }
        }
    }
}

unsafe impl<L: Copy + Send, I: Iterator + Send> Send for LabeledIterator<L, I> {}

pub struct LabeledRangeIterator<L: Copy + Send, I: Copy + Send + IndexType> {
    labels: Vec<L>,
    iterators: Vec<RangeIterator<I>>,
    cur: usize,
}

impl<L: Copy + Send, I: Copy + Send + IndexType> LabeledRangeIterator<L, I> {
    pub fn new(labels: Vec<L>, iterators: Vec<RangeIterator<I>>) -> Self {
        Self { labels, iterators, cur: 0 }
    }
}

impl<L: Copy + Send, I: Copy + Send + IndexType> Iterator for LabeledRangeIterator<L, I> {
    type Item = (L, I);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur == self.labels.len() {
                return None;
            }
            if let Some(item) = self.iterators[self.cur].next() {
                return Some((self.labels[self.cur], item));
            } else {
                self.cur += 1;
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut remaining = n;
        while remaining != 0 {
            if self.cur == self.labels.len() {
                return None;
            }
            let cur_remaining = self.iterators[self.cur].end.index() - self.iterators[self.cur].cur.index();
            let cur_cur = self.iterators[self.cur].cur.index();
            if cur_remaining >= remaining {
                self.iterators[self.cur].cur = I::new(cur_cur + remaining);
                return self.next();
            } else {
                remaining -= cur_remaining;
                self.cur += 1;
            }
        }
        None
    }
}

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
                    properties.push(Item::Date(crate::date::parse_date(val)?));
                }
                DataType::DateTime => {
                    properties.push(Item::DateTime(crate::date_time::parse_datetime(val)));
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
