use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use csv::ReaderBuilder;
use rayon::prelude::*;
use rust_htslib::bgzf::Reader as GzReader;

use crate::bmscsr::BatchMutableSingleCsr;
use crate::col_table::{parse_properties, ColTable, parse_properties_by_mappings};
use crate::columns::DataType;
use crate::columns::{Column, StringColumn};
use crate::csr::CsrTrait;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::graph_loader::{get_files_list, get_files_list_beta};
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, LabelId};

fn process_csv_rows<F>(path: &PathBuf, mut process_row: F, skip_header: bool, delim: u8)
    where
        F: FnMut(&csv::StringRecord),
{
    if let Some(path_str) = path.clone().to_str() {
        if path_str.ends_with(".csv.gz") {
            if let Ok(gz_reader) = GzReader::from_path(&path) {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(gz_reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        } else if path_str.ends_with(".csv") {
            if let Ok(file) = File::open(&path) {
                let reader = BufReader::new(file);
                let mut rdr = ReaderBuilder::new()
                    .delimiter(delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(skip_header)
                    .from_reader(reader);
                for result in rdr.records() {
                    if let Ok(record) = result {
                        process_row(&record);
                    }
                }
            }
        }
    }
}

pub struct DeleteGenerator<G: FromStr + Send + Sync + IndexType + std::fmt::Display = DefaultId> {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,

    persons: Vec<(String, G)>,
    comments: Vec<(String, G)>,
    posts: Vec<(String, G)>,
    forums: Vec<(String, G)>,

    person_set: HashSet<G>,
    comment_set: HashSet<G>,
    post_set: HashSet<G>,
    forum_set: HashSet<G>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq + std::fmt::Display> DeleteGenerator<G> {
    pub fn new(input_dir: &PathBuf) -> DeleteGenerator<G> {
        Self {
            input_dir: input_dir.clone(),
            delim: b'|',
            skip_header: false,

            persons: vec![],
            comments: vec![],
            posts: vec![],
            forums: vec![],

            person_set: HashSet::new(),
            comment_set: HashSet::new(),
            post_set: HashSet::new(),
            forum_set: HashSet::new(),
        }
    }

    fn load_vertices(&self, input_prefix: PathBuf, label: LabelId) -> Vec<(String, G)> {
        let mut ret = vec![];

        let suffixes = vec!["*.csv.gz".to_string(), "*.csv".to_string()];
        let files = get_files_list(&input_prefix, &suffixes);
        if files.is_err() {
            warn!(
                "Get vertex files {:?}/{:?} failed: {:?}",
                &input_prefix,
                &suffixes,
                files.err().unwrap()
            );
            return ret;
        }
        let files = files.unwrap();
        if files.is_empty() {
            return ret;
        }
        let parser = LDBCVertexParser::<G>::new(label, 1);
        for file in files {
            process_csv_rows(
                &file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    ret.push((
                        record
                            .get(0)
                            .unwrap()
                            .parse::<String>()
                            .unwrap(),
                        vertex_meta.global_id,
                    ));
                },
                self.skip_header,
                self.delim,
            );
        }

        ret
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn iterate_persons<I>(&mut self, graph: &GraphDB<G, I>)
        where
            I: Send + Sync + IndexType,
    {
        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();

        let hasCreator_label = graph
            .graph_schema
            .get_edge_label_id("HASCREATOR")
            .unwrap();
        let hasModerator_label = graph
            .graph_schema
            .get_edge_label_id("HASMODERATOR")
            .unwrap();

        let comment_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, comment_label, Direction::Incoming);
        let post_hasCreator_person =
            graph.get_sub_graph(person_label, hasCreator_label, post_label, Direction::Incoming);
        let forum_hasModerator_person =
            graph.get_sub_graph(person_label, hasModerator_label, forum_label, Direction::Incoming);

        let forum_title_column = graph.vertex_prop_table[forum_label as usize]
            .get_column_by_name("title")
            .as_any()
            .downcast_ref::<StringColumn>()
            .unwrap();

        for (dt, id) in self.persons.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != person_label {
                    warn!("Vertex {} is not a person", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }
                for e in comment_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }

                for e in post_hasCreator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }

                for e in forum_hasModerator_person
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let title = forum_title_column.get(e.index()).unwrap();
                    let title_string = title.to_string();
                    if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                        let oid = graph
                            .vertex_map
                            .get_global_id(forum_label, *e)
                            .unwrap();
                        self.forums.push((dt.clone(), oid));
                    }
                }
            } else {
                warn!("Vertex Person - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_forums<I>(&mut self, graph: &GraphDB<G, I>)
        where
            I: Send + Sync + IndexType,
    {
        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();

        let containerOf_label = graph
            .graph_schema
            .get_edge_label_id("CONTAINEROF")
            .unwrap();

        let forum_containerOf_post =
            graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
        for (dt, id) in self.forums.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != forum_label {
                    warn!("Vertex {} is not a forum", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in forum_containerOf_post
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(post_label, *e)
                        .unwrap();
                    self.posts.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Forum - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_posts<I>(&mut self, graph: &GraphDB<G, I>)
        where
            I: Send + Sync + IndexType,
    {
        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_post =
            graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Incoming);
        for (dt, id) in self.posts.iter() {
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
                if got_label != post_label {
                    warn!("Vertex {} is not a post", LDBCVertexParser::<G>::get_original_id(*id));
                    continue;
                }

                for e in comment_replyOf_post.get_adj_list(lid).unwrap() {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
            } else {
                warn!("Vertex Post - {} does not exist", LDBCVertexParser::<G>::get_original_id(*id));
                continue;
            }
        }
    }

    fn iterate_comments<I>(&mut self, graph: &GraphDB<G, I>)
        where
            I: Send + Sync + IndexType,
    {
        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();

        let replyOf_label = graph
            .graph_schema
            .get_edge_label_id("REPLYOF")
            .unwrap();

        let comment_replyOf_comment =
            graph.get_sub_graph(comment_label, replyOf_label, comment_label, Direction::Incoming);
        let mut index = 0;
        while index < self.comments.len() {
            let (dt, id) = self.comments[index].clone();
            if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(id) {
                if got_label != comment_label {
                    warn!("Vertex {} is not a comment", LDBCVertexParser::<G>::get_original_id(id));
                    index += 1;
                    continue;
                }

                for e in comment_replyOf_comment
                    .get_adj_list(lid)
                    .unwrap()
                {
                    let oid = graph
                        .vertex_map
                        .get_global_id(comment_label, *e)
                        .unwrap();
                    self.comments.push((dt.clone(), oid));
                }
                index += 1;
            } else {
                warn!("Vertex Comment - {} does not exist", LDBCVertexParser::<G>::get_original_id(id));
                index += 1;
                continue;
            }
        }
    }

    pub fn generate<I>(&mut self, graph: &GraphDB<G, I>, batch_id: &str)
        where
            I: Send + Sync + IndexType,
    {
        let output_dir = self
            .input_dir
            .join("extra_deletes")
            .join("dynamic");
        std::fs::create_dir_all(&output_dir).unwrap();

        let prefix = self.input_dir.join("deletes").join("dynamic");

        let person_label = graph
            .graph_schema
            .get_vertex_label_id("PERSON")
            .unwrap();
        self.persons = self.load_vertices(
            prefix
                .clone()
                .join("Person")
                .join(format!("batch_id={}", batch_id)),
            person_label,
        );
        self.person_set = self.persons.iter().map(|(_, id)| *id).collect();

        let comment_label = graph
            .graph_schema
            .get_vertex_label_id("COMMENT")
            .unwrap();
        self.comments = self.load_vertices(
            prefix
                .clone()
                .join("Comment")
                .join(format!("batch_id={}", batch_id)),
            comment_label,
        );
        self.comment_set = self
            .comments
            .iter()
            .map(|(_, id)| *id)
            .collect();

        let post_label = graph
            .graph_schema
            .get_vertex_label_id("POST")
            .unwrap();
        self.posts = self.load_vertices(
            prefix
                .clone()
                .join("Post")
                .join(format!("batch_id={}", batch_id)),
            post_label,
        );
        self.post_set = self.posts.iter().map(|(_, id)| *id).collect();

        let forum_label = graph
            .graph_schema
            .get_vertex_label_id("FORUM")
            .unwrap();
        self.forums = self.load_vertices(
            prefix
                .clone()
                .join("Forum")
                .join(format!("batch_id={}", batch_id)),
            forum_label,
        );
        self.forum_set = self.forums.iter().map(|(_, id)| *id).collect();

        self.iterate_persons(graph);
        self.iterate_forums(graph);
        self.iterate_posts(graph);
        self.iterate_comments(graph);

        let batch_dir = format!("batch_id={}", batch_id);

        let person_dir_path = output_dir
            .clone()
            .join("Person")
            .join(&batch_dir);
        std::fs::create_dir_all(&person_dir_path).unwrap();
        let mut person_file = File::create(person_dir_path.join("part-0.csv")).unwrap();
        writeln!(person_file, "deletionDate|id").unwrap();
        for (dt, id) in self.persons.iter() {
            if !self.person_set.contains(id) {
                self.person_set.insert(*id);
                writeln!(person_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let forum_dir_path = output_dir
            .clone()
            .join("Forum")
            .join(&batch_dir);
        std::fs::create_dir_all(&forum_dir_path).unwrap();
        let mut forum_file = File::create(forum_dir_path.join("part-0.csv")).unwrap();
        writeln!(forum_file, "deletionDate|id").unwrap();
        for (dt, id) in self.forums.iter() {
            if !self.forum_set.contains(id) {
                self.forum_set.insert(*id);
                writeln!(forum_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let post_dir_path = output_dir.clone().join("Post").join(&batch_dir);
        std::fs::create_dir_all(&post_dir_path).unwrap();
        let mut post_file = File::create(post_dir_path.join("part-0.csv")).unwrap();
        writeln!(post_file, "deletionDate|id").unwrap();
        for (dt, id) in self.posts.iter() {
            if !self.post_set.contains(id) {
                self.post_set.insert(*id);
                writeln!(post_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }

        let comment_dir_path = output_dir
            .clone()
            .join("Comment")
            .join(&batch_dir);
        std::fs::create_dir_all(&comment_dir_path).unwrap();
        let mut comment_file = File::create(comment_dir_path.join("part-0.csv")).unwrap();
        writeln!(comment_file, "deletionDate|id").unwrap();
        for (dt, id) in self.comments.iter() {
            if !self.comment_set.contains(id) {
                self.comment_set.insert(*id);
                writeln!(comment_file, "{}|{}", dt, LDBCVertexParser::<G>::get_original_id(*id)).unwrap();
            }
        }
    }
}

pub struct GraphModifier {
    input_dir: PathBuf,

    delim: u8,
    skip_header: bool,
    parallel: u32,
}

struct CsrRep<I> {
    src_label: LabelId,
    edge_label: LabelId,
    dst_label: LabelId,

    ie_csr: Box<dyn CsrTrait<I>>,
    ie_prop: Option<ColTable>,
    oe_csr: Box<dyn CsrTrait<I>>,
    oe_prop: Option<ColTable>,
}

impl GraphModifier {
    pub fn new<D: AsRef<Path>>(input_dir: D) -> GraphModifier {
        Self { input_dir: input_dir.as_ref().to_path_buf(), delim: b'|', skip_header: false, parallel: 0 }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    pub fn parallel(&mut self, parallel: u32) {
        self.parallel = parallel;
    }

    fn take_csr<G, I>(
        &self, graph: &mut GraphDB<G, I>, src_label_i: LabelId, dst_label_i: LabelId, e_label_i: LabelId,
    ) -> CsrRep<I>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let index = graph.edge_label_to_index(src_label_i, dst_label_i, e_label_i, Direction::Outgoing);

        CsrRep {
            src_label: src_label_i,
            edge_label: e_label_i,
            dst_label: dst_label_i,

            ie_csr: std::mem::replace(&mut graph.ie[index], Box::new(BatchMutableSingleCsr::new())),
            ie_prop: graph.ie_edge_prop_table.remove(&index),
            oe_csr: std::mem::replace(&mut graph.oe[index], Box::new(BatchMutableSingleCsr::new())),
            oe_prop: graph.oe_edge_prop_table.remove(&index),
        }
    }

    fn take_csrs_with_label<G, I>(&self, graph: &mut GraphDB<G, I>, label: LabelId) -> Vec<CsrRep<I>>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut results = vec![];
        for e_label_i in 0..edge_label_num {
            for label_i in 0..vertex_label_num {
                if !graph
                    .graph_schema
                    .get_edge_header(label as LabelId, e_label_i as LabelId, label_i as LabelId)
                    .is_none()
                {
                    let index = graph.edge_label_to_index(
                        label as LabelId,
                        label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );
                    results.push(CsrRep {
                        src_label: label as LabelId,
                        edge_label: e_label_i as LabelId,
                        dst_label: label_i as LabelId,
                        ie_csr: std::mem::replace(
                            &mut graph.ie[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        ie_prop: graph.ie_edge_prop_table.remove(&index),
                        oe_csr: std::mem::replace(
                            &mut graph.oe[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        oe_prop: graph.oe_edge_prop_table.remove(&index),
                    });
                }
                if !graph
                    .graph_schema
                    .get_edge_header(label_i as LabelId, e_label_i as LabelId, label as LabelId)
                    .is_none()
                {
                    if label_i as LabelId != label {
                        let index = graph.edge_label_to_index(
                            label_i as LabelId,
                            label as LabelId,
                            e_label_i as LabelId,
                            Direction::Outgoing,
                        );
                        results.push(CsrRep {
                            src_label: label_i as LabelId,
                            edge_label: e_label_i as LabelId,
                            dst_label: label as LabelId,
                            ie_csr: std::mem::replace(
                                &mut graph.ie[index],
                                Box::new(BatchMutableSingleCsr::new()),
                            ),
                            ie_prop: graph.ie_edge_prop_table.remove(&index),
                            oe_csr: std::mem::replace(
                                &mut graph.oe[index],
                                Box::new(BatchMutableSingleCsr::new()),
                            ),
                            oe_prop: graph.oe_edge_prop_table.remove(&index),
                        });
                    }
                }
            }
        }
        results
    }
    fn take_csrs<G, I>(&self, graph: &mut GraphDB<G, I>) -> Vec<CsrRep<I>>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
        let mut results = vec![];

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    if graph
                        .graph_schema
                        .get_edge_header(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        )
                        .is_none()
                    {
                        continue;
                    }

                    let index = graph.edge_label_to_index(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                        Direction::Outgoing,
                    );

                    results.push(CsrRep {
                        src_label: src_label_i as LabelId,
                        edge_label: e_label_i as LabelId,
                        dst_label: dst_label_i as LabelId,

                        ie_csr: std::mem::replace(
                            &mut graph.ie[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        ie_prop: graph.ie_edge_prop_table.remove(&index),
                        oe_csr: std::mem::replace(
                            &mut graph.oe[index],
                            Box::new(BatchMutableSingleCsr::new()),
                        ),
                        oe_prop: graph.oe_edge_prop_table.remove(&index),
                    });
                }
            }
        }

        results
    }

    fn set_csr<G, I>(&self, graph: &mut GraphDB<G, I>, reps: CsrRep<I>)
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let index =
            graph.edge_label_to_index(reps.src_label, reps.dst_label, reps.edge_label, Direction::Outgoing);

        graph.ie[index] = reps.ie_csr;
        if let Some(table) = reps.ie_prop {
            graph.ie_edge_prop_table.insert(index, table);
        }
        graph.oe[index] = reps.oe_csr;
        if let Some(table) = reps.oe_prop {
            graph.oe_edge_prop_table.insert(index, table);
        }
    }

    fn set_csrs<G, I>(&self, graph: &mut GraphDB<G, I>, mut reps: Vec<CsrRep<I>>)
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        for result in reps.drain(..) {
            let index = graph.edge_label_to_index(
                result.src_label,
                result.dst_label,
                result.edge_label,
                Direction::Outgoing,
            );

            graph.ie[index] = result.ie_csr;
            if let Some(table) = result.ie_prop {
                graph.ie_edge_prop_table.insert(index, table);
            }
            graph.oe[index] = result.oe_csr;
            if let Some(table) = result.oe_prop {
                graph.oe_edge_prop_table.insert(index, table);
            }
        }
    }

    fn parallel_delete_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, edge_file_strings: &Vec<String>,
        input_header: &[(String, DataType)], delete_sets: &Vec<HashSet<I>>, p: u32,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return ();
        }

        let src_delete_set = &delete_sets[src_label as usize];
        let dst_delete_set = &delete_sets[dst_label as usize];
        let mut delete_edge_set = Vec::new();

        let mut src_col_id = 0;
        let mut dst_col_id = 1;

        for (index, (n, _)) in input_header.iter().enumerate() {
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }

        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return ();
        }

        let edge_files = edge_files.unwrap();
        for edge_file in edge_files.iter() {
            process_csv_rows(
                edge_file,
                |record| {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Some((got_src_label, src_lid)) = graph
                        .vertex_map
                        .get_internal_id(edge_meta.src_global_id)
                    {
                        if let Some((got_dst_label, dst_lid)) = graph
                            .vertex_map
                            .get_internal_id(edge_meta.dst_global_id)
                        {
                            if got_src_label != src_label || got_dst_label != dst_label {
                                return;
                            }
                            if src_delete_set.contains(&src_lid) || dst_delete_set.contains(&dst_lid) {
                                return;
                            }
                            delete_edge_set.push((src_lid, dst_lid));
                        }
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty() {
            return ();
        }

        let mut oe_to_delete = Vec::new();
        let mut ie_to_delete = Vec::new();

        for v in src_delete_set.iter() {
            if let Some(oe_list) = input.oe_csr.get_edges(*v) {
                for e in oe_list {
                    if !dst_delete_set.contains(e) {
                        oe_to_delete.push((*v, *e));
                    }
                }
            }
        }
        for v in dst_delete_set.iter() {
            if let Some(ie_list) = input.ie_csr.get_edges(*v) {
                for e in ie_list {
                    if !src_delete_set.contains(e) {
                        ie_to_delete.push((*e, *v));
                    }
                }
            }
        }

        input.oe_csr.delete_vertices(src_delete_set);
        if let Some(table) = input.oe_prop.as_mut() {
            input
                .oe_csr
                .parallel_delete_edges_with_props(&delete_edge_set, false, table, p);
            input
                .oe_csr
                .parallel_delete_edges_with_props(&ie_to_delete, false, table, p);
        } else {
            input
                .oe_csr
                .parallel_delete_edges(&delete_edge_set, false, p);
            input
                .oe_csr
                .parallel_delete_edges(&ie_to_delete, false, p);
        }

        input.ie_csr.delete_vertices(dst_delete_set);
        if let Some(table) = input.ie_prop.as_mut() {
            input
                .ie_csr
                .parallel_delete_edges_with_props(&delete_edge_set, true, table, p);
            input
                .ie_csr
                .parallel_delete_edges_with_props(&oe_to_delete, true, table, p);
        } else {
            input
                .ie_csr
                .parallel_delete_edges(&delete_edge_set, true, p);
            input
                .ie_csr
                .parallel_delete_edges(&oe_to_delete, true, p);
        }
    }

    pub fn apply_vertices_delete_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, label: LabelId, filenames: &Vec<String>, id_col: i32,
    ) -> GDBResult<()>
        where
            G: FromStr + Send + Sync + IndexType + Eq,
            I: Send + Sync + IndexType,
    {
        let mut delete_sets = vec![HashSet::new(); graph.vertex_label_num as usize];
        let mut delete_set = HashSet::new();
        info!("Deleting vertex - {}", graph.graph_schema.vertex_label_names()[label as usize]);
        let vertex_files_prefix = self.input_dir.clone();
        let vertex_files = get_files_list(&vertex_files_prefix, filenames).unwrap();
        if vertex_files.is_empty() {
            return Ok(());
        }

        let parser = LDBCVertexParser::<G>::new(label as LabelId, id_col as usize);
        for vertex_file in vertex_files.iter() {
            process_csv_rows(
                vertex_file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    let (got_label, lid) = graph
                        .vertex_map
                        .get_internal_id(vertex_meta.global_id)
                        .unwrap();
                    if got_label == label as LabelId {
                        delete_set.insert(lid);
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        delete_sets[label as usize] = delete_set;

        let mut input_reps = self.take_csrs_with_label(graph, label);
        input_reps.iter_mut().for_each(|rep| {
            let edge_file_strings = vec![];
            let input_header = graph
                .graph_schema
                .get_edge_header(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap();
            self.parallel_delete_rep(
                rep,
                graph,
                &edge_file_strings,
                &input_header,
                &delete_sets,
                self.parallel,
            );
        });
        self.set_csrs(graph, input_reps);
        let delete_set = &delete_sets[label as usize];
        for v in delete_set.iter() {
            graph
                .vertex_map
                .remove_vertex(label, v);
        }

        Ok(())
    }

    pub fn apply_edges_delete_with_filename<G, I>
    (
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32,
    ) -> GDBResult<()>
        where
            G: FromStr + Send + Sync + IndexType + Eq,
            I: Send + Sync + IndexType,
    {
        let mut input_resp = self.take_csr(graph, src_label, dst_label, edge_label);
        let input_header: Vec<(String, DataType)> = vec![];
        let delete_sets = vec![HashSet::new(); graph.vertex_label_num as usize];
        self.parallel_delete_rep(
            &mut input_resp,
            graph,
            filenames,
            &input_header,
            &delete_sets,
            self.parallel,
        );
        self.set_csr(graph, input_resp);
        Ok(())
    }

    fn apply_deletes<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema,
    ) -> GDBResult<()>
        where
            G: FromStr + Send + Sync + IndexType + Eq,
            I: Send + Sync + IndexType,
    {
        let vertex_label_num = graph.vertex_label_num;
        let mut delete_sets = vec![];
        for v_label_i in 0..vertex_label_num {
            let mut delete_set = HashSet::new();
            if let Some(vertex_file_strings) = delete_schema.get_vertex_file(v_label_i as LabelId) {
                if !vertex_file_strings.is_empty() {
                    info!(
                        "Deleting vertex - {}",
                        graph.graph_schema.vertex_label_names()[v_label_i as usize]
                    );
                    let vertex_files_prefix = self.input_dir.clone();
                    let vertex_files = get_files_list_beta(&vertex_files_prefix, &vertex_file_strings);
                    if vertex_files.is_empty() {
                        delete_sets.push(delete_set);
                        continue;
                    }
                    let input_header = delete_schema
                        .get_vertex_header(v_label_i as LabelId)
                        .unwrap();
                    let mut id_col = 0;
                    for (index, (n, _)) in input_header.iter().enumerate() {
                        if n == "id" {
                            id_col = index;
                            break;
                        }
                    }
                    let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col);
                    for vertex_file in vertex_files.iter() {
                        process_csv_rows(
                            vertex_file,
                            |record| {
                                let vertex_meta = parser.parse_vertex_meta(&record);
                                let (got_label, lid) = graph
                                    .vertex_map
                                    .get_internal_id(vertex_meta.global_id)
                                    .unwrap();
                                if got_label == v_label_i as LabelId {
                                    delete_set.insert(lid);
                                }
                            },
                            self.skip_header,
                            self.delim,
                        );
                    }
                }
            }
            delete_sets.push(delete_set);
        }

        let mut input_reps = self.take_csrs(graph);
        input_reps.iter_mut().for_each(|rep| {
            let default_vec: Vec<String> = vec![];
            let edge_file_strings = delete_schema
                .get_edge_file(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap_or_else(|| &default_vec);
            let input_header = delete_schema
                .get_edge_header(rep.src_label, rep.edge_label, rep.dst_label)
                .unwrap_or_else(|| &[]);

            self.parallel_delete_rep(
                rep,
                graph,
                &edge_file_strings,
                &input_header,
                &delete_sets,
                self.parallel,
            );
        });
        self.set_csrs(graph, input_reps);

        for v_label_i in 0..vertex_label_num {
            let delete_set = &delete_sets[v_label_i as usize];
            if delete_set.is_empty() {
                continue;
            }
            for v in delete_set.iter() {
                graph
                    .vertex_map
                    .remove_vertex(v_label_i as LabelId, v);
            }
        }

        Ok(())
    }

    pub fn apply_vertices_insert_with_filename<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, label: LabelId, filenames: &Vec<String>, id_col: i32, mappings: &Vec<i32>,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let graph_header = graph
            .graph_schema
            .get_vertex_header(label as LabelId)
            .unwrap();
        let header = graph_header.to_vec();

        let parser = LDBCVertexParser::<G>::new(label as LabelId, id_col as usize);
        let vertex_files_prefix = self.input_dir.clone();

        let vertex_files = get_files_list(&vertex_files_prefix, filenames);
        if vertex_files.is_err() {
            warn!(
                    "Get vertex files {:?}/{:?} failed: {:?}",
                    &vertex_files_prefix,
                    filenames,
                    vertex_files.err().unwrap()
                );
            return Ok(());
        }
        let vertex_files = vertex_files.unwrap();
        if vertex_files.is_empty() {
            return Ok(());
        }
        for vertex_file in vertex_files.iter() {
            process_csv_rows(
                vertex_file,
                |record| {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if let Ok(properties) = parse_properties_by_mappings(&record, &header, mappings) {
                        graph.insert_vertex(vertex_meta.label, vertex_meta.global_id, Some(properties));
                    }
                },
                self.skip_header,
                self.delim,
            );
        }

        Ok(())
    }

    fn apply_vertices_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let v_label_num = graph.vertex_label_num;
        for v_label_i in 0..v_label_num {
            if let Some(vertex_file_strings) = input_schema.get_vertex_file(v_label_i as LabelId) {
                if vertex_file_strings.is_empty() {
                    continue;
                }

                let input_header = input_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let graph_header = graph
                    .graph_schema
                    .get_vertex_header(v_label_i as LabelId)
                    .unwrap();
                let mut keep_set = HashSet::new();
                for pair in graph_header {
                    keep_set.insert(pair.0.clone());
                }
                let mut selected = vec![false; input_header.len()];
                let mut id_col_id = 0;
                for (index, (n, _)) in input_header.iter().enumerate() {
                    if keep_set.contains(n) {
                        selected[index] = true;
                    }
                    if n == "id" {
                        id_col_id = index;
                    }
                }
                let parser = LDBCVertexParser::<G>::new(v_label_i as LabelId, id_col_id);
                let vertex_files_prefix = self.input_dir.clone();

                let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings);
                if vertex_files.is_err() {
                    warn!(
                        "Get vertex files {:?}/{:?} failed: {:?}",
                        &vertex_files_prefix,
                        &vertex_file_strings,
                        vertex_files.err().unwrap()
                    );
                    continue;
                }
                let vertex_files = vertex_files.unwrap();
                if vertex_files.is_empty() {
                    continue;
                }
                for vertex_file in vertex_files.iter() {
                    process_csv_rows(
                        vertex_file,
                        |record| {
                            let vertex_meta = parser.parse_vertex_meta(&record);
                            if let Ok(properties) =
                                parse_properties(&record, input_header, selected.as_slice())
                            {
                                graph.insert_vertex(
                                    vertex_meta.label,
                                    vertex_meta.global_id,
                                    Some(properties),
                                );
                            }
                        },
                        self.skip_header,
                        self.delim,
                    );
                }
            }
        }

        Ok(())
    }

    fn load_insert_edges<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        input_header: &[(String, DataType)], graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
        where
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
            keep_set.insert(pair.0.clone());
        }

        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "start_id" {
                src_col_id = index;
            }
            if n == "end_id" {
                dst_col_id = index;
            }
        }

        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        if table_header.is_empty() {
            for file in files.iter() {
                process_csv_rows(
                    file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    },
                    self.skip_header,
                    self.delim,
                );
            }
            Ok((edges, None))
        } else {
            let mut prop_table = ColTable::new(table_header);
            for file in files.iter() {
                process_csv_rows(
                    file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    },
                    self.skip_header,
                    self.delim,
                )
            }
            Ok((edges, Some(prop_table)))
        }
    }

    fn parallel_insert_rep<G, I>(
        &self, input: &mut CsrRep<I>, graph: &GraphDB<G, I>, edge_file_strings: &Vec<String>,
        input_header: &[(String, DataType)], p: u32,
    ) where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let t = Instant::now();
        let src_label = input.src_label;
        let edge_label = input.edge_label;
        let dst_label = input.dst_label;

        let graph_header = graph
            .graph_schema
            .get_edge_header(src_label, edge_label, dst_label);
        if graph_header.is_none() {
            return;
        }

        if edge_file_strings.is_empty() {
            return;
        }

        let edge_files = get_files_list(&self.input_dir.clone(), edge_file_strings);
        if edge_files.is_err() {
            return;
        }
        let edge_files = edge_files.unwrap();
        if edge_files.is_empty() {
            return;
        }

        let (edges, table) = self
            .load_insert_edges::<G>(
                src_label,
                edge_label,
                dst_label,
                input_header,
                &graph.graph_schema,
                &edge_files,
            )
            .unwrap();

        let parsed_edges: Vec<(I, I)> = edges
            .par_iter()
            .map(|(src, dst)| {
                let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(*src).unwrap();
                let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(*dst).unwrap();
                if got_src_label != src_label || got_dst_label != dst_label {
                    warn!("insert edges with wrong label");
                    (<I as IndexType>::max(), <I as IndexType>::max())
                } else {
                    (src_lid, dst_lid)
                }
            })
            .collect();

        let new_src_num = graph.vertex_map.vertex_num(src_label);
        input.oe_prop = if let Some(old_table) = input.oe_prop.take() {
            Some(input.oe_csr.insert_edges_with_prop(
                new_src_num,
                &parsed_edges,
                table.as_ref().unwrap(),
                false,
                p,
                old_table,
            ))
        } else {
            input
                .oe_csr
                .insert_edges(new_src_num, &parsed_edges, false, p);
            None
        };

        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        input.ie_prop = if let Some(old_table) = input.ie_prop.take() {
            Some(input.ie_csr.insert_edges_with_prop(
                new_dst_num,
                &parsed_edges,
                table.as_ref().unwrap(),
                true,
                p,
                old_table,
            ))
        } else {
            input
                .ie_csr
                .insert_edges(new_dst_num, &parsed_edges, true, p);
            None
        };

        println!(
            "insert edge (parallel{}): {} - {} - {}: {}",
            p,
            graph.graph_schema.vertex_label_names()[src_label as usize],
            graph.graph_schema.edge_label_names()[edge_label as usize],
            graph.graph_schema.vertex_label_names()[dst_label as usize],
            t.elapsed().as_secs_f32(),
        );
    }

    pub fn apply_edges_insert_with_filename<G, I>
    (
        &mut self, graph: &mut GraphDB<G, I>, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, filenames: &Vec<String>, src_id_col: i32, dst_id_col: i32, mappings: &Vec<i32>,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut parser = LDBCEdgeParser::<G>::new(src_label, dst_label, edge_label);
        parser.with_endpoint_col_id(src_id_col as usize, dst_id_col as usize);

        let edge_files_prefix = self.input_dir.clone();
        let edge_files = get_files_list(&edge_files_prefix, filenames);
        if edge_files.is_err() {
            warn!(
                    "Get vertex files {:?}/{:?} failed: {:?}",
                    &edge_files_prefix,
                    filenames,
                    edge_files.err().unwrap()
                );
            return Ok(());
        }
        let edge_files = edge_files.unwrap();
        let mut input_reps = self.take_csr(graph, src_label, dst_label, edge_label);
        let mut edges = vec![];
        let graph_header = graph.graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
        }
        let mut prop_table = ColTable::new(table_header.clone());
        if table_header.is_empty() {
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    },
                    self.skip_header,
                    self.delim,
                );
            }
        } else {
            for file in edge_files {
                process_csv_rows(
                    &file,
                    |record| {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        if let Ok(properties) =
                            parse_properties_by_mappings(&record, &graph_header, mappings)
                        {
                            prop_table.push(&properties);
                        }
                    },
                    self.skip_header,
                    self.delim,
                )
            }
        }

        let parsed_edges: Vec<(I, I)> = edges
            .par_iter()
            .map(|(src, dst)| {
                let (got_src_label, src_lid) = graph.vertex_map.get_internal_id(*src).unwrap();
                let (got_dst_label, dst_lid) = graph.vertex_map.get_internal_id(*dst).unwrap();
                if got_src_label != src_label || got_dst_label != dst_label {
                    warn!("insert edges with wrong label");
                    (<I as IndexType>::max(), <I as IndexType>::max())
                } else {
                    (src_lid, dst_lid)
                }
            })
            .collect();
        let new_src_num = graph.vertex_map.vertex_num(src_label);
        input_reps.oe_prop = if let Some(old_table) = input_reps.oe_prop.take() {
            Some(input_reps.oe_csr.insert_edges_with_prop(
                new_src_num,
                &parsed_edges,
                &prop_table,
                false,
                self.parallel,
                old_table,
            ))
        } else {
            input_reps
                .oe_csr
                .insert_edges(new_src_num, &parsed_edges, false, self.parallel);
            None
        };

        let new_dst_num = graph.vertex_map.vertex_num(dst_label);
        input_reps.ie_prop = if let Some(old_table) = input_reps.ie_prop.take() {
            Some(input_reps.ie_csr.insert_edges_with_prop(
                new_dst_num,
                &parsed_edges,
                &prop_table,
                true,
                self.parallel,
                old_table,
            ))
        } else {
            input_reps
                .ie_csr
                .insert_edges(new_dst_num, &parsed_edges, true, self.parallel);
            None
        };
        self.set_csr(graph, input_reps);
        Ok(())
    }

    fn apply_edges_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut input_reps = self.take_csrs(graph);
        for ir in input_reps.iter_mut() {
            let edge_files = input_schema.get_edge_file(ir.src_label, ir.edge_label, ir.dst_label);
            if edge_files.is_none() {
                continue;
            }
            let input_header = input_schema
                .get_edge_header(ir.src_label, ir.edge_label, ir.dst_label)
                .unwrap();
            self.parallel_insert_rep(ir, graph, edge_files.unwrap(), input_header, self.parallel);
        }
        self.set_csrs(graph, input_reps);

        Ok(())
    }

    pub fn insert<G, I>(&mut self, graph: &mut GraphDB<G, I>, insert_schema: &InputSchema) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_vertices_inserts(graph, &insert_schema)?;
        self.apply_edges_inserts(graph, &insert_schema)?;
        Ok(())
    }

    pub fn delete<G, I>(&mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema) -> GDBResult<()>
        where
            I: Send + Sync + IndexType,
            G: FromStr + Send + Sync + IndexType + Eq,
    {
        self.apply_deletes(graph, &delete_schema)?;
        Ok(())
    }
}
