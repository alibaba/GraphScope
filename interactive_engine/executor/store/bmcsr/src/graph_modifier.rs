use crate::col_table::{parse_properties, ColTable};
use crate::columns::{Column, StringColumn};
use csv::ReaderBuilder;
use rust_htslib::bgzf::Reader as GzReader;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::GraphDB;
use crate::graph_loader::get_files_list;
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, LabelId};

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
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(b'|')
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let vertex_meta = parser.parse_vertex_meta(&record);
                        ret.push((
                            record
                                .get(0)
                                .unwrap()
                                .parse::<String>()
                                .unwrap(),
                            vertex_meta.global_id,
                        ));
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(b'|')
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let vertex_meta = parser.parse_vertex_meta(&record);
                        ret.push((
                            record
                                .get(0)
                                .unwrap()
                                .parse::<String>()
                                .unwrap(),
                            vertex_meta.global_id,
                        ));
                    }
                }
            }
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
                .join("batch_id=2012-11-29"),
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
                .join("batch_id=2012-11-29"),
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
                .join("batch_id=2012-11-29"),
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
                .join("batch_id=2012-11-29"),
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
}

impl GraphModifier {
    pub fn new<D: AsRef<Path>>(input_dir: D) -> GraphModifier {
        Self { input_dir: input_dir.as_ref().to_path_buf(), delim: b'|', skip_header: false }
    }

    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn apply_deletes<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, delete_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
        I: Send + Sync + IndexType,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;
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
                    let vertex_files = get_files_list(&vertex_files_prefix, &vertex_file_strings);
                    if vertex_files.is_err() {
                        warn!(
                            "Get vertex files {:?}/{:?} failed: {:?}",
                            &vertex_files_prefix,
                            &vertex_file_strings,
                            vertex_files.err().unwrap()
                        );
                        delete_sets.push(delete_set);
                        continue;
                    }
                    let vertex_files = vertex_files.unwrap();
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
                        if vertex_file
                            .clone()
                            .to_str()
                            .unwrap()
                            .ends_with(".csv.gz")
                        {
                            let mut rdr = ReaderBuilder::new()
                                .delimiter(self.delim)
                                .buffer_capacity(4096)
                                .comment(Some(b'#'))
                                .flexible(true)
                                .has_headers(self.skip_header)
                                .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                            for result in rdr.records() {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    let (got_label, lid) = graph
                                        .vertex_map
                                        .get_internal_id(vertex_meta.global_id)
                                        .unwrap();
                                    if got_label == v_label_i as LabelId {
                                        delete_set.insert(lid);
                                    }
                                }
                            }
                        } else if vertex_file
                            .clone()
                            .to_str()
                            .unwrap()
                            .ends_with(".csv")
                        {
                            let mut rdr = ReaderBuilder::new()
                                .delimiter(self.delim)
                                .buffer_capacity(4096)
                                .comment(Some(b'#'))
                                .flexible(true)
                                .has_headers(self.skip_header)
                                .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                            for result in rdr.records() {
                                if let Ok(record) = result {
                                    let vertex_meta = parser.parse_vertex_meta(&record);
                                    let (got_label, lid) = graph
                                        .vertex_map
                                        .get_internal_id(vertex_meta.global_id)
                                        .unwrap();
                                    if got_label == v_label_i as LabelId {
                                        delete_set.insert(lid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            delete_sets.push(delete_set);
        }

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                let src_delete_set = &delete_sets[src_label_i];
                for dst_label_i in 0..vertex_label_num {
                    let dst_delete_set = &delete_sets[dst_label_i];
                    let mut delete_edge_set = HashSet::new();
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
                    if let Some(edge_file_strings) = delete_schema.get_edge_file(
                        src_label_i as LabelId,
                        e_label_i as LabelId,
                        dst_label_i as LabelId,
                    ) {
                        if edge_file_strings.is_empty() {
                            continue;
                        }
                        info!(
                            "Deleting edge - {} - {} - {}",
                            graph.graph_schema.vertex_label_names()[src_label_i as usize],
                            graph.graph_schema.edge_label_names()[e_label_i as usize],
                            graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                        );
                        let input_header = delete_schema
                            .get_edge_header(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                            )
                            .unwrap();

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

                        let mut parser = LDBCEdgeParser::<G>::new(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        );
                        parser.with_endpoint_col_id(src_col_id, dst_col_id);

                        let edge_files_prefix = self.input_dir.clone();
                        let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings);
                        if edge_files.is_err() {
                            warn!(
                                "Get edge files {:?}/{:?} failed: {:?}",
                                &edge_files_prefix,
                                &edge_file_strings,
                                edge_files.err().unwrap()
                            );
                            continue;
                        }
                        let edge_files = edge_files.unwrap();
                        if edge_files.is_empty() {
                            continue;
                        }

                        for edge_file in edge_files.iter() {
                            if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv.gz")
                            {
                                let mut rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                                for result in rdr.records() {
                                    if let Ok(record) = result {
                                        let edge_meta = parser.parse_edge_meta(&record);
                                        let (got_src_label, src_lid) = graph
                                            .vertex_map
                                            .get_internal_id(edge_meta.src_global_id)
                                            .unwrap();
                                        let (got_dst_label, dst_lid) = graph
                                            .vertex_map
                                            .get_internal_id(edge_meta.dst_global_id)
                                            .unwrap();
                                        if got_src_label != src_label_i as LabelId
                                            || got_dst_label != dst_label_i as LabelId
                                        {
                                            warn!(
                                                "Edge - {} - {} does not exist",
                                                LDBCVertexParser::<G>::get_original_id(
                                                    edge_meta.src_global_id
                                                )
                                                .index(),
                                                LDBCVertexParser::<G>::get_original_id(
                                                    edge_meta.dst_global_id
                                                )
                                                .index()
                                            );
                                            continue;
                                        }
                                        if src_delete_set.contains(&src_lid)
                                            || dst_delete_set.contains(&dst_lid)
                                        {
                                            // warn!("Edge - {} - {} will be removed by vertices", LDBCVertexParser::<G>::get_original_id(edge_meta.src_global_id).index(), LDBCVertexParser::<G>::get_original_id(edge_meta.dst_global_id).index());
                                            continue;
                                        }
                                        delete_edge_set.insert((src_lid, dst_lid));
                                    }
                                }
                            } else if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv")
                            {
                                let mut rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                for result in rdr.records() {
                                    if let Ok(record) = result {
                                        let edge_meta = parser.parse_edge_meta(&record);
                                        let (got_src_label, src_lid) = graph
                                            .vertex_map
                                            .get_internal_id(edge_meta.src_global_id)
                                            .unwrap();
                                        let (got_dst_label, dst_lid) = graph
                                            .vertex_map
                                            .get_internal_id(edge_meta.dst_global_id)
                                            .unwrap();
                                        if got_src_label != src_label_i as LabelId
                                            || got_dst_label == dst_label_i as LabelId
                                        {
                                            continue;
                                        }
                                        if src_delete_set.contains(&src_lid)
                                            || dst_delete_set.contains(&dst_lid)
                                        {
                                            continue;
                                        }
                                        delete_edge_set.insert((src_lid, dst_lid));
                                    }
                                }
                            }
                        }
                    }

                    if src_delete_set.is_empty() && dst_delete_set.is_empty() && delete_edge_set.is_empty()
                    {
                        continue;
                    }
                    graph.delete_edges(
                        src_label_i as LabelId,
                        e_label_i as LabelId,
                        dst_label_i as LabelId,
                        src_delete_set,
                        dst_delete_set,
                        &delete_edge_set,
                    );
                }
            }
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
                    if vertex_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv.gz")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
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
                            }
                        }
                    } else if vertex_file
                        .clone()
                        .to_str()
                        .unwrap()
                        .ends_with(".csv")
                    {
                        let mut rdr = ReaderBuilder::new()
                            .delimiter(self.delim)
                            .buffer_capacity(4096)
                            .comment(Some(b'#'))
                            .flexible(true)
                            .has_headers(self.skip_header)
                            .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                        for result in rdr.records() {
                            if let Ok(record) = result {
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
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn load_insert_edges_with_no_prop<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input_schema: &InputSchema,
        graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let input_header = input_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
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

        for file in files.iter() {
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                    }
                }
            }
        }

        Ok((edges, None))
    }

    fn load_insert_edges_with_prop<G>(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, input_schema: &InputSchema,
        graph_schema: &CsrGraphSchema, files: &Vec<PathBuf>,
    ) -> GDBResult<(Vec<(G, G)>, Option<ColTable>)>
    where
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let mut edges = vec![];

        let input_header = input_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let graph_header = graph_schema
            .get_edge_header(src_label, edge_label, dst_label)
            .unwrap();
        let mut table_header = vec![];
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            table_header.push((pair.1.clone(), pair.0.clone()));
            keep_set.insert(pair.0.clone());
        }
        let mut prop_table = ColTable::new(table_header);

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

        for file in files.iter() {
            if file
                .clone()
                .to_str()
                .unwrap()
                .ends_with(".csv.gz")
            {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    }
                }
            } else if file.clone().to_str().unwrap().ends_with(".csv") {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(self.skip_header)
                    .from_reader(BufReader::new(File::open(&file).unwrap()));
                for result in rdr.records() {
                    if let Ok(record) = result {
                        let edge_meta = parser.parse_edge_meta(&record);
                        let properties =
                            parse_properties(&record, input_header, selected.as_slice()).unwrap();
                        edges.push((edge_meta.src_global_id, edge_meta.dst_global_id));
                        prop_table.push(&properties);
                    }
                }
            }
        }

        Ok((edges, Some(prop_table)))
    }

    fn apply_edges_inserts<G, I>(
        &mut self, graph: &mut GraphDB<G, I>, input_schema: &InputSchema,
    ) -> GDBResult<()>
    where
        I: Send + Sync + IndexType,
        G: FromStr + Send + Sync + IndexType + Eq,
    {
        let vertex_label_num = graph.vertex_label_num;
        let edge_label_num = graph.edge_label_num;

        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    if let Some(edge_file_strings) = input_schema.get_edge_file(
                        src_label_i as LabelId,
                        e_label_i as LabelId,
                        dst_label_i as LabelId,
                    ) {
                        if edge_file_strings.is_empty() {
                            continue;
                        }
                        let graph_header = graph
                            .graph_schema
                            .get_edge_header(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                            )
                            .unwrap();
                        let edge_files_prefix = self.input_dir.clone();
                        let edge_files = get_files_list(&edge_files_prefix, &edge_file_strings);
                        if edge_files.is_err() {
                            warn!(
                                "Get edge files {:?}/{:?} failed: {:?}",
                                &edge_files_prefix,
                                &edge_file_strings,
                                edge_files.err().unwrap()
                            );
                            continue;
                        }
                        let edge_files = edge_files.unwrap();
                        if edge_files.is_empty() {
                            continue;
                        }
                        let (edges, table) = if graph_header.len() > 0 {
                            info!(
                                "Loading edges with properties: {}, {}, {}",
                                graph.graph_schema.vertex_label_names()[src_label_i as usize],
                                graph.graph_schema.edge_label_names()[e_label_i as usize],
                                graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                            );
                            self.load_insert_edges_with_prop(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                                input_schema,
                                &graph.graph_schema,
                                &edge_files,
                            )
                            .unwrap()
                        } else {
                            info!(
                                "Loading edges with no property: {}, {}, {}",
                                graph.graph_schema.vertex_label_names()[src_label_i as usize],
                                graph.graph_schema.edge_label_names()[e_label_i as usize],
                                graph.graph_schema.vertex_label_names()[dst_label_i as usize]
                            );
                            self.load_insert_edges_with_no_prop(
                                src_label_i as LabelId,
                                e_label_i as LabelId,
                                dst_label_i as LabelId,
                                input_schema,
                                &graph.graph_schema,
                                &edge_files,
                            )
                            .unwrap()
                        };

                        graph.insert_edges(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                            edges,
                            table,
                        );
                    }
                }
            }
        }

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
