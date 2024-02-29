use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::str::FromStr;

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_loader::get_files_list;
use bmcsr::schema::Schema;
use bmcsr::graph::{Direction, IndexType};
use bmcsr::columns::*;
use bmcsr::types::LabelId;
use bmcsr::ldbc_parser::LDBCVertexParser;

use clap::{App, Arg};
use log::{info, warn};
use csv::ReaderBuilder;
use rust_htslib::bgzf::Reader as GzReader;

fn load_input(input_dir: &str, label: LabelId) -> Vec<(String, usize)> {
    let mut ret = vec![];
    let suffixes = vec!["*.csv.gz".to_string(), "*.csv".to_string()];
    let files = get_files_list(&PathBuf::from_str(input_dir).unwrap(), &suffixes).unwrap();
    let parser = LDBCVertexParser::<usize>::new(label, 1);
    for file in files {
        if file.clone().to_str().unwrap().ends_with(".csv.gz") {
            let mut rdr = ReaderBuilder::new()
                .delimiter(b'|')
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(true)
                .from_reader(BufReader::new(GzReader::from_path(&file).unwrap()));
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    ret.push((record.get(0).unwrap().parse::<String>().unwrap(), vertex_meta.global_id.index()));
                }
            }
        }
    }

    ret
}

fn iterate_persons(graph: &GraphDB<usize, usize>, persons: &Vec<(String, usize)>, comments: &mut Vec<(String, usize)>, posts: &mut Vec<(String, usize)>, forums: &mut Vec<(String, usize)>) {
    let person_label = graph.graph_schema.get_vertex_label_id("PERSON").unwrap();

    let comment_label = graph.graph_schema.get_vertex_label_id("COMMENT").unwrap();
    let post_label = graph.graph_schema.get_vertex_label_id("POST").unwrap();
    let forum_label = graph.graph_schema.get_vertex_label_id("FORUM").unwrap();

    let hasCreator_label = graph.graph_schema.get_edge_label_id("HASCREATOR").unwrap();
    let hasModerator_label = graph.graph_schema.get_edge_label_id("HASMODERATOR").unwrap();

    let comment_hasCreator_person = graph.get_sub_graph(person_label, hasCreator_label, comment_label, Direction::Incoming);
    let post_hasCreator_person = graph.get_sub_graph(person_label, hasCreator_label, post_label, Direction::Incoming);
    let forum_hasModerator_person = graph.get_sub_graph(person_label, hasModerator_label, forum_label, Direction::Incoming);

    let forum_title_column = graph.vertex_prop_table[forum_label as usize]
        .get_column_by_name("title")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap();

    for (dt, id) in persons.iter() {
        if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
            if got_label != person_label {
                warn!("Vertex {} is not a person", LDBCVertexParser::<usize>::get_original_id(*id));
                continue;
            }
            for e in comment_hasCreator_person.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(comment_label, *e).unwrap();
                comments.push((dt.clone(), oid));
            }

            for e in post_hasCreator_person.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(post_label, *e).unwrap();
                posts.push((dt.clone(), oid));
            }

            for e in forum_hasModerator_person.get_adj_list(lid).unwrap() {
                let title = forum_title_column.get(*e).unwrap();
                let title_string = title.to_string();
                if title_string.starts_with("Album") || title_string.starts_with("Wall") {
                    let oid = graph.vertex_map.get_global_id(forum_label, *e).unwrap();
                    forums.push((dt.clone(), oid));
                }
            }
        } else {
            warn!("Vertex Person - {} does not exist", LDBCVertexParser::<usize>::get_original_id(*id));
            continue;
        }
    }
}

fn iterate_forums(graph: &GraphDB<usize, usize>, forums: &Vec<(String, usize)>, posts: &mut Vec<(String, usize)>) {
    let forum_label = graph.graph_schema.get_vertex_label_id("FORUM").unwrap();
    let post_label = graph.graph_schema.get_vertex_label_id("POST").unwrap();

    let containerOf_label = graph.graph_schema.get_edge_label_id("CONTAINEROF").unwrap();

    let forum_containerOf_post = graph.get_sub_graph(forum_label, containerOf_label, post_label, Direction::Outgoing);
    for (dt, id) in forums.iter() {
        if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
            if got_label != forum_label {
                warn!("Vertex {} is not a forum", LDBCVertexParser::<usize>::get_original_id(*id));
                continue;
            }

            for e in forum_containerOf_post.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(post_label, *e).unwrap();
                posts.push((dt.clone(), oid));
            }
        } else {
            warn!("Vertex Forum - {} does not exist", LDBCVertexParser::<usize>::get_original_id(*id));
            continue;
        }
    }
}

fn iterate_posts(graph: &GraphDB<usize, usize>, posts: &Vec<(String, usize)>, comments: &mut Vec<(String, usize)>) {
    let post_label = graph.graph_schema.get_vertex_label_id("POST").unwrap();
    let comment_label = graph.graph_schema.get_vertex_label_id("COMMENT").unwrap();

    let replyOf_label = graph.graph_schema.get_edge_label_id("REPLYOF").unwrap();

    let comment_replyOf_post = graph.get_sub_graph(post_label, replyOf_label, comment_label, Direction::Incoming);
    for (dt, id) in posts.iter() {
        if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(*id) {
            if got_label != post_label {
                warn!("Vertex {} is not a post", LDBCVertexParser::<usize>::get_original_id(*id));
                continue;
            }

            for e in comment_replyOf_post.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(comment_label, *e).unwrap();
                comments.push((dt.clone(), oid));
            }
        } else {
            warn!("Vertex Post - {} does not exist", LDBCVertexParser::<usize>::get_original_id(*id));
            continue;
        }
    }
}

fn iterate_comments(graph: &GraphDB<usize, usize>, comments: &mut Vec<(String, usize)>) {
    let comment_label = graph.graph_schema.get_vertex_label_id("COMMENT").unwrap();

    let replyOf_label = graph.graph_schema.get_edge_label_id("REPLYOF").unwrap();

    let comment_replyOf_comment = graph.get_sub_graph(comment_label, replyOf_label, comment_label, Direction::Incoming);
    let mut index = 0;
    while index < comments.len() {
        let (dt, id) = comments[index].clone();
        if let Some((got_label, lid)) = graph.vertex_map.get_internal_id(id) {
            if got_label != comment_label {
                warn!("Vertex {} is not a comment", LDBCVertexParser::<usize>::get_original_id(id));
                index += 1;
                continue;
            }

            for e in comment_replyOf_comment.get_adj_list(lid).unwrap() {
                let oid = graph.vertex_map.get_global_id(comment_label, *e).unwrap();
                comments.push((dt.clone(), oid));
            }
            index += 1;
        } else {
            warn!("Vertex Comment - {} does not exist", LDBCVertexParser::<usize>::get_original_id(id));
            index += 1;
            continue;
        }
    }
}

fn main() {
    env_logger::init();
    let matches = App::new("generate_delete")
        .version("0.1.0")
        .args(&[
            Arg::with_name("graph_data_dir")
                .short("g")
                .long("graph_data_dir")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("input_dir")
                .short("i")
                .long("input_dir")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("output_dir")
                .short("o")
                .long_help("The directory to place output files")
                .required(true)
                .takes_value(true)
                .index(3),
        ]).get_matches();
    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let input_dir = matches
        .value_of("input_dir")
        .unwrap()
        .to_string();
    let output_dir = matches
        .value_of("output_dir")
        .unwrap()
        .to_string();

    let graph = GraphDB::<usize, usize>::deserialize(&graph_data_dir, 0, None).unwrap();

    info!("process persons");
    let person_label = graph.graph_schema.get_vertex_label_id("PERSON").unwrap();
    let mut input_persons = load_input(&(input_dir.clone() + "/deletes/dynamic/Person/batch_id=2012-11-29"), person_label);
    let mut input_persons_set = HashSet::new();
    for (_, id) in input_persons.iter() {
        input_persons_set.insert(*id);
    }

    info!("process forums");
    let forum_label = graph.graph_schema.get_vertex_label_id("FORUM").unwrap();
    let mut input_forums = load_input(&(input_dir.clone() + "/deletes/dynamic/Forum/batch_id=2012-11-29"), forum_label);
    let mut input_forums_set = HashSet::new();
    for (_, id) in input_forums.iter() {
        input_forums_set.insert(*id);
    }

    info!("process posts");
    let post_label = graph.graph_schema.get_vertex_label_id("POST").unwrap();
    let mut input_posts = load_input(&(input_dir.clone() + "/deletes/dynamic/Post/batch_id=2012-11-29"), post_label);
    let mut input_posts_set = HashSet::new();
    for (_, id) in input_posts.iter() {
        input_posts_set.insert(*id);
    }

    info!("process comments");
    let comment_label = graph.graph_schema.get_vertex_label_id("COMMENT").unwrap();
    let mut input_comments = load_input(&(input_dir.clone() + "/deletes/dynamic/Comment/batch_id=2012-11-29"), comment_label);
    let mut input_comments_set = HashSet::new();
    for (_, id) in input_comments.iter() {
        input_comments_set.insert(*id);
    }

    info!("iterate persons");
    iterate_persons(&graph, &input_persons, &mut input_comments, &mut input_posts, &mut input_forums);
    info!("iterate forums");
    iterate_forums(&graph, &input_forums, &mut input_posts);
    info!("iterate posts");
    iterate_posts(&graph, &input_posts, &mut input_comments);
    info!("iterate comments");
    iterate_comments(&graph, &mut input_comments);

    let mut person_file = File::create(&(output_dir.clone() + "/Person.csv")).unwrap();
    writeln!(person_file, "deletionDate|id").unwrap();
    for (dt, id) in input_persons.iter() {
        if !input_persons_set.contains(id) {
            input_persons_set.insert(*id);
            writeln!(person_file, "{}|{}", dt, LDBCVertexParser::<usize>::get_original_id(*id)).unwrap();
        }
    }

    let mut forum_file = File::create(&(output_dir.clone() + "/Forum.csv")).unwrap();
    writeln!(forum_file, "deletionDate|id").unwrap();
    for (dt, id) in input_forums.iter() {
        if !input_forums_set.contains(id) {
            input_forums_set.insert(*id);
            writeln!(forum_file, "{}|{}", dt, LDBCVertexParser::<usize>::get_original_id(*id)).unwrap();
        }
    }

    let mut post_file = File::create(&(output_dir.clone() + "/Post.csv")).unwrap();
    writeln!(post_file, "deletionDate|id").unwrap();
    for (dt, id) in input_posts.iter() {
        if !input_posts_set.contains(id) {
            input_posts_set.insert(*id);
            writeln!(post_file, "{}|{}", dt, LDBCVertexParser::<usize>::get_original_id(*id)).unwrap();
        }
    }

    let mut comment_file = File::create(&(output_dir.clone() + "/Comment.csv")).unwrap();
    writeln!(comment_file, "deletionDate|id").unwrap();
    for (dt, id) in input_comments.iter() {
        if !input_comments_set.contains(id) {
            input_comments_set.insert(*id);
            writeln!(comment_file, "{}|{}", dt, LDBCVertexParser::<usize>::get_original_id(*id)).unwrap();
        }
    }
}