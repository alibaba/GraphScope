use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};

use rpc_proxy::request::JobClient;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "e", long = "endpoint")]
    endpoint: String,
    #[structopt(short = "w", long = "workers")]
    workers: u32,
    #[structopt(short = "c", long = "query_config")]
    query_config: String,
    #[structopt(short = "r", long = "raw_data_path")]
    raw_data_path: String,
    #[structopt(short = "i", long = "input_dir")]
    input_dir: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Param {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryConfig {
    pub name: String,
    pub description: String,
    pub mode: String,
    pub extension: String,
    pub library: String,
    pub params: Option<Vec<Param>>,
    pub returns: Option<Vec<Param>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    pub queries: Option<Vec<QueryConfig>>,
}

async fn run_batch_update(client: &mut JobClient, start_index: &mut u64, raw_data_path: &String, batch_id: String) {
    let write_queries = [
        ("insert_comment", "Comment"),
        ("insert_forum", "Forum"),
        ("insert_person", "Person"),
        ("insert_post", "Post"),
        ("insert_comment_hasCreator_person", "Comment_hasCreator_Person"),
        ("insert_comment_hasTag_tag", "Comment_hasTag_Tag"),
        ("insert_comment_isLocatedIn_country", "Comment_isLocatedIn_Country"),
        ("insert_comment_replyOf_comment", "Comment_replyOf_Comment"),
        ("insert_comment_replyOf_post", "Comment_replyOf_Post"),
        ("insert_forum_containerOf_post", "Forum_containerOf_Post"),
        ("insert_forum_hasMember_person", "Forum_hasMember_Person"),
        ("insert_forum_hasModerator_person", "Forum_hasModerator_Person"),
        ("insert_forum_hasTag_tag", "Forum_hasTag_Tag"),
        ("insert_person_hasInterest_tag", "Person_hasInterest_Tag"),
        ("insert_person_isLocatedIn_city", "Person_isLocatedIn_City"),
        ("insert_person_knows_person", "Person_knows_Person"),
        ("insert_person_likes_comment", "Person_likes_Comment"),
        ("insert_person_likes_post", "Person_likes_Post"),
        ("insert_person_studyAt_university", "Person_studyAt_University"),
        ("insert_person_workAt_company", "Person_workAt_Company"),
        ("insert_post_hasCreator_person", "Post_hasCreator_Person"),
        ("insert_post_hasTag_tag", "Post_hasTag_Tag"),
        ("insert_post_isLocatedIn_country", "Post_isLocatedIn_Country"),
        ("delete_comment", "Comment"),
        ("delete_forum", "Forum"),
        ("delete_person_message", "Person"),
        ("delete_person_forum", "Person"),
        ("delete_post", "Post"),
        ("delete_comment_hasCreator_person", "Comment_hasCreator_Person"),
        ("delete_comment_isLocatedIn_country", "Comment_isLocatedIn_Country"),
        ("delete_comment_replyOf_comment", "Comment_replyOf_Comment"),
        ("delete_comment_replyOf_post", "Comment_replyOf_Post"),
        ("delete_forum_containerOf_post", "Forum_containerOf_Post"),
        ("delete_forum_hasMember_person", "Forum_hasMember_Person"),
        ("delete_forum_hasModerator_person", "Forum_hasModerator_Person"),
        ("delete_person_isLocatedIn_city", "Person_isLocatedIn_City"),
        ("delete_person_knows_person", "Person_knows_Person"),
        ("delete_person_likes_comment", "Person_likes_Comment"),
        ("delete_person_likes_post", "Person_likes_Post"),
        ("delete_post_hasCreator_person", "Post_hasCreator_Person"),
        ("delete_post_isLocatedIn_country", "Post_isLocatedIn_Country")
    ];
    for (query_name, dir_name) in write_queries {
        let query_name = query_name.to_string();
        let csv_path = if query_name.starts_with("insert") {
            format!("{}/inserts/dynamic/{}/batch_id={}/*.csv.gz", raw_data_path, dir_name, batch_id)
        } else {
            format!("{}/deletes/dynamic/{}/batch_id={}/*.csv.gz", raw_data_path, dir_name, batch_id)
        };
        let mut params = HashMap::new();
        params.insert("csv_path".to_string(), csv_path);
        let status = client.submitProcedure(*start_index, query_name, params).await;
        if !status.is_ok() {
            break;
        }
        *start_index += 1;
    }
}

async fn run_precompute(client: &mut JobClient, start_index: &mut u64) {
    let precompute_queries = [
        "bi4_precompute",
        "bi6_precompute",
        "bi14_precompute",
        "bi19_precompute",
        "bi20_precompute"
    ];
    for query_name in precompute_queries {
        let query_name = query_name.to_string();
        let mut params = HashMap::new();
        client.submitProcedure(*start_index, query_name, params).await;
        *start_index += 1;
    }
}

async fn run_queries(client: &mut JobClient, start_index: &mut u64, input_dir: &String, inputs_info: &HashMap<String, Vec<String>>) {
    let precompute_queries = [
        ("bi1", "bi-1"),
        ("bi2", "bi-2a"),
        ("bi2", "bi-2b"),
        ("bi3", "bi-3"),
        ("bi4", "bi-4"),
        ("bi5", "bi-5"),
        ("bi6", "bi-6"),
        ("bi7", "bi-7"),
        ("bi8", "bi-8a"),
        ("bi8", "bi-8b"),
        ("bi9", "bi-9"),
        ("bi10", "bi-10a"),
        ("bi10", "bi-10b"),
        ("bi11", "bi-11"),
        ("bi12", "bi-12"),
        ("bi13", "bi-13"),
        ("bi14", "bi-14a"),
        ("bi14", "bi-14b"),
        ("bi15", "bi-15a"),
        ("bi15", "bi-15b"),
        ("bi16", "bi-16a"),
        ("bi16", "bi-16b"),
        ("bi17", "bi-17"),
        ("bi18", "bi-18"),
        ("bi19", "bi-19a"),
        ("bi19", "bi-19b"),
        ("bi20", "bi-20a"),
        ("bi20", "bi-20b"),
    ];
    for (query_name, input_file) in precompute_queries {
        let query_name = query_name.to_string();
        let query_path = format!("{}/{}.csv", input_dir, input_file.to_string());
        let file = File::open(query_path).unwrap();
        let lines = io::BufReader::new(file).lines();
        let mut count = 0;
        for line in lines {
            if count == 0 {
                count +=1;
                continue;
            }
            let line = line.unwrap();
            let mut params = HashMap::new();
            let mut split = line.trim().split("|").collect::<Vec<&str>>();
            if let Some(input_info) = inputs_info.get(&query_name) {
                for (index, name) in input_info.iter().enumerate() {
                    params.insert(name.clone(), split[index].to_string());
                }
            }
            client.submitProcedure(*start_index, query_name.clone(), params).await;
            *start_index += 1;
            count+=1;
            if count > 30 {
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = Config::from_args();
    let endpoint = config.endpoint;
    let mut rpc_client = JobClient::new(endpoint, config.workers).await?;

    let mut input_info = HashMap::<String, Vec<String>>::new();
    let query_config = config.query_config;
    let file = File::open(query_config).unwrap();
    let queries_config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values");
    if let Some(queries) = queries_config.queries {
        for query in queries {
            let query_name = query.name;
            if let Some(params) = query.params {
                let mut inputs = vec![];
                for param in params {
                    inputs.push(param.name);
                }
                input_info.insert(query_name, inputs);
            }
        }
    }


    let batches = [
        "2012-11-29",
        "2012-11-30",
        "2012-12-01",
        "2012-12-02",
        "2012-12-03",
        "2012-12-04",
        "2012-12-05",
        "2012-12-06",
        "2012-12-07",
        "2012-12-08",
        "2012-12-09",
        "2012-12-10",
        "2012-12-11",
        "2012-12-12",
        "2012-12-13",
        "2012-12-14",
        "2012-12-15",
        "2012-12-16",
        "2012-12-17",
        "2012-12-18",
        "2012-12-19",
        "2012-12-20",
        "2012-12-21",
        "2012-12-22",
        "2012-12-23",
        "2012-12-24",
        "2012-12-25",
        "2012-12-26",
        "2012-12-27",
        "2012-12-28",
        "2012-12-29",
        "2012-12-30",
        "2012-12-31",
    ];
    let batches = ["2012-11-29"];

    let mut index = 0;
    for batch_id in batches {
        println!("Start update batch {}", batch_id);
        run_batch_update(&mut rpc_client, &mut index, &config.raw_data_path, batch_id.to_string()).await;
        run_precompute(&mut rpc_client, &mut index).await;
        run_queries(&mut rpc_client, &mut index, &config.input_dir, &input_info).await;
    }
    Ok(())
}