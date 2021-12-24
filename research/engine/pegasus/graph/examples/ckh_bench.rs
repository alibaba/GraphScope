// use std::time::Instant;
// use futures::StreamExt;
// use klickhouse::*;
//
// #[derive(Row, Debug, Default)]
// struct Predict {
//     model_id: u32,
//     datetime: DateTime,
//     stock: String,
//     score: f64,
// }
//
// #[tokio::main]
// async fn main() {
//     pegasus_common::logs::init_log();
//     let mut opt = ClientOptions::default();
//     opt.default_database = "telescope".to_owned();
//     let client = Client::connect("100.81.128.150:9001", opt)
//         .await
//         .unwrap();
//     let query = "select * from us_cls_predict where model_id in (8,10,11,13,15,17,19,20,22,24,26,28,30)";
//     let start = Instant::now();
//     let mut all_rows = client
//         .query_raw(query)
//         .await
//         .unwrap();
//
//     let mut count = 0u64;
//     while let Some(row) = all_rows.next().await {
//         println!("row {}", row.rows);
//         count += row.rows;
//     }
//     println!("total received {} records, used {:?};", count, start.elapsed());
// }

use std::collections::HashMap;
use std::io::Read;
//use std::str::FromStr;
use std::time::Instant;

use byteorder::{LittleEndian, ReadBytesExt};
use futures_util::stream;
//use klickhouse::Type;
use pegasus_graph::ckh::{ClickHouseClient, Compression, QueryInfo};

#[tokio::main]
async fn main() {
    let mut client = ClickHouseClient::connect("http://100.81.128.150:9010")
        .await
        .unwrap();
    let ids: Vec<u64> = vec![
        26388281136663,
        13194140916590,
        30786326937488,
        24189256988753,
        8796094070929,
        28587304322313,
        28587304792431,
        2125462,
        26388282199678,
        21990235451026,
        21990235597655,
        21990235639208,
        15393165062444,
        15393165285050,
        15393165817581,
        30786328653168,
        17592189206907,
        24189258738599,
        28587305051971,
        28587305148849,
        28587305424253,
        19791211884447,
        19791212578815,
        19791212889775,
        28587302902162,
        28587303415134,
        13194142818954,
        15393163129096,
        19791209384491,
        32985350210506,
        24189259355357,
        26388279558703,
        26388280210284,
        30786326404180,
        32985351505648,
    ];

    let query =
        format!("select p_personid from person where p_personid in {:?} and p_firstname = 'Chau'", ids);
    let mut settings = HashMap::new();
    settings.insert("max_block_size".to_string(), "1000000".to_string());
    // let compression = Compression { algorithm: 1, level: 3};
    let query = QueryInfo {
        query,
        query_id: "".to_string(),
        settings,
        database: "ldbc".to_string(),
        input_data: Default::default(),
        input_data_delimiter: Default::default(),
        output_format: "Native".to_string(),
        external_tables: vec![],
        user_name: "".to_string(),
        password: "".to_string(),
        quota: "".to_string(),
        session_id: "".to_string(),
        session_check: false,
        session_timeout: 0,
        cancel: false,
        next_query_info: false,
        result_compression: None,
        compression_type: "".to_string(),
        compression_level: 0,
    };

    let start = Instant::now();
    let mut results = client.execute_query_with_stream_output(query).await.unwrap().into_inner();
    let mut result_binary = vec![];
    while let Some(res) = results.message().await.unwrap() {
        if let Some(err) = res.exception {
            println!(
                "get error code {:?} name {}, {}, {}",
                err.code, err.name, err.display_text, err.stack_trace
            );
        } else {
            //println!("totals: bytes(len={})", res.totals.len());
            //println!("extremes: bytes(len={})", res.extremes.len());
            if let Some(ref progress) = res.progress {
                println!("Progress(read_rows={}, read_bytes={}, total={})", progress.read_rows, progress.read_bytes, progress.total_rows_to_read)
            }
            if !res.output.is_empty() {
                println!("output: bytes(len={})", res.output.len());
            //     let mut bytes = res.output.as_slice();
            //     let columns = read_var_uint(&mut bytes).unwrap();
            //     let rows = read_var_uint(&mut bytes).unwrap();
            //     println!("columns = {}, rows = {}", columns, rows);
            //     for _ in 0..columns {
            //         let name = read_string(&mut bytes).unwrap();
            //         let type_name = read_string(&mut bytes).unwrap();
            //         let type_ = Type::from_str(&*type_name).unwrap();
            //         println!("read column {}, type {} {:?}", name, type_name, type_);
            //         for _ in 0..rows {
            //             let v = bytes.read_u64::<LittleEndian>().unwrap();
            //             result_binary.push(v);
            //         }
            //     }
                result_binary.push(res.output);
            }
        }
    }
    println!("get {} records cost {:?}",  result_binary.len(), start.elapsed());
}

fn read_var_uint<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut out = 0u64;
    for i in 0..9u64 {
        let mut octet = [0u8];
        reader.read_exact(&mut octet[..])?;
        out |= ((octet[0] & 0x7F) as u64) << (7 * i);
        if (octet[0] & 0x80) == 0 {
            break;
        }
    }
    Ok(out)
}

pub const MAX_STRING_SIZE: usize = 1 << 30;
fn read_string<R: Read>(reader: &mut R) -> std::io::Result<String> {
    let len = read_var_uint(reader)?;
    if len as usize > MAX_STRING_SIZE {
        panic!("string too large");
    }
    let mut buf = Vec::with_capacity(len as usize);
    unsafe { buf.set_len(len as usize) };

    reader.read_exact(&mut buf[..])?;

    Ok(String::from_utf8(buf).unwrap())
}
