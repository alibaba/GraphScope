use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use clap::{App, Arg};
use mcsr::col_table::ColTable;
use mcsr::io::import;
use mcsr::mcsr::MutableCsr;
use mcsr::vertex_map::VertexMap;
use mcsr::{
    scsr::SingleCsr,
    types::{DefaultId, InternalId, NAME, VERSION},
};

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("graph_data_path")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("output_path")
                .short("o")
                .long_help("The directory to place output files")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("type")
                .short("t")
                .long_help("The operaton type")
                .required(true)
                .takes_value(true)
                .index(3),
        ])
        .get_matches();

    let graph_data_path = matches
        .value_of("graph_data_path")
        .unwrap()
        .to_string();
    let output_path = matches
        .value_of("output_path")
        .unwrap()
        .to_string();
    let t = matches
        .value_of("type")
        .unwrap()
        .parse::<i32>()
        .unwrap();

    if t == 0 {
        // scsr

        let csr = import::<SingleCsr<InternalId>, _>(graph_data_path.clone()).unwrap();

        csr.serialize(&output_path);

        let now = Instant::now();
        let new_csr1 = import::<SingleCsr<InternalId>, _>(graph_data_path.clone()).unwrap();
        println!("scsr old: {}", now.elapsed().as_secs());

        let now = Instant::now();
        let mut new_csr2 = SingleCsr::<InternalId>::new();
        new_csr2.deserialize(&output_path);
        println!("scsr new: {}", now.elapsed().as_secs());

        if !new_csr1.is_same(&new_csr2) {
            println!("not same");
        }
    } else if t == 1 {
        // mcsr

        let csr = import::<MutableCsr<InternalId>, _>(graph_data_path.clone()).unwrap();

        csr.serialize(&output_path);

        let now = Instant::now();
        let new_csr1 = import::<MutableCsr<InternalId>, _>(graph_data_path.clone()).unwrap();
        println!("mcsr old: {}", now.elapsed().as_secs());

        let now = Instant::now();
        let mut new_csr2 = MutableCsr::<InternalId>::new();
        new_csr2.deserialize(&output_path);
        println!("mcsr new: {}", now.elapsed().as_secs());

        if !new_csr1.is_same(&new_csr2) {
            println!("not same");
        }
    } else if t == 2 {
        // table

        let table = ColTable::import(graph_data_path.clone()).unwrap();

        table.serialize_table(&output_path);

        let now = Instant::now();
        let table1 = ColTable::import(graph_data_path.clone()).unwrap();
        println!("table old: {}", now.elapsed().as_secs());

        let now = Instant::now();
        let mut table2 = ColTable::new(vec![]);
        table2.deserialize_table(&output_path);
        println!("table new: {}", now.elapsed().as_secs());

        if !table1.is_same(&table2) {
            println!("not same");
        }
    } else if t == 3 {
        let mut vertex_map = VertexMap::<DefaultId, InternalId>::new(8_usize);
        let partition_dir = PathBuf::from_str(&graph_data_path).unwrap();
        let now = Instant::now();
        vertex_map
            .import_native(&partition_dir.join("vm.native"))
            .unwrap();
        println!("native vm: {}", now.elapsed().as_secs());
        let now = Instant::now();
        vertex_map
            .import_corner(&partition_dir.join("vm.corner"))
            .unwrap();
        println!("corner vm: {}", now.elapsed().as_secs());
        let now = Instant::now();

        vertex_map.serialize(&output_path);

        println!("vm serialize: {}", now.elapsed().as_secs());

        let now = Instant::now();
        let mut vm2 = VertexMap::<DefaultId, InternalId>::new(8_usize);
        vm2.deserialize(&output_path);

        println!("vm deserialize: {}", now.elapsed().as_secs());
        if !vertex_map.is_same(&vm2) {
            println!("not same");
        }
    }
}
