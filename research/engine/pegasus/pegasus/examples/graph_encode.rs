use pegasus::api::{Sink, Map};
use pegasus::errors::JobSubmitError;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};

fn main() {
    std::env::set_var("GRAPH_SPLIT", "\t");
    let input_path = std::env::args().nth(1).unwrap();
    pegasus_graph::encode(&input_path, |_| true)
        .expect("encode input graph error.");
}