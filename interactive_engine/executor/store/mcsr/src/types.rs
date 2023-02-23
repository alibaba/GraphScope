pub type DefaultId = usize;
pub type InternalId = usize;
pub type LabelId = u8;

pub static INVALID_LABEL_ID: LabelId = 0xff;
pub static VERSION: &str = env!("CARGO_PKG_VERSION");
pub static NAME: &str = env!("CARGO_PKG_NAME");

pub const FILE_SCHEMA: &'static str = "schema.json";
pub const DIR_GRAPH_SCHEMA: &'static str = "graph_schema";

pub const DIR_BINARY_DATA: &'static str = "graph_data_bin";
pub const DIR_SPLIT_RAW_DATA: &'static str = "graph_split_raw";
