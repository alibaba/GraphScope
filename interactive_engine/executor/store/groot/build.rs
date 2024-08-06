extern crate protoc_grpcio;

fn main() {
    let proto_root = "../../../../proto";
    protoc_grpcio::compile_grpc_protos(
        &[
            proto_root.to_owned() + "/groot/sdk/model.proto",
            proto_root.to_owned() + "/groot/sdk/schema.proto",
            proto_root.to_owned() + "/schema_common.proto",
            proto_root.to_owned() + "/error/insight.proto",
        ],
        &[proto_root],
        "./src/db/proto",
        None,
    )
    .expect("Failed to compile gRPC definitions!");
}
