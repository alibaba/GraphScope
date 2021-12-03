extern crate protoc_grpcio;

fn main() {
    let proto_root = "../../proto";
    protoc_grpcio::compile_grpc_protos(
        &[proto_root.to_owned() + "/model.proto", proto_root.to_owned() + "/sdk/common.proto"],
        &[proto_root],
        "./src/db/proto",
    ).expect("Failed to compile gRPC definitions!");
}
