extern crate protoc_grpcio;

fn main() {
    let proto_root = "../../v2/src/main/proto";
    protoc_grpcio::compile_grpc_protos(
        &[proto_root.to_owned() + "/common.proto"],
        &[proto_root],
        "./src/db/proto"
    ).expect("Failed to compile gRPC definitions!");

}
