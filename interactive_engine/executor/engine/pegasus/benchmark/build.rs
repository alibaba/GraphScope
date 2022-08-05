fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/clickhouse_grpc.proto");
    codegen_inplace()
}

#[cfg(feature = "gcip")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    let dir = "src/graph/storage/clickhouse/pb_gen";
    if std::path::Path::new(&dir).exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    std::fs::create_dir(&dir).unwrap();
    tonic_build::configure()
        .build_server(false)
        .out_dir("src/graph/storage/clickhouse/pb_gen")
        .compile(&["proto/clickhouse_grpc.proto"], &["proto"])?;
    Ok(())
}

#[cfg(not(feature = "gcip"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/clickhouse_grpc.proto"], &["proto"])?;
    Ok(())
}
