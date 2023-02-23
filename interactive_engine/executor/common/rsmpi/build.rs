fn main() {
    let is_msmpi = match build_probe_mpi::probe() {
        Ok(lib) => lib.version == "MS-MPI",
        _ => false,
    };

    if is_msmpi {
        println!("cargo:rustc-cfg=msmpi");
    }
}
