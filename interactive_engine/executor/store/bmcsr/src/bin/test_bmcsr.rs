use bmcsr::bmcsr::{BatchMutableCsr, BatchMutableCsrBuilder};
use bmcsr::csr::CsrTrait;
use env_logger;

fn main() {
    env_logger::init();

    let mut csr_builder = BatchMutableCsrBuilder::<usize>::new();
    csr_builder.init(&vec![1, 1, 1, 1, 1], 1.0);
    csr_builder.put_edge(0, 1).unwrap();
    csr_builder.put_edge(1, 2).unwrap();
    csr_builder.put_edge(2, 3).unwrap();
    csr_builder.put_edge(3, 4).unwrap();
    csr_builder.put_edge(4, 0).unwrap();

    let csr = csr_builder.finish().unwrap();
    let serial_path = "./csr.bin".to_string();
    csr.serialize(&serial_path);

    let mut new_csr = BatchMutableCsr::<usize>::new();
    new_csr.deserialize(&serial_path);
}
