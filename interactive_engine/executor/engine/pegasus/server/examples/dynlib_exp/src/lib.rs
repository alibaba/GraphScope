use pegasus::api::{Map, Sink};
use pegasus::BuildJobError;
use pegasus::Worker;

#[no_mangle]
pub extern "Rust" fn build_job(source: &[u8], worker: &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError> {
    let src = source.to_vec();
    worker.dataflow(|input, output| {
        input.input_from(Some(src))?
            .map(|mut bytes| {
                for b in bytes.iter_mut() {
                    *b += 1;
                }
                Ok(bytes)
            })?
            .sink_into(output)
    })
}