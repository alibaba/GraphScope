use pegasus::api::{Count, Merge, Sink};
use pegasus::JobConf;

#[test]
fn merge_test() {
    let conf = JobConf::new("merge_test");
    let mut result = pegasus::run(conf, || {
        |input, output| {
            let src1 = input.input_from(0u32..1000)?;
            let src2 = input.input_from(1000..2000)?;
            src1.merge(src2)?.count()?.sink_into(output)
        }
    })
    .expect("submit job failure");

    while let Some(Ok(count)) = result.next() {
        assert_eq!(count, 2000);
    }
}

#[test]
fn copy_merge_test() {
    let conf = JobConf::new("copy_merge_test");
    let mut result = pegasus::run(conf, || {
        |input, output| {
            let src = input.input_from(0u32..1000)?;
            let (s1, s2) = src.copied()?;
            let (s3, s4) = s1.copied()?;
            s2.merge(s4)?.merge(s3)?.count()?.sink_into(output)
        }
    })
        .expect("submit job failure");

    while let Some(Ok(count)) = result.next() {
        assert_eq!(count, 3000);
    }
}