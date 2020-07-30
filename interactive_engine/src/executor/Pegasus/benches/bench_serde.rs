//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

//#![feature(test)]
//extern crate test;
//extern crate tiny_dataflow;
//
//use tiny_dataflow::serialize::Serializable;
//use tiny_dataflow::event::Event;
//use tiny_dataflow::{WorkerId, ChannelId};
//
//#[bench]
//fn bench_serde_serializable(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    b.iter(|| {
//        let _n = obj.length_in_bytes();
//        obj.to_bytes(&mut buf.as_mut_slice());
//        Event::from_bytes(buf.as_mut_slice());
//    });
//}
//
//
//#[bench]
//fn bench_serde_abomonation(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    b.iter(|| unsafe {
//        let _n = abomonation::measure(&obj);
//        abomonation::encode(&obj, &mut buf.as_mut_slice()).unwrap();
//        let _cloned = abomonation::decode::<Event>(buf.as_mut_slice()).unwrap().0.clone();
//    });
//}
//
//#[bench]
//fn bench_serde_abomonation_len(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    b.iter(|| {
//        let _n = abomonation::measure(&obj);
//    });
//}
//
//
//#[bench]
//fn bench_serde_abomonation_ser(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    b.iter(|| unsafe {
//        abomonation::encode(&obj, &mut buf.as_mut_slice()).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_abomonation_deser(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    unsafe { abomonation::encode(&obj, &mut buf.as_mut_slice()) }.unwrap();
//    b.iter(|| unsafe {
//        abomonation::decode::<Event>(buf.as_mut_slice()).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_abomonation_deser_clone(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    unsafe { abomonation::encode(&obj, &mut buf.as_mut_slice()) }.unwrap();
//    b.iter(|| unsafe {
//        let _cloned = abomonation::decode::<Event>(buf.as_mut_slice()).unwrap().0.clone();
//    });
//}
//
//#[bench]
//fn bench_serde_bincode(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    b.iter(|| {
//        let _n = bincode::serialized_size(&obj).unwrap();
//        bincode::serialize_into(&mut buf.as_mut_slice(), &obj).unwrap();
//        bincode::deserialize::<Event>(buf.as_slice()).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_bincode_copy(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    let mut buf2 = vec![0; 1024];
//    b.iter(|| {
//        let _n = bincode::serialized_size(&obj).unwrap();
//        bincode::serialize_into(&mut buf.as_mut_slice(), &obj).unwrap();
//        buf2.clear();
//        buf2.extend_from_slice(buf.as_slice());
//        bincode::deserialize::<Event>(buf.as_slice()).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_bincode_len(b: &mut test::Bencher) {
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    b.iter(|| {
//        let _n = bincode::serialized_size(&obj).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_bincode_ser(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    b.iter(|| {
//        bincode::serialize_into(&mut buf.as_mut_slice(), &obj).unwrap();
//    });
//}
//
//#[bench]
//fn bench_serde_bincode_deser(b: &mut test::Bencher) {
////    let obj = DataSet::new([1, 2], vec![9; 64]);
//    let obj = Event::HighWaterMark(WorkerId(1024), ChannelId(1024), [1, 2].into());
//    let mut buf = vec![0; 1024];
//    bincode::serialize_into(&mut buf.as_mut_slice(), &obj).unwrap();
//    b.iter(|| {
//        bincode::deserialize::<Event>(buf.as_slice()).unwrap();
//    });
//}
