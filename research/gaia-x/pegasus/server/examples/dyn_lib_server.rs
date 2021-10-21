// #[macro_use]
// extern crate log;
//
// use std::collections::HashMap;
// use std::fmt::Debug;
// use std::io::Write;
// use std::ops::{Deref, DerefMut};
// use std::path::PathBuf;
// use std::sync::{Arc, RwLock};
//
// use libloading::os::unix::Symbol;
// use libloading::Library;
// use pegasus::api::{Iteration, Map, Reduce, Sink, Source};
// use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
// use pegasus::result::ResultSink;
// use pegasus::stream::{SingleItem, Stream};
// use pegasus::BuildJobError;
// use pegasus_server::pb::aggregation_operator::Kind;
// use pegasus_server::pb::operator::OpKind;
// use pegasus_server::pb::{
//     BinaryResource, CommunicateKind, ExecutionPlan, Inputs, MapKind, ReduceKind, UnfoldKind,
// };
// use pegasus_server::service::JobParser;
// use pegasus_server::JobRequest;
//
// fn main() {}
//
// struct VectorService {
//     lib_path: PathBuf,
//     libs: RwLock<HashMap<u64, Arc<Library>>>,
// }
//
// impl VectorService {
//     fn parse_input(&self, _res: Option<&Inputs>) -> Box<dyn Iterator<Item = Vector> + Send + 'static> {
//         todo!()
//     }
//
//     fn load_lib(&self, job_id: u64) -> Result<(), BuildJobError> {
//         let name = format!("libjob-{}.so", job_id);
//         let mut path = self.lib_path.clone();
//         path.push(name);
//         match unsafe { libloading::Library::new(path) } {
//             Ok(lib) => {
//                 let mut libs = self
//                     .libs
//                     .write()
//                     .expect("fetch lib write lock failure");
//                 libs.insert(job_id, Arc::new(lib));
//             }
//             Err(err) => {
//                 let msg = format!("load library of job {} failure {}", job_id, err);
//                 error!("{}", msg);
//                 Err(msg)?
//             }
//         }
//         Ok(())
//     }
//
//     fn get_lib(&self, job_id: u64) -> Option<Arc<Library>> {
//         let libs = self
//             .libs
//             .read()
//             .expect("fetch lib read lock failure");
//         libs.get(&job_id).map(|b| b.clone())
//     }
//
//     fn get_symbol<T>(&self, lib: &Arc<Library>, symbol: &[u8]) -> Result<Symbol<T>, BuildJobError> {
//         match unsafe { lib.get::<T>(symbol) } {
//             Ok(sym) => unsafe { Ok(sym.into_raw()) },
//             Err(e) => {
//                 error!("get symbol failure: {}", e);
//                 Err(format!("get symbol failure: {}", e))?
//             }
//         }
//     }
//
//     fn assemble(
//         &self, plan: &ExecutionPlan, lib: &Arc<Library>, stream: Stream<Vector>,
//     ) -> Result<StreamOrSingle, BuildJobError> {
//         let mut stream = stream;
//         for op in plan.plan.iter() {
//             let kind = op.op_kind.as_ref().expect("");
//             match kind {
//                 OpKind::Comm(comm) => match CommunicateKind::from_i32(comm.kind) {
//                     Some(CommunicateKind::Exchange) => {
//                         if let Some(res) = comm.resource.as_ref() {
//                             let symbol = res.resource.as_slice();
//                             let func: Symbol<unsafe extern "C" fn(*const Vector) -> u64> =
//                                 self.get_symbol(&lib, symbol)?;
//                             stream =
//                                 stream.repartition(move |item| unsafe { Ok(func(item as *const Vector)) });
//                         } else {
//                             Err("exchange function lost")?
//                         }
//                     }
//                     Some(CommunicateKind::Broadcast) => {
//                         stream = stream.broadcast();
//                     }
//                     Some(CommunicateKind::Aggregate) => {
//                         stream = stream.aggregate();
//                     }
//                     None => Err("unrecognized communication operator;")?,
//                 },
//                 OpKind::Map(mapper) => match MapKind::from_i32(mapper.kind) {
//                     Some(MapKind::Map) => {
//                         if let Some(res) = mapper.resource.as_ref() {
//                             let symbol = res.resource.as_slice();
//                             let func: Symbol<unsafe extern "C" fn(Vector) -> Vector> =
//                                 self.get_symbol(&lib, symbol)?;
//                             stream = stream.map(move |item| {
//                                 let mapped = unsafe { func(item) };
//                                 Ok(mapped)
//                             })?;
//                         }
//                     }
//                     Some(MapKind::Flatmap) => {
//                         todo!()
//                     }
//                     Some(MapKind::FilterMap) => {
//                         todo!()
//                     }
//                     Some(MapKind::Filter) => {
//                         todo!()
//                     }
//                     None => Err("unrecognized map operator;")?,
//                 },
//                 OpKind::Agg(aggregation) => {
//                     stream = {
//                         let single_item = match &aggregation.kind {
//                             Some(Kind::Fold(_f)) => {
//                                 todo!()
//                             }
//                             Some(Kind::Reduce(r)) => match ReduceKind::from_i32(*r) {
//                                 Some(ReduceKind::Sum) => {
//                                     todo!()
//                                 }
//                                 Some(ReduceKind::Max) => {
//                                     todo!()
//                                 }
//                                 Some(ReduceKind::Min) => {
//                                     todo!()
//                                 }
//                                 Some(ReduceKind::CustomReduce) => {
//                                     if let Some(res) = aggregation.resource.as_ref() {
//                                         let symbol = res.resource.as_slice();
//                                         let func: Symbol<unsafe extern "C" fn(Vector, Vector) -> Vector> =
//                                             self.get_symbol(&lib, symbol)?;
//                                         stream.reduce(move || {
//                                             let f = func.clone();
//                                             move |a, b| Ok(unsafe { f(a, b) })
//                                         })?
//                                     } else {
//                                         Err("reduce resources lost")?
//                                     }
//                                 }
//                                 None => Err("unrecognized reduce operator")?,
//                             },
//                             None => Err("unrecognized aggregation operator;")?,
//                         };
//                         if let Some(ref unfold) = aggregation.unfold {
//                             match UnfoldKind::from_i32(unfold.kind) {
//                                 Some(UnfoldKind::Direct) => single_item.into_stream()?,
//                                 Some(UnfoldKind::FlatMap) => {
//                                     todo!()
//                                 }
//                                 None => Err("unrecognized unfold operator")?,
//                             }
//                         } else {
//                             return Ok(StreamOrSingle::Single(single_item));
//                         }
//                     }
//                 }
//                 OpKind::Iter(iteration) => {
//                     if let Some(ref _until) = iteration.until {
//                         todo!()
//                     } else {
//                         if let Some(ref plan) = iteration.plan {
//                             stream = stream.iterate(iteration.max_iters, |start| {
//                                 self.assemble(plan, &lib, start)?.into_stream()
//                             })?;
//                         } else {
//                             Err("iteration body lost")?
//                         }
//                     }
//                 }
//             }
//         }
//         Ok(StreamOrSingle::Stream(stream))
//     }
// }
//
// fn create_library(job_id: u64, mut path: PathBuf, res: &BinaryResource) -> std::io::Result<()> {
//     path.push(format!("/libjob-{}", job_id));
//
//     path.with_extension("so");
//     let path = path.as_path();
//     let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
//     f.write_all(&res.resource)?;
//     f.flush()?;
//     Ok(())
// }
//
// impl JobParser<Vector, Vector> for VectorService {
//     fn parse(
//         &self, plan: &JobRequest, input: Source<Vector>, output: ResultSink<Vector>,
//     ) -> Result<(), BuildJobError> {
//         let worker_id = pegasus::get_current_worker();
//         let job_id = worker_id.job_id;
//         if worker_id.index == 0 {
//             if let Some(res) = plan.resource.as_ref() {
//                 PathBuf::new();
//                 let path = self.lib_path.clone();
//                 create_library(job_id, path, res).expect("create library file failure");
//                 self.load_lib(job_id)?;
//             }
//         }
//
//         let lib = self.get_lib(job_id);
//         if lib.is_none() {
//             return Err(format!("library for job {} not found;", job_id))?;
//         }
//         let lib = lib.unwrap();
//
//         let src = self.parse_input(plan.source.as_ref());
//         let stream = input.input_from(src)?;
//         if let Some(ref plan) = plan.plan {
//             match self.assemble(plan, &lib, stream)? {
//                 StreamOrSingle::Stream(stream) => stream.sink_into(output),
//                 StreamOrSingle::Single(single) => single.sink_into(output),
//             }
//         } else {
//             stream.sink_into(output)
//         }
//     }
// }
//
// enum StreamOrSingle {
//     Stream(Stream<Vector>),
//     Single(SingleItem<Vector>),
// }
//
// impl StreamOrSingle {
//     fn into_stream(self) -> Result<Stream<Vector>, BuildJobError> {
//         match self {
//             StreamOrSingle::Stream(s) => Ok(s),
//             StreamOrSingle::Single(_) => Err("expected stream but get single item")?,
//         }
//     }
// }
//
// #[repr(C)]
// #[derive(Debug, Clone)]
// struct Vector {
//     inner: Vec<f64>,
// }
//
// impl Encode for Vector {
//     fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
//         todo!()
//     }
// }
//
// impl Decode for Vector {
//     fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
//         todo!()
//     }
// }
//
// impl Deref for Vector {
//     type Target = Vec<f64>;
//
//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }
//
// impl DerefMut for Vector {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.inner
//     }
// }
fn main() {}
