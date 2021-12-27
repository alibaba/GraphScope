use std::fmt::Debug;
use pegasus::api::{Dedup, Filter, Iteration, IterCondition, Map, Unary};
use pegasus::JobConf;
use pegasus::resource::PartitionResource;


pub enum Compare {
    Eq,
    NotEq,
    Great,
    GreatEq,
    Less,
    LessEq,
}

pub enum Value {
    Int(u32),
    Long(u64),
    Float(f64),
    Str(String)
}

pub struct Predict {
    pub cmp : Compare,
    pub value: Value
}

impl Predict {
    pub fn eq_str(value: String) -> Self {
        Predict { cmp: Compare::Eq, value: Value::Str(value) }
    }
}

pub trait FilterStatement: Send + 'static{
    fn filter(&self, vids: &[u64]) -> Box<dyn Iterator<Item = u64>>;
}

pub trait GraphPartition : Send + Sync + 'static {
    fn get_neighbors(&self, src: u64) -> Box<dyn Iterator<Item = u64>>;

    fn prepare_filter_vertex(&self, p: Predict) -> Box<dyn FilterStatement>;
}

pub struct Graph;
impl PartitionResource for Graph {
    type Res = Box<dyn GraphPartition>;

    fn get_resource(&self, par: usize) -> Option<&Self::Res> {
        todo!()
    }

    fn take_resource(&mut self, par: usize) -> Option<Self::Res> {
        todo!()
    }
}



// g.V().hasLabel('person').has('person_id', $id)
// .repeat(both('knows').has('firstName', $name).has('person_id', neq($id)).dedup()).emit().times(3)
// .dedup().limit(20)
// .project('dist', 'person').by(loops()).by(identity())
// .fold()
// .map{ 排序， 取属性 }
fn ic1() {
    let graph = Graph;
    let person_id = 0u64;
    let first_name = "Chau".to_string();
    pegasus::run_with_resources(JobConf::default(), graph,  ||
        move |source, sink| {
            let (emit, leave) = source.input_from(vec![0u64])?
                .iterate_emit(IterCondition::max_iters(3), |start| {
                    let graph = pegasus::resource::get_resource::<Box<dyn GraphPartition>>().unwrap();
                    let stream = start.flat_map(move |src_id| {
                        Ok(graph.get_neighbors(src_id).filter(|id| *id != source))
                    })?
                        .unary("filter", || {
                            let graph = pegasus::resource::get_resource::<Box<dyn GraphPartition>>().unwrap();
                            let mut vec = vec![];
                            let stat = graph.prepare_filter_vertex(Predict::eq_str(first_name));
                            move |input, output| {
                                input.for_each_batch(|batch| {
                                    if !batch.is_empty() {
                                        for id in batch.drain() {
                                            vec.push(id);
                                        }
                                        let result = stat.filter(&vec);
                                        vec.clear();
                                        output.new_session(batch.tag())?.give_iterator(result)?;
                                    }
                                    Ok(())
                                })
                            }
                        })?
                        .repartition(|id| Ok(*id))
                        .dedup()?;
                    stream.copied()
                })?;
            
                .sink_into(sink)
        }
    ).expect("");
}