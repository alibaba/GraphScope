use crate::generated::gremlin as pb;
use crate::generated::protobuf as result_pb;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::group_by::GroupFunctionGen;
use crate::process::traversal::step::order_by::Order;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::result_process::pair_element_to_pb;
use crate::structure::codec::ParseError;
use crate::structure::{Details, Element, Token};
use crate::{str_to_dyn_error, DynResult, FromPb};
use pegasus::api::accum::{AccumFactory, Accumulator, CountAccum, ToListAccum};
use pegasus::api::function::{DynIter, EncodeFunction, FlatMapFunction, FnResult};
use pegasus::codec::{Decode, Encode};
use pegasus::preclude::function::KeyFunction;
use pegasus_common::collections::{Map, MapFactory};
use pegasus_server::factory::{
    CompileResult, DynGroupSink, DynGroupUnfold, DynMap, DynMapFactory, GroupFunction,
};
use prost::Message;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Clone, Copy, Debug)]
pub enum AccumKind {
    Cnt = 0,
    Sum = 1,
    Max = 2,
    Min = 3,
    ToList = 4,
    ToSet = 5,
}

impl FromPb<pb::group_by_step::AccumKind> for AccumKind {
    fn from_pb(accum_kind: pb::group_by_step::AccumKind) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match accum_kind {
            pb::group_by_step::AccumKind::Cnt => Ok(AccumKind::Cnt),
            pb::group_by_step::AccumKind::Sum => Ok(AccumKind::Sum),
            pb::group_by_step::AccumKind::Max => Ok(AccumKind::Max),
            pb::group_by_step::AccumKind::Min => Ok(AccumKind::Min),
            pb::group_by_step::AccumKind::ToList => Ok(AccumKind::ToList),
            pb::group_by_step::AccumKind::ToSet => Ok(AccumKind::ToSet),
        }
    }
}

pub struct GroupByStep {
    tag_key: TagKey,
    map_opt: MapOpt,
    unfold_opt: UnfoldOpt,
}

impl GroupByStep {
    pub fn new(tag_key: TagKey, map_opt: MapOpt) -> Self {
        GroupByStep { tag_key, map_opt, unfold_opt: UnfoldOpt {} }
    }

    pub fn set_unfold_opt(&mut self, unfold_opt: UnfoldOpt) {
        self.unfold_opt = unfold_opt;
    }
}

impl Step for GroupByStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Group
    }
}

struct GroupBy {
    tag_key: TagKey,
    map_opt: MapOpt,
    unfold_opt: UnfoldOpt,
}

pub struct KeyBy {
    pub tag_key: TagKey,
}

#[derive(Clone, Debug)]
pub struct MapOpt {
    pub accum_kind: AccumKind,
    pub order_tag_key: Option<Vec<(TagKey, Order)>>,
}

#[derive(Clone, Debug)]
pub struct UnfoldOpt {}

struct GroupBySink {}

impl GroupFunction<Traverser> for GroupBy {
    fn key(&self) -> CompileResult<Box<dyn KeyFunction<Traverser, Key = Traverser>>> {
        let key_by = KeyBy { tag_key: self.tag_key.clone() };
        Ok(Box::new(key_by) as Box<dyn KeyFunction<Traverser, Key = Traverser>>)
    }

    fn map_factory(&self) -> CompileResult<DynMapFactory<Traverser>> {
        let map_opt = self.map_opt.clone();
        Ok(Box::new(map_opt) as DynMapFactory<Traverser>)
    }

    fn unfold(&self) -> CompileResult<DynGroupUnfold<Traverser>> {
        let unfold_opt = self.unfold_opt.clone();
        Ok(Box::new(unfold_opt) as DynGroupUnfold<Traverser>)
    }

    fn sink(&self) -> CompileResult<DynGroupSink<Traverser>> {
        let group_by_sink = GroupBySink {};
        Ok(Box::new(group_by_sink) as DynGroupSink<Traverser>)
    }
}

impl KeyFunction<Traverser> for KeyBy {
    type Key = Traverser;

    fn select_key(&self, item: &Traverser) -> FnResult<Self::Key> {
        let (tag, by_key) = (self.tag_key.tag.as_ref(), self.tag_key.by_key.as_ref());
        if let Some(key) = by_key {
            match key {
                // "a" or head should be a graph_element
                ByStepOption::OptToken(token) => {
                    let graph_element = item
                        .select_as_element(tag)
                        .ok_or(str_to_dyn_error("should be graph_element"))?;
                    let obj = match token {
                        // by select("a").by(id) or select(id)
                        Token::Id => graph_element.id().into(),
                        // by select("a").by(label) or select(label)
                        Token::Label => graph_element.label().into(),
                        // by select("a").by("name") or select("name")
                        Token::Property(prop_name) => graph_element
                            .details()
                            .get_property(&prop_name)
                            .ok_or(str_to_dyn_error("cannot get property"))?
                            .try_to_owned()
                            .ok_or(str_to_dyn_error("Can't get owned property value"))?,
                    };
                    Ok(Traverser::Object(obj))
                }
                // TODO: by select("a").by(valueMap("name")) or by(valueMap("name"))
                ByStepOption::OptProperties(_) => {
                    Err(str_to_dyn_error("Have not support by valueMap in group by yet"))?
                }
                // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                // TODO(bingqing): by(select("a").by(keys)), by(select("a").by(values)), by(by(keys)) or by(by(values))
                ByStepOption::OptGroupKeys(_) | ByStepOption::OptGroupValues(_) => {
                    Err(str_to_dyn_error("Have not support OptGroupKeys/Values in group by yet"))?
                }
                // by a value computed in sub-traversal
                ByStepOption::OptSubtraversal => {
                    let obj = item
                        .get_element()
                        .ok_or(str_to_dyn_error("should be graph_element"))?
                        .get_attached()
                        .ok_or(str_to_dyn_error("should with attached object"))?
                        .clone();
                    Ok(Traverser::Object(obj))
                }
            }
        } else {
            // by select("a") where "a" is a precomputed value
            if let Some(tag) = tag {
                let obj = item
                    .select_as_value(tag)
                    .ok_or(str_to_dyn_error("Select tag as value error!"))?
                    .clone();
                Ok(Traverser::Object(obj))
            } else {
                // group by self, no need to keep path
                if let Some(element) = item.get_element() {
                    Ok(Traverser::new(element.clone()))
                } else if let Some(object) = item.get_object() {
                    Ok(Traverser::Object(object.clone()))
                } else {
                    unreachable!()
                }
            }
        }
    }
}

struct DefaultHashMap<F: AccumFactory<Traverser>> {
    inner: HashMap<Traverser, F::Target>,
    factory: F,
}

impl<F: AccumFactory<Traverser>> DefaultHashMap<F> {
    pub fn new(factory: F) -> Self {
        DefaultHashMap { inner: HashMap::new(), factory }
    }
}

impl<F: AccumFactory<Traverser>> Debug for DefaultHashMap<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<F: AccumFactory<Traverser>> Map<Traverser, Traverser> for DefaultHashMap<F>
where
    F::Target: Eq + Clone + Decode + Encode + 'static,
{
    type Target = Box<dyn Iterator<Item = (Traverser, Traverser)> + Send>;

    fn insert(&mut self, k: Traverser, v: Traverser) -> Option<Traverser> {
        let accum = self.inner.entry(k).or_insert(self.factory.create());
        accum.accum(v).expect("accum failed");
        None
    }

    fn clear(&mut self) {
        self.inner.clear()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn drain(self: Box<Self>) -> Self::Target {
        Box::new(self.inner.into_iter().map(|(k, v)| (k, Traverser::with(v))))
            as Box<dyn Iterator<Item = (Traverser, Traverser)> + Send>
    }
}

impl MapFactory<Traverser, Traverser> for MapOpt {
    type Target = DynMap<Traverser>;

    fn create(&self) -> Self::Target {
        if let Some(_order_tag_key) = self.order_tag_key.as_ref() {
            // TODO: map for order
        } else {
            // default hash map
        }
        match self.accum_kind {
            AccumKind::Cnt => {
                let default_map = DefaultHashMap::new(CountAccum::new());
                Box::new(default_map) as DynMap<Traverser>
            }
            AccumKind::ToList => {
                let default_map = DefaultHashMap::new(ToListAccum::new());
                Box::new(default_map) as DynMap<Traverser>
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

// TODO: We may able to do some optimization here, e.g., we only need keys/values/keys.prop/values.prop after group by
impl FlatMapFunction<DynMap<Traverser>, Traverser> for UnfoldOpt {
    type Target = DynIter<Traverser>;

    fn exec(&self, input: DynMap<Traverser>) -> FnResult<Self::Target> {
        Ok(Box::new(input.drain().map(|pair| Ok(Traverser::with(pair)))))
    }
}

impl EncodeFunction<DynMap<Traverser>> for GroupBySink {
    fn encode(&self, data: Vec<DynMap<Traverser>>) -> Vec<u8> {
        let mut pairs_encode = vec![];
        for map in data {
            for (k, v) in map.drain() {
                let key_pb = pair_element_to_pb(&k);
                let value_pb = pair_element_to_pb(&v);
                let map_pair_pb =
                    result_pb::MapPair { first: Some(key_pb), second: Some(value_pb) };
                pairs_encode.push(map_pair_pb);
            }
        }
        let map = result_pb::MapArray { item: pairs_encode };
        let result = result_pb::Result { inner: Some(result_pb::result::Inner::MapResult(map)) };
        let mut bytes = vec![];
        result.encode_raw(&mut bytes);
        bytes
    }
}

impl GroupFunctionGen for GroupByStep {
    fn gen(&self) -> DynResult<Box<dyn GroupFunction<Traverser>>> {
        // check tag_key
        let (tag, key) = (self.tag_key.tag.as_ref(), self.tag_key.by_key.as_ref());
        if let Some(key) = key {
            match key {
                ByStepOption::OptProperties(_) => {
                    Err(str_to_dyn_error("Have not support by valueMap in group by yet"))?;
                }
                ByStepOption::OptGroupKeys(_) | ByStepOption::OptGroupValues(_) => {
                    Err(str_to_dyn_error("Have not support OptGroupKeys/Values in group by yet"))?;
                }
                ByStepOption::OptSubtraversal => {
                    if tag.is_some() {
                        Err(str_to_dyn_error(
                            "Do not support tag when OptSubtraversal in group by",
                        ))?;
                    }
                }
                _ => {}
            }
        }
        // check accum_kind
        match self.map_opt.accum_kind {
            AccumKind::Sum | AccumKind::Max | AccumKind::Min | AccumKind::ToSet => {
                Err(str_to_dyn_error("Do not support the accum kind group by yet"))?;
            }
            _ => {}
        }
        Ok(Box::new(GroupBy {
            tag_key: self.tag_key.clone(),
            map_opt: self.map_opt.clone(),
            unfold_opt: self.unfold_opt.clone(),
        }))
    }
}
