//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::generated::gremlin as pb;
use crate::process::traversal::pop::Pop;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::util::result_downcast::{
    try_downcast_group_count_value, try_downcast_group_key, try_downcast_group_value,
};
use crate::process::traversal::step::MapFuncGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Details, GraphElement, PropKey, Tag, Token};
use crate::{str_to_dyn_error, DynResult, Element, FromPb};
use dyn_type::Object;
use pegasus::api::function::*;
use pegasus::codec::{Encode, WriteExt};
use std::io;

struct SelectStep {
    tag_keys: Vec<TagKey>,
    pop: Pop,
}

#[derive(Clone, Debug)]
pub struct ResultProperty {
    pub tag_entries: Vec<(Tag, OneTagValue)>,
}

#[derive(Clone, Debug, Default)]
pub struct OneTagValue {
    pub graph_element: Option<GraphElement>,
    pub value: Option<Object>,
    pub properties: Option<Vec<(PropKey, Object)>>,
}

impl OneTagValue {
    fn new_element<E: Into<GraphElement>>(e: E) -> Self {
        OneTagValue { graph_element: Some(e.into()), value: None, properties: None }
    }
    fn new_value<T: Into<Object>>(o: T) -> Self {
        OneTagValue { graph_element: None, value: Some(o.into()), properties: None }
    }
    fn new_props(props: Vec<(PropKey, Object)>) -> Self {
        OneTagValue { graph_element: None, value: None, properties: Some(props) }
    }
}

// TODO(yyy)
impl Encode for ResultProperty {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> io::Result<()> {
        unimplemented!()
    }
}

impl ResultProperty {
    pub fn new() -> Self {
        ResultProperty { tag_entries: vec![] }
    }
}

impl MapFunction<Traverser, Traverser> for SelectStep {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        let tag_keys = self.tag_keys.clone();
        let pop = self.pop;
        let mut result = ResultProperty::new();
        for tag_key in tag_keys.iter() {
            let (tag, by_key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            let mut tag_value = OneTagValue::default();
            if let Some(key) = by_key {
                match key {
                    // select("a").by(id/label/prop), where "a" should be a graph_element
                    ByStepOption::OptToken(token) => {
                        let graph_element = input
                            .select_pop_as_element(
                                pop,
                                tag.ok_or(str_to_dyn_error("cannot select head by key"))?,
                            )
                            .ok_or(str_to_dyn_error(&format!(
                                "Select tag {:?} as element error in select step!",
                                tag
                            )))?;
                        match token {
                            // select("a").by(id) or select(id)
                            Token::Id => {
                                tag_value = OneTagValue::new_value(graph_element.id());
                            }
                            // select("a").by(label) or select(label)
                            Token::Label => {
                                tag_value =
                                    OneTagValue::new_value(graph_element.label().as_object());
                            }
                            // select("a").by("name") or select("name")
                            Token::Property(prop_name) => {
                                let prop_value = graph_element.details().get_property(prop_name);
                                if let Some(prop_value) = prop_value {
                                    tag_value = OneTagValue::new_value(
                                        prop_value.try_to_owned().ok_or(str_to_dyn_error(
                                            "Can't get owned property value in select step",
                                        ))?,
                                    );
                                }
                            }
                        }
                    }
                    // select("a").by(valueMap(xxx)), where "a" should be a graph_element
                    ByStepOption::OptProperties(prop_names) => {
                        let graph_element = input
                            .select_pop_as_element(
                                pop,
                                tag.ok_or(str_to_dyn_error("cannot select head by key"))?,
                            )
                            .ok_or(str_to_dyn_error(&format!(
                                "Select tag {:?} as element error!",
                                tag
                            )))?;
                        let mut props = vec![];
                        for prop_name in prop_names {
                            let prop_value = graph_element.details().get_property(prop_name);
                            if let Some(prop_value) = prop_value {
                                props.push((
                                    prop_name.clone(),
                                    prop_value.try_to_owned().ok_or(str_to_dyn_error(
                                        "Can't get owned property value",
                                    ))?,
                                ));
                            }
                        }
                        tag_value = OneTagValue::new_props(props);
                    }
                    // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                    // TODO: support more group key/value types
                    ByStepOption::OptGroupKeys(_) => {
                        let map_object = if let Some(tag) = tag {
                            input.select_pop_as_value(pop, tag).ok_or(str_to_dyn_error(
                                &format!("Select tag {:?} as element error!", tag),
                            ))?
                        } else {
                            input.get_object().ok_or(str_to_dyn_error("should with an object"))?
                        };
                        let get_keys_trav = try_downcast_group_key(map_object).ok_or(
                            str_to_dyn_error(&format!(
                                "downcast group key failed in select step {:?}",
                                map_object
                            )),
                        )?;
                        return Ok(get_keys_trav.clone());
                    }
                    ByStepOption::OptGroupValues(_) => {
                        let map_object = if let Some(tag) = tag {
                            input.select_pop_as_value(pop, tag).ok_or(str_to_dyn_error(
                                &format!("Select tag {:?} as element error!", tag),
                            ))?
                        } else {
                            input.get_object().ok_or(str_to_dyn_error("should with an object"))?
                        };
                        if let Some(count_value) = try_downcast_group_count_value(map_object) {
                            return Ok(Traverser::Object(count_value.into()));
                        } else if let Some(traverser_value) = try_downcast_group_value(map_object) {
                            return Ok(traverser_value.clone());
                        } else {
                            Err(str_to_dyn_error(&format!(
                                "downcast group value failed in select step {:?}",
                                map_object
                            )))?
                        }
                    }
                    ByStepOption::OptSubtraversal => {
                        Err(str_to_dyn_error("Do not support OptSubtraversal in select step"))?;
                    }
                }
            } else {
                // select("a") where "a" is a preserved value, or select("a","b","c") where any tag may refer to a graph element
                // For the case of select("a") where "a" is a graph element, use SelectOneStep instead
                if let Some(tag) = tag {
                    if let Some(object) = input.select_pop_as_value(pop, tag) {
                        tag_value = OneTagValue::new_value(object.clone());
                    } else if let Some(element) = input.select_pop_as_element(pop, tag) {
                        tag_value = OneTagValue::new_element(element.clone());
                    } else {
                        Err(str_to_dyn_error(&format!("Select tag {:?} error in select!", tag)))?
                    }
                } else {
                    Err(str_to_dyn_error("no tag or key is provided in select"))?
                }
            }
            if tag.is_some() {
                result.tag_entries.push((tag.unwrap().clone(), tag_value));
            } else {
                Err(str_to_dyn_error("no tag is provided in select, should be unreachable"))?;
            }
        }
        Ok(Traverser::Object(Object::DynOwned(Box::new(result))))
    }
}

impl MapFuncGen for pb::SelectStep {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let pop_pb = unsafe { std::mem::transmute(self.pop) };
        let pop = Pop::from_pb(pop_pb)?;
        let mut tag_keys = vec![];
        let tag_keys_pb = self.select_keys;
        for tag_key_pb in tag_keys_pb {
            tag_keys.push(TagKey::from_pb(tag_key_pb)?);
        }
        for tag_key in tag_keys.iter() {
            let (tag, key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            if let Some(key) = key {
                match key {
                    ByStepOption::OptToken(_) => {
                        if tag.is_none() {
                            Err(str_to_dyn_error(
                                "Only support select(tag).by(id/label/prop) in select step",
                            ))?;
                        }
                    }
                    ByStepOption::OptProperties(_) => {
                        if tag.is_none() {
                            Err(str_to_dyn_error(
                                "Only support select(tag).by(valueMap) in select step",
                            ))?;
                        }
                    }
                    ByStepOption::OptGroupKeys(opt_group)
                    | ByStepOption::OptGroupValues(opt_group) => {
                        if opt_group.is_some() {
                            Err(str_to_dyn_error(
                                "Have not support select(keys/values).by(id/label/prop) in select step yet",
                            ))?;
                        }
                        if tag_keys.len() > 1 {
                            Err(str_to_dyn_error("Do not support multiple TagKeys when Key is OptGroupKeys/OptGroupValues in select step"))?;
                        }
                    }
                    ByStepOption::OptSubtraversal => {
                        Err(str_to_dyn_error("Do not support OptSubtraversal in select step"))?;
                    }
                }
            } else {
                if tag.is_none() {
                    Err(str_to_dyn_error("No tag or key is provided in select step"))?;
                }
            }
        }

        Ok(Box::new(SelectStep { tag_keys, pop }))
    }
}
