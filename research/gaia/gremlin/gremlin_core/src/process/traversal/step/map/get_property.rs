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

use crate::common::object::Object;
use crate::process::traversal::pop::Pop;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::util::result_downcast::{
    try_downcast_group_count_value, try_downcast_group_key, try_downcast_group_value,
};
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Details, Tag, Token};
use crate::{str_to_dyn_error, DynResult, Element};
use pegasus::api::function::*;
use pegasus::codec::{Encode, WriteExt};
use std::collections::HashMap;
use std::io;

pub struct GetPropertyStep {
    tag_keys: Vec<TagKey>,
    pop: Pop,
}

impl GetPropertyStep {
    pub fn new(tag_keys: Vec<TagKey>, pop: Pop) -> Self {
        GetPropertyStep { tag_keys, pop }
    }
}

pub struct GetPropertyFunc {
    tag_keys: Vec<TagKey>,
    pop: Pop,
}

#[derive(Clone, Debug)]
pub struct ResultProperty {
    pub properties: HashMap<Tag, Vec<(String, Object)>>,
}

// TODO(yyy)
impl Encode for ResultProperty {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> io::Result<()> {
        unimplemented!()
    }
}

impl ResultProperty {
    pub fn new() -> Self {
        ResultProperty { properties: HashMap::new() }
    }
}

impl Step for GetPropertyStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Select
    }
}

// TODO: modify output
impl MapFunction<Traverser, Traverser> for GetPropertyFunc {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        let tag_keys = self.tag_keys.clone();
        let pop = self.pop;
        let mut result = ResultProperty::new();
        for tag_key in tag_keys.iter() {
            let (tag, by_key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            let mut properties: Vec<(String, Object)> = vec![];
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
                                properties.push(("id".to_string(), graph_element.id().into()));
                            }
                            // select("a").by(label) or select(label)
                            Token::Label => {
                                properties
                                    .push(("label".to_string(), graph_element.label().into()));
                            }
                            // select("a").by("name") or select("name")
                            Token::Property(prop_name) => {
                                let prop_value = graph_element.details().get_property(prop_name);
                                if let Some(prop_value) = prop_value {
                                    properties.push((
                                        prop_name.clone(),
                                        prop_value.try_to_owned().ok_or(str_to_dyn_error(
                                            "Can't get owned property value in select step",
                                        ))?,
                                    ));
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
                        for prop_name in prop_names {
                            let prop_value = graph_element.details().get_property(prop_name);
                            if let Some(prop_value) = prop_value {
                                properties.push((
                                    prop_name.clone(),
                                    prop_value.try_to_owned().ok_or(str_to_dyn_error(
                                        "Can't get owned property value",
                                    ))?,
                                ));
                            }
                        }
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
                        return Ok(get_keys_trav);
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
                            return Ok(traverser_value);
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
                        properties.push(("computed_value".to_string(), object.clone()));
                    } else if let Some(element) = input.select_pop_as_element(pop, tag) {
                        properties.push(("graph_element".to_string(), element.id().into()));
                    } else {
                        Err(str_to_dyn_error(&format!("Select tag {:?} error in select!", tag)))?
                    }
                } else {
                    Err(str_to_dyn_error("no tag or key is provided in select"))?
                }
            }
            if tag.is_some() {
                result.properties.insert(tag.unwrap().clone(), properties);
            } else {
                result.properties.insert(0, properties);
            }
        }
        Ok(Traverser::Object(Object::UnknownOwned(Box::new(result))))
    }
}

impl MapFuncGen for GetPropertyStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        for tag_key in self.tag_keys.iter() {
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
                        if self.tag_keys.len() > 1 {
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
        Ok(Box::new(GetPropertyFunc { tag_keys: self.tag_keys.clone(), pop: self.pop }))
    }
}
