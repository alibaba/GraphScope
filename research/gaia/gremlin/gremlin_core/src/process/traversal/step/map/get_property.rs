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

use crate::object::Object;
use crate::process::traversal::pop::Pop;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::util::result_downcast::{
    try_downcast_group_by_value, try_downcast_group_key,
};
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Details, Tag, Token};
use crate::Element;
use pegasus::api::function::*;
use pegasus_server::AnyData;
use std::collections::HashMap;

pub struct GetPropertyStep {
    tag_keys: Vec<TagKey>,
    pop: Pop,
}

impl GetPropertyStep {
    pub fn new(tag_keys: Vec<TagKey>, pop: Pop) -> Self {
        GetPropertyStep { tag_keys, pop }
    }
}

#[derive(Clone, Debug)]
pub struct ResultProperty {
    pub properties: HashMap<Tag, Vec<(String, Object)>>,
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

    fn add_tag(&mut self, _: String) {
        unimplemented!();
    }

    fn tags(&self) -> &[String] {
        &[]
    }
}

// TODO: modify output
// TODO(bingqing): throw exception instead of panic(), expect() or unwrap()
impl MapFuncGen for GetPropertyStep {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let tag_keys = self.tag_keys.clone();
        let pop = self.pop;
        let func = map!(move |item: Traverser| {
            let mut result = ResultProperty::new();
            let mut get_keys_opt = false;
            let mut get_values_opt = false;
            let mut get_keys_trav = Traverser::Unknown(0.into());
            let mut get_values_trav_vec = vec![];
            for tag_key in tag_keys.iter() {
                let (tag, by_key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
                let mut properties: Vec<(String, Object)> = vec![];
                if let Some(key) = by_key {
                    match key {
                        // "a" or head should be a graph_element
                        ByStepOption::OptToken(token) => {
                            let graph_element = item
                                .select_pop_as_element(pop, tag.expect("cannot select head by key"))
                                .expect(&format!("Select tag {:?} as element error!", tag));
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
                                    let prop_value =
                                        graph_element.details().get_property(prop_name);
                                    if let Some(prop_value) = prop_value {
                                        properties.push((
                                            prop_name.clone(),
                                            prop_value
                                                .try_to_owned()
                                                .expect("Can't get owned property value"),
                                        ));
                                    }
                                }
                            }
                        }
                        ByStepOption::OptProperties(prop_names) => {
                            let graph_element = item
                                .select_pop_as_element(pop, tag.expect("cannot select head by key"))
                                .expect(&format!("Select tag {:?} as element error!", tag));
                            for prop_name in prop_names {
                                let prop_value = graph_element.details().get_property(prop_name);
                                if let Some(prop_value) = prop_value {
                                    properties.push((
                                        prop_name.clone(),
                                        prop_value
                                            .try_to_owned()
                                            .expect("Can't get owned property value"),
                                    ));
                                }
                            }
                        }
                        // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                        // TODO: support more group key/value types
                        ByStepOption::OptGroupKeys => {
                            let map_object = if let Some(tag) = tag {
                                item.select_pop_as_value(pop, tag)
                                    .expect(&format!("Select tag {:?} as value error!", tag))
                            } else {
                                item.get_object().expect("should with an object")
                            };
                            get_keys_opt = true;
                            get_keys_trav = try_downcast_group_key(map_object)
                                .expect(&format!("downcast group key failed {:?}", map_object));
                        }
                        ByStepOption::OptGroupValues => {
                            let map_object = if let Some(tag) = tag {
                                item.select_pop_as_value(pop, tag)
                                    .expect(&format!("Select tag {:?} as value error!", tag))
                            } else {
                                item.get_object().expect("should with an object")
                            };
                            get_values_opt = true;
                            get_values_trav_vec = try_downcast_group_by_value(map_object)
                                .expect(&format!("downcast group value failed {:?}", map_object));
                        }
                        ByStepOption::OptSubtraversal => panic!("do not support SubKey in select"),
                        ByStepOption::OptInvalid => panic!("invalid by option"),
                    }
                } else {
                    // select("a") where "a" is a preserved value
                    if let Some(tag) = tag {
                        let object = item
                            .select_pop_as_value(pop, tag)
                            .expect(&format!("Select tag {:?} as value error!", tag));
                        properties.push(("computed_value".to_string(), object.clone()));
                    } else {
                        panic!("no tag or key is provided in select");
                    }
                }
                if tag.is_some() {
                    result.properties.insert(tag.unwrap().clone(), properties);
                } else {
                    result.properties.insert("".to_string(), properties);
                }
            }
            if get_keys_opt {
                Ok(get_keys_trav)
            } else if get_values_opt {
                Ok(Traverser::with(get_values_trav_vec))
            } else {
                Ok(Traverser::Unknown(Object::UnknownOwned(Box::new(result))))
            }
        });
        Box::new(func)
    }
}
