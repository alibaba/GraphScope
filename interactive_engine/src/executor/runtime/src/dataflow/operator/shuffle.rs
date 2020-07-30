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

use dataflow::builder::{InputStreamShuffle, ShuffleType};
use dataflow::message::RawMessage;
use maxgraph_common::util::hash::murmur_hash64;
use std::sync::Arc;
use utils::{PROP_ID, PROP_ID_LABEL, PROP_KEY, PROP_VALUE};
use execution::build_empty_router;

type Route = Arc<dyn Fn(&i64) -> u64 + Send + Sync>;
// Only shuffle by id, ignore extra key such as out/values operator
pub struct StreamShuffleType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    shuffle_type: ShuffleType,
    route: Option<Arc<F>>,
    prop_label_id: i32,
    const_flag: bool,
}

impl<F> StreamShuffleType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn forward() -> Self {
        StreamShuffleType {
            shuffle_type: ShuffleType::PIPELINE,
            route: None,
            prop_label_id: 0,
            const_flag: false,
        }
    }

    pub fn broadcast() -> Self {
        StreamShuffleType {
            shuffle_type: ShuffleType::BROADCAST,
            route: None,
            prop_label_id: 0,
            const_flag: false,
        }
    }

    pub fn constant() -> Self {
        StreamShuffleType {
            shuffle_type: ShuffleType::EXCHANGE,
            route: None,
            prop_label_id: 0,
            const_flag: true,
        }
    }

    pub fn exchange(route: Arc<F>,
                    prop_label_id: i32) -> Self {
        StreamShuffleType {
            shuffle_type: ShuffleType::EXCHANGE,
            route: Some(route),
            prop_label_id,
            const_flag: false,
        }
    }
}

impl<F> Clone for StreamShuffleType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn clone(&self) -> Self {
        StreamShuffleType {
            shuffle_type: self.shuffle_type.clone(),
            route: self.route.clone(),
            prop_label_id: self.prop_label_id,
            const_flag: self.const_flag,
        }
    }
}

impl<F> InputStreamShuffle for StreamShuffleType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_shuffle_type(&self) -> &ShuffleType {
        &self.shuffle_type
    }

    fn route(&self, message: &RawMessage) -> u64 {
        if self.const_flag {
            return 0;
        }
        if let Some(ref route) = self.route {
            if self.prop_label_id == 0 {
                return route.as_ref()(&message.get_shuffle_id());
            } else if self.prop_label_id == PROP_ID {
                return route.as_ref()(&message.get_id());
            } else if self.prop_label_id == PROP_ID_LABEL {
                return route.as_ref()(&(message.get_label_id() as i64));
            } else if self.prop_label_id == PROP_KEY || self.prop_label_id == PROP_VALUE {
                if let Some(value) = message.get_value() {
                    if let Ok(entry) = value.get_entry() {
                        if self.prop_label_id == PROP_KEY {
                            return route.as_ref()(&entry.get_key().get_shuffle_id());
                        } else {
                            return route.as_ref()(&entry.get_value().get_shuffle_id());
                        }
                    }
                }
            } else if self.prop_label_id < 0 {
                if let Some(label_entity) = message.get_label_entity_by_id(self.prop_label_id) {
                    return route.as_ref()(&label_entity.get_message().get_shuffle_id());
                } else {
                    return route.as_ref()(&message.get_shuffle_id());
                }
            } else {
                if let Some(prop_entity) = message.get_property(self.prop_label_id) {
                    let empty_fn = Arc::new(build_empty_router());
                    return route.as_ref()(&murmur_hash64(prop_entity.get_value().to_proto(Some(empty_fn.as_ref())).get_payload()));
                }
            }
            return route.as_ref()(&message.get_shuffle_id());
        } else {
            return 0;
        }
    }
}

// first consider the extra key, then consider the id
pub struct StreamShuffleKeyType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    shuffle_type: ShuffleType,
    route_func: Arc<F>,
    prop_label_id: i32,
    const_flag: bool,
}

impl<F> StreamShuffleKeyType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn exchange(route_func: Arc<F>,
                    prop_label_id: i32) -> Self {
        StreamShuffleKeyType {
            shuffle_type: ShuffleType::EXCHANGE,
            route_func,
            prop_label_id,
            const_flag: false,
        }
    }

    pub fn constant(route_func: Arc<F>) -> Self {
        StreamShuffleKeyType {
            shuffle_type: ShuffleType::EXCHANGE,
            route_func,
            prop_label_id: 0,
            const_flag: true,
        }
    }
}

impl<F> Clone for StreamShuffleKeyType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn clone(&self) -> Self {
        StreamShuffleKeyType {
            shuffle_type: self.shuffle_type.clone(),
            route_func: self.route_func.clone(),
            prop_label_id: self.prop_label_id,
            const_flag: self.const_flag,
        }
    }
}

impl<F> InputStreamShuffle for StreamShuffleKeyType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_shuffle_type(&self) -> &ShuffleType {
        &self.shuffle_type
    }

    fn route(&self, message: &RawMessage) -> u64 {
        if let Some(key) = message.get_extend_key_payload() {
            return self.route_func.as_ref()(&murmur_hash64(key));
        } else {
            if self.const_flag {
                return 0;
            }

            if self.prop_label_id == 0 {
                return self.route_func.as_ref()(&message.get_shuffle_id());
            } else if self.prop_label_id == PROP_ID {
                return self.route_func.as_ref()(&message.get_id());
            } else if self.prop_label_id == PROP_ID_LABEL {
                return self.route_func.as_ref()(&(message.get_label_id() as i64));
            } else if self.prop_label_id == PROP_KEY || self.prop_label_id == PROP_VALUE {
                if let Some(value) = message.get_value() {
                    if let Ok(entry) = value.get_entry() {
                        if self.prop_label_id == PROP_KEY {
                            return self.route_func.as_ref()(&entry.get_key().get_shuffle_id());
                        } else {
                            return self.route_func.as_ref()(&entry.get_value().get_shuffle_id());
                        }
                    }
                }
            } else if self.prop_label_id < 0 {
                if let Some(label_entity) = message.get_label_entity_by_id(self.prop_label_id) {
                    return self.route_func.as_ref()(&label_entity.get_message().get_shuffle_id());
                }
            } else {
                if let Some(prop_entity) = message.get_property(self.prop_label_id) {
                    let empty_fn = Arc::new(build_empty_router());
                    return self.route_func.as_ref()(&murmur_hash64(prop_entity.get_value().to_proto(Some(empty_fn.as_ref())).get_payload()));
                }
            }
            return self.route_func.as_ref()(&message.get_shuffle_id());
        }
    }
}

// composite shuffle type with key and id for unary chain
pub struct StreamShuffleCompositeType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    shuffle_by_id: Option<StreamShuffleType<F>>,
    shuffle_by_key: Option<StreamShuffleKeyType<F>>,
}

impl<F> StreamShuffleCompositeType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn composite(shuffle_by_id: Option<StreamShuffleType<F>>,
                     shuffle_by_key: Option<StreamShuffleKeyType<F>>) -> Self {
        StreamShuffleCompositeType {
            shuffle_by_id,
            shuffle_by_key,
        }
    }
}

impl<F> Clone for StreamShuffleCompositeType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn clone(&self) -> Self {
        StreamShuffleCompositeType::composite(self.shuffle_by_id.clone(), self.shuffle_by_key.clone())
    }
}

impl<F> InputStreamShuffle for StreamShuffleCompositeType<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_shuffle_type(&self) -> &ShuffleType {
        if let Some(ref by_id) = self.shuffle_by_id {
            return by_id.get_shuffle_type();
        } else {
            return self.shuffle_by_key.as_ref().unwrap().get_shuffle_type();
        }
    }

    fn route(&self, message: &RawMessage) -> u64 {
        if let Some(ref shuffle_id) = self.shuffle_by_id {
            return shuffle_id.route(message);
        } else {
            return self.shuffle_by_key.as_ref().unwrap().route(message);
        }
    }
}




