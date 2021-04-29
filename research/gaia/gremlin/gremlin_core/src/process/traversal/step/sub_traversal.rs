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

use crate::process::traversal::step::util::result_downcast::try_downcast_group_key;
use crate::process::traversal::traverser::Traverser;
use bit_set::BitSet;
use pegasus::api::function::LeftJoinFunction;
use std::sync::Arc;

pub struct JoinFuncGen {
    func: Arc<dyn LeftJoinFunction<Traverser> + Sync>,
}

impl JoinFuncGen {
    pub fn new(func: Arc<dyn LeftJoinFunction<Traverser> + Sync>) -> Self {
        JoinFuncGen { func }
    }
}

impl JoinFuncGen {
    pub fn gen(&self) -> Box<dyn LeftJoinFunction<Traverser>> {
        let func = self.func.clone();
        Box::new(func)
    }
}

// for e.g., where(out().out().as("a"))
pub struct HasAnyJoin;

impl LeftJoinFunction<Traverser> for HasAnyJoin {
    fn exec(&self, parent: &Traverser, _sub: Traverser) -> Option<Traverser> {
        Some(parent.clone())
    }
}

// for e.g., order().by(out().out().count())
pub struct BySubJoin;

// TODO: throw error
impl LeftJoinFunction<Traverser> for BySubJoin {
    fn exec(&self, parent: &Traverser, sub: Traverser) -> Option<Traverser> {
        let mut parent = parent.clone();
        if let Some(mutp) = parent.get_element_mut() {
            if let Some(obj) = sub.get_object() {
                mutp.attach(obj.clone());
            }
            Some(parent)
        } else {
            None
        }
    }
}

// for e.g., group().by().by(out().out().count()), where we return traverser of ShadeSync{(traverser, traverser)}
pub struct GroupBySubJoin;

// TODO: throw error
impl LeftJoinFunction<Traverser> for GroupBySubJoin {
    fn exec(&self, parent: &Traverser, sub: Traverser) -> Option<Traverser> {
        if let Some(parent_obj) = parent.get_object() {
            try_downcast_group_key(parent_obj)
                .and_then(|first| Some(Traverser::with((first.clone(), sub))))
        } else {
            None
        }
    }
}

// for e.g., select("a").by(out().out().count())
pub struct SelectBySubJoin;

impl LeftJoinFunction<Traverser> for SelectBySubJoin {
    fn exec(&self, parent: &Traverser, sub: Traverser) -> Option<Traverser> {
        if let Some(obj) = sub.get_object() {
            let mut parent = parent.clone();
            parent.split_with_value(obj.clone(), &BitSet::default());
            Some(parent)
        } else {
            None
        }
    }
}
