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
use crate::process::traversal::step::util::result_downcast::try_downcast_group_key;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{BinaryFunction, FnResult};

#[enum_dispatch]
pub trait TraverserLeftJoinGen {
    fn gen_subtask(
        self,
    ) -> DynResult<Box<dyn BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>>>>;
}

impl TraverserLeftJoinGen for pb::SubTaskJoiner {
    fn gen_subtask(
        self,
    ) -> DynResult<Box<dyn BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>>>> {
        match self.inner {
            Some(pb::sub_task_joiner::Inner::WhereJoiner(_)) => Ok(Box::new(HasAnySubJoin)),
            Some(pb::sub_task_joiner::Inner::ByJoiner(_)) => Ok(Box::new(OrderBySubJoin)),
            Some(pb::sub_task_joiner::Inner::GroupValueJoiner(_)) => Ok(Box::new(GroupBySubJoin)),
            Some(pb::sub_task_joiner::Inner::SelectByJoiner(_)) => Ok(Box::new(SelectBySubJoin)),
            None => Err(str_to_dyn_error("join information not found;"))?,
        }
    }
}

// for e.g., where(out().out().as("a"))
pub struct HasAnySubJoin;

impl BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>> for HasAnySubJoin {
    fn exec(&self, parent: Traverser, sub: Vec<Traverser>) -> FnResult<Option<Traverser>> {
        if sub.is_empty() {
            Ok(None)
        } else {
            Ok(Some(parent))
        }
    }
}

// for e.g., order().by(out().out().count())
pub struct OrderBySubJoin;

impl BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>> for OrderBySubJoin {
    fn exec(&self, mut parent: Traverser, sub: Vec<Traverser>) -> FnResult<Option<Traverser>> {
        if let Some(mutp) = parent.get_element_mut() {
            if let Some(sub) = sub.get(0) {
                if let Some(obj) = sub.get_object() {
                    mutp.attach(obj.clone());
                }
                Ok(Some(parent))
            } else {
                Err(str_to_dyn_error("Subquery does not output any results in OrderBySubJoin"))
            }
        } else {
            Err(str_to_dyn_error("get_element failed in OrderBySubJoin"))
        }
    }
}

// for e.g., group().by().by(out().out().count()), where we return traverser of ShadeSync{(traverser, traverser)}
pub struct GroupBySubJoin;

impl BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>> for GroupBySubJoin {
    fn exec(&self, parent: Traverser, sub: Vec<Traverser>) -> FnResult<Option<Traverser>> {
        if let Some(parent_obj) = parent.get_object() {
            if let Some(first) = try_downcast_group_key(parent_obj) {
                if let Some(sub) = sub.get(0) {
                    Ok(Some(Traverser::with((first.clone(), sub.clone()))))
                } else {
                    Err(str_to_dyn_error("Subquery does not output any results in GroupBySubJoin"))
                }
            } else {
                Err(str_to_dyn_error("try_downcast_group_key failed in GroupBySubJoin"))
            }
        } else {
            Err(str_to_dyn_error("get_obj failed in GroupBySubJoin"))
        }
    }
}

// for e.g., select("a").by(out().out().count())
pub struct SelectBySubJoin;

impl BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>> for SelectBySubJoin {
    fn exec(&self, mut parent: Traverser, sub: Vec<Traverser>) -> FnResult<Option<Traverser>> {
        if let Some(sub) = sub.get(0) {
            if let Some(obj) = sub.get_object() {
                parent.split_with_value(obj.clone(), &BitSet::default());
                Ok(Some(parent))
            } else {
                Err(str_to_dyn_error("get_obj failed in SelectBySubJoin"))
            }
        } else {
            Err(str_to_dyn_error("Subquery does not output any results in SelectBySubJoin"))
        }
    }
}
