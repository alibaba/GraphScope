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

use crate::generated::algebra as pb;

impl From<pb::Project> for pb::logical_plan::Operator {
    fn from(opr: pb::Project) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Project(opr)),
        }
    }
}

impl From<pb::Select> for pb::logical_plan::Operator {
    fn from(opr: pb::Select) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Select(opr)),
        }
    }
}

impl From<pb::Join> for pb::logical_plan::Operator {
    fn from(opr: pb::Join) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Join(opr)),
        }
    }
}

impl From<pb::Union> for pb::logical_plan::Operator {
    fn from(opr: pb::Union) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Union(opr)),
        }
    }
}

impl From<pb::GroupBy> for pb::logical_plan::Operator {
    fn from(opr: pb::GroupBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::GroupBy(opr)),
        }
    }
}

impl From<pb::OrderBy> for pb::logical_plan::Operator {
    fn from(opr: pb::OrderBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::OrderBy(opr)),
        }
    }
}

impl From<pb::Dedup> for pb::logical_plan::Operator {
    fn from(opr: pb::Dedup) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Dedup(opr)),
        }
    }
}

impl From<pb::Unfold> for pb::logical_plan::Operator {
    fn from(opr: pb::Unfold) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Unfold(opr)),
        }
    }
}

impl From<pb::Apply> for pb::logical_plan::Operator {
    fn from(opr: pb::Apply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Apply(opr)),
        }
    }
}

impl From<pb::SegmentApply> for pb::logical_plan::Operator {
    fn from(opr: pb::SegmentApply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::SegApply(opr)),
        }
    }
}

impl From<pb::Source> for pb::logical_plan::Operator {
    fn from(opr: pb::Source) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Source(opr)),
        }
    }
}

impl From<pb::EdgeExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::EdgeExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Edge(opr)),
        }
    }
}

impl From<pb::PathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::PathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Path(opr)),
        }
    }
}

impl From<pb::ShortestPathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::ShortestPathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::ShortestPath(opr)),
        }
    }
}
