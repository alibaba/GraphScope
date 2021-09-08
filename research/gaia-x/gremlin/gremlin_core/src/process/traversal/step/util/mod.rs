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

pub mod accum;
pub mod functions;
pub mod predicate;
pub mod result_downcast;
pub mod result_process;

pub use predicate::TraverserPredicate;

#[derive(AsRefStr, Copy, Clone)]
pub enum StepSymbol {
    Map = 0x01,
    FlatMap = 0x02,
    Id = 0x03,
    Label = 0x04,
    Identity = 0x05,
    Constant = 0x06,
    V = 0x07,
    E = 0x08,
    To = 0x09,
    Out = 0x0A,
    In = 0x0B,
    Both = 0x0C,
    ToE = 0x0D,
    OutE = 0x0E,
    InE = 0x0F,
    BothE = 0x10,
    ToV = 0x11,
    OutV = 0x12,
    InV = 0x13,
    BothV = 0x14,
    OtherV = 0x15,
    Order = 0x16,
    Properties = 0x17,
    Values = 0x18,
    PropertyMap = 0x19,
    ValueMap = 0x1A,
    ElementMap = 0x1B,
    Select = 0x1C,
    Key = 0x1D,
    Value = 0x1E,
    Path = 0x1F,
    Match = 0x20,
    Sack = 0x21,
    Loops = 0x22,
    Project = 0x23,
    Unfold = 0x24,
    Fold = 0x25,
    Count = 0x26,
    Sum = 0x27,
    Max = 0x28,
    Min = 0x29,
    Mean = 0x2A,
    Group = 0x2B,
    GroupCount = 0x2C,
    Tree = 0x2D,
    AddV = 0x2E,
    AddE = 0x2F,
    From = 0x30,
    Filter = 0x31,
    Or = 0x32,
    And = 0x33,
    Inject = 0x34,
    Dedup = 0x35,
    Where = 0x36,
    Has = 0x37,
    HasNot = 0x38,
    HasLabel = 0x39,
    HasId = 0x3A,
    HasKey = 0x3B,
    HasValue = 0x3C,
    Is = 0x3D,
    Not = 0x3E,
    Range = 0x3F,
    Limit = 0x40,
    Skip = 0x41,
    Tail = 0x42,
    Coin = 0x43,
    IO = 0x44,
    Read = 0x45,
    Write = 0x46,
    TimeLimit = 0x47,
    SimplePath = 0x48,
    CyclicPath = 0x49,
    Sample = 0x4A,
    Drop = 0x4B,
    SideEffect = 0x4C,
    Cap = 0x4D,
    Property = 0x4E,
    Store = 0x4F,
    Aggregate = 0x50,
    SubGraph = 0x51,
    Barrier = 0x52,
    Index = 0x53,
    Local = 0x54,
    Emit = 0x55,
    Repeat = 0x56,
    Until = 0x57,
    Branch = 0x58,
    Union = 0x59,
    Coalesce = 0x5A,
    Choose = 0x5B,
    Optional = 0x5C,
    PageRank = 0x5D,
    PeerPressure = 0x5E,
    ConnectedComponent = 0x5F,
    ShortestPath = 0x60,
    Program = 0x61,
    By = 0x62,
    With = 0x63,
    Times = 0x64,
    As = 0x65,
    Option = 0x66,
}
