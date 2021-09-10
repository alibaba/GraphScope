/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.tree;

/**
 * Gremlin step
 */
public enum TreeNodeStep {
    CreateGraphStep,
    MaxGraphStep,
    GraphStep,
    EstimateCountStep,
    VertexStep,
    EdgeVertexStep,
    HasStep,
    SelectOneStep,
    SelectStep,
    CountGlobalStep,
    PathStep,
    RepeatStep,
    NoOpBarrierStep,
    TraversalFilterStep,
    WherePredicateStep,
    DedupGlobalStep,
    RepeatEndStep,
    OrderGlobalStep,
    CountByLabelStep,
    IdStep,
    RangeGlobalStep,
    CountLocalStep,
    FoldStep,
    UnfoldStep,
    GroupCountStep,
    IsStep,
    PropertiesStep,
    PropertyMapStep,
    ChooseStep,
    BranchStep,
    SimplePathStep,
    RangeLocalStep,
    OrderLocalStep,
    SackStep,
    SackValueStep,
    LambdaFilterStep,
    NotStep,
    EdgeOtherVertexStep,
    UnionStep,
    EndStep,
    SumGlobalStep,
    AndStep,
    TraversalMapStep,
    ConstantStep,
    MaxGlobalStep,
    MinGlobalStep,
    GroupStep,
    LabelStep,
    PropertyValueStep,
    PropertyKeyStep,
    LambdaMapStep,
    LambdaFlatMapStep,
    SampleGlobalStep,
    SampleLocalStep,
    VertexByModulatingStep,
    OrStep,
    CustomVertexProgramStep,
    VertexWithByStep,
    StoreStep,
    LoopsStep,
    EdgeVertexWithByStep,
    IdentityStep,
    SubgraphStep,
    SideEffectCapStep,
    CacheStep,
    OptionalStep,
    PathFilterStep,
    ConnectedComponentsStep,
    ConnectedComponentVertexProgramStep,
    LabelPropagationStep,
    PageRankStep,
    PageRankVertexProgramStep,
    HitsStep,
    HitsVertexProgramStep,
    AllPathStep,
    ShortestPathStep,
    ShortestPathVertexProgramStep,
    PeerPressureVertexProgramStep,
    OutputStep,
    MathStep,
    TraversalFlatMapStep,
    HasNextStep,
    LpaVertexProgramStep,
    TraversalVertexProgramStep,
    ComputerResultStep,
    OutputVineyardStep,
}
