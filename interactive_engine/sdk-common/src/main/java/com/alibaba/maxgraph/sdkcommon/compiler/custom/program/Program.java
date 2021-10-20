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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.program;

import org.apache.tinkerpop.gremlin.process.traversal.P;

public class Program {
    public static ConnectedComponentVertexProgram cc() {
        return new ConnectedComponentVertexProgram();
    }

    public static GraphConnectedComponentVertexProgram graphCC() {
        return new GraphConnectedComponentVertexProgram();
    }

    public static GraphPageRankVertexProgram pageRank(){
        return new GraphPageRankVertexProgram();
    }

    public static GraphHitsVertexProgram hits() { return new GraphHitsVertexProgram();}

    public static GraphLpaVertexProgram lpa() {return new GraphLpaVertexProgram();}


    public static CustomVertexProgram run(String code) {
        return new CustomVertexProgram(code);
    }

    public static VertexRatioProgram ratio(P predicate) {
        return new VertexRatioProgram(predicate);
    }

    public static EarlyStopProgram earlyStop() {
        return new EarlyStopProgram();
    }
}
