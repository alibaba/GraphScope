/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.graph;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;

import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexFactory {

    public static <
                    OID_T extends WritableComparable,
                    VDATA_T extends Writable,
                    EDATA_T extends Writable>
            VertexImpl<OID_T, VDATA_T, EDATA_T> createDefaultVertex(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VDATA_T> vdataClass,
                    Class<? extends EDATA_T> edataClass,
                    GiraphComputationAdaptorContext context) {
        return new VertexImpl<OID_T, VDATA_T, EDATA_T>(context);
    }
}
