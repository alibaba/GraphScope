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
package com.alibaba.graphscope.gaia.common;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

public class LdbcQuery1 extends AbstractLdbcWithSubQuery {
    public LdbcQuery1(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    String buildSubQuery(Result result, HashMap<String, String> singleParameter) {
        Vertex v = (Vertex) ((Map<Vertex, Object>) result.getObject()).get("a");
        return String.format("g.V(%s).union(out('ISLOCATEDIN').values('name'),outE('STUDYAT').inV().as('u').out('ISLOCATEDIN').as('city').select('u','city'),outE('WORKAT').inV().as('company').out('ISLOCATEDIN').as('country').select('company','country'))",
                v.id().toString());
    }

}
