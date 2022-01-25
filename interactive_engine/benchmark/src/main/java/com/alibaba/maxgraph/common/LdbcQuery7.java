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
package com.alibaba.maxgraph.common;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

public class LdbcQuery7 extends LdbcWithSubQuery {
    public LdbcQuery7(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    String buildSubQuery(Result result, HashMap<String, String> singleParameter) {
        String personId = singleParameter.get("personId");
        Vertex friend = (Vertex) ((Map) result.getObject()).get("liker");
        long friendId = (long) friend.property("id").value();
        return String.format(
                "g.V().hasLabel('person').has('id',%s).both('knows').has('person', 'id', %d)",
                personId, friendId);
    }

    @Override
    String generateGremlinQuery(
            HashMap<String, String> singleParameter, String gremlinQueryPattern) {
        for (String parameter : singleParameter.keySet()) {
            gremlinQueryPattern =
                    gremlinQueryPattern.replace(
                            getParameterPrefix() + parameter + getParameterPostfix(),
                            singleParameter.get(parameter));
        }
        return gremlinQueryPattern;
    }
}
