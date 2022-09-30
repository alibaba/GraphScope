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

public class LdbcQuery12 extends AbstractLdbcWithSubQuery {
    public LdbcQuery12(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    String buildSubQuery(Result result, HashMap<String, String> singleParameter) {
        String tagClassName = singleParameter.get("tagClassName");
        Map<Vertex, Long> map = ((Map) result.getObject());
        return String.format(
                "g.V(%s).in('HASCREATOR').hasLabel('COMMENT').out('REPLYOF').hasLabel('POST').out('HASTAG').as('tag').out('HASTYPE').has('name',eq('%s')).select('tag').values('name').dedup()",
                map.entrySet().iterator().next().getKey().id().toString(), tagClassName);
    }
}
