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

import java.util.HashMap;

public class LdbcQuery4 extends CommonQuery {
    public LdbcQuery4(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    String generateGremlinQuery(HashMap<String, String> singleParameter,
                                String gremlinQueryPattern) {
        singleParameter.put("startDate",  transformDate(singleParameter.get("startDate")));
        singleParameter.put("endDate", getEndDate(singleParameter.get("startDate"), singleParameter.get("durationDays")));

        for (String parameter : singleParameter.keySet()) {
            gremlinQueryPattern = gremlinQueryPattern.replace(
                    getParameterPrefix() + parameter + getParameterPostfix(),
                    singleParameter.get(parameter)
            );
        }
        return gremlinQueryPattern;
    }
}
