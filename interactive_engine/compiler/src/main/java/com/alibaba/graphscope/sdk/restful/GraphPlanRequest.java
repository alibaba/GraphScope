/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.sdk.restful;

import java.io.Serializable;

public class GraphPlanRequest implements Serializable {
    private String configPath;
    private String query;
    private String schemaYaml;
    private String statsJson;

    public GraphPlanRequest() {}

    public GraphPlanRequest(String configPath, String query, String schemaYaml, String statsJson) {
        this.configPath = configPath;
        this.query = query;
        this.schemaYaml = schemaYaml;
        this.statsJson = statsJson;
    }

    public String getConfigPath() {
        return configPath;
    }

    public String getQuery() {
        return query;
    }

    public String getSchemaYaml() {
        return schemaYaml;
    }

    public String getStatsJson() {
        return statsJson;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setSchemaYaml(String schemaYaml) {
        this.schemaYaml = schemaYaml;
    }

    public void setStatsJson(String statsJson) {
        this.statsJson = statsJson;
    }
}
