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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.output;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.List;

public class OutputOdpsTable implements OutputTable {
    private static final long serialVersionUID = 7028142022222284987L;
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String project;
    private String table;
    private String ds;
    private List<String> propNameList;

    public OutputOdpsTable endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public OutputOdpsTable accessId(String accessId) {
        this.accessId = accessId;
        return this;
    }

    public OutputOdpsTable accessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public OutputOdpsTable project(String project) {
        this.project = project;
        return this;
    }

    public OutputOdpsTable table(String table) {
        this.table = table;
        return this;
    }

    public OutputOdpsTable ds(String ds) {
        this.ds = ds;
        return this;
    }

    public OutputOdpsFunction write(String... propNameList) {
        this.propNameList = Lists.newArrayList(propNameList);
        return new OutputOdpsFunction(this);
    }

    public String getProject() {
        return project;
    }

    public String getTable() {
        return table;
    }

    public String getDs() {
        return ds;
    }

    public List<String> getPropNameList() {
        return propNameList;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputOdpsTable that = (OutputOdpsTable) o;
        return Objects.equal(endpoint, that.endpoint) &&
                Objects.equal(accessId, that.accessId) &&
                Objects.equal(accessKey, that.accessKey) &&
                Objects.equal(project, that.project) &&
                Objects.equal(table, that.table) &&
                Objects.equal(ds, that.ds) &&
                Objects.equal(propNameList, that.propNameList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(endpoint, accessId, accessKey, project, table, ds, propNameList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("endpoint", endpoint)
                .add("accessId", accessId)
                .add("accessKey", accessKey)
                .add("project", project)
                .add("table", table)
                .add("ds", ds)
                .add("propNameList", propNameList)
                .toString();
    }
}
