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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.dim;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.List;

public class DimOdpsTable implements DimTable {
    private String endpoint;
    private String accessKey;
    private String accessId;
    private String project;
    private String table;
    private String ds;
    private List<String> pkList;
    private List<Pair<String, P<?>>> filterPairList = com.google.common.collect.Lists.newArrayList();

    public DimOdpsTable project(String project) {
        this.project = project;
        return this;
    }

    public DimOdpsTable table(String table) {
        this.table = table;
        return this;
    }

    public DimOdpsTable ds(String ds) {
        this.ds = ds;
        return this;
    }

    public DimOdpsTable pklist(String... pkList) {
        this.pkList = com.google.common.collect.Lists.newArrayList(pkList);
        return this;
    }

    public <V> DimOdpsTable has(String columnName, P<V> predicate) {
        if (predicate.getBiPredicate() instanceof Compare) {
            filterPairList.add(Pair.of(columnName, predicate));
            return this;
        } else {
            throw new UnsupportedOperationException(predicate.toString());
        }
    }

    public DimOdpsTable endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public DimOdpsTable accessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public DimOdpsTable accessId(String accessId) {
        this.accessId = accessId;
        return this;
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

    public List<String> getPkList() {
        return pkList;
    }

    public List<Pair<String, P<?>>> getFilterPairList() {
        return filterPairList;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccessId() {
        return accessId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DimOdpsTable that = (DimOdpsTable) o;
        return Objects.equal(endpoint, that.endpoint) &&
                Objects.equal(accessKey, that.accessKey) &&
                Objects.equal(accessId, that.accessId) &&
                Objects.equal(project, that.project) &&
                Objects.equal(table, that.table) &&
                Objects.equal(ds, that.ds) &&
                Objects.equal(pkList, that.pkList) &&
                Objects.equal(filterPairList, that.filterPairList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(endpoint, accessKey, accessId, project, table, ds, pkList, filterPairList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("endpoint", endpoint)
                .add("accessKey", accessKey)
                .add("accessId", accessId)
                .add("project", project)
                .add("table", table)
                .add("ds", ds)
                .add("pkList", pkList)
                .add("filterPairList", filterPairList)
                .toString();
    }
}
