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
package com.alibaba.maxgraph.common.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class RequestResourceInfo implements Comparable<RequestResourceInfo> {
    public final int memMb;
    public final int vCores;
    public final String host;
    public final int diskSizeMb;

    public RequestResourceInfo(int memMb, int vCores, String host) {
        this(memMb, vCores, host, 0);
    }

    public RequestResourceInfo(int diskSizeMb) {
        this(0, 0, "localhost", diskSizeMb);
    }

    public RequestResourceInfo(int memMb, int vCores) {
        this(memMb, vCores, "", Integer.MAX_VALUE);
    }

    public RequestResourceInfo(int memMb, int vCores, int diskMb) {
        this(memMb, vCores, "", diskMb);
    }

    @JsonCreator
    private RequestResourceInfo(int memMb, int vCores, String host, int diskSizeMb) {
        this.memMb = memMb;
        this.vCores = vCores;
        this.host = host;
        this.diskSizeMb = diskSizeMb;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("memMb", memMb)
                .add("vCores", vCores)
                .add("host", host)
                .add("diskSizeMb", diskSizeMb)
                .toString();
    }

    @Override
    public int compareTo(RequestResourceInfo o) {
        return memMb - o.memMb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestResourceInfo that = (RequestResourceInfo) o;
        return memMb == that.memMb &&
                vCores == that.vCores &&
                diskSizeMb == that.diskSizeMb &&
                Objects.equal(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(memMb, vCores, host, diskSizeMb);
    }
}
