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
package com.alibaba.maxgraph.admin.memory;

public class InstanceEntity {
    private String frontEndpoint;
    private String podNameList;
    private String containerNameList;
    private String closeScript;

    public InstanceEntity(String frontEndpoint, String podNameList, String containerNameList, String closeScript) {
        this.frontEndpoint = frontEndpoint;
        this.podNameList = podNameList;
        this.containerNameList = containerNameList;
        this.closeScript = closeScript;
    }

    public String getFrontEndpoint() {
        return frontEndpoint;
    }

    public String getPodNameList() {
        return podNameList;
    }

    public String getContainerNameList() {
        return containerNameList;
    }

    public String getCloseScript() {
        return this.closeScript;
    }
}
