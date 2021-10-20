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
package com.alibaba.maxgraph.coordinator.manager.runtime;

import com.alibaba.maxgraph.proto.RuntimeAddressProto;
import com.alibaba.maxgraph.proto.RuntimeHBReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @Author: peaker.lgf
 * @Date: 2020-01-19 11:27
 **/
public class PegasusGroupInfo extends GroupInfoCommon {

    private static final Logger LOG = LoggerFactory.getLogger(PegasusGroupInfo.class);

    public PegasusGroupInfo(List<Integer> serverList, long version, String graph, int serverId) {
        super(serverList, version, GroupStatus.STARTING, graph, serverId);
    }

    /**
     * @param serverList  server_id list, server id is the server in all cluster
     * @param version     group version
     * @param groupStatus initialize group status
     */
    public PegasusGroupInfo(List<Integer> serverList, long version, GroupStatus groupStatus, String graph, int serverId) {
        super(serverList, version, groupStatus, graph, serverId);
    }

    public void updateNodeInfo(int nodeIndex, NodeInfo nodeInfo) {
        this.nodes.put(nodeIndex, nodeInfo);
        this.updateGroupStatus();
    }

    /**
     * IP list is returned only when group receive complete ip list
     *
     * @return servers ip list in this group
     */
    public List<RuntimeAddressProto> getAvaliableAddressList() {
        List<RuntimeAddressProto> addressList = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            NodeInfo nodeInfo = nodes.get(i);
            if (!"".equals(nodeInfo.ip) && nodeInfo.port != 0 && nodeInfo.storePort != 0) {
                addressList.add(RuntimeAddressProto.newBuilder()
                        .setIp(nodeInfo.ip)
                        .setRuntimePort(nodeInfo.port)
                        .setStorePort(nodeInfo.storePort)
                        .build());
            } else {
                LOG.warn("invalid node info " + nodeInfo);
                return Collections.emptyList();
            }
        }
        return addressList;
    }

    private void updateGroupStatus() {
        for(Map.Entry<Integer, NodeInfo> entry : this.nodes.entrySet()) {
            if(!entry.getValue().status.equals(RuntimeHBReq.RuntimeStatus.RUNNING)) {
                this.groupStatus = GroupStatus.STARTING;
                return;
            }
        }
        this.groupStatus = GroupStatus.RUNNING;
    }

    public void invalidGroup() {
        for(Map.Entry<Integer, NodeInfo> entry : this.nodes.entrySet()) {
            entry.getValue().resetInfo();
        }
    }

    public GroupStatus getGroupStatus() {
        return this.groupStatus;
    }
}
