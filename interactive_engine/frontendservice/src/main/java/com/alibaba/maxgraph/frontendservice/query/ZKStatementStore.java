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
package com.alibaba.maxgraph.frontendservice.query;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.zookeeper.ZKPaths;
import com.alibaba.maxgraph.common.zookeeper.ZkUtils;
import com.alibaba.maxgraph.compiler.prepare.store.PrepareStoreEntity;
import com.alibaba.maxgraph.compiler.prepare.store.StatementStore;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: peaker.lgf
 * @Email: peaker.lgf@alibaba-inc.com
 * @create: 2018-12-18 19:47
 **/
public class ZKStatementStore implements StatementStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKStatementStore.class);
    private final String prepare_query_info_path;
    //prepared cache
    private Map<String, PrepareStoreEntity> prepareStoreEntityMap = new ConcurrentHashMap<>();
    private ZkUtils zkUtilsStore;

    public ZKStatementStore(InstanceConfig instanceConfig) {
        this.prepare_query_info_path = ZKPaths.getPrepareQueryInfo(instanceConfig.getGraphName());
        this.zkUtilsStore = ZkUtils.getZKUtils(instanceConfig);
    }

    /**
     * Persist prepare query to ZK, include prepare query used by timely, and compile arguments used by compiler
     * @param prepareId   prepare query name
     * @param prepareStoreEntity  prepare query class
     * @throws Exception
     */
    @Override
    public void save(String prepareId, PrepareStoreEntity prepareStoreEntity) throws Exception{
        String subPreparePath = buildSubPreparePath(prepareId);
        String subCompilePath = buildSubCompilePath(prepareId);
        try {
            this.zkUtilsStore.createOrUpdatePath(subPreparePath, prepareStoreEntity.getQueryFlow().toByteArray(), CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOGGER.error("Persist prepare query " + prepareId + " failed: " + e);
            throw new RuntimeException("Store prepare query " + prepareId + " failed: " + e);
        }
        try {
            this.zkUtilsStore.createOrUpdatePath(subCompilePath, prepareStoreEntity.toByteArray(), CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOGGER.error("Persist compile query " + prepareId + " failed: " + e);
            String preparePath = buildPreparePath(prepareId);
            this.zkUtilsStore.deletePath(preparePath);
            throw new RuntimeException("Store compile query " + prepareId + " failed: " + e);
        }

        // cache prepare query
        this.prepareStoreEntityMap.put(prepareId, prepareStoreEntity);
    }

    /**
     * Get prepareStoreEnty by prepareId, which is used by compiler
     * @param prepareId   prepare query name
     * @return PrepareStoreEntity
     * @throws Exception
     */
    @Override
    public PrepareStoreEntity get(String prepareId) {
        // first get prepareStoreEntity from cache
        if(prepareStoreEntityMap.containsKey(prepareId)) {
            return prepareStoreEntityMap.get(prepareId);
        }

        String subCompilePath = buildSubCompilePath(prepareId);
        try {
            byte[] bytes = this.zkUtilsStore.readBinaryData(subCompilePath);
            PrepareStoreEntity prepareStoreEntity = PrepareStoreEntity.toPrepareStoreEntity(bytes);
            this.prepareStoreEntityMap.put(prepareId, prepareStoreEntity);  // cache prepare query
            return prepareStoreEntity;
        } catch (Exception e) {
            LOGGER.error("Get compile query from ZK failed: " + e);
            throw new RuntimeException("Get compile query from ZK failed: " + e);
        }
    }

    /**
     * Delete prepare query from zk
     * @param prepareId
     */
    @Override
    public void delete(String prepareId) {
        this.prepareStoreEntityMap.remove(prepareId);    // remove prepare from cache

        String preparePath = buildPreparePath(prepareId);
        try {
            this.zkUtilsStore.deletePath(preparePath);
        } catch (Exception e) {
            LOGGER.error("Delete prepare path " + preparePath + " failed, caused: " + e );
            throw new RuntimeException("Delete prepare path " + preparePath + "failed, caused: " + e);
        }
    }

    /**
     * Check whether prepare query has been persisted
     * @param prepareId  prepare query name
     * @return
     */
    @Override
    public boolean checkExist(String prepareId) {
        if(this.prepareStoreEntityMap.containsKey(prepareId)) {
            return true;
        }

        String subLockPath = this.buildSubLockPath(prepareId);
        if(this.zkUtilsStore.pathExists(subLockPath)) {
            return true;
        } else {
            try {
                this.zkUtilsStore.createPath(subLockPath, "".getBytes(), CreateMode.PERSISTENT);
                return false;
            } catch (Exception e) {
                LOGGER.error("Create prepare lock path in zk failed: " + e);
                throw new RuntimeException("Create prepare lock path in zk failed" + e);
            }
        }
    }

    private String buildPreparePath(String prepareId) {
        String preparePath = this.prepare_query_info_path + "/" + prepareId;
        return preparePath;
    }

    private String buildSubPreparePath(String prepareId) {
        String preparePath = this.prepare_query_info_path + "/" + prepareId + "/prepare";
        return preparePath;
    }

    private String buildSubCompilePath(String prepareId) {
        String compilePath = this.prepare_query_info_path + "/" + prepareId + "/compile";
        return compilePath;
    }

    private String buildSubLockPath(String prepareId) {
        String lockPath = this.prepare_query_info_path + "/" + prepareId + "/lock";
        return lockPath;
    }

}
