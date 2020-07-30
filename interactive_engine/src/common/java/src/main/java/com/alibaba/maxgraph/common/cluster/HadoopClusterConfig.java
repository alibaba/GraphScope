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

import java.util.Map;

/**
 * include information about cluster address / cluster total resources,etc
 *
 * @author lvshuang.xjs@alibaba-inc.com
 * @create 2018-05-07 下午8:04
 **/

public class HadoopClusterConfig extends MaxGraphConfiguration {

    /**
     * yarn
     */
    public static final String YARN_RM_ADDRESS = "yarn.resourcemanager.address";
    public static final String YARN_RM_SCHEDULER_ADDRESS = "yarn.resourcemanager.scheduler.address";
    public static final String HDFS_DEFAULT_FS = "hdfs.default.fs";
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    public static final String YARN_MIN_CONTAINER_MEM_MB = "yarn.min.container.mem.mb";
    public static final String YARN_MAX_CONTAINER_MEM_MB = "yarn.max.container.mem.mb";
    public static final String YARN_MIN_CONTAINER_CPU = "yarn.min.container.cpu";
    public static final String YARN_MAX_CONTAINER_CPU = "yarn.max.container.cpu";
    public static final String MAPRED_JAR = "mapred.jar";

    public HadoopClusterConfig(Map<String, String> map) {
        super(map);
    }

    public String getYarnRmAddress() {
        return getString(YARN_RM_ADDRESS);
    }

    public String getYarnRmSchedulerAddress() {
        return getString(YARN_RM_SCHEDULER_ADDRESS);
    }

    public String getYarnHdfsAddress() {
        return getString(HDFS_DEFAULT_FS);
    }

    public String getHadoopUserName() {
        return getString(HADOOP_USER_NAME);
    }

    public int getMinContainerMemMb() {
        return getInt(YARN_MIN_CONTAINER_MEM_MB, 4096);
    }

    public int getMaxContainerMemMb() {
        return getInt(YARN_MAX_CONTAINER_MEM_MB, 65536);
    }

    public int getMinContainerCpu() {
        return getInt(YARN_MIN_CONTAINER_CPU, 1);
    }

    public int getMaxContainerCpu() {
        return getInt(YARN_MAX_CONTAINER_CPU, 32);
    }
}

