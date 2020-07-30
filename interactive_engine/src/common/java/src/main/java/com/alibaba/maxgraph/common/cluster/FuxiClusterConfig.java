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

public class FuxiClusterConfig extends MaxGraphConfiguration {

    public enum NuwaNamingMode {
        config_server,
        vip
    }

    public static final String YARN_IPC_CLIENT_FACTORY_CLASS = "com.alibaba.fuxi.yarn.FuxiClientFactoryImpl";

    public static final String FUXI_BASE_URL = "fuxi.base.url";
    public static final String YARN_WEB_PROXY_ADDRESS = "yarn.web-proxy.address";
    public static final String FUXI_NUWA_HTTP_PROXY = "nuwa.http.proxy";
    public static final String ALIYARN_CLUSTER_NAME = "aliyarn.cluster.name";
    public static final String FUXI_PACKAGE_CAPABILITY = "fuxi.package.capability";
    public static final String FUXI_BASE_URL_ENABLED = "fuxi.base.url.enabled";
    public static final String NUWA_HTTP_PROXY_MODE = "nuwa.http.proxy.mode";
    public static final String ALIYARN_PROJECT_NAME = "aliyarn.cluster.project";
    public static final String NUWA_HTTP_TENANT = "nuwa.http.tenant";

    public FuxiClusterConfig(Map<String, String> map) {
        super(map);
    }

    public String getFuxiBaseUrl() {
        return getString(FUXI_BASE_URL);
    }

    public String getYarnWebProxyAddress() {
        return getString(YARN_WEB_PROXY_ADDRESS);
    }

    public String getFuxiNuwaHttpProxy() {
        return getString(FUXI_NUWA_HTTP_PROXY);
    }

    public String getFuxiYarnClusterName() {
        return getString(ALIYARN_CLUSTER_NAME);
    }

    public String getFuxiPackageCapability() {
        return getString(FUXI_PACKAGE_CAPABILITY);
    }

    // 线上环境当配置多个nuwa proxy时设置为false
    public Boolean isFuxiBaseUrlEnabled() {
        return getBoolean(FUXI_BASE_URL_ENABLED, false);
    }

    public NuwaNamingMode getNuwaHttpProxyMode() {
        return NuwaNamingMode.valueOf(getString(NUWA_HTTP_PROXY_MODE, "vip"));
    }

    public String getAliyarnProjectName() {
        return getString(ALIYARN_PROJECT_NAME);
    }

    public String getNuwaHttpTenant() {
        return getString(NUWA_HTTP_TENANT);
    }
}
