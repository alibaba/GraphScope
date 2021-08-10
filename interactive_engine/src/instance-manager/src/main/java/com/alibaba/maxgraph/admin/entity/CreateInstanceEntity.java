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
package com.alibaba.maxgraph.admin.entity;

public class CreateInstanceEntity {
    private int errorCode;
    private String errorMessage;
    private String frontHost;
    private int frontPort;
    private String gaiaFrontHost;
    private int gaiaFrontPort;

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setFrontHost(String frontHost) {
        this.frontHost = frontHost;
    }

    public void setFrontPort(int frontPort) {
        this.frontPort = frontPort;
    }

    public void setGaiaFrontHost(String frontHost) {
        this.gaiaFrontHost = frontHost;
    }

    public void setGaiaFrontPort(int frontPort) {
        this.gaiaFrontPort = frontPort;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getFrontHost() {
        return frontHost;
    }

    public int getFrontPort() {
        return frontPort;
    }

    public String getGaiaFrontHost() {
        return gaiaFrontHost;
    }

    public int getGaiaFrontPort() {
        return gaiaFrontPort;
    }
}
