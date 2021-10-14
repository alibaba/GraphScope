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
package com.alibaba.maxgraph.common.client;

import com.alibaba.maxgraph.sdkcommon.MaxGraphFunctional;
import com.alibaba.maxgraph.sdkcommon.exception.MetaException;
import com.alibaba.maxgraph.common.util.CommonUtil;
import com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils;
import com.alibaba.maxgraph.proto.Response;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract client that do refresh stuff automatically by wrapping actual in to
 * <p>
 * Created by xiafei.qiuxf on 2016/9/27.
 */

public abstract class BaseAutoRefreshClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAutoRefreshClient.class);

    private AtomicLong lastRefreshTime = new AtomicLong(0L);

    private final int maxRetry;
    private final long refreshIntervalMs;
    private int clientCallMaxRetry;
    private int clientCallRetryIntervalMs;

    public BaseAutoRefreshClient(String name, int maxRetry, int intervalSec) {
        this.maxRetry = maxRetry;
        this.refreshIntervalMs = intervalSec * 1000L;
    }

    public BaseAutoRefreshClient(int clientCallMaxRetry, int clientCallRetryIntervalMs) {
        this.maxRetry = 3;
        this.refreshIntervalMs = 1000;
        this.clientCallMaxRetry = clientCallMaxRetry;
        this.clientCallRetryIntervalMs = clientCallRetryIntervalMs;
    }

    public void start() throws Exception {
        refresh(false);
    }

    protected abstract void doRefresh() throws Exception;

    protected synchronized void refresh(boolean force) throws Exception {
        if (force || System.currentTimeMillis() - lastRefreshTime.get() >= refreshIntervalMs) {
            for (int i = 1; i <= this.maxRetry && !Thread.interrupted(); i++) {
                try {
                    doRefresh();
                    lastRefreshTime.set(System.currentTimeMillis());
                    return;
                } catch (Exception e) {
                    if (i == this.maxRetry) { // retry exhausted
                        throw e;
                    } else {
                        LOG.warn("exception when refresh for replicas, wait {} second(s)", i, e);
                        CommonUtil.sleepSilently(1000 * i);
                    }
                }
            }
        } else {
            LOG.debug("skip refresh, last refresh time: {}", lastRefreshTime);
        }
    }

    public void run(MaxGraphFunctional.Runnable runnable) throws Exception {
        call(() -> {
            runnable.run();
            return null;
        });
    }

    public <T> T call(MaxGraphFunctional.Callable<T> callable) throws Exception {
        try {
            return callable.call();
        } catch (MetaException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            // all business-layer exception are properly handled on server side, so catch StoreRuntimeException
            // and StatusRuntimeException
            LOG.warn("exception when execution, refresh and retry", e);
            refresh(false);
        }

        return callable.call();
    }

    public <T> T tryCallWithSpecifyTimes(MaxGraphFunctional.Callable<T> callable) throws Exception {

        Throwable lastError = null;
        for (int i = 0; i < clientCallMaxRetry; i ++) {
            try {
                return callable.call();
            } catch (StatusRuntimeException e) {
                lastError = e;
                Status.Code errCode = e.getStatus().getCode();
                if (errCode.equals(Status.Code.CANCELLED)) {
                    break;
                }

                if (errCode.equals(Status.Code.UNAVAILABLE) || errCode.equals(Status.Code.INTERNAL)) {
                    refresh(true);
                }
            } catch (Exception e) {
                lastError = e;
                // all business-layer exception are properly handled on server side, so catch StoreRuntimeException
                // and StatusRuntimeException
                LOG.warn("exception when execution, refresh and retry", e);
            }

            if (i < clientCallMaxRetry - 1) {
                CommonUtil.sleepSilently(clientCallRetryIntervalMs);
            }
        }

        throw new RuntimeException("exceeds max try nums:" + clientCallMaxRetry, lastError);
    }

    public static void validateResponse(Response resp) throws Exception {
        LOG.debug("resp code: {} , message: {}",resp.getErrCode(), resp.getErrMsg());
        ExceptionUtils.checkAndThrow(resp.getErrCode(), resp.getErrMsg());
    }

    public void close() throws IOException {

    }

}
