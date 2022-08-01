/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.manager;

import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;

import java.util.Optional;

public class IrMetaQueryCallback {
    protected IrMetaFetcher fetcher;

    public IrMetaQueryCallback(IrMetaFetcher fetcher) {
        this.fetcher = fetcher;
    }

    // generate and manage the IrMeta before the query is actually executed
    public IrMeta beforeExec() {
        Optional<IrMeta> metaOpt = fetcher.fetch();
        if (!metaOpt.isPresent()) {
            throw new RuntimeException("ir meta is not ready");
        }
        return metaOpt.get();
    }

    // do sth after the query is done
    public void afterExec(IrMeta meta) {}
}
