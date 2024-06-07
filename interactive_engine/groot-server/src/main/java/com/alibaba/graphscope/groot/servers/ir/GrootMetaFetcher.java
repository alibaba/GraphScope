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

package com.alibaba.graphscope.groot.servers.ir;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.fetcher.IrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GrootMetaFetcher extends IrMetaFetcher {
    private static final Logger logger = LoggerFactory.getLogger(GrootMetaFetcher.class);

    public GrootMetaFetcher(IrMetaReader reader, IrMetaTracker tracker) {
        super(reader, tracker);
    }

    @Override
    public Optional<IrMeta> fetch() {
        try {
            return Optional.of(reader.readMeta());
        } catch (Exception e) {
            logger.warn("fetch ir meta from groot failed: {}", e);
            return Optional.empty();
        }
    }
}
