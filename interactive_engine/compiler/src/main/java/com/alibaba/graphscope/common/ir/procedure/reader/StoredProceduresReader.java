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

package com.alibaba.graphscope.common.ir.procedure.reader;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;

// procedure reader is used to read stored procedures from a given uri (can be a local file or
// remote web service)
public interface StoredProceduresReader {
    // get all procedure uris under the root uri
    List<URI> getAllProcedureUris();

    // read procedure meta from its uri
    String getProcedureMeta(URI uri) throws IOException;

    // provide a factory to create a reader according to uri schema automatically
    class Factory {
        public static StoredProceduresReader create(Configs configs) {
            String rootUriConfig = GraphConfig.GRAPH_STORED_PROCEDURES_URI.get(configs);
            if (StringUtils.isEmpty(rootUriConfig)) {
                return createEmpty();
            }
            URI rootUri = URI.create(rootUriConfig);
            switch (rootUri.getScheme()) {
                case "file":
                    return new StoredProceduresFileReader(rootUri);
                default:
                    throw new NotImplementedException(
                            "unsupported uri scheme for stored procedures path: "
                                    + rootUri.getScheme());
            }
        }

        public static StoredProceduresReader createEmpty() {
            return new StoredProceduresReader() {
                @Override
                public List<URI> getAllProcedureUris() {
                    return ImmutableList.of();
                }

                @Override
                public String getProcedureMeta(URI uri) throws IOException {
                    return StringUtils.EMPTY;
                }
            };
        }
    }
}
