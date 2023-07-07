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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

// a file implementation of StoredProceduresReader
public class StoredProceduresFileReader implements StoredProceduresReader {
    private final URI rootUri;

    public StoredProceduresFileReader(URI rootUri) {
        this.rootUri = rootUri;
    }

    @Override
    public List<URI> getAllProcedureUris() {
        File rootDir = new File(rootUri);
        Preconditions.checkArgument(rootDir.exists() && rootDir.isDirectory());
        List<URI> procedureUris = Lists.newArrayList();
        for (File file : rootDir.listFiles()) {
            if (file.getName().endsWith(".yaml")) {
                procedureUris.add(file.toURI());
            }
        }
        return Collections.unmodifiableList(procedureUris);
    }

    @Override
    public String getProcedureMeta(URI uri) throws IOException {
        return FileUtils.readFileToString(new File(uri), StandardCharsets.UTF_8);
    }
}
