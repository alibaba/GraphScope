/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.example.simple.bfs;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.IntArrayWrapper;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class BFSDefaultContext implements DefaultContextBase<Long, Long, Long, Double> {
    public long sourceOid;
    public IntArrayWrapper partialResults;
    // public BooleanArrayWrapper currendInnerUpdated;
    public VertexSet currentInnerUpdated, nextInnerUpdated;
    public int currentDepth;

    @Override
    public void Init(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager messageManager,
            StdVector<FFIByteString> args) {
        sourceOid = Long.valueOf(args.get(0).toString());
        partialResults = new IntArrayWrapper(frag.getVerticesNum().intValue(), Integer.MAX_VALUE);
        currentInnerUpdated = new VertexSet(frag.innerVertices());
        nextInnerUpdated = new VertexSet(frag.innerVertices());
        currentDepth = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/bfs_default_output";
        System.out.println("depth " + currentDepth);
        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(oid + " " + partialResults.get(index) + "\n");
            }
            bufferedWriter.close();
            System.out.println("writing output to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
