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

package com.alibaba.graphscope.example.simple.sssp;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.stdcxx.StdVector;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SSSPGrapeVertexArrayDefaultContext
        implements DefaultContextBase<Long, Long, Long, Double> {
    // public DoubleArrayWrapper partialResults;
    public VertexArray<Double, Long> partialResults;
    public VertexArray.Factory factory =
            FFITypeFactory.getFactory(VertexArray.class, "grape::VertexArray<double,uint64_t>");
    // private BooleanArrayWrapper curModified;
    // private BooleanArrayWrapper nextModified;
    public VertexSet curModified;
    public VertexSet nextModified;
    public double execTime = 0.0;
    public double sendMessageTime = 0.0;
    public double receiveMessageTime = 0.0;
    public double postProcessTime = 0.0;
    public long nbrSize = 0;
    public long numOfNbrs = 0;

    // public DoubleMessageAdaptor<Long, Long> messager;

    public Long sourceOid;

    public SSSPGrapeVertexArrayDefaultContext() {}

    public VertexArray<Double, Long> getPartialResults() {
        return partialResults;
    }

    public VertexSet getCurModified() {
        return curModified;
    }

    public VertexSet getNextModified() {
        return nextModified;
    }

    public Long getSourceOid() {
        return sourceOid;
    }

    @Override
    public void Init(
            ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
            DefaultMessageManager mm,
            StdVector<FFIByteString> args) {
        Long allVertexNum = frag.getVerticesNum();
        VertexRange<Long> vertices = frag.vertices();
        // partialResults = new DoubleArrayWrapper(allVertexNum.intValue(), Double.MAX_VALUE);
        partialResults = factory.create();
        partialResults.init(vertices, Double.MAX_VALUE);
        curModified = new VertexSet(0, allVertexNum.intValue());
        nextModified = new VertexSet(0, allVertexNum.intValue());

        // args 0, 1 are app class and app ctx class
        sourceOid = Long.valueOf(args.get(0).toString());
        // System.out.println("SSSPContext.init : source oid = " + sourceOid);

        // messager = new DoubleMessageAdaptor<Long, Long>(frag);
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/sssp_output";
        System.out.println(
                "frag: " + frag.fid() + " sendMessageTime: " + sendMessageTime / 1000000000);
        System.out.println(
                "frag: " + frag.fid() + " receiveMessageTime: " + receiveMessageTime / 1000000000);
        System.out.println("frag: " + frag.fid() + " execTime: " + execTime / 1000000000);
        System.out.println(
                "frag: " + frag.fid() + " postProcessTime: " + postProcessTime / 1000000000);
        System.out.println("frag: " + frag.fid() + " number of neighbor: " + numOfNbrs);

        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            // ArrayListWrapper<Long> partialResults = this.getPartialResults();
            // System.out.println(frag.GetInnerVerticesNum() + " " + innerNodes.begin().GetValue() +
            // " "
            // + innerNodes.end().GetValue());
            // for (Vertex<Long> cur = innerNodes.begin(); cur.GetValue() !=
            // innerNodes.end().GetValue();
            // cur.inc()) {
            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(
                        cur.GetValue() + "\t" + oid + "\t" + partialResults.get(cur) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
