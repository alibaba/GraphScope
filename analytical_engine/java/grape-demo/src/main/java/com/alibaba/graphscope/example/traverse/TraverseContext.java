package com.alibaba.graphscope.example.traverse;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

public class TraverseContext extends VertexDataContext<IFragment<Long, Long, Double, Long>, Long>
        implements ParallelContextBase<Long, Long, Double, Long> {

    public GSVertexArray<Long> vertexArray;
    public int maxIteration;

    @Override
    public void Init(
            IFragment<Long, Long, Double, Long> frag,
            ParallelMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(frag, Long.class, false);
        // This vertex Array is created by our framework. Data stored in this array will be
        // available
        // after execution, you can receive them by invoking method provided in Python Context.
        vertexArray = data();
        maxIteration = 10;
        if (jsonObject.containsKey("maxIteration")) {
            maxIteration = jsonObject.getInteger("maxIteration");
        }
    }

    @Override
    public void Output(IFragment<Long, Long, Double, Long> frag) {
        // You can also write output logic in this function.
    }
}
