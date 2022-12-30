package com.alibaba.graphscope.annotation;

import org.junit.Assert;
import org.junit.Test;

public class AppScannerTest {

    @Test
    public void test1() {
        String graphString =
                "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t,vineyard::ArrowVertexMap<int64_t,uint64_t>>";
        String[] res = GraphScopeAppScanner.parseGraphTemplateStr(graphString);
        Assert.assertEquals(5, res.length);
        Assert.assertEquals("gs::ArrowProjectedFragment", res[0]);
        Assert.assertEquals("int64_t", res[1]);
        Assert.assertEquals("uint64_t", res[2]);
        Assert.assertEquals("int64_t", res[3]);
        Assert.assertEquals("int64_t", res[4]);
    }
}
