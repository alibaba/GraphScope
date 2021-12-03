package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;

public interface PropertyParallelContextBase<OID_T> extends ContextBase {

    void Init(
            ArrowFragment<OID_T> fragment,
            ParallelPropertyMessageManager messageManager,
            JSONObject jsonObject);
}
