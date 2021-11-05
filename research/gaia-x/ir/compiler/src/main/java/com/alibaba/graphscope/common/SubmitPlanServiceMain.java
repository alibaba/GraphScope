package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.client.*;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiJobBuffer;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.sun.jna.Pointer;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SubmitPlanServiceMain {
    private static final Logger logger = LoggerFactory.getLogger(SubmitPlanServiceMain.class);

    public static void main(String[] args) throws Exception {
        Pointer ptrPlan;
        String opt = "poc";
        if (args.length > 0) {
            opt = args[0];
        }
        if (opt.equals("poc")) {
            ptrPlan = PlanFactory.getPocPlan();
        } else if (opt.equals("cr2")) {
            ptrPlan = PlanFactory.getCR2Plan();
        } else {
            throw new NotImplementedException("unimplemented opt type " + opt);
        }

        IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
        irCoreLib.debugPlan(ptrPlan);

        FfiJobBuffer jobBuffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        assert jobBuffer.len > 0;
        byte[] physicalPlanBytes = jobBuffer.getBytes();

        jobBuffer.close();
        irCoreLib.destroyLogicalPlan(ptrPlan);

        Configs configs = new Configs("conf/ir.plan.properties", FileLoadType.RELATIVE_PATH);
        int serverNum = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
        List<Long> servers = new ArrayList<>();
        for (long i = 0; i < serverNum; ++i) {
            servers.add(i);
        }

        PegasusClient.JobRequest request = PegasusClient.JobRequest.parseFrom(physicalPlanBytes);
        PegasusClient.JobConfig jobConfig = PegasusClient.JobConfig.newBuilder()
                .setJobId(1)
                .setJobName("ir_plan_1")
                .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(configs))
                .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(configs))
                .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs))
                .setOutputCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs))
                .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(configs))
                .addAllServers(servers)
                .build();
        request = request.toBuilder().setConf(jobConfig).build();

        RpcChannelFetcher channelFetcher = new HostsChannelFetcher(configs);
        AbstractBroadcastProcessor processor = new RpcBroadcastProcessor(channelFetcher);

        processor.broadcast(request, new IrResultProcessor(new ResultParser() {
            @Override
            public List<Object> parseFrom(PegasusClient.JobResponse response) {
                return Collections.singletonList(response.getData());
            }
        }));
    }
}
