package com.alibaba.graphscope.gaia.broadcast;

import com.alibaba.graphscope.gaia.broadcast.channel.AsyncRpcChannelFetcher;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.CloseableIterator;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AsyncRpcBroadcastProcessor extends AbstractBroadcastProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcBroadcastProcessor.class);
    private AsyncRpcChannelFetcher fetcher;

    public AsyncRpcBroadcastProcessor(AsyncRpcChannelFetcher fetcher) {
        super(fetcher);
        this.fetcher = fetcher;
    }

    @Override
    public void broadcast(PegasusClient.JobRequest request, ResultProcessor processor) {
        CloseableIterator<PegasusClient.JobResponse> iterator = null;
        // ResultProcessor processor = new GremlinResultProcessor(writeResult);
        try {
            // refresh
            this.rpcClient = new RpcClient(fetcher.refresh());
            iterator = rpcClient.submit(request);
            // process response
            while (iterator.hasNext()) {
                PegasusClient.JobResponse response = iterator.next();
                processor.process(response);
            }
            processor.finish();
        } catch (Exception e) {
            logger.error("broadcast exception {}", e);
            processor.error(Status.fromThrowable(e));
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (IOException ioe) {
                    logger.error("iterator close fail {}", ioe);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.rpcClient.shutdown();
    }
}
