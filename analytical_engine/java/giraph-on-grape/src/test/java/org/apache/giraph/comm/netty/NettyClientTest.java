package org.apache.giraph.comm.netty;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyWritableMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.graph.impl.AggregatorManagerNettyImpl;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NettyClientTest {

    private static Logger logger = LoggerFactory.getLogger(NettyClientTest.class);

    private NettyServer server;
    private NettyClientV2 client;
    private ImmutableClassesGiraphConfiguration configuration;
    private WorkerInfo workerInfo;
    private AggregatorManager aggregatorManager;


    @Before
    public void prepare() throws InstantiationException, IllegalAccessException {
        ImmutableClassesGiraphConfiguration conf = mock(ImmutableClassesGiraphConfiguration.class);
        aggregatorManager = new AggregatorManagerNettyImpl(conf, 0, 2);
        aggregatorManager.registerAggregator("sum", LongSumAggregator.class);
        aggregatorManager.setAggregatedValue("sum", new LongWritable(0));
        //        when(conf.)
//        String hostName = System.getenv("HOSTNAME");
        String hostName = getHostIp();
        workerInfo = new WorkerInfo(0, 2,hostName , 30000, null);
        server =
                new NettyServer(
                        conf,
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
        client =
                new NettyClientV2(
                        conf,
                        aggregatorManager,
                        workerInfo,
                        new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(t.getId() + ": " + e.toString());
                            }
                        });
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 10; ++i) {
            NettyWritableMessage send = new NettyWritableMessage(new LongWritable(i), 100, "sum");
            Future<NettyMessage> msg = client.sendMessage(send);
            //while (!msg.isDone()) {}
            NettyMessage received = client.getResponse();
            logger.info("reponse for round: " + i + ": " + received);
        }
    }

    @After
    public void close() {
        client.close();
        server.close();
    }

    private static String getHostIp(){
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new IllegalStateException("Failed to get master host address");
        }
    }
}
