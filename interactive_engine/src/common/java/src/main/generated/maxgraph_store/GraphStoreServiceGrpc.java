package maxgraph_store;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: store_node.proto")
public class GraphStoreServiceGrpc {

  private GraphStoreServiceGrpc() {}

  public static final String SERVICE_NAME = "maxgraph_store.GraphStoreService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<maxgraph_store.StoreNode.ScanDataRequest,
      maxgraph_store.StoreNode.VertexData> METHOD_SCAN_VERTEX =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph_store.GraphStoreService", "scanVertex"),
          io.grpc.protobuf.ProtoUtils.marshaller(maxgraph_store.StoreNode.ScanDataRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(maxgraph_store.StoreNode.VertexData.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<maxgraph_store.StoreNode.ScanDataRequest,
      maxgraph_store.StoreNode.EdgeData> METHOD_SCAN_EDGE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "maxgraph_store.GraphStoreService", "scanEdge"),
          io.grpc.protobuf.ProtoUtils.marshaller(maxgraph_store.StoreNode.ScanDataRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(maxgraph_store.StoreNode.EdgeData.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GraphStoreServiceStub newStub(io.grpc.Channel channel) {
    return new GraphStoreServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GraphStoreServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GraphStoreServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static GraphStoreServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GraphStoreServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class GraphStoreServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void scanVertex(maxgraph_store.StoreNode.ScanDataRequest request,
        io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.VertexData> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN_VERTEX, responseObserver);
    }

    /**
     */
    public void scanEdge(maxgraph_store.StoreNode.ScanDataRequest request,
        io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.EdgeData> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN_EDGE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_SCAN_VERTEX,
            asyncServerStreamingCall(
              new MethodHandlers<
                maxgraph_store.StoreNode.ScanDataRequest,
                maxgraph_store.StoreNode.VertexData>(
                  this, METHODID_SCAN_VERTEX)))
          .addMethod(
            METHOD_SCAN_EDGE,
            asyncServerStreamingCall(
              new MethodHandlers<
                maxgraph_store.StoreNode.ScanDataRequest,
                maxgraph_store.StoreNode.EdgeData>(
                  this, METHODID_SCAN_EDGE)))
          .build();
    }
  }

  /**
   */
  public static final class GraphStoreServiceStub extends io.grpc.stub.AbstractStub<GraphStoreServiceStub> {
    private GraphStoreServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GraphStoreServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GraphStoreServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GraphStoreServiceStub(channel, callOptions);
    }

    /**
     */
    public void scanVertex(maxgraph_store.StoreNode.ScanDataRequest request,
        io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.VertexData> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN_VERTEX, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scanEdge(maxgraph_store.StoreNode.ScanDataRequest request,
        io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.EdgeData> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN_EDGE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GraphStoreServiceBlockingStub extends io.grpc.stub.AbstractStub<GraphStoreServiceBlockingStub> {
    private GraphStoreServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GraphStoreServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GraphStoreServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GraphStoreServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<maxgraph_store.StoreNode.VertexData> scanVertex(
        maxgraph_store.StoreNode.ScanDataRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN_VERTEX, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<maxgraph_store.StoreNode.EdgeData> scanEdge(
        maxgraph_store.StoreNode.ScanDataRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN_EDGE, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GraphStoreServiceFutureStub extends io.grpc.stub.AbstractStub<GraphStoreServiceFutureStub> {
    private GraphStoreServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GraphStoreServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GraphStoreServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GraphStoreServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SCAN_VERTEX = 0;
  private static final int METHODID_SCAN_EDGE = 1;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GraphStoreServiceImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(GraphStoreServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SCAN_VERTEX:
          serviceImpl.scanVertex((maxgraph_store.StoreNode.ScanDataRequest) request,
              (io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.VertexData>) responseObserver);
          break;
        case METHODID_SCAN_EDGE:
          serviceImpl.scanEdge((maxgraph_store.StoreNode.ScanDataRequest) request,
              (io.grpc.stub.StreamObserver<maxgraph_store.StoreNode.EdgeData>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_SCAN_VERTEX,
        METHOD_SCAN_EDGE);
  }

}
