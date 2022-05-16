package com.alibaba.maxgraph.sdkcommon;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Base64;
import java.util.concurrent.Executor;

public class BasicAuth extends CallCredentials {
    public static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final String TOKEN_HEAD = "Basic ";

    private String secret;

    public BasicAuth(String username, String password) {
        this.secret = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    @Override
    public void applyRequestMetadata(
            RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
        executor.execute(
                () -> {
                    try {
                        Metadata metadata = new Metadata();
                        metadata.put(AUTH_KEY, TOKEN_HEAD + secret);
                        metadataApplier.apply(metadata);
                    } catch (Throwable t) {
                        metadataApplier.fail(Status.UNAUTHENTICATED.withCause(t));
                    }
                });
    }

    @Override
    public void thisUsesUnstableApi() {}
}
