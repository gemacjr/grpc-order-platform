package com.example.order.api.client;

import io.grpc.*;

/**
 * Copies the caller's bearer token into gRPC metadata. Reactor context, not
 * ThreadLocal, is the source of truth in WebFlux, so the token is read from the
 * call options populated per-request rather than from SecurityContextHolder.
 */
public class AuthPropagatingInterceptor implements ClientInterceptor {

    public static final CallOptions.Key<String> BEARER_TOKEN =
            CallOptions.Key.createWithDefault("bearer-token", null);

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String token = callOptions.getOption(BEARER_TOKEN);
                if (token != null) {
                    headers.put(AUTHORIZATION, "Bearer " + token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
