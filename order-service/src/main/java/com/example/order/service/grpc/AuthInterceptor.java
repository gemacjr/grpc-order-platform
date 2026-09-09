package com.example.order.service.grpc;

import io.grpc.*;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;

import java.util.List;
import java.util.Set;

/**
 * Validates the bearer token at the gRPC boundary. The REST edge already validated
 * it, but this service does not trust the edge: anything on the network can open a
 * channel, and an internal caller is not automatically an authorized one.
 */
public class AuthInterceptor implements ServerInterceptor {

    public static final Context.Key<String> PRINCIPAL_KEY = Context.key("principal");
    public static final Context.Key<List<String>> SCOPES_KEY = Context.key("scopes");

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    // Probes must work without credentials or Kubernetes restarts the pod forever.
    private static final Set<String> EXEMPT_SERVICES =
            Set.of("grpc.health.v1.Health", "grpc.reflection.v1alpha.ServerReflection",
                   "grpc.reflection.v1.ServerReflection");

    private static final Set<String> WRITE_METHODS = Set.of("CreateOrder", "UpdateOrderStatus");

    private final JwtDecoder decoder;

    public AuthInterceptor(JwtDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        var descriptor = call.getMethodDescriptor();
        if (EXEMPT_SERVICES.contains(descriptor.getServiceName())) {
            return next.startCall(call, headers);
        }

        String header = headers.get(AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return close(call, Status.UNAUTHENTICATED.withDescription("Missing bearer token"));
        }

        try {
            var jwt = decoder.decode(header.substring("Bearer ".length()));
            List<String> scopes = List.of(
                    String.valueOf(jwt.getClaims().getOrDefault("scope", "")).split(" "));

            String required = WRITE_METHODS.contains(descriptor.getBareMethodName())
                    ? "orders:write" : "orders:read";
            if (!scopes.contains(required)) {
                return close(call, Status.PERMISSION_DENIED
                        .withDescription("Requires scope " + required));
            }

            Context context = Context.current()
                    .withValue(PRINCIPAL_KEY, jwt.getSubject())
                    .withValue(SCOPES_KEY, scopes);
            return Contexts.interceptCall(context, call, headers, next);
        } catch (JwtException e) {
            // The reason is deliberately not echoed: it tells an attacker which part
            // of their forged token to fix.
            return close(call, Status.UNAUTHENTICATED.withDescription("Invalid token"));
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> close(ServerCall<ReqT, RespT> call, Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {
        };
    }
}
