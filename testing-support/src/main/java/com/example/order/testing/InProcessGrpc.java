package com.example.order.testing;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/** In-process gRPC server plus channel: no ports, no TLS, millisecond startup. */
public final class InProcessGrpc {

    public record Harness(ManagedChannel channel, Server server) implements AutoCloseable {
        @Override
        public void close() {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private InProcessGrpc() {
    }

    public static Harness start(BindableService service, ServerInterceptor... interceptors) {
        String name = "inproc-" + UUID.randomUUID();
        try {
            Server server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(io.grpc.ServerInterceptors.intercept(service, List.of(interceptors)))
                    .build()
                    .start();
            ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            return new Harness(channel, server);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start in-process gRPC server", e);
        }
    }
}
