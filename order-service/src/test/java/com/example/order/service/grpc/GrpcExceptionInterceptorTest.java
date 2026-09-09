package com.example.order.service.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcExceptionInterceptorTest {

    private final GrpcExceptionInterceptor interceptor = new GrpcExceptionInterceptor();

    @Test
    void deadlineExceededPassesThroughUnchangedRatherThanBecomingInternal() {
        var thrown = Status.DEADLINE_EXCEEDED
                .withDescription("Deadline expired before the server began work")
                .asRuntimeException();

        var status = interceptor.toStatus(thrown);

        assertThat(status.getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED.value());
        assertThat(status.getMessage()).isEqualTo("Deadline expired before the server began work");
    }

    @Test
    void arbitraryStatusRuntimeExceptionPassesThroughUnchanged() {
        var thrown = Status.UNAVAILABLE.withDescription("shedding load").asRuntimeException();

        var status = interceptor.toStatus(thrown);

        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE.value());
        assertThat(status.getMessage()).isEqualTo("shedding load");
    }

    @Test
    void statusExceptionAlsoPassesThroughUnchanged() throws StatusException {
        var thrown = new StatusException(Status.RESOURCE_EXHAUSTED.withDescription("no room"));

        var status = interceptor.toStatus(thrown);

        assertThat(status.getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED.value());
        assertThat(status.getMessage()).isEqualTo("no room");
    }

    @Test
    void unhandledRuntimeExceptionStillBecomesOpaqueInternal() {
        var status = interceptor.toStatus(new IllegalStateException("boom"));

        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL.value());
        assertThat(status.getMessage()).doesNotContain("boom");
    }
}
