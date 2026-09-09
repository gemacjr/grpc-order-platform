package com.example.order.api.client;

import com.example.order.api.client.exception.*;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcStatusTranslatorTest {

    private static io.grpc.StatusRuntimeException remote(Status.Code code, String reason, Any... extra) {
        var builder = com.google.rpc.Status.newBuilder()
                .setCode(code.value())
                .setMessage("boom")
                .addDetails(Any.pack(ErrorInfo.newBuilder()
                        .setReason(reason).setDomain("order.v1").build()));
        for (Any any : extra) {
            builder.addDetails(any);
        }
        return StatusProto.toStatusRuntimeException(builder.build());
    }

    @Test
    void notFoundBecomesRemoteOrderNotFound() {
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.NOT_FOUND, "ORDER_NOT_FOUND")))
                .isInstanceOf(RemoteOrderNotFound.class);
    }

    @Test
    void invalidArgumentCarriesFieldViolationsThrough() {
        var badRequest = Any.pack(BadRequest.newBuilder()
                .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                        .setField("currency").setDescription("must be 3 letters").build())
                .build());

        var translated = GrpcStatusTranslator.translate(
                remote(Status.Code.INVALID_ARGUMENT, "INVALID_ARGUMENT", badRequest));

        assertThat(translated).isInstanceOf(RemoteValidationFailed.class);
        assertThat(((RemoteValidationFailed) translated).violations())
                .extracting(RemoteFieldViolation::field).containsExactly("currency");
    }

    @Test
    void abortedBecomesVersionConflictAndFailedPreconditionBecomesIllegalTransition() {
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.ABORTED, "VERSION_CONFLICT")))
                .isInstanceOf(RemoteVersionConflict.class);
        assertThat(GrpcStatusTranslator.translate(remote(Status.Code.FAILED_PRECONDITION, "ILLEGAL_STATUS_TRANSITION")))
                .isInstanceOf(RemoteIllegalTransition.class);
    }

    @Test
    void transportFailuresAreDistinguishedFromServerErrors() {
        assertThat(GrpcStatusTranslator.translate(Status.UNAVAILABLE.asRuntimeException()))
                .isInstanceOf(RemoteUnavailable.class);
        assertThat(GrpcStatusTranslator.translate(Status.DEADLINE_EXCEEDED.asRuntimeException()))
                .isInstanceOf(RemoteTimeout.class);
        assertThat(GrpcStatusTranslator.translate(Status.INTERNAL.asRuntimeException()))
                .isInstanceOf(RemoteInternal.class);
    }

    @Test
    void nonGrpcThrowablesPassThroughUnchanged() {
        var original = new IllegalStateException("local");
        assertThat(GrpcStatusTranslator.translate(original)).isSameAs(original);
    }
}
