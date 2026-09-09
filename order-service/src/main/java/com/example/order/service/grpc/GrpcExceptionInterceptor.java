package com.example.order.service.grpc;

import com.example.order.service.domain.*;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * Translates domain exceptions into gRPC statuses with machine-readable trailers.
 * Wrapping the listener (rather than only the call) is required: exceptions thrown
 * while reading the request message surface through onHalfClose, not through the
 * initial call handler.
 *
 * <p>Not a {@code @Component}: it is registered explicitly, once, as an ordered
 * {@code @GlobalServerInterceptor} bean in {@code GrpcServerConfig}. Leaving it
 * annotated here as well would double-apply it via component scan.
 */
public class GrpcExceptionInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(GrpcExceptionInterceptor.class);
    private static final String DOMAIN = "order.v1";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (RuntimeException e) {
                    closeWith(call, e);
                }
            }

            @Override
            public void onMessage(ReqT message) {
                try {
                    super.onMessage(message);
                } catch (RuntimeException e) {
                    closeWith(call, e);
                }
            }
        };
    }

    private <ReqT, RespT> void closeWith(ServerCall<ReqT, RespT> call, RuntimeException e) {
        var status = toStatus(e);
        call.close(Status.fromCodeValue(status.getCode()).withDescription(status.getMessage()),
                trailersFor(status));
    }

    public com.google.rpc.Status toStatus(Throwable e) {
        if (e instanceof OrderNotFound notFound) {
            return build(Status.Code.NOT_FOUND, notFound.getMessage(),
                    errorInfo("ORDER_NOT_FOUND", Map.of("orderId", notFound.orderId().toString())));
        }
        if (e instanceof OrderValidationException validation) {
            var badRequest = BadRequest.newBuilder();
            validation.violations().forEach(v -> badRequest.addFieldViolations(
                    BadRequest.FieldViolation.newBuilder()
                            .setField(v.field()).setDescription(v.description()).build()));
            return com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.INVALID_ARGUMENT.value())
                    .setMessage(validation.getMessage())
                    .addDetails(Any.pack(errorInfoMessage("INVALID_ARGUMENT", Map.of())))
                    .addDetails(Any.pack(badRequest.build()))
                    .build();
        }
        if (e instanceof IllegalStatusTransition transition) {
            return build(Status.Code.FAILED_PRECONDITION, transition.getMessage(),
                    errorInfo("ILLEGAL_STATUS_TRANSITION",
                            Map.of("from", transition.from().name(), "to", transition.to().name())));
        }
        if (e instanceof OrderVersionConflict conflict) {
            return build(Status.Code.ABORTED, conflict.getMessage(),
                    errorInfo("VERSION_CONFLICT", Map.of(
                            "orderId", conflict.orderId().toString(),
                            "expectedVersion", String.valueOf(conflict.expectedVersion()),
                            "actualVersion", String.valueOf(conflict.actualVersion()))));
        }

        // Unexpected: log everything, return an opaque id. Internal detail must not
        // reach the caller, but support needs a handle to correlate against the logs.
        String errorId = UUID.randomUUID().toString();
        log.error("Unhandled exception, errorId={}", errorId, e);
        return build(Status.Code.INTERNAL, "Internal error. errorId=" + errorId,
                errorInfo("INTERNAL", Map.of("errorId", errorId)));
    }

    private com.google.rpc.Status build(Status.Code code, String message, Any detail) {
        return com.google.rpc.Status.newBuilder()
                .setCode(code.value()).setMessage(message).addDetails(detail).build();
    }

    private Any errorInfo(String reason, Map<String, String> metadata) {
        return Any.pack(errorInfoMessage(reason, metadata));
    }

    private ErrorInfo errorInfoMessage(String reason, Map<String, String> metadata) {
        return ErrorInfo.newBuilder().setReason(reason).setDomain(DOMAIN).putAllMetadata(metadata).build();
    }

    private Metadata trailersFor(com.google.rpc.Status status) {
        return StatusProto.toStatusRuntimeException(status).getTrailers();
    }
}
