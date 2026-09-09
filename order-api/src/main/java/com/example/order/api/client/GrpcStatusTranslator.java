package com.example.order.api.client;

import com.example.order.api.client.exception.*;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.util.List;

public final class GrpcStatusTranslator {

    private GrpcStatusTranslator() {
    }

    public static Throwable translate(Throwable throwable) {
        if (!(throwable instanceof StatusRuntimeException sre)) {
            return throwable;
        }
        var rpcStatus = StatusProto.fromThrowable(sre);
        String message = sre.getStatus().getDescription() == null
                ? sre.getStatus().getCode().name()
                : sre.getStatus().getDescription();

        return switch (sre.getStatus().getCode()) {
            case NOT_FOUND -> new RemoteOrderNotFound(message);
            case INVALID_ARGUMENT -> new RemoteValidationFailed(message, fieldViolations(rpcStatus));
            case FAILED_PRECONDITION -> new RemoteIllegalTransition(message);
            case ABORTED -> new RemoteVersionConflict(message);
            case UNAVAILABLE -> new RemoteUnavailable(message, sre);
            case DEADLINE_EXCEEDED -> new RemoteTimeout(message, sre);
            default -> new RemoteInternal(message, errorId(rpcStatus), sre);
        };
    }

    private static List<RemoteFieldViolation> fieldViolations(com.google.rpc.Status status) {
        if (status == null) {
            return List.of();
        }
        for (var any : status.getDetailsList()) {
            if (any.is(BadRequest.class)) {
                try {
                    return any.unpack(BadRequest.class).getFieldViolationsList().stream()
                            .map(v -> new RemoteFieldViolation(v.getField(), v.getDescription()))
                            .toList();
                } catch (Exception e) {
                    return List.of();
                }
            }
        }
        return List.of();
    }

    private static String errorId(com.google.rpc.Status status) {
        if (status == null) {
            return null;
        }
        for (var any : status.getDetailsList()) {
            if (any.is(ErrorInfo.class)) {
                try {
                    return any.unpack(ErrorInfo.class).getMetadataMap().get("errorId");
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return null;
    }
}
