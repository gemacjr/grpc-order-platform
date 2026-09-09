package com.example.order.api.web;

import com.example.order.api.client.exception.*;
import com.example.order.api.dto.UnknownOrderStatusException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class ProblemDetailAdvice {

    private static final String BASE = "https://errors.example.com/orders/";
    private static final String RETRY_AFTER_SECONDS = "5";

    @ExceptionHandler(RemoteOrderNotFound.class)
    public ProblemDetail notFound(RemoteOrderNotFound e) {
        return problem(HttpStatus.NOT_FOUND, "Order not found", e.getMessage(), "not-found");
    }

    @ExceptionHandler(RemoteValidationFailed.class)
    public ProblemDetail validation(RemoteValidationFailed e) {
        var detail = problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage(), "validation");
        detail.setProperty("errors", e.violations().stream()
                .map(v -> Map.of("field", v.field(), "message", v.description()))
                .toList());
        return detail;
    }

    @ExceptionHandler(UnknownOrderStatusException.class)
    public ProblemDetail unknownStatus(UnknownOrderStatusException e) {
        var detail = problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage(), "validation");
        detail.setProperty("errors", List.of(Map.of("field", e.field(), "message", e.getMessage())));
        return detail;
    }

    @ExceptionHandler(RemoteIllegalTransition.class)
    public ProblemDetail illegalTransition(RemoteIllegalTransition e) {
        return problem(HttpStatus.CONFLICT, "Illegal status transition", e.getMessage(), "illegal-transition");
    }

    @ExceptionHandler(RemoteVersionConflict.class)
    public ProblemDetail versionConflict(RemoteVersionConflict e) {
        var detail = problem(HttpStatus.CONFLICT, "Version conflict", e.getMessage(), "version-conflict");
        detail.setProperty("remediation", "Re-read the order and retry with its current version");
        return detail;
    }

    @ExceptionHandler(RemoteUnavailable.class)
    public ResponseEntity<ProblemDetail> unavailable(RemoteUnavailable e) {
        var detail = problem(HttpStatus.SERVICE_UNAVAILABLE, "Order service unavailable",
                "The order service is not reachable. Retry shortly.", "unavailable");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", RETRY_AFTER_SECONDS)
                .body(detail);
    }

    @ExceptionHandler(RemoteTimeout.class)
    public ProblemDetail timeout(RemoteTimeout e) {
        return problem(HttpStatus.GATEWAY_TIMEOUT, "Order service timed out",
                "The order service did not respond within the deadline.", "timeout");
    }

    @ExceptionHandler(RemoteInternal.class)
    public ProblemDetail internal(RemoteInternal e) {
        // The upstream message may contain internal detail; only the correlation id escapes.
        var detail = problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "An unexpected error occurred.", "internal");
        detail.setProperty("errorId", e.errorId());
        return detail;
    }

    private ProblemDetail problem(HttpStatus status, String title, String detailText, String slug) {
        var detail = ProblemDetail.forStatusAndDetail(status, detailText);
        detail.setTitle(title);
        detail.setType(URI.create(BASE + slug));
        return detail;
    }
}
