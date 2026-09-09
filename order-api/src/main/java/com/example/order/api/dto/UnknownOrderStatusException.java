package com.example.order.api.dto;

/**
 * Thrown when a caller-supplied status string does not map to a known {@code OrderStatus}.
 * This is a local rejection — we never even ask the server — so it is deliberately distinct
 * from {@code RemoteValidationFailed}, which means the server rejected the request.
 */
public class UnknownOrderStatusException extends RuntimeException {

    private final String field;
    private final String value;

    public UnknownOrderStatusException(String field, String value) {
        super("'" + value + "' is not a known order status");
        this.field = field;
        this.value = value;
    }

    public String field() {
        return field;
    }

    public String value() {
        return value;
    }
}
