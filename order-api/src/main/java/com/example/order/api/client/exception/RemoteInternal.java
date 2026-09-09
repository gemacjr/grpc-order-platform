package com.example.order.api.client.exception;

public class RemoteInternal extends RemoteOrderException {
    private final String errorId;

    public RemoteInternal(String message, String errorId, Throwable cause) {
        super(message, cause);
        this.errorId = errorId;
    }

    public String errorId() { return errorId; }
}
