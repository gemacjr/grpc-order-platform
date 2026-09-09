package com.example.order.api.client.exception;

public class RemoteOrderException extends RuntimeException {
    public RemoteOrderException(String message) { super(message); }
    public RemoteOrderException(String message, Throwable cause) { super(message, cause); }
}
