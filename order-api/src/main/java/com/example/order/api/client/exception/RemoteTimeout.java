package com.example.order.api.client.exception;

public class RemoteTimeout extends RemoteOrderException {
    public RemoteTimeout(String message, Throwable cause) { super(message, cause); }
}
