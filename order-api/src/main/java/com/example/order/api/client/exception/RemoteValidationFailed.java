package com.example.order.api.client.exception;

import java.util.List;

public class RemoteValidationFailed extends RemoteOrderException {
    private final List<RemoteFieldViolation> violations;

    public RemoteValidationFailed(String message, List<RemoteFieldViolation> violations) {
        super(message);
        this.violations = List.copyOf(violations);
    }

    public List<RemoteFieldViolation> violations() { return violations; }
}
