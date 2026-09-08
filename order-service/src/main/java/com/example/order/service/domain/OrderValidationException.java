package com.example.order.service.domain;

import java.util.List;

public class OrderValidationException extends RuntimeException {
    private final List<FieldViolation> violations;

    public OrderValidationException(List<FieldViolation> violations) {
        super("Request has %d field violation(s)".formatted(violations.size()));
        this.violations = List.copyOf(violations);
    }

    public List<FieldViolation> violations() {
        return violations;
    }
}
