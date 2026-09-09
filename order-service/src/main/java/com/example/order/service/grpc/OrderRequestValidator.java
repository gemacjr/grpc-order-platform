package com.example.order.service.grpc;

import com.example.order.proto.*;
import com.example.order.service.domain.FieldViolation;
import com.example.order.service.domain.OrderValidationException;
import com.example.order.service.persistence.PageCursor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class OrderRequestValidator {

    public void validateCreate(CreateOrderRequest request) {
        var violations = new ArrayList<FieldViolation>();
        if (request.getCustomerId().isBlank()) {
            violations.add(new FieldViolation("customer_id", "must not be blank"));
        }
        if (request.getIdempotencyKey().isBlank()) {
            violations.add(new FieldViolation("idempotency_key", "must not be blank"));
        }
        if (request.getCurrency().length() != 3) {
            violations.add(new FieldViolation("currency", "must be a 3-letter ISO-4217 code"));
        }
        if (request.getLinesCount() == 0) {
            violations.add(new FieldViolation("lines", "must contain at least one line"));
        }
        for (int i = 0; i < request.getLinesCount(); i++) {
            OrderLine line = request.getLines(i);
            if (line.getSku().isBlank()) {
                violations.add(new FieldViolation("lines[%d].sku".formatted(i), "must not be blank"));
            }
            if (line.getQuantity() <= 0) {
                violations.add(new FieldViolation("lines[%d].quantity".formatted(i), "must be greater than zero"));
            }
            if (line.getUnitPriceMinor() < 0) {
                violations.add(new FieldViolation("lines[%d].unit_price_minor".formatted(i), "must not be negative"));
            }
        }
        throwIfAny(violations);
    }

    public UUID validateGet(GetOrderRequest request) {
        return parseId(request.getId(), "id");
    }

    public PageCursor validateList(ListOrdersRequest request) {
        var violations = new ArrayList<FieldViolation>();
        if (request.getPageSize() < 0) {
            violations.add(new FieldViolation("page_size", "must not be negative"));
        }
        PageCursor cursor = null;
        if (!request.getPageToken().isBlank()) {
            cursor = PageCursor.decode(request.getPageToken()).orElse(null);
            if (cursor == null) {
                violations.add(new FieldViolation("page_token", "is not a valid page token"));
            }
        }
        throwIfAny(violations);
        return cursor;
    }

    public UUID validateUpdateStatus(UpdateOrderStatusRequest request) {
        var violations = new ArrayList<FieldViolation>();
        UUID id = null;
        try {
            id = UUID.fromString(request.getId());
        } catch (IllegalArgumentException e) {
            violations.add(new FieldViolation("id", "must be a UUID"));
        }
        if (request.getNewStatus() == OrderStatus.ORDER_STATUS_UNSPECIFIED
                || request.getNewStatus() == OrderStatus.UNRECOGNIZED) {
            violations.add(new FieldViolation("new_status", "must be a known, specified status"));
        }
        if (request.getExpectedVersion() <= 0) {
            violations.add(new FieldViolation("expected_version", "must be greater than zero"));
        }
        throwIfAny(violations);
        return id;
    }

    private UUID parseId(String raw, String field) {
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new OrderValidationException(List.of(new FieldViolation(field, "must be a UUID")));
        }
    }

    private void throwIfAny(List<FieldViolation> violations) {
        if (!violations.isEmpty()) {
            throw new OrderValidationException(violations);
        }
    }
}
