package com.example.order.service.grpc;

import io.grpc.Context;
import io.grpc.Status;

/**
 * Checked before expensive work. If the caller's budget is already spent, the
 * response can never be delivered, so doing the work is pure waste — it burns a
 * connection, a thread, and a database transaction for nobody.
 */
public final class DeadlineGuard {

    private DeadlineGuard() {
    }

    public static void checkNotExpired() {
        var deadline = Context.current().getDeadline();
        if (deadline != null && deadline.isExpired()) {
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("Deadline expired before the server began work")
                    .asRuntimeException();
        }
    }
}
