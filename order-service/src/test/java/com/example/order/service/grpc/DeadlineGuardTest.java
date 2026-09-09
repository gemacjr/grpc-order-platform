package com.example.order.service.grpc;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class DeadlineGuardTest {

    @Test
    void passesWhenBudgetRemains() {
        var context = Context.current().withDeadline(
                Deadline.after(5, TimeUnit.SECONDS), Executors.newSingleThreadScheduledExecutor());
        context.run(() -> assertThatNoException().isThrownBy(DeadlineGuard::checkNotExpired));
    }

    @Test
    void throwsDeadlineExceededWhenBudgetIsSpent() {
        var context = Context.current().withDeadline(
                Deadline.after(-1, TimeUnit.SECONDS), Executors.newSingleThreadScheduledExecutor());
        context.run(() -> assertThatThrownBy(DeadlineGuard::checkNotExpired)
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED"));
    }

    @Test
    void passesWhenCallerSetNoDeadline() {
        assertThatNoException().isThrownBy(DeadlineGuard::checkNotExpired);
    }
}
