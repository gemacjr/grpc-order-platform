package com.example.order.service.domain;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static com.example.order.service.domain.OrderStatus.*;

class StatusTransitionsTest {

    @Test
    void pendingMayBeConfirmedOrCancelled() {
        assertThat(StatusTransitions.isAllowed(PENDING, CONFIRMED)).isTrue();
        assertThat(StatusTransitions.isAllowed(PENDING, CANCELLED)).isTrue();
    }

    @Test
    void pendingMayNotSkipStraightToShipped() {
        assertThat(StatusTransitions.isAllowed(PENDING, SHIPPED)).isFalse();
    }

    @Test
    void shippedAndCancelledAreTerminal() {
        for (OrderStatus target : OrderStatus.values()) {
            assertThat(StatusTransitions.isAllowed(SHIPPED, target)).isFalse();
            assertThat(StatusTransitions.isAllowed(CANCELLED, target)).isFalse();
        }
    }

    @Test
    void selfTransitionIsRejected() {
        assertThat(StatusTransitions.isAllowed(PENDING, PENDING)).isFalse();
    }

    @Test
    void assertAllowedThrowsWithBothStatusesNamed() {
        assertThatThrownBy(() -> StatusTransitions.assertAllowed(CANCELLED, SHIPPED))
                .isInstanceOf(IllegalStatusTransition.class)
                .hasMessageContaining("CANCELLED")
                .hasMessageContaining("SHIPPED");
    }
}
