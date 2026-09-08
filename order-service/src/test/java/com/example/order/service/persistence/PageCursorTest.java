package com.example.order.service.persistence;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

class PageCursorTest {

    @Test
    void roundTripsThroughEncoding() {
        var cursor = new PageCursor(Instant.parse("2026-09-08T12:00:00Z"), UUID.randomUUID());
        assertThat(PageCursor.decode(cursor.encode())).contains(cursor);
    }

    @Test
    void tokenIsOpaqueBase64WithNoPlaintextTimestamp() {
        var cursor = new PageCursor(Instant.parse("2026-09-08T12:00:00Z"), UUID.randomUUID());
        assertThat(cursor.encode()).doesNotContain("2026");
    }

    @Test
    void garbageTokenDecodesToEmptyRatherThanThrowing() {
        assertThat(PageCursor.decode("not-a-cursor")).isEmpty();
        assertThat(PageCursor.decode("")).isEmpty();
        assertThat(PageCursor.decode(null)).isEmpty();
    }
}
