package com.example.order.service.persistence;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

public record PageCursor(Instant createdAt, UUID id) {

    public String encode() {
        String raw = createdAt.toEpochMilli() + ":" + id;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Optional<PageCursor> decode(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            int split = raw.indexOf(':');
            if (split < 0) {
                return Optional.empty();
            }
            return Optional.of(new PageCursor(
                    Instant.ofEpochMilli(Long.parseLong(raw.substring(0, split))),
                    UUID.fromString(raw.substring(split + 1))));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
