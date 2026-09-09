package com.example.order.service.grpc;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuthInterceptorScopeParsingTest {

    @Test
    void spaceSeparatedStringIsSplit() {
        var scopes = AuthInterceptor.parseScopes(Map.of("scope", "orders:read orders:write"));
        assertThat(scopes).containsExactlyInAnyOrder("orders:read", "orders:write");
    }

    @Test
    void commaSeparatedStringIsSplit() {
        var scopes = AuthInterceptor.parseScopes(Map.of("scope", "orders:read,orders:write"));
        assertThat(scopes).containsExactlyInAnyOrder("orders:read", "orders:write");
    }

    @Test
    void collectionFormIsMappedToStrings() {
        var scopes = AuthInterceptor.parseScopes(Map.of("scope", List.of("orders:read", "orders:write")));
        assertThat(scopes).containsExactlyInAnyOrder("orders:read", "orders:write");
    }

    @Test
    void missingClaimYieldsEmptySet() {
        var scopes = AuthInterceptor.parseScopes(Map.of());
        assertThat(scopes).isEmpty();
    }

    @Test
    void nullClaimYieldsEmptySet() {
        var claims = new java.util.HashMap<String, Object>();
        claims.put("scope", null);
        var scopes = AuthInterceptor.parseScopes(claims);
        assertThat(scopes).isEmpty();
    }

    @Test
    void scpClaimIsUsedAsFallbackWhenScopeAbsent() {
        var scopes = AuthInterceptor.parseScopes(Map.of("scp", "orders:read orders:write"));
        assertThat(scopes).containsExactlyInAnyOrder("orders:read", "orders:write");
    }
}
