package com.example.order.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.server.SecurityWebFilterChain;

import java.util.List;

/**
 * Security defaults to OFF ({@code order.security.enabled=false}) so tests and local
 * runs work without a JWKS endpoint; the compose stack turns it on explicitly. Even
 * with the edge enforcing authentication, the gRPC service re-validates the token
 * itself rather than trusting the edge.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    @Order(1)
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "true")
    public SecurityWebFilterChain securedFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)   // stateless bearer-token API
                .authorizeExchange(exchanges -> exchanges
                        .pathMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .pathMatchers("/actuator/**").authenticated()
                        .anyExchange().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> {
                }))
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "true")
    public JwtDecoder jwtDecoder(
            @Value("${order.security.jwk-set-uri:}") String jwkSetUri,
            @Value("${order.security.issuer:https://dev.example.com}") String expectedIssuer,
            @Value("${order.security.audience:order-api}") String expectedAudience) {
        if (jwkSetUri == null || jwkSetUri.isBlank()) {
            throw new IllegalStateException(
                    "order.security.enabled=true but order.security.jwk-set-uri is not set. "
                            + "Set the ORDER_SECURITY_JWK_SET_URI environment variable (or the "
                            + "order.security.jwk-set-uri property) to a reachable JWKS endpoint.");
        }
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();
        OAuth2TokenValidator<Jwt> withIssuerAndAudience = new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefault(),
                new JwtIssuerValidator(expectedIssuer),
                new JwtClaimValidator<List<String>>(JwtClaimNames.AUD,
                        aud -> aud != null && aud.contains(expectedAudience)));
        decoder.setJwtValidator(withIssuerAndAudience);
        return decoder;
    }

    // order.security.enabled=false (the default): no authentication is enforced, so
    // existing tests - especially EndToEndIT, which boots both services with no
    // tokens - keep passing untouched.
    @Bean
    @Order(2)
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "false", matchIfMissing = true)
    public SecurityWebFilterChain openFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges.anyExchange().permitAll())
                .build();
    }
}
