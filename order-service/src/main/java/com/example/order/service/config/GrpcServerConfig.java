package com.example.order.service.config;

import com.example.order.service.grpc.AuthInterceptor;
import com.example.order.service.grpc.GrpcExceptionInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

import java.util.List;

@Configuration
public class GrpcServerConfig {

    @Bean
    @GlobalServerInterceptor
    @Order(Ordered.HIGHEST_PRECEDENCE + 100)
    public GrpcExceptionInterceptor grpcExceptionInterceptor() {
        return new GrpcExceptionInterceptor();
    }

    // Between the observation interceptor (HIGHEST_PRECEDENCE) and the exception
    // interceptor (HIGHEST_PRECEDENCE + 100), so an auth failure is still traced.
    // AuthInterceptor sits OUTSIDE the exception interceptor and closes the call
    // itself on failure (UNAUTHENTICATED/PERMISSION_DENIED) - it is not routed
    // through GrpcExceptionInterceptor.toStatus.
    @Bean
    @GlobalServerInterceptor
    @Order(Ordered.HIGHEST_PRECEDENCE + 50)
    @ConditionalOnProperty(name = "order.security.enabled", havingValue = "true")
    public AuthInterceptor authInterceptor(JwtDecoder decoder) {
        return new AuthInterceptor(decoder);
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
}
