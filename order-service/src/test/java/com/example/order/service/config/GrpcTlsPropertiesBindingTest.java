package com.example.order.service.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@code spring.grpc.server.ssl.*} actually binds to
 * {@link GrpcServerProperties.Ssl}. That class (decompiled from
 * spring-grpc-server-spring-boot-autoconfigure 1.0.3) exposes only
 * enabled/clientAuth/bundle/secure - there is no certificate or private-key field, so
 * TLS material must be supplied through a named {@code spring.ssl.bundle.*} bundle
 * referenced by {@code bundle}, not inline. A regression back to the wrong property
 * names would silently leave TLS "configured" but inert (Boot ignores unknown nested
 * keys), which is why this is asserted directly rather than relying on a full
 * handshake test that certificates aren't available for in CI.
 */
class GrpcTlsPropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(PropertiesConfig.class);

    @Test
    void sslEnabledAndBundleBindFromConfiguredProperties() {
        contextRunner
                .withPropertyValues(
                        "spring.grpc.server.ssl.enabled=true",
                        "spring.grpc.server.ssl.bundle=grpc-server")
                .run(context -> {
                    var properties = context.getBean(GrpcServerProperties.class);
                    assertThat(properties.getSsl().getEnabled()).isTrue();
                    assertThat(properties.getSsl().getBundle()).isEqualTo("grpc-server");
                });
    }

    @Test
    void sslDefaultsToDisabledWithNoBundle() {
        contextRunner.run(context -> {
            var properties = context.getBean(GrpcServerProperties.class);
            assertThat(properties.getSsl().determineEnabled()).isFalse();
            assertThat(properties.getSsl().getBundle()).isNull();
        });
    }

    @EnableConfigurationProperties(GrpcServerProperties.class)
    static class PropertiesConfig {
    }
}
