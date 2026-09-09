package com.example.order.api.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@code spring.grpc.client.channels.<name>.ssl.*} actually binds to
 * {@link GrpcClientProperties.ChannelConfig.Ssl}. That class (decompiled from
 * spring-grpc-client-spring-boot-autoconfigure 1.0.3) exposes only enabled/bundle -
 * there is no trust-store field - so, as on the server, TLS material must be supplied
 * through a named {@code spring.ssl.bundle.*} bundle referenced by {@code bundle}.
 * A regression back to the wrong property name would silently leave TLS "configured"
 * but inert (Boot ignores unknown nested keys), hence this direct assertion rather
 * than a full handshake test that certificates aren't available for in CI.
 */
class GrpcTlsPropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(PropertiesConfig.class);

    @Test
    void sslEnabledAndBundleBindFromConfiguredProperties() {
        contextRunner
                .withPropertyValues(
                        "spring.grpc.client.channels.order-service.ssl.enabled=true",
                        "spring.grpc.client.channels.order-service.ssl.bundle=grpc-client")
                .run(context -> {
                    var properties = context.getBean(GrpcClientProperties.class);
                    var ssl = properties.getChannel("order-service").getSsl();
                    assertThat(ssl.isEnabled()).isTrue();
                    assertThat(ssl.getBundle()).isEqualTo("grpc-client");
                });
    }

    @EnableConfigurationProperties(GrpcClientProperties.class)
    static class PropertiesConfig {
    }
}
