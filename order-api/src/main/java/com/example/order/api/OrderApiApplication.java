package com.example.order.api;

import org.springframework.boot.SpringApplication;
import reactor.core.publisher.Hooks;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderApiApplication {
    public static void main(String[] args) {
        // In WebFlux the observation context lives in the Reactor context, not a
        // ThreadLocal. Without this, the HTTP server span never reaches the gRPC
        // client call further down the reactive chain, and one request produces TWO
        // disconnected traces instead of one spanning REST -> gRPC.
        Hooks.enableAutomaticContextPropagation();
        SpringApplication.run(OrderApiApplication.class, args);
    }
}
