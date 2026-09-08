package com.example.order.service.persistence;

import com.example.order.testing.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SchemaIT {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        PostgresFixture.registerProperties(registry);
    }

    @Autowired JdbcClient jdbc;

    @Test
    void flywayCreatesAllTables() {
        var tables = jdbc.sql("select table_name from information_schema.tables where table_schema = 'public'")
                .query(String.class).list();
        assertThat(tables).contains("orders", "order_lines", "order_events");
    }

    @Test
    void idempotencyKeyIsUniquePerCustomer() {
        var indexes = jdbc.sql("select indexname from pg_indexes where tablename = 'orders'")
                .query(String.class).list();
        assertThat(indexes).contains("orders_customer_idempotency_uk");
    }
}
