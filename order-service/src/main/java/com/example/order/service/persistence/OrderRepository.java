package com.example.order.service.persistence;

import com.example.order.service.domain.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.*;

@Repository
public class OrderRepository {

    private static final int MAX_PAGE_SIZE = 500;

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Order insert(Order order, String idempotencyKey) {
        jdbc.sql("""
                insert into orders (id, customer_id, total_minor, currency, status,
                                    version, idempotency_key, created_at, updated_at)
                values (:id, :customerId, :totalMinor, :currency, :status,
                        :version, :idempotencyKey, :createdAt, :updatedAt)
                """)
                .param("id", order.id())
                .param("customerId", order.customerId())
                .param("totalMinor", order.totalMinor())
                .param("currency", order.currency())
                .param("status", order.status().name())
                .param("version", order.version())
                .param("idempotencyKey", idempotencyKey)
                .param("createdAt", java.sql.Timestamp.from(order.createdAt()))
                .param("updatedAt", java.sql.Timestamp.from(order.updatedAt()))
                .update();

        for (OrderLine line : order.lines()) {
            jdbc.sql("""
                    insert into order_lines (order_id, sku, quantity, unit_price_minor)
                    values (:orderId, :sku, :quantity, :unitPrice)
                    """)
                    .param("orderId", order.id())
                    .param("sku", line.sku())
                    .param("quantity", line.quantity())
                    .param("unitPrice", line.unitPriceMinor())
                    .update();
        }
        return order;
    }

    public Optional<Order> findById(UUID id) {
        return jdbc.sql("select * from orders where id = :id")
                .param("id", id)
                .query(this::mapOrder)
                .optional()
                .map(this::withLines);
    }

    public Optional<Order> findByIdempotencyKey(String customerId, String key) {
        if (key == null || key.isBlank()) {
            return Optional.empty();
        }
        return jdbc.sql("select * from orders where customer_id = :customerId and idempotency_key = :key")
                .param("customerId", customerId)
                .param("key", key)
                .query(this::mapOrder)
                .optional()
                .map(this::withLines);
    }

    public OrderPage list(String customerId, OrderStatus status, int pageSize, PageCursor cursor) {
        int limit = Math.clamp(pageSize <= 0 ? 50 : pageSize, 1, MAX_PAGE_SIZE);

        // Fetch one extra row: its presence is how we know another page exists,
        // without a second COUNT query that would be inconsistent under concurrent writes.
        var sql = new StringBuilder("select * from orders where 1 = 1");
        var params = new HashMap<String, Object>();
        if (customerId != null) {
            sql.append(" and customer_id = :customerId");
            params.put("customerId", customerId);
        }
        if (status != null) {
            sql.append(" and status = :status");
            params.put("status", status.name());
        }
        if (cursor != null) {
            sql.append(" and (created_at, id) < (:cursorCreatedAt, :cursorId)");
            params.put("cursorCreatedAt", java.sql.Timestamp.from(cursor.createdAt()));
            params.put("cursorId", cursor.id());
        }
        sql.append(" order by created_at desc, id desc limit :limit");
        params.put("limit", limit + 1);

        var spec = jdbc.sql(sql.toString());
        for (var entry : params.entrySet()) {
            spec = spec.param(entry.getKey(), entry.getValue());
        }

        List<Order> rows = spec.query(this::mapOrder).list();
        boolean hasMore = rows.size() > limit;
        List<Order> page = hasMore ? rows.subList(0, limit) : rows;
        List<Order> hydrated = page.stream().map(this::withLines).toList();

        String nextToken = "";
        if (hasMore && !hydrated.isEmpty()) {
            Order last = hydrated.get(hydrated.size() - 1);
            nextToken = new PageCursor(last.createdAt(), last.id()).encode();
        }
        return new OrderPage(hydrated, nextToken);
    }

    public Order updateStatus(UUID id, OrderStatus newStatus, long expectedVersion, Instant now) {
        int updated = jdbc.sql("""
                update orders
                   set status = :status, version = version + 1, updated_at = :now
                 where id = :id and version = :expectedVersion
                """)
                .param("status", newStatus.name())
                .param("now", java.sql.Timestamp.from(now))
                .param("id", id)
                .param("expectedVersion", expectedVersion)
                .update();

        if (updated == 0) {
            // Zero rows is ambiguous: the order may be absent, or present at another
            // version. Distinguish them, because they mean different things to callers.
            Order current = findById(id).orElseThrow(() -> new OrderNotFound(id));
            throw new OrderVersionConflict(id, expectedVersion, current.version());
        }
        return findById(id).orElseThrow(() -> new OrderNotFound(id));
    }

    public long appendEvent(UUID orderId, OrderStatus oldStatus, OrderStatus newStatus, Instant at) {
        return jdbc.sql("""
                insert into order_events (order_id, old_status, new_status, occurred_at)
                values (:orderId, :oldStatus, :newStatus, :at)
                returning sequence
                """)
                .param("orderId", orderId)
                .param("oldStatus", oldStatus.name())
                .param("newStatus", newStatus.name())
                .param("at", java.sql.Timestamp.from(at))
                .query(Long.class)
                .single();
    }

    public List<OrderEvent> eventsAfter(long sequence, String customerId, int limit) {
        var sql = new StringBuilder("""
                select e.* from order_events e
                  join orders o on o.id = e.order_id
                 where e.sequence > :sequence
                """);
        if (customerId != null) {
            sql.append(" and o.customer_id = :customerId");
        }
        sql.append(" order by e.sequence asc limit :limit");

        var spec = jdbc.sql(sql.toString())
                .param("sequence", sequence)
                .param("limit", limit);
        if (customerId != null) {
            spec = spec.param("customerId", customerId);
        }
        return spec.query((rs, rowNum) -> new OrderEvent(
                rs.getLong("sequence"),
                rs.getObject("order_id", UUID.class),
                OrderStatus.valueOf(rs.getString("old_status")),
                OrderStatus.valueOf(rs.getString("new_status")),
                rs.getTimestamp("occurred_at").toInstant())).list();
    }

    private Order mapOrder(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new Order(
                rs.getObject("id", UUID.class),
                rs.getString("customer_id"),
                List.of(),
                rs.getLong("total_minor"),
                rs.getString("currency"),
                OrderStatus.valueOf(rs.getString("status")),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private Order withLines(Order order) {
        List<OrderLine> lines = jdbc.sql("""
                select sku, quantity, unit_price_minor from order_lines
                 where order_id = :orderId order by id
                """)
                .param("orderId", order.id())
                .query((rs, rowNum) -> new OrderLine(
                        rs.getString("sku"),
                        rs.getInt("quantity"),
                        rs.getLong("unit_price_minor")))
                .list();

        return new Order(order.id(), order.customerId(), lines, order.totalMinor(),
                order.currency(), order.status(), order.version(),
                order.createdAt(), order.updatedAt());
    }
}
