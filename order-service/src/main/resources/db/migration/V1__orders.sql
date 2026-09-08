create table orders (
    id              uuid primary key,
    customer_id     text        not null,
    total_minor     bigint      not null,
    currency        char(3)     not null,
    status          text        not null,
    version         bigint      not null default 1,
    idempotency_key text,
    created_at      timestamptz not null,
    updated_at      timestamptz not null
);

-- Makes CreateOrder retry-safe: a replayed insert collides instead of duplicating.
create unique index orders_customer_idempotency_uk
    on orders (customer_id, idempotency_key)
    where idempotency_key is not null;

-- Serves the (created_at, id) keyset cursor used by ListOrders.
create index orders_created_at_id_idx on orders (created_at desc, id desc);
create index orders_customer_idx on orders (customer_id);

create table order_lines (
    id               bigserial primary key,
    order_id         uuid   not null references orders (id) on delete cascade,
    sku              text   not null,
    quantity         int    not null check (quantity > 0),
    unit_price_minor bigint not null check (unit_price_minor >= 0)
);

create index order_lines_order_idx on order_lines (order_id);
