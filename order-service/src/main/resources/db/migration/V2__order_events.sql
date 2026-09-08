create table order_events (
    sequence    bigserial primary key,
    order_id    uuid        not null references orders (id) on delete cascade,
    old_status  text        not null,
    new_status  text        not null,
    occurred_at timestamptz not null
);

create index order_events_order_idx on order_events (order_id);
create index order_events_sequence_idx on order_events (sequence);
