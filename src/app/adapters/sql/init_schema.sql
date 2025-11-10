CREATE TABLE IF NOT EXISTS events (
    event_name VARCHAR,
    event_timestamp BIGINT,
    user_pseudo_id VARCHAR,
    hostname VARCHAR,
    event_params STRUCT(key VARCHAR, value STRUCT(string_value VARCHAR, int_value BIGINT, float_value DOUBLE, double_value DOUBLE))[],
    items STRUCT(item_id VARCHAR, item_name VARCHAR, item_brand VARCHAR, item_variant VARCHAR, item_category VARCHAR, item_category2 VARCHAR, item_category3 VARCHAR, item_category4 VARCHAR, item_category5 VARCHAR, price_in_usd DOUBLE, price DOUBLE, quantity BIGINT, item_revenue_in_usd DOUBLE, item_revenue DOUBLE, item_refund_in_usd DOUBLE, item_refund DOUBLE, coupon VARCHAR, affiliation VARCHAR, location_id VARCHAR, item_list_id VARCHAR, item_list_name VARCHAR, item_list_index VARCHAR, promotion_id VARCHAR, promotion_name VARCHAR, creative_name VARCHAR, creative_slot VARCHAR, item_params STRUCT("key" VARCHAR, "value" STRUCT(string_value VARCHAR, int_value BIGINT, float_value DOUBLE, double_value DOUBLE))[])[]
);

CREATE TABLE products (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    feed_id     VARCHAR NOT NULL,
    name        VARCHAR NOT NULL,
    UNIQUE (feed_id, name)
);

CREATE TABLE orders (
    id                UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    event_timestamp   TIMESTAMP NOT NULL,
    hostname          VARCHAR NOT NULL,
    user_pseudo_id    VARCHAR NOT NULL,
    currency          VARCHAR,
    value             DOUBLE,
    source            VARCHAR,
    medium            VARCHAR,
    campaign          VARCHAR
);

CREATE TABLE order_to_products (
    order_id    UUID NOT NULL REFERENCES orders(id),
    product_id  UUID NOT NULL REFERENCES products(id),
    price       DOUBLE,
    quantity    INTEGER,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS purchase_last_click_attributions (
    user_pseudo_id VARCHAR,
    event_timestamp BIGINT,
    hostname VARCHAR,
    source VARCHAR,
    medium VARCHAR,
    campaign VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_pseudo_id);
CREATE INDEX IF NOT EXISTS idx_events_hostname ON events(hostname);