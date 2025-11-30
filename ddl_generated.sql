-- Data Vault 2.0 DDL (generated from ddl_config.yaml)
CREATE TABLE IF NOT EXISTS dwh_detailed.user_service_public_USERS (
  hk_user BYTEA PRIMARY KEY,
  user_external_id VARCHAR NOT NULL,
  load_dts TIMESTAMP DEFAULT NOW(),
  rec_src VARCHAR DEFAULT 'source'
);

CREATE INDEX idx_user_service_public_USERS_bk ON dwh_detailed.user_service_public_USERS(user_external_id);

CREATE TABLE IF NOT EXISTS dwh_detailed.user_service_public_USERS_sat (
  hk_ BYTEA NOT NULL REFERENCES dwh_detailed.user_service_public_USERS(hk_),
  load_dts TIMESTAMP NOT NULL,
  rec_src VARCHAR DEFAULT 'source',
  hashdiff BYTEA NOT NULL,
  email VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  phone VARCHAR,
  date_of_birth VARCHAR,
  PRIMARY KEY ({hash_key[:-4]}, load_dts)
);

CREATE INDEX idx_user_service_public_USERS_sat_hashdiff ON dwh_detailed.user_service_public_USERS_sat(hashdiff);

CREATE TABLE IF NOT EXISTS dwh_detailed.order_service_public_ORDERS (
  hk_order BYTEA PRIMARY KEY,
  order_external_id VARCHAR NOT NULL,
  load_dts TIMESTAMP DEFAULT NOW(),
  rec_src VARCHAR DEFAULT 'source'
);

CREATE INDEX idx_order_service_public_ORDERS_bk ON dwh_detailed.order_service_public_ORDERS(order_external_id);

CREATE TABLE IF NOT EXISTS dwh_detailed.order_service_public_ORDERS_sat (
  hk_o BYTEA NOT NULL REFERENCES dwh_detailed.order_service_public_ORDERS(hk_o),
  load_dts TIMESTAMP NOT NULL,
  rec_src VARCHAR DEFAULT 'source',
  hashdiff BYTEA NOT NULL,
  user_external_id VARCHAR,
  order_date VARCHAR,
  subtotal VARCHAR,
  tax_amount VARCHAR,
  shipping_cost VARCHAR,
  PRIMARY KEY ({hash_key[:-4]}, load_dts)
);

CREATE INDEX idx_order_service_public_ORDERS_sat_hashdiff ON dwh_detailed.order_service_public_ORDERS_sat(hashdiff);

CREATE TABLE IF NOT EXISTS dwh_detailed.logistics_service_public_SHIPMENTS (
  hk_shipment BYTEA PRIMARY KEY,
  shipment_external_id VARCHAR NOT NULL,
  load_dts TIMESTAMP DEFAULT NOW(),
  rec_src VARCHAR DEFAULT 'source'
);

CREATE INDEX idx_logistics_service_public_SHIPMENTS_bk ON dwh_detailed.logistics_service_public_SHIPMENTS(shipment_external_id);

CREATE TABLE IF NOT EXISTS dwh_detailed.link_user_order (
  hk_link_user_order BYTEA PRIMARY KEY,
  hk_user BYTEA NOT NULL REFERENCES dwh_detailed.hub_user(hk_user),
  hk_order BYTEA NOT NULL REFERENCES dwh_detailed.hub_order(hk_order),
  load_dts TIMESTAMP DEFAULT NOW(),
  rec_src VARCHAR DEFAULT 'source'
);

CREATE INDEX idx_link_user_order_hk1 ON dwh_detailed.link_user_order(hk_user);

CREATE INDEX idx_link_user_order_hk2 ON dwh_detailed.link_user_order(hk_order);

CREATE TABLE IF NOT EXISTS dwh_detailed.link_order_shipment (
  hk_link_order_shipment BYTEA PRIMARY KEY,
  hk_order BYTEA NOT NULL REFERENCES dwh_detailed.hub_order(hk_order),
  hk_shipment BYTEA NOT NULL REFERENCES dwh_detailed.hub_shipment(hk_shipment),
  load_dts TIMESTAMP DEFAULT NOW(),
  rec_src VARCHAR DEFAULT 'source'
);

CREATE INDEX idx_link_order_shipment_hk1 ON dwh_detailed.link_order_shipment(hk_order);

CREATE INDEX idx_link_order_shipment_hk2 ON dwh_detailed.link_order_shipment(hk_shipment);