-- 01-dwh.sql → Data Vault 2.0 (БЕЗ REFERENCES до INSERT!)
CREATE SCHEMA IF NOT EXISTS dwh_detailed;

-- ========================================
-- 1. HUB_USER 
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.hub_user (
    hk_user BYTEA PRIMARY KEY,
    user_external_id VARCHAR NOT NULL,
    load_dts TIMESTAMP DEFAULT NOW(),
    rec_src VARCHAR DEFAULT 'user_service'
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hub_user_bk ON dwh_detailed.hub_user(user_external_id);

-- ========================================
-- 2. SAT_USER (ТЕПЕРЬ REFERENCES ОК!)
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.sat_user (
    hk_user BYTEA NOT NULL REFERENCES dwh_detailed.hub_user(hk_user),
    load_dts TIMESTAMP NOT NULL,
    rec_src VARCHAR DEFAULT 'user_service',
    hashdiff BYTEA NOT NULL,
    email VARCHAR, first_name VARCHAR, last_name VARCHAR, phone VARCHAR,
    date_of_birth DATE, registration_date TIMESTAMP, status VARCHAR,
    effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP, created_by VARCHAR, updated_by VARCHAR,
    PRIMARY KEY (hk_user, load_dts)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sat_user_hashdiff ON dwh_detailed.sat_user(hashdiff);

-- ========================================
-- 3. HUB_ORDER
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.hub_order (
    hk_order BYTEA PRIMARY KEY,
    order_external_id VARCHAR NOT NULL,
    load_dts TIMESTAMP DEFAULT NOW(),
    rec_src VARCHAR DEFAULT 'order_service'
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hub_order_bk ON dwh_detailed.hub_order(order_external_id);

-- ========================================
-- 4. SAT_ORDER
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.sat_order (
    hk_order BYTEA NOT NULL REFERENCES dwh_detailed.hub_order(hk_order),
    load_dts TIMESTAMP NOT NULL,
    rec_src VARCHAR DEFAULT 'order_service',
    hashdiff BYTEA NOT NULL,
    user_external_id VARCHAR, order_date TIMESTAMP,
    subtotal DECIMAL(15,2), tax_amount DECIMAL(15,2), shipping_cost DECIMAL(15,2),
    discount_amount DECIMAL(15,2), total_amount DECIMAL(15,2), currency VARCHAR DEFAULT 'RUB',
    status VARCHAR, payment_method VARCHAR, payment_status VARCHAR,
    effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP, created_by VARCHAR, updated_by VARCHAR,
    PRIMARY KEY (hk_order, load_dts)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sat_order_hashdiff ON dwh_detailed.sat_order(hashdiff);

-- ========================================
-- 5. HUB_SHIPMENT
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.hub_shipment (
    hk_shipment BYTEA PRIMARY KEY,
    shipment_external_id UUID NOT NULL,
    load_dts TIMESTAMP DEFAULT NOW(),
    rec_src VARCHAR DEFAULT 'logistics_service'
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hub_shipment_bk ON dwh_detailed.hub_shipment(shipment_external_id);

-- ========================================
-- 6. LINK_USER_ORDER (ПОСЛЕДНИЙ!)
-- ========================================
CREATE TABLE IF NOT EXISTS dwh_detailed.link_user_order (
    hk_link_user_order BYTEA PRIMARY KEY,
    hk_user BYTEA NOT NULL REFERENCES dwh_detailed.hub_user(hk_user),
    hk_order BYTEA NOT NULL REFERENCES dwh_detailed.hub_order(hk_order),
    load_dts TIMESTAMP DEFAULT NOW(),
    rec_src VARCHAR DEFAULT 'order_service'
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_link_user_order_hk_user ON dwh_detailed.link_user_order(hk_user);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_link_user_order_hk_order ON dwh_detailed.link_user_order(hk_order);

-- ========================================
-- ТЕСТОВЫЕ ДАННЫЕ (DMP валидация!)
-- ========================================
INSERT INTO dwh_detailed.hub_user (hk_user, user_external_id) VALUES
(decode(md5('user_001'), 'hex'), 'user_001'),
(decode(md5('user_002'), 'hex'), 'user_002'),
(decode(md5('user_003'), 'hex'), 'user_003') ON CONFLICT DO NOTHING;

INSERT INTO dwh_detailed.sat_user (hk_user, load_dts, hashdiff, email, first_name, last_name, phone, date_of_birth, status, registration_date) VALUES
(decode(md5('user_001'), 'hex'), NOW(), decode(md5('ivan.petrov@test.com|Иван|Петров'), 'hex'), 'ivan.petrov@test.com', 'Иван', 'Петров', '+7-900-123-45-67', '1985-03-15', 'active', NOW()),
(decode(md5('user_002'), 'hex'), NOW(), decode(md5('maria.sidorova@test.com|Мария|Сидорова'), 'hex'), 'maria.sidorova@test.com', 'Мария', 'Сидорова', '+7-900-987-65-43', '1990-07-22', 'active', NOW())
ON CONFLICT DO NOTHING;

INSERT INTO dwh_detailed.hub_order (hk_order, order_external_id) VALUES
(decode(md5('order_001'), 'hex'), 'order_001'),
(decode(md5('order_002'), 'hex'), 'order_002'),
(decode(md5('order_003'), 'hex'), 'order_003') ON CONFLICT DO NOTHING;

INSERT INTO dwh_detailed.sat_order (hk_order, load_dts, hashdiff, user_external_id, total_amount, status, order_date) VALUES
(decode(md5('order_001'), 'hex'), NOW(), decode(md5('user_001|1430.00|completed'), 'hex'), 'user_001', 1430.00, 'completed', '2025-11-28 10:30:00'),
(decode(md5('order_002'), 'hex'), NOW(), decode(md5('user_002|1085.00|shipped'), 'hex'), 'user_002', 1085.00, 'shipped', '2025-11-29 14:15:00')
ON CONFLICT DO NOTHING;

INSERT INTO dwh_detailed.link_user_order (hk_link_user_order, hk_user, hk_order) VALUES
(decode(md5('user_001|order_001'), 'hex'), decode(md5('user_001'), 'hex'), decode(md5('order_001'), 'hex')),
(decode(md5('user_002|order_002'), 'hex'), decode(md5('user_002'), 'hex'), decode(md5('order_002'), 'hex'))
ON CONFLICT DO NOTHING;


