-- Regular table with integer PK
CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
INSERT INTO public.users (name, email)
SELECT 'user_' || i, 'user_' || i || '@example.com'
FROM generate_series(1, 10000) AS i;

-- Table without PK
CREATE TABLE public.logs (
    ts TIMESTAMPTZ DEFAULT now(),
    level TEXT,
    message TEXT
);
INSERT INTO public.logs (level, message)
SELECT CASE WHEN i % 3 = 0 THEN 'ERROR' WHEN i % 2 = 0 THEN 'WARN' ELSE 'INFO' END,
    'Log message number ' || i
FROM generate_series(1, 5000) AS i;

-- Small config table
CREATE TABLE public.config (
    key TEXT PRIMARY KEY,
    value TEXT
);
INSERT INTO public.config (key, value) VALUES ('version', '1.0'), ('feature_flag', 'true'), ('max_retries', '5');

-- Table with generated column
CREATE TABLE public.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    tax NUMERIC(10, 2) GENERATED ALWAYS AS (price * 0.1) STORED
);
INSERT INTO public.products (name, price)
SELECT 'product_' || i, (random() * 100)::numeric(10, 2)
FROM generate_series(1, 1000) AS i;

-- Separate schema
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
INSERT INTO analytics.events (event_type, payload)
SELECT CASE WHEN i % 2 = 0 THEN 'click' ELSE 'view' END,
    jsonb_build_object('item_id', i, 'source', 'test')
FROM generate_series(1, 3000) AS i;

-- Sequence
CREATE SEQUENCE public.custom_seq START 42;
SELECT nextval('public.custom_seq');
SELECT nextval('public.custom_seq');
