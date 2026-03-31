-- Fixtures for integration tests
-- Creates schemas and tables
-- Clean schemas
DROP SCHEMA IF EXISTS test_alpha CASCADE;
DROP SCHEMA IF EXISTS test_beta CASCADE;

CREATE SCHEMA test_alpha;
CREATE SCHEMA test_beta;

-- test_alpha: 2 tables (clients, orders)
CREATE TABLE test_alpha.clients (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO test_alpha.clients (username) VALUES
('alice'), ('bob'), ('charlie');

CREATE TABLE test_alpha.orders (
    id SERIAL PRIMARY KEY,
    client_id INT REFERENCES test_alpha.clients(id),
    amount DECIMAL(10,2) NOT NULL
);

INSERT INTO test_alpha.orders (client_id, amount) VALUES
(1, 100.50), (1, 25.00), (2, 50.00);

-- test_beta: 1 table (products)
CREATE TABLE test_beta.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

INSERT INTO test_beta.products (name, price) VALUES
('widget', 9.99), ('gadget', 19.99);
