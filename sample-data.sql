-- Create source database tables and data
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending'
);

CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_name VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10,2)
);

-- Insert sample data for users
INSERT INTO users (username, email, status) VALUES
    ('john_doe', 'john@example.com', 'active'),
    ('jane_smith', 'jane@example.com', 'active'),
    ('bob_wilson', 'bob@example.com', 'inactive'),
    ('alice_brown', 'alice@example.com', 'active'),
    ('charlie_davis', 'charlie@example.com', 'active');

-- Insert sample data for orders
INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 150.00, 'completed'),
    (1, 75.50, 'pending'),
    (2, 200.00, 'completed'),
    (3, 50.00, 'cancelled'),
    (4, 300.00, 'completed'),
    (5, 125.00, 'pending');

-- Insert sample data for order_items
INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES
    (1, 'Laptop', 1, 150.00),
    (2, 'Mouse', 1, 25.50),
    (2, 'Keyboard', 1, 50.00),
    (3, 'Monitor', 2, 100.00),
    (4, 'Headphones', 1, 50.00),
    (5, 'Printer', 1, 200.00),
    (5, 'Paper', 2, 50.00),
    (6, 'USB Drive', 5, 25.00);

-- Create target database tables (with some intentional differences)
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending'
);

CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_name VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(10,2)
);

-- Insert sample data for users (with one missing record)
INSERT INTO users (username, email, status) VALUES
    ('john_doe', 'john@example.com', 'active'),
    ('jane_smith', 'jane@example.com', 'active'),
    ('bob_wilson', 'bob@example.com', 'inactive'),
    ('alice_brown', 'alice@example.com', 'active');
    -- charlie_davis is missing

-- Insert sample data for orders (with one modified record)
INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 150.00, 'completed'),
    (1, 75.50, 'pending'),
    (2, 200.00, 'completed'),
    (3, 50.00, 'cancelled'),
    (4, 300.00, 'completed'),
    (5, 125.00, 'pending');

-- Insert sample data for order_items (with one modified record)
INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES
    (1, 'Laptop', 1, 150.00),
    (2, 'Mouse', 1, 25.50),
    (2, 'Keyboard', 1, 50.00),
    (3, 'Monitor', 2, 100.00),
    (4, 'Headphones', 1, 50.00),
    (5, 'Printer', 1, 200.00),
    (5, 'Paper', 2, 45.00),  -- Modified price
    (6, 'USB Drive', 5, 25.00);

-- Create a table with different column order to test schema comparison
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the same table in target with different column order
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    description TEXT,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    price DECIMAL(10,2)
);

-- Insert sample data for products
INSERT INTO products (name, price, description) VALUES
    ('Product A', 100.00, 'Description A'),
    ('Product B', 200.00, 'Description B'),
    ('Product C', 300.00, 'Description C');

-- Insert the same data in target
INSERT INTO products (name, price, description) VALUES
    ('Product A', 100.00, 'Description A'),
    ('Product B', 200.00, 'Description B'),
    ('Product C', 300.00, 'Description C');
