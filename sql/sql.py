CREATE TABLE IF NOT EXISTS test_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);



TRUNCATE TABLE test_users RESTART IDENTITY;



INSERT INTO test_users (name) VALUES
('Alice'), ('Bob'), ('Charlie'), ('Diana');




UPDATE test_users
SET is_active = FALSE
WHERE name = 'Charlie';