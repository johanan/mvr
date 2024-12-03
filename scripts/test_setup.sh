set -e
docker compose -f tests/docker-compose.postgres.yaml up -d

export MVR_SOURCE='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
export MVR_DEST='stdout://'

until docker exec postgres_test pg_isready -U postgres -d postgres; do
  echo "Waiting for postgres..."
  sleep 2
done

# Set the server timezone to UTC
docker exec postgres_test psql -U postgres -c "SET TIME ZONE 'UTC';"

docker exec postgres_test psql -U postgres -d postgres -c "DROP TABLE IF EXISTS users; CREATE EXTENSION IF NOT EXISTS "pgcrypto";"

docker exec postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS users (
  name VARCHAR(100) NOT NULL,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  createdz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  unique_id uuid DEFAULT gen_random_uuid(),
  nullable_id uuid NULL,
  active BOOLEAN DEFAULT TRUE
);"
docker exec postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE users;"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO users (name, created, createdz, unique_id, active) VALUES ('John Doe', '2024-10-08 17:22:00', '2024-10-08 17:22:00', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', TRUE), ('Test Tester', '2024-10-08 17:22:00', '2024-10-08 17:22:00', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', FALSE);"

docker exec postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS numbers (
  smallint_value SMALLINT DEFAULT RANDOM(),
  integer_value INTEGER DEFAULT RANDOM(),
  bigint_value BIGINT DEFAULT RANDOM(),
  decimal_value NUMERIC(38, 15) DEFAULT (RANDOM() * 1000000)::NUMERIC(38, 15),
  double_value DOUBLE PRECISION DEFAULT RANDOM(),
  float_value REAL DEFAULT RANDOM()
);"

docker exec postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE numbers;"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO numbers (smallint_value, integer_value, bigint_value, decimal_value, double_value, float_value) VALUES (1, 1, 1, 1.0, 1.0, 1.0), (2, 2, 2, 2.0, 2.0, 2.0), (3, 3, 3, 3.0, 3.0, 3.0);"
# now insert numbers that push the limits of the types
# for double and float, we push just pass the limits of the mantissa
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO numbers (smallint_value, integer_value, bigint_value, decimal_value, double_value, float_value) VALUES (32767, 2147483647, 9223372036854775807, 507531.111989867000000, 1.2345678901234567890123456789e+20, 12345678.9), (0, 0, 0, 468797.177024568000000, 1234567890.12345, 12345.67);"

docker exec postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS strings (
  char_value CHAR(10) DEFAULT 'a',
  varchar_value VARCHAR(10) DEFAULT 'a',
  text_value TEXT DEFAULT 'a',
  json_value JSON DEFAULT '{}',
  jsonb_value JSONB DEFAULT '{}',
  array_value TEXT[] DEFAULT '{}'
);"

docker exec postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE strings;"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO strings (char_value, varchar_value, text_value, json_value, jsonb_value, array_value) VALUES ('a', 'a', 'a', '{}', '{}', '{}'), ('b', 'b', 'b', '{\"key\": \"value\"}', '{\"key\": \"value\"}', '{\"a\"}');"