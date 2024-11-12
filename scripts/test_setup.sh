set -e
docker compose -f tests/docker-compose.postgres.yaml up -d

export MVR_SOURCE='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
export MVR_DEST='stdout://'

until docker exec postgres_test pg_isready -U postgres -d postgres; do
  echo "Waiting for postgres..."
  sleep 2
done

docker exec postgres_test psql -U postgres -d postgres -c "DROP TABLE IF EXISTS users; CREATE EXTENSION IF NOT EXISTS "pgcrypto";"

docker exec postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  big_id BIGSERIAL,
  small_id SMALLSERIAL,
  name VARCHAR(100) NOT NULL,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  createdz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  unique_id uuid DEFAULT gen_random_uuid(),
  nullable_id uuid NULL,
  decimal_value NUMERIC(38, 15) DEFAULT (RANDOM() * 1000000)::NUMERIC(38, 15),
  double_value DOUBLE PRECISION DEFAULT RANDOM(),
  float_value REAL DEFAULT RANDOM()
);"
docker exec postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE users;"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO users (name) VALUES ('John Doe'), ('Jane Smith'), ('Alice Johnson'), ('Bob Brown'), ('Jim Smith');"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO users (name, created) VALUES ('Time Test', '2024-10-08 17:22:00'), ('Time Test 2', '2024-10-08 17:22:00');"

docker exec postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS numbers (
  smallint_value SMALLINT DEFAULT RANDOM(),
  integer_value INTEGER DEFAULT RANDOM(),
  bigint_value BIGINT DEFAULT RANDOM(),
  decimal_value NUMERIC(38, 15) DEFAULT (RANDOM() * 1000000)::NUMERIC(38, 15)
);"

docker exec postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE numbers;"
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO numbers (smallint_value, integer_value, bigint_value, decimal_value) VALUES (1, 1, 1, 468797.177024568000000), (2, 2, 2, 191886.800531254000000), (3, 3, 3, 723041.165430700000000);"
# now insert numbers that push the limits of the types
docker exec postgres_test psql -U postgres -d postgres -c "INSERT INTO numbers (smallint_value, integer_value, bigint_value, decimal_value) VALUES (32767, 2147483647, 9223372036854775807, 507531.111989867000000);"