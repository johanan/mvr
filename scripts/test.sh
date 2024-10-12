set -e
docker compose -f tests/docker-compose.postgres.yaml up -d

export MVR_SOURCE='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
export MVR_DEST='stdout://'

until docker exec -it postgres_test pg_isready -U postgres -d postgres; do
  echo "Waiting for postgres..."
  sleep 2
done

docker exec -it postgres_test psql -U postgres -d postgres -c "DROP TABLE IF EXISTS users;"

docker exec -it postgres_test psql -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  createdz TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);"
docker exec -it postgres_test psql -U postgres -d postgres -c "TRUNCATE TABLE users;"
docker exec -it postgres_test psql -U postgres -d postgres -c "INSERT INTO users (name) VALUES ('John Doe'), ('Jane Smith'), ('Alice Johnson'), ('Bob Brown'), ('Jim Smith');"
docker exec -it postgres_test psql -U postgres -d postgres -c "INSERT INTO users (name, created) VALUES ('Time Test', '2024-10-08 17:22:00');"
