#!/bin/bash

export ENV=dev
export DATABASE_URL=postgres://root:benchmarkPassword@localhost:5432/test
export MAX_POOL_SIZE=25
export LOG_LEVEL=warn
export MQS_SERVER=localhost

docker run --rm \
  --detach \
  --name postgres-for-benchmark \
  --env POSTGRES_USER=root \
  --env POSTGRES_PASSWORD=benchmarkPassword \
  --env POSTGRES_DB=test \
  --publish 5432:5432 \
  postgres:11.2

cargo build --release

cargo run --release --bin wait-db
cd mqs-server && diesel migration run && cd ..
cargo run --release --bin mqs &
MQS_PID=$!
cargo run --release --bin wait-http
cargo run --release --bin bench

kill -TERM "$MQS_PID"
wait

docker stop postgres-for-benchmark
