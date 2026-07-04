# Roadmap

Post-M6 work, roughly in priority order.

## Protobuf remote-read

Replace the JSON `/api/v1/read` endpoint with Prometheus-compatible protobuf+snappy encoding so ingothttp works as a native remote-read target for Prometheus and Grafana. Adds protobuf and snappy as dependencies to the cmd binary; the library stays zero-dep.

## Grafana demo

Docker-compose setup with ingothttp and Grafana pre-configured. Include a data generator that writes a few series so the dashboard isn't empty on first load. `docker compose up`, open localhost:3000.

## Query path benchmarks

`Benchmark*` tests for `Querier`, `Select`, and iteration across varying series/chunk/block counts. Track ns/query and allocations. The write path has benchmarks already; the read path has none.

## CI

At minimum:
- `go test -race -short ./...` on every push
- `go test -race -run TestSoak ./...` with a 10-minute timeout on main/nightly
- `go vet ./...`
- A linter (staticcheck or golangci-lint)
