.PHONY: serve proto test bench bench-wal lint

serve:
	go run ./cmd/server

test:
	go test ./... -race

lint:
	go vet ./...
	gofmt -l . | tee /tmp/oms-fmt.out && test ! -s /tmp/oms-fmt.out

# Regenerate the gRPC stubs. The plugin versions come from the `tool`
# directives in go.mod rather than from whatever happens to be on PATH, so this
# produces the same output on any machine — and does not disturb a globally
# installed protoc-gen-go that other projects may depend on.
proto:
	@mkdir -p .tools internal/pb
	go build -o .tools/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
	go build -o .tools/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc
	PATH="$(CURDIR)/.tools:$$PATH" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=github.com/Poudel0/OMS \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Poudel0/OMS \
		proto/dhukuti/oms/v1/oms.proto

bench:
	go test -bench='BenchmarkSequencer_|BenchmarkMutex_' -benchtime=2s -benchmem -run='^$$' ./internal/oms/

# Durable throughput. OMS_BENCH_WAL_DIR must point at REAL storage: without it
# the WAL lands in $$TMPDIR, which is usually a tmpfs, where fsync is nearly
# free and the result measures encoding overhead rather than durability.
bench-wal:
	OMS_BENCH_WAL_DIR=$${OMS_BENCH_WAL_DIR:-$$HOME/.cache} \
		go test -bench=SequencerWAL -benchtime=2s -benchmem -run='^$$' ./internal/oms/
