.PHONY: build run test lint clean

build:
	go build -o bin/tdb ./cmd/tdb

run:
	go run ./cmd/tdb

test:
	go test ./...

lint:
	go vet ./...

clean:
	rm -rf bin/
