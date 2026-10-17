.PHONY: build run test

build:
	go build -o bin/main cmd/main.go

run:
	-go run cmd/main.go

test:
	go test -race ./...