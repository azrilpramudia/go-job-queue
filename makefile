.PHONY: build run test serve

build:
	go build -o bin/main cmd/main.go

run:
	-go run cmd/main.go

serve:
	-go run cmd/httpapi/main.go

test:
	go test -race ./...