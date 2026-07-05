BINARY  := pcs
MODULE  := github.com/ChaosHour/Partition-Control-System
VERSION ?= $(shell git describe --tags --dirty 2>/dev/null || echo 4.0.0-dev)
LDFLAGS := -X $(MODULE)/internal/cli.version=$(VERSION)

.PHONY: build test vet fmt clean linux

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/pcs

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# Static binary for Linux deploy hosts (cron/systemd).
linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-amd64 ./cmd/pcs

clean:
	rm -rf bin
