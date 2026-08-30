.PHONY: all build test vet fmt clean

# Default target
all: vet test build

# Format the code
fmt:
	gofmt -w .

# Vet the code
vet:
	go vet ./...

# Run the tests
test:
	go test -v ./...
	go test -race ./...

# Build the binary
build:
	go build -o concurrent-counter .

# Clean build artifacts
clean:
	rm -f concurrent-counter
