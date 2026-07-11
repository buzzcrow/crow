.PHONY: default install coverage loc clean build server doc e2e test

# Default target: run pre-commit checks
default:
	@echo "Running pre-commit checks..."
	cargo fmt
	cargo clippy --all-targets -- -D warnings
	$(MAKE) test

# Run all tests including e2e
test:
	@echo "Running tests (including e2e)..."
	cargo test --workspace --all-targets --exclude crowkv-server
	cargo test -p crowkv-server --test management_api
	cargo test -p crowkv-server --test cluster_e2e

# Install dependencies and pre-commit hooks
install:
	@echo "Installing dependencies..."
	cargo install cargo-tarpaulin
	@echo "Installing pre-commit hooks..."
	cp .githooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Installation complete."

# Build in release mode
build:
	@echo "Building in release mode..."
	cargo build --release

# Start crowkv-server in release mode with logging
run: build
	@echo "Starting crowkv-server in release mode with logging..."
	cargo run --release -p crowkv-server -- -l

# Run test coverage with tarpaulin and generate HTML report
coverage:
	@echo "Running test coverage..."
	cargo tarpaulin --workspace --out Html --output-dir target/coverage --exclude-files '*/tests/*'

doc:
	@echo "Exporting OpenAPI JSON..."
	cargo run -p crowkv-server --example openapi-export

# Count lines of code using tokei
loc:
	@echo "Counting lines of code..."
	tokei

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	@echo "Removing log directory..."
	rm -rf log
