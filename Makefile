.PHONY: default install coverage loc clean build doc e2e test test-web \
        ui-dev web reset

# Frontend directory
UI_DIR := crowkv-console/web/ui

# Default target: run pre-commit checks
default:
	@echo "Running pre-commit checks..."
	cargo fmt
	cargo clippy --all-targets -- -D warnings
	$(MAKE) test

# Run all tests including e2e
test:
	@echo "Running tests ..."
	cargo test --workspace --all-targets

# Run web-related tests: crowkv-web Rust crate + UI vitest unit tests
test-web:
	@echo "Running crowkv-web Rust tests..."
	cargo test -p crowkv-web --all-targets
	@echo "Running UI unit tests (vitest)..."
	cd $(UI_DIR) && npm test

# Install dependencies and pre-commit hooks
install:
	@echo "Installing dependencies..."
	cargo install cargo-tarpaulin
	@echo "Installing UI dependencies (npm ci)..."
	cd $(UI_DIR) && npm ci
	@echo "Installing pre-commit hooks..."
	cp .githooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Installation complete."

# Build in release mode
build:
	@echo "Building in release mode..."
	cargo build --release
	@echo "Building React SPA (vite build)..."
	cd $(UI_DIR) && npm run build

# Start crowkv-server in release mode with logging
run: build
	@echo "Starting crowkv-server in release mode..."
	@echo "Access OpenAPI JSON at http://127.0.0.1:9910/openapi.json"
	@echo "Access topology at http://127.0.0.1:9910/top"
	@echo "Press Ctrl+C to stop"
	cargo run --release -p crowkv-server -- -l --stores 1..2 --groups 1..3 --replica 1

# Start crowkv-web in release mode
web: build
	@echo "Starting crowkv-web in release mode..."
	@echo "Access Swagger UI at http://127.0.0.1:9920/api"
	@echo "Access React SPA at http://127.0.0.1:9920/"
	@echo "Press Ctrl+C to stop"
	cargo run --release -p crowkv-web

# Run test coverage with tarpaulin and generate HTML report
coverage:
	@echo "Running test coverage..."
	cargo tarpaulin --workspace --out Html --output-dir target/coverage --exclude-files '*/tests/*'

# Count lines of code using tokei
loc:
	@echo "Counting lines of code..."
	tokei

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	@echo "Cleaning UI build output..."
	rm -rf $(UI_DIR)/dist $(UI_DIR)/node_modules
	@echo "Removing log directories..."
	find . -type d -name "log" ! -path "*/src/*" -prune -exec rm -rf {} +

# Reset: kill processes, remove logs and config, keep build artifacts
reset:
	@echo "Killing crowkv-* processes..."
	-pkill crowkv-server || true
	-pkill crowkv-web || true
	-pkill crowkv-cli || true
	@echo "Removing log directories (includes config)..."
	find . -type d -name "log" ! -path "*/src/*" -prune -exec rm -rf {} +
	@echo "Reset complete (build artifacts preserved)"

# ── React UI development ─────────────────────────────────────────
# For development with hot reload, use ui-dev. This runs the Vite
# dev server which proxies /api requests to crowkv-web at :9920.

ui-dev:
	@echo "Starting Vite dev server (proxies /api -> 127.0.0.1:9920)..."
	cd $(UI_DIR) && npm run dev
