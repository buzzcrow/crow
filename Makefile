.PHONY: default install coverage loc clean build doc e2e test test-web \
        ui-dev web reset \
        crowtree crowtree-test crowtree-asan crowtree-tsan crowtree-clean

# ── crowtree (C++ storage engine, libcrowtree) ───────────────────
CROWTREE_DIR := crowtree
CROWTREE_BUILD := $(CROWTREE_DIR)/build

# Configure + build the static library and the GoogleTest binary.
crowtree:
	cmake -S $(CROWTREE_DIR) -B $(CROWTREE_BUILD) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(CROWTREE_BUILD) -j

# Build + run the unit/integration test suite via ctest.
crowtree-test: crowtree
	ctest --test-dir $(CROWTREE_BUILD) --output-on-failure

# AddressSanitizer build + tests (separate build dir).
# `setarch -R` disables ASLR, which sanitizers require on some kernels.
crowtree-asan:
	cmake -S $(CROWTREE_DIR) -B $(CROWTREE_BUILD)-asan -DCMAKE_BUILD_TYPE=Debug -DCROWTREE_SANITIZER=address
	cmake --build $(CROWTREE_BUILD)-asan -j
	setarch -R ctest --test-dir $(CROWTREE_BUILD)-asan --output-on-failure

# ThreadSanitizer build + tests (separate build dir).
crowtree-tsan:
	cmake -S $(CROWTREE_DIR) -B $(CROWTREE_BUILD)-tsan -DCMAKE_BUILD_TYPE=Debug -DCROWTREE_SANITIZER=thread
	cmake --build $(CROWTREE_BUILD)-tsan -j
	setarch -R ctest --test-dir $(CROWTREE_BUILD)-tsan --output-on-failure

crowtree-clean:
	rm -rf $(CROWTREE_BUILD) $(CROWTREE_BUILD)-asan $(CROWTREE_BUILD)-tsan

# Frontend directory
UI_DIR := crowkv-console/web/ui
UI_NODE_MODULES := $(UI_DIR)/node_modules

# Detect OS
UNAME_S := $(shell uname -s)

# Default target: run pre-commit checks
default:
	@echo "Running pre-commit checks..."
	cargo fmt
	cargo clippy --all-targets -- -D warnings
	$(MAKE) test

# Run all tests including e2e
test:
	@echo "Cleaning test-logs ..."
	rm -rf test-logs
	@echo "Running tests ..."
	cargo test --workspace --all-targets

# Run web-related tests: crowkv-web Rust crate + UI vitest unit tests
test-web: $(UI_NODE_MODULES)
	@echo "Cleaning test-logs ..."
	rm -rf test-logs
	@echo "Running crowkv-web Rust tests..."
	cargo test -p crowkv-web --all-targets
	@echo "Running UI unit tests (vitest)..."
	cd $(UI_DIR) && npm test

# Run the real-backend Playwright E2E suite (workers=1). The Playwright
# config's webServer builds the SPA and boots `crowkv-web --test-mode`; we
# only need the debug `crowkv-server` binary the lifecycle layer deploys.
# Override the browser with PLAYWRIGHT_CHROMIUM_EXECUTABLE / PLAYWRIGHT_CHANNEL,
# or run `npx playwright install chromium` once to use the bundled browser.
e2e: $(UI_NODE_MODULES)
	@echo "Cleaning test-logs ..."
	rm -rf test-logs
	@echo "Building crowkv-server (debug) for E2E..."
	cargo build -p crowkv-server
	@echo "Running real-backend Playwright suite..."
	cd $(UI_DIR) && CROWKV_SERVER_BINARY=$(CURDIR)/target/debug/crowkv-server \
		npx playwright test --config=e2e/realBackend.config.ts

# Install dependencies and pre-commit hooks
install:
ifeq ($(UNAME_S),Darwin)
	@echo "Detected macOS — checking Homebrew dependencies..."
	@which brew > /dev/null || (echo "Homebrew not found. Please install it from https://brew.sh/" && exit 1)
	@brew list pkgconf >/dev/null 2>&1 || brew install pkgconf
	@brew list openssl >/dev/null 2>&1 || brew install openssl
	@brew list protobuf >/dev/null 2>&1 || brew install protobuf
	@brew list node >/dev/null 2>&1 || brew install node
endif
	@echo "Installing Rust tools..."
	cargo install cargo-tarpaulin
	@echo "Installing UI dependencies (npm ci)..."
	cd $(UI_DIR) && npm ci
	@echo "Installing pre-commit hooks..."
	cp .githooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Installation complete."

$(UI_NODE_MODULES): $(UI_DIR)/package.json $(UI_DIR)/package-lock.json
	@echo "Installing UI dependencies (npm ci)..."
	cd $(UI_DIR) && npm ci

# Build in release mode
build: $(UI_NODE_MODULES)
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
	@echo "Removing test-logs ..."
	rm -rf test-logs
	@echo "Removing log directories..."
	find . -type d -name "log" ! -path "*/src/*" -prune -exec rm -rf {} +
	@echo "Removing runtime-data directories..."
	find . -type d -name "runtime-data" ! -path "*/src/*" -prune -exec rm -rf {} +

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

ui-dev: $(UI_NODE_MODULES)
	@echo "Starting Vite dev server (proxies /api -> 127.0.0.1:9920)..."
	cd $(UI_DIR) && npm run dev
