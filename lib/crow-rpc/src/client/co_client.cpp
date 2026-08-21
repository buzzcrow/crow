// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/co_client.h"

#include "crow-rpc/buffer.h"
#include "crow-rpc/c_api.h"
#include "crow-rpc/c_api_internal.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/connection.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace crow::rpc
{

// Coroutine state machine: synchronizes co_on_complete (I/O worker
// thread) with await_suspend (coroutine thread). The race is:
//   1. Coroutine submits request, then tries to suspend.
//   2. I/O worker receives response, calls co_on_complete.
//   3. If co_on_complete runs before await_suspend sets the handle,
//      resuming a null handle crashes.
// The state machine ensures co_on_complete only resumes when the
// coroutine is actually suspended (handle is valid). If the response
// arrives while RUNNING, it sets RESPONSE_READY — the coroutine sees
// this in await_suspend and doesn't suspend (returns false → resumes).
enum CoStateFlag : int {
    CO_IDLE           = 0, // coroutine not started or already exited
    CO_RUNNING        = 1, // coroutine is running (handle may be stale)
    CO_SUSPENDED      = 2, // coroutine is suspended, handle is valid
    CO_RESPONSE_READY = 3, // response arrived while RUNNING — don't suspend
};

// Per-coroutine state. Allocated on the heap (one per coroutine),
// lives until the coroutine exits + is joined.
struct CoState
{
    // Rust callbacks.
    crow_rpc_co_build_request build_fn;
    crow_rpc_co_on_response   on_response_fn;
    void                     *rust_ctx;

    // C++ handles.
    RpcClient  *client;
    Transport  *transport;
    Connection *conn;
    uint16_t    msg_type;

    // Per-coroutine stats.
    uint64_t total_ops        = 0;
    uint64_t total_errors     = 0;
    uint64_t total_latency_ns = 0;
    uint64_t min_latency_ns   = UINT64_MAX;
    uint64_t max_latency_ns   = 0;

    // The awaitable for the current in-flight request. Filled by
    // co_on_complete; read by the coroutine after resume.
    crow_rpc_buffer_t       resp_control = nullptr;
    crow_rpc_buffer_t       resp_data    = nullptr;
    crow_rpc_status         resp_status  = CROW_RPC_OK;
    std::coroutine_handle<> handle; // set by await_suspend

    std::atomic<bool> *running;

    // State machine: IDLE → RUNNING → SUSPENDED ↔ RESPONSE_READY → IDLE.
    // Replaces the old `active` boolean — see CoStateFlag above.
    std::atomic<int> co_state{CO_IDLE};

    // Slot index for this coroutine (fixed, no collision with others).
    // request_id = slot_index + iteration * (pool_mask + 1).
    // This guarantees each coroutine always uses the same slab slot,
    // so no two coroutines ever collide on the same slot.
    size_t   slot_index  = 0;
    size_t   pool_mask   = 0;
    uint64_t next_req_id = 0; // per-coroutine request_id counter
};

// The C++ → Rust callback that resumes the coroutine. Set as the slab
// slot's callback. Called inline on the I/O worker thread when the
// response arrives.
[[maybe_unused]] static void co_on_complete(uint64_t /*request_id*/, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                            crow_rpc_status status, void *user_data)
{
    auto *s = static_cast<CoState *>(user_data);
    // Store the response — the coroutine reads it after resume (or
    // immediately if it hasn't suspended yet).
    s->resp_control = control;
    s->resp_data    = data;
    s->resp_status  = status;
    // Try to transition SUSPENDED → RESPONSE_READY. If successful,
    // the coroutine was suspended (handle is valid) — resume it.
    int expected = CO_SUSPENDED;
    if (s->co_state.compare_exchange_strong(expected, CO_RESPONSE_READY, std::memory_order_acq_rel)) {
        s->handle.resume();
        return;
    }
    // If RUNNING, the coroutine hasn't suspended yet (handle may be
    // null). Set RESPONSE_READY so await_suspend sees it and doesn't
    // suspend — the coroutine picks up the response immediately.
    if (expected == CO_RUNNING) {
        s->co_state.store(CO_RESPONSE_READY, std::memory_order_release);
        return;
    }
    // IDLE or RESPONSE_READY — late/duplicate response, drop it.
}

// Awaitable: suspends the coroutine, resumed by co_on_complete.
// The CoState pointer is passed to the awaitable so co_on_complete
// can store the response + resume.
struct CoAwait
{
    CoState *state;

    constexpr bool await_ready() const noexcept
    {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept
    {
        state->handle = h;
        // Try to transition RUNNING → SUSPENDED. If successful, the
        // response hasn't arrived yet — suspend and wait for
        // co_on_complete to resume us.
        int expected = CO_RUNNING;
        if (state->co_state.compare_exchange_strong(expected, CO_SUSPENDED, std::memory_order_acq_rel)) {
            return true; // suspend
        }
        // State is not RUNNING — co_on_complete already set
        // RESPONSE_READY (response arrived before we suspended).
        // Don't suspend — resume immediately. The response is already
        // stored in state->resp_*.
        return false; // don't suspend — resume inline
    }

    void await_resume() noexcept
    {
        // Coroutine is about to continue — mark it as running.
        state->co_state.store(CO_RUNNING, std::memory_order_release);
    }
};

// ── Coroutine task type ───────────────────────────────────────────
//
// We use a minimal coroutine type. The promise stores nothing — all
// state is in the CoState struct, accessed via the captured pointer
// in the coroutine body. The coroutine body is a lambda that captures
// CoState* by value.

struct CoTask
{
    struct promise_type
    {
        CoTask get_return_object()
        {
            return CoTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept
        {
            return {};
        }

        std::suspend_always final_suspend() noexcept
        {
            return {};
        }

        void return_void() noexcept
        {
        }

        void unhandled_exception() noexcept
        {
            std::abort();
        }
    };

    std::coroutine_handle<promise_type> handle;

    explicit CoTask(std::coroutine_handle<promise_type> h) : handle(h)
    {
    }
};

// The coroutine body. Captures CoState* by value (it's a pointer,
// stable on the heap). Loops: build → submit → co_await → on_response.
//
// This is a coroutine function (contains co_await). The compiler
// generates the coroutine frame + state machine. The frame is
// heap-allocated on first suspend (one alloc per coroutine, not per-op).
static CoTask co_run(CoState *s)
{
    s->co_state.store(CO_RUNNING, std::memory_order_release);
    while (s->running->load(std::memory_order_relaxed)) {
        // 1. Build request via Rust callback.
        crow_rpc_buffer_t control = nullptr;
        crow_rpc_buffer_t data    = nullptr;
        // Per-coroutine request_id: slot_index + N * pool_size.
        // Guarantees no slab slot collision between coroutines.
        uint64_t req_id = s->slot_index + s->next_req_id * (s->pool_mask + 1);
        s->next_req_id++;
        if (!s->build_fn(s->rust_ctx, req_id, &control, &data)) {
            break;
        }

        // 2. Record start time.
        auto start = std::chrono::steady_clock::now();

        // 3. Submit via send(). user_data = s (CoState*).
        //    co_on_complete will fill s->resp_* and resume.
        bool ok = s->client->send(s->transport, s->conn, req_id, (control != nullptr) ? control->buf : nullptr,
                                  (data != nullptr) ? data->buf : nullptr, s->msg_type, co_on_complete, s);

        if (!ok) {
            // Submit failed (send queue full) — release buffers, yield
            // to let I/O workers drain, then retry.
            if (control != nullptr)
                crow_rpc_buffer_release(control);
            if (data != nullptr)
                crow_rpc_buffer_release(data);
            s->total_errors++;
            // Yield this thread — suspend until resumed. We use a
            // self-resume: set handle, then suspend. A helper thread
            // (or the spin-wait loop) will resume us shortly.
            // For now, just yield the OS thread.
            std::this_thread::yield();
            continue;
        }

        // 4. Suspend. co_on_complete resumes us when the response
        //    arrives. After resume, s->resp_* has the response.
        co_await CoAwait{s};

        // 5. Process response via Rust callback.
        auto elapsed    = std::chrono::steady_clock::now() - start;
        auto elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());

        bool keep_going =
            s->on_response_fn(s->rust_ctx, req_id, s->resp_control, s->resp_data, s->resp_status, elapsed_ns);

        // Release the response buffers.
        if (s->resp_control != nullptr)
            crow_rpc_buffer_release(s->resp_control);
        if (s->resp_data != nullptr)
            crow_rpc_buffer_release(s->resp_data);

        // Update stats.
        s->total_ops++;
        if (s->resp_status != CROW_RPC_OK) {
            s->total_errors++;
        }
        s->total_latency_ns += elapsed_ns;
        if (elapsed_ns < s->min_latency_ns)
            s->min_latency_ns = elapsed_ns;
        if (elapsed_ns > s->max_latency_ns)
            s->max_latency_ns = elapsed_ns;

        if (!keep_going) {
            break;
        }
    }
    // Mark idle before final_suspend — late responses will see
    // CO_IDLE and drop (avoiding UB on a done coroutine).
    s->co_state.store(CO_IDLE, std::memory_order_release);
}

} // namespace crow::rpc

// ── C API ─────────────────────────────────────────────────────────

extern "C" void crow_rpc_co_spawn(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t *conns,
                                  size_t num_conns, uint32_t num_coroutines, uint16_t msg_type,
                                  crow_rpc_co_build_request build_request, crow_rpc_co_on_response on_response,
                                  void *ctx)
{
    if (client == nullptr || server == nullptr || conns == nullptr || num_conns == 0) {
        return;
    }

    auto *client_ptr = client->client;
    auto *transport  = server->server->transport();

    std::atomic<bool> running{true};

    // Create + start N coroutines.
    std::vector<crow::rpc::CoTask> tasks;
    tasks.reserve(num_coroutines);

    std::vector<std::unique_ptr<crow::rpc::CoState>> states;
    states.reserve(num_coroutines);

    // Compute pool size (next power of two >= num_coroutines).
    size_t pool_size = 1;
    while (pool_size < num_coroutines) {
        pool_size *= 2;
    }
    size_t pool_mask = pool_size - 1;

    for (uint32_t i = 0; i < num_coroutines; i++) {
        auto state            = std::make_unique<crow::rpc::CoState>();
        state->build_fn       = build_request;
        state->on_response_fn = on_response;
        state->rust_ctx       = ctx;
        state->client         = client_ptr;
        state->transport      = transport;
        state->conn           = conns[i % num_conns]->conn.get();
        state->msg_type       = msg_type;
        state->running        = &running;
        state->slot_index     = i;
        state->pool_mask      = pool_mask;
        state->next_req_id    = 0;

        auto *state_ptr = state.get();
        states.push_back(std::move(state));

        // Create the coroutine (starts suspended).
        auto task = crow::rpc::co_run(state_ptr);
        tasks.push_back(std::move(task));
    }

    // Start all coroutines. Each runs until its first co_await, then
    // suspends (yields the I/O worker thread back to epoll_wait).
    // Resume in small batches with a yield between batches to avoid
    // overwhelming the send queue during startup.
    constexpr size_t RESUME_BATCH = 16;
    constexpr auto   BATCH_DELAY  = std::chrono::microseconds(500);
    for (size_t i = 0; i < tasks.size(); i++) {
        tasks[i].handle.resume();
        if ((i + 1) % RESUME_BATCH == 0 && i + 1 < tasks.size()) {
            std::this_thread::sleep_for(BATCH_DELAY);
        }
    }

    // Wait for all coroutines to complete. The coroutines exit when
    // build_fn or on_response_fn returns false (Rust sets this when
    // the deadline is reached).
    auto wait_start = std::chrono::steady_clock::now();
    while (true) {
        uint32_t not_done = 0;
        for (auto &task : tasks) {
            if (!task.handle.done()) {
                not_done++;
            }
        }
        if (not_done == 0) {
            break;
        }
        // After 5s past the expected deadline, force-stop: set running=false
        // and resume any still-suspended coroutines.
        auto elapsed = std::chrono::steady_clock::now() - wait_start;
        if (elapsed > std::chrono::seconds(30)) {
            std::fprintf(stderr, "co_spawn: timeout, %u coroutines still running, forcing exit\n", not_done);
            running.store(false, std::memory_order_release);
            for (auto &task : tasks) {
                if (!task.handle.done()) {
                    task.handle.resume();
                }
            }
            break;
        }
        std::this_thread::yield();
    }

    // Aggregate stats.
    uint64_t total_ops        = 0;
    uint64_t total_errors     = 0;
    uint64_t total_latency_ns = 0;
    uint64_t min_latency_ns   = UINT64_MAX;
    uint64_t max_latency_ns   = 0;
    for (auto &state : states) {
        total_ops += state->total_ops;
        total_errors += state->total_errors;
        total_latency_ns += state->total_latency_ns;
        if (state->min_latency_ns < min_latency_ns)
            min_latency_ns = state->min_latency_ns;
        if (state->max_latency_ns > max_latency_ns)
            max_latency_ns = state->max_latency_ns;
    }
    client->co_stats.total_ops        = total_ops;
    client->co_stats.total_errors     = total_errors;
    client->co_stats.total_latency_ns = total_latency_ns;
    client->co_stats.min_latency_ns   = (total_ops > 0) ? min_latency_ns : 0;
    client->co_stats.max_latency_ns   = max_latency_ns;

    // Destroy the coroutines (frees the coroutine frames).
    for (auto &task : tasks) {
        task.handle.destroy();
    }
}

extern "C" void crow_rpc_co_get_stats(crow_rpc_client_t client, crow_rpc_co_stats_t *out)
{
    if (out == nullptr || client == nullptr) {
        return;
    }
    out->total_ops        = client->co_stats.total_ops;
    out->total_errors     = client->co_stats.total_errors;
    out->total_latency_ns = client->co_stats.total_latency_ns;
    out->min_latency_ns   = client->co_stats.min_latency_ns;
    out->max_latency_ns   = client->co_stats.max_latency_ns;
}
