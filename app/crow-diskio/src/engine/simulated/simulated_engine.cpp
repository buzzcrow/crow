// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/simulated/simulated_engine.h"

#include "disk/simulated_disk.h"

#include <chrono>
#include <memory>
#include <thread>

namespace crow::diskio
{

void SimulatedEngine::inject_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                                   std::function<void(int)> on_complete)
{
    auto *sim = dynamic_cast<SimulatedDisk *>(disk);
    if (sim == nullptr) {
        // Not a SimulatedDisk — delegate directly.
        inner_->submit_write(disk, phys_offset, data, size, std::move(on_complete));
        return;
    }
    uint32_t latency_ms = sim->draw_latency();
    bool     inject_err = sim->draw_error();
    if (inject_err) {
        // Schedule error after latency delay.
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    // Delegate to inner with a wrapped callback that delays by latency.
    // Pass the inner disk (unwrapped) so the inner engine sees the real disk.
    auto *data_copy  = new std::vector<uint8_t>(data, data + size);
    Disk *inner_disk = sim->inner().get();
    inner_->submit_write(inner_disk, phys_offset, data_copy->data(), data_copy->size(),
                         [latency_ms, data_copy, cb = std::move(on_complete)](int res) {
                             if (latency_ms > 0) {
                                 std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
                             }
                             delete data_copy;
                             if (cb) {
                                 cb(res);
                             }
                         });
}

void SimulatedEngine::submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                                   std::function<void(int)> on_complete)
{
    inject_write(disk, phys_offset, data, size, std::move(on_complete));
}

void SimulatedEngine::inject_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                                  std::function<void(int)> on_complete)
{
    auto *sim = dynamic_cast<SimulatedDisk *>(disk);
    if (sim == nullptr) {
        inner_->submit_read(disk, phys_offset, buf, size, std::move(on_complete));
        return;
    }
    uint32_t latency_ms = sim->draw_latency();
    bool     inject_err = sim->draw_error();
    if (inject_err) {
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    Disk *inner_disk = sim->inner().get();
    inner_->submit_read(inner_disk, phys_offset, buf, size, [latency_ms, cb = std::move(on_complete)](int res) {
        if (latency_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
        }
        if (cb) {
            cb(res);
        }
    });
}

void SimulatedEngine::submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                                  std::function<void(int)> on_complete)
{
    inject_read(disk, phys_offset, buf, size, std::move(on_complete));
}

void SimulatedEngine::inject_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    auto *sim = dynamic_cast<SimulatedDisk *>(disk);
    if (sim == nullptr) {
        inner_->submit_fsync(disk, std::move(on_complete));
        return;
    }
    uint32_t latency_ms = sim->draw_latency();
    bool     inject_err = sim->draw_error();
    if (inject_err) {
        std::thread([latency_ms, cb = std::move(on_complete)]() {
            if (latency_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
            }
            if (cb) {
                cb(-EIO);
            }
        }).detach();
        return;
    }
    Disk *inner_disk = sim->inner().get();
    inner_->submit_fsync(inner_disk, [latency_ms, cb = std::move(on_complete)](int res) {
        if (latency_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
        }
        if (cb) {
            cb(res);
        }
    });
}

void SimulatedEngine::submit_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    inject_fsync(disk, std::move(on_complete));
}

} // namespace crow::diskio
