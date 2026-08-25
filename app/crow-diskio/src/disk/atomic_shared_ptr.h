// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Portable atomic shared_ptr: std::atomic<std::shared_ptr<T>> is C++20
// but not implemented in Apple's libc++ (as of macOS 15). When the
// __cpp_lib_atomic_shared_ptr feature macro is absent, fall back to the
// C++11 free-function std::atomic_load/store, which is deprecated in
// C++20 but functional on all platforms.
#pragma once

#include <atomic>
#include <memory>

#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
#define CROW_USE_ATOMIC_SHARED_PTR 1
#endif

namespace crow::diskio
{

template <typename T>
class AtomicSharedPtr
{
  public:
    explicit AtomicSharedPtr(std::shared_ptr<T> init = nullptr)
#ifdef CROW_USE_ATOMIC_SHARED_PTR
        : ptr_(std::move(init))
#else
        : ptr_(std::move(init))
#endif
    {
    }

    std::shared_ptr<T> load(std::memory_order order = std::memory_order_acquire) const
    {
#ifdef CROW_USE_ATOMIC_SHARED_PTR
        return ptr_.load(order);
#else
#  if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdeprecated-declarations"
#  endif
        return std::atomic_load_explicit(&ptr_, order);
#  if defined(__clang__)
#    pragma clang diagnostic pop
#  endif
#endif
    }

    void store(std::shared_ptr<T> value, std::memory_order order = std::memory_order_release)
    {
#ifdef CROW_USE_ATOMIC_SHARED_PTR
        ptr_.store(std::move(value), order);
#else
#  if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wdeprecated-declarations"
#  endif
        std::atomic_store_explicit(&ptr_, std::move(value), order);
#  if defined(__clang__)
#    pragma clang diagnostic pop
#  endif
#endif
    }

  private:
#ifdef CROW_USE_ATOMIC_SHARED_PTR
    std::atomic<std::shared_ptr<T>> ptr_;
#else
    std::shared_ptr<T> ptr_;
#endif
};

} // namespace crow::diskio
