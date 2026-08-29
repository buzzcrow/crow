// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Status: coarse result code + optional message. Public API returns Status
// instead of throwing, so the future C ABI can map it to ct_status directly.
#pragma once

#include <string>
#include <utility>

namespace crowdb::tree
{

enum class Code : int8_t {
    kOk              = 0,
    kNotFound        = -1,
    kInvalidArgument = -2,
    kCorruption      = -3,
    kIoError         = -4,
    kNotSupported    = -5,
    kInternal        = -6,
};

class Status
{
  public:
    Status() : code_(Code::kOk)
    {
    }

    static Status Ok() // NOLINT(readability-identifier-naming) conflicts with bool ok() const
    {
        return {};
    }

    static Status not_found(std::string m = {})
    {
        return {Code::kNotFound, std::move(m)};
    }

    static Status invalid_argument(std::string m = {})
    {
        return {Code::kInvalidArgument, std::move(m)};
    }

    static Status corruption(std::string m = {})
    {
        return {Code::kCorruption, std::move(m)};
    }

    static Status io_error(std::string m = {})
    {
        return {Code::kIoError, std::move(m)};
    }

    static Status not_supported(std::string m = {})
    {
        return {Code::kNotSupported, std::move(m)};
    }

    static Status internal_error(std::string m = {})
    {
        return {Code::kInternal, std::move(m)};
    }

    [[nodiscard]] bool ok() const
    {
        return code_ == Code::kOk;
    }

    [[nodiscard]] Code code() const
    {
        return code_;
    }

    [[nodiscard]] const std::string &message() const
    {
        return msg_;
    }

    [[nodiscard]] std::string to_string() const
    {
        if (ok()) {
            return "Ok";
        }
        return "Status(" + std::to_string(static_cast<int32_t>(code_)) + "): " + msg_;
    }

  private:
    Status(Code c, std::string m) : code_(c), msg_(std::move(m))
    {
    }

    Code        code_;
    std::string msg_;
};

} // namespace crowdb::tree
