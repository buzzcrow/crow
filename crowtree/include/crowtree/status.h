// Status: coarse result code + optional message. Public API returns Status
// instead of throwing, so the future C ABI can map it to ct_status directly.
#pragma once

#include <string>
#include <utility>

namespace crowtree {

enum class Code : int32_t {
  kOk = 0,
  kNotFound = -1,
  kInvalidArgument = -2,
  kCorruption = -3,
  kIoError = -4,
  kNotSupported = -5,
  kInternal = -6,
};

class Status {
 public:
  Status() : code_(Code::kOk) {}

  static Status Ok() { return Status(); }
  static Status not_found(std::string m = {}) { return Status(Code::kNotFound, std::move(m)); }
  static Status invalid_argument(std::string m = {}) {
    return Status(Code::kInvalidArgument, std::move(m));
  }
  static Status corruption(std::string m = {}) { return Status(Code::kCorruption, std::move(m)); }
  static Status io_error(std::string m = {}) { return Status(Code::kIoError, std::move(m)); }
  static Status not_supported(std::string m = {}) {
    return Status(Code::kNotSupported, std::move(m));
  }
  static Status internal_error(std::string m = {}) { return Status(Code::kInternal, std::move(m)); }

  bool ok() const { return code_ == Code::kOk; }
  Code code() const { return code_; }
  const std::string& message() const { return msg_; }

  std::string to_string() const {
    if (ok())
    {
      return "Ok";
    }
    return "Status(" + std::to_string(static_cast<int32_t>(code_)) + "): " + msg_;
  }

 private:
  Status(Code c, std::string m) : code_(c), msg_(std::move(m)) {}
  Code code_;
  std::string msg_;
};

}  // namespace crowtree
