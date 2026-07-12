// CrowtreeEnv: process-wide shared services for all Crowtree instances on a
// node (epoch manager; later: GC / consolidation worker pools). One Env is
// shared by many lightweight Crowtree instances (one per consensus group).
#pragma once

#include "crowtree/epoch.h"

#include <memory>

namespace crowtree {

class CrowtreeEnv {
 public:
  CrowtreeEnv() = default;
  ~CrowtreeEnv() = default;

  CrowtreeEnv(const CrowtreeEnv&) = delete;
  CrowtreeEnv& operator=(const CrowtreeEnv&) = delete;

  EpochManager& epoch() { return epoch_; }

  // Process-wide default Env (used by tests and simple single-process setups).
  static CrowtreeEnv& default_env();

 private:
  EpochManager epoch_;
};

}  // namespace crowtree
