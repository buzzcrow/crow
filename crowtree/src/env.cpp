#include "crowtree/env.h"

namespace crowtree {

CrowtreeEnv& CrowtreeEnv::default_env() {
  static CrowtreeEnv env;
  return env;
}

}  // namespace crowtree
