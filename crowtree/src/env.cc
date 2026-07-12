#include "crowtree/env.h"

namespace crowtree {

CrowtreeEnv& CrowtreeEnv::Default() {
  static CrowtreeEnv env;
  return env;
}

}  // namespace crowtree
