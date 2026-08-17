<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R93: chunkdb — Mirror-to-EC Conversion

**Problem**: Shared chunks initially use mirror strips for simplicity. As data ages and becomes colder, converting to EC strips can provide better space efficiency while maintaining durability.

**Solution**: Implement background conversion of mirror strips to EC strips in shared chunks with conversion triggers and strip replacement logic.

**Scope**: Placeholder - detailed design to be refined before implementation.