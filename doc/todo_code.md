# CrowKV - Code-Level TODO

Open implementation items in code. Add new entries when you encounter a TODO/FIXME/unimplemented marker. Delete when resolved.

---

## Open Items

| Location | Description | Priority |
| --- | --- | --- |
| `crowkv/src/cluster/px_kv_store.rs:38` | Track revision for reads (currently hardcoded to 0) | Medium |

---

## Conventions

- Add an entry here when you encounter a `TODO`/`FIXME`/`unimplemented!` marker in code
- Remove the entry when the TODO is resolved and the marker is deleted
- Keep this file under ~50 lines; split to `todo_code-<topic>.md` if it grows

---

* KV pannel along side the hierchy view.
* add metrics mod, and emtrics logs by time. 
* we persistent config, find a better way for the cluster config, like config file in each node, I may cause some UT bugs.