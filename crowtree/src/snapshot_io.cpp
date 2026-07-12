#include "crowtree/snapshot_io.h"

#include "crowtree/cell.h"
#include "crowtree/crc32c.h"
#include "crowtree/crowtree.h"
#include "crowtree/page_types.h"

#include <cstring>
#include <fstream>
#include <vector>

namespace crowtree {

namespace {

constexpr uint32_t kSnapMagic = 0x4E535443;  // 'CTSN' little-endian
constexpr uint32_t kSnapVersion = 1;
// Header: magic + version + format + at_slot + entry_count.
constexpr size_t kSnapHeader = 4 + 4 + 1 + 8 + 8;
constexpr size_t kSnapTrailer = 4;  // whole-stream CRC32C

void put_u32(std::string* o, uint32_t v) {
  for (int i = 0; i < 4; ++i)
  {
    o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}
void put_u64(std::string* o, uint64_t v) {
  for (int i = 0; i < 8; ++i)
  {
    o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}
uint32_t get_u32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
  {
    v |= static_cast<uint32_t>(p[i]) << (8 * i);
  }
  return v;
}
uint64_t get_u64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i)
  {
    v |= static_cast<uint64_t>(p[i]) << (8 * i);
  }
  return v;
}

}  // namespace

Status snapshot_export_begin(Crowtree& tree, uint64_t at_slot, snapshot_format fmt,
                             size_t chunk_bytes, std::unique_ptr<SnapshotExport>* out) {
  if (fmt != snapshot_format::kPortable)
  {
    return Status::not_supported("snapshot export: only portable format in v1");
  }
  // v1 exports the current durable view; an arbitrary historical pin is deferred
  // until path-copy COW RootVersions exist. A request for a future slot the
  // engine has not applied is unsatisfiable.
  std::shared_ptr<Snapshot> snap = tree.snapshot_view();
  uint64_t slot = snap->at_slot();
  if (at_slot != 0 && at_slot > slot)
  {
    return Status::invalid_argument("snapshot export: at_slot beyond last_applied_slot");
  }

  std::string s;
  put_u32(&s, kSnapMagic);
  put_u32(&s, kSnapVersion);
  s.push_back(static_cast<char>(snapshot_format::kPortable));
  put_u64(&s, slot);
  put_u64(&s, static_cast<uint64_t>(snap->entries().size()));
  for (const leaf_entry& e : snap->entries())
  {
    CellView v{Slice(e.cell)};
    put_u32(&s, static_cast<uint32_t>(e.key.size()));
    s.append(e.key);
    put_u64(&s, v.slot());
    s.push_back(static_cast<char>(v.is_tombstone() ? 1 : 0));
    Slice val = v.value();
    put_u32(&s, static_cast<uint32_t>(val.size()));
    s.append(val.data(), val.size());
  }
  uint32_t crc = crc32c(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  put_u32(&s, crc);

  auto exp = std::make_unique<SnapshotExport>(std::move(s), chunk_bytes);
  exp->at_slot_ = slot;
  *out = std::move(exp);
  return Status::Ok();
}

Status SnapshotExport::next_chunk(std::string* out, bool* done) {
  out->clear();
  size_t remaining = stream_.size() - pos_;
  size_t n = remaining < chunk_bytes_ ? remaining : chunk_bytes_;
  out->assign(stream_, pos_, n);
  pos_ += n;
  if (done)
  {
    *done = (pos_ >= stream_.size());
  }
  return Status::Ok();
}

Status snapshot_dump_to_file(Crowtree& tree, uint64_t at_slot, snapshot_format fmt,
                             const std::string& path) {
  std::unique_ptr<SnapshotExport> exp;
  Status s = snapshot_export_begin(tree, at_slot, fmt, kSnapshotChunkBytes, &exp);
  if (!s.ok())
  {
    return s;
  }
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  if (!f)
  {
    return Status::io_error("snapshot dump: cannot open " + path);
  }
  bool done = false;
  while (!done)
  {
    std::string chunk;
    Status cs = exp->next_chunk(&chunk, &done);
    if (!cs.ok())
    {
      return cs;
    }
    if (!chunk.empty())
    {
      f.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    }
    if (!f)
    {
      return Status::io_error("snapshot dump: write failed");
    }
  }
  f.flush();
  if (!f)
  {
    return Status::io_error("snapshot dump: flush failed");
  }
  return Status::Ok();
}

Status SnapshotImport::feed(Slice chunk) {
  buf_.append(chunk.data(), chunk.size());
  return Status::Ok();
}

Status SnapshotImport::finish(uint64_t* out_at_slot) {
  const size_t len = buf_.size();
  if (len < kSnapHeader + kSnapTrailer)
  {
    return Status::invalid_argument("snapshot: stream too short");
  }
  const uint8_t* p = reinterpret_cast<const uint8_t*>(buf_.data());

  if (get_u32(p) != kSnapMagic)
  {
    return Status::corruption("snapshot: bad magic");
  }
  if (get_u32(p + 4) != kSnapVersion)
  {
    return Status::not_supported("snapshot: version");
  }
  if (static_cast<snapshot_format>(p[8]) != snapshot_format::kPortable)
  {
    return Status::not_supported("snapshot: only portable format in v1");
  }
  // Verify the whole-stream CRC over everything but the trailing 4 bytes.
  uint32_t want_crc = get_u32(p + (len - kSnapTrailer));
  if (crc32c(p, len - kSnapTrailer) != want_crc)
  {
    return Status::corruption("snapshot: CRC mismatch");
  }

  uint64_t at_slot = get_u64(p + 9);
  uint64_t count = get_u64(p + 17);

  std::vector<leaf_entry> entries;
  entries.reserve(count);
  size_t pos = kSnapHeader;
  const size_t body_end = len - kSnapTrailer;
  for (uint64_t i = 0; i < count; ++i)
  {
    if (pos + 4 > body_end)
    {
      return Status::corruption("snapshot: truncated key len");
    }
    uint32_t klen = get_u32(p + pos);
    pos += 4;
    if (pos + klen > body_end)
    {
      return Status::corruption("snapshot: truncated key");
    }
    std::string key(reinterpret_cast<const char*>(p + pos), klen);
    pos += klen;
    if (pos + 8 + 1 + 4 > body_end)
    {
      return Status::corruption("snapshot: truncated cell header");
    }
    uint64_t slot = get_u64(p + pos);
    pos += 8;
    uint8_t kind = p[pos];
    pos += 1;
    uint32_t vlen = get_u32(p + pos);
    pos += 4;
    if (pos + vlen > body_end)
    {
      return Status::corruption("snapshot: truncated value");
    }
    Slice value(reinterpret_cast<const char*>(p + pos), vlen);
    pos += vlen;
    std::string cell = encode_cell(slot, kind ? OpKind::kDelete : OpKind::kPut, value);
    entries.push_back(leaf_entry{std::move(key), std::move(cell)});
  }
  if (pos != body_end)
  {
    return Status::corruption("snapshot: trailing bytes");
  }

  Status is = tree_.install_snapshot(std::move(entries), at_slot);
  if (!is.ok())
  {
    return is;
  }
  if (out_at_slot)
  {
    *out_at_slot = at_slot;
  }
  return Status::Ok();
}

Status snapshot_load_from_file(Crowtree& tree, const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f)
  {
    return Status::io_error("snapshot load: cannot open " + path);
  }
  SnapshotImport imp(tree);
  char buf[1u << 16];
  while (f)
  {
    f.read(buf, sizeof(buf));
    std::streamsize got = f.gcount();
    if (got > 0)
    {
      Status s = imp.feed(Slice(buf, static_cast<size_t>(got)));
      if (!s.ok())
      {
        return s;
      }
    }
  }
  if (f.bad())
  {
    return Status::io_error("snapshot load: read failed");
  }
  return imp.finish(nullptr);
}

}  // namespace crowtree
