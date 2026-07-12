#include "crowtree/page_codec.h"

#include <cstring>

#include "crowtree/crc32c.h"

namespace crowtree {

namespace {

void PutU32(std::vector<uint8_t>* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}
void PutU64(std::vector<uint8_t>* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}

// Bounds-checked little-endian readers over a fixed buffer.
class Reader {
 public:
  Reader(const uint8_t* p, size_t n) : p_(p), n_(n) {}
  bool U32(uint32_t* v) {
    if (pos_ + 4 > n_) return false;
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i) r |= static_cast<uint32_t>(p_[pos_ + i]) << (8 * i);
    pos_ += 4;
    *v = r;
    return true;
  }
  bool U64(uint64_t* v) {
    if (pos_ + 8 > n_) return false;
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) r |= static_cast<uint64_t>(p_[pos_ + i]) << (8 * i);
    pos_ += 8;
    *v = r;
    return true;
  }
  bool U8(uint8_t* v) {
    if (pos_ + 1 > n_) return false;
    *v = p_[pos_++];
    return true;
  }
  bool Bytes(size_t len, std::string* out) {
    if (pos_ + len > n_) return false;
    out->assign(reinterpret_cast<const char*>(p_ + pos_), len);
    pos_ += len;
    return true;
  }

 private:
  const uint8_t* p_;
  size_t n_;
  size_t pos_ = 0;
};

void EncodeLeafBody(const LeafBase* leaf, std::vector<uint8_t>* body) {
  body->push_back(static_cast<uint8_t>(PageType::kLeafBase));
  PutU64(body, leaf->pid);
  PutU64(body, leaf->right_sibling());
  const auto& entries = leaf->entries();
  PutU32(body, static_cast<uint32_t>(entries.size()));
  for (const auto& e : entries) {
    PutU32(body, static_cast<uint32_t>(e.key.size()));
    PutU32(body, static_cast<uint32_t>(e.cell.size()));
  }
  for (const auto& e : entries) body->insert(body->end(), e.key.begin(), e.key.end());
  for (const auto& e : entries) body->insert(body->end(), e.cell.begin(), e.cell.end());
}

void EncodeInnerBody(const InnerBase* inner, std::vector<uint8_t>* body) {
  body->push_back(static_cast<uint8_t>(PageType::kInnerBase));
  PutU64(body, inner->pid);
  const auto& children = inner->children();
  const auto& seps = inner->separators();
  PutU32(body, static_cast<uint32_t>(children.size()));
  PutU32(body, static_cast<uint32_t>(seps.size()));
  for (uint64_t c : children) PutU64(body, c);
  for (const auto& s : seps) PutU32(body, static_cast<uint32_t>(s.size()));
  for (const auto& s : seps) body->insert(body->end(), s.begin(), s.end());
}

Status DecodeLeafBody(Reader* r, uint64_t self_pid, PageBase** out) {
  uint64_t right_sibling = 0;
  uint32_t count = 0;
  if (!r->U64(&right_sibling) || !r->U32(&count)) {
    return Status::Corruption("leaf header");
  }
  std::vector<uint32_t> klens(count), clens(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (!r->U32(&klens[i]) || !r->U32(&clens[i])) return Status::Corruption("leaf sizes");
  }
  std::vector<LeafEntry> entries(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (!r->Bytes(klens[i], &entries[i].key)) return Status::Corruption("leaf key");
  }
  for (uint32_t i = 0; i < count; ++i) {
    if (!r->Bytes(clens[i], &entries[i].cell)) return Status::Corruption("leaf cell");
  }
  LeafBase* leaf = LeafBase::Build(std::move(entries), right_sibling);
  leaf->pid = self_pid;
  *out = leaf;
  return Status::Ok();
}

Status DecodeInnerBody(Reader* r, uint64_t self_pid, PageBase** out) {
  uint32_t nchild = 0, nsep = 0;
  if (!r->U32(&nchild) || !r->U32(&nsep)) return Status::Corruption("inner header");
  std::vector<uint64_t> children(nchild);
  for (uint32_t i = 0; i < nchild; ++i) {
    if (!r->U64(&children[i])) return Status::Corruption("inner child");
  }
  std::vector<uint32_t> slens(nsep);
  for (uint32_t i = 0; i < nsep; ++i) {
    if (!r->U32(&slens[i])) return Status::Corruption("inner sep size");
  }
  std::vector<std::string> seps(nsep);
  for (uint32_t i = 0; i < nsep; ++i) {
    if (!r->Bytes(slens[i], &seps[i])) return Status::Corruption("inner sep");
  }
  InnerBase* inner = InnerBase::Build(std::move(seps), std::move(children));
  inner->pid = self_pid;
  *out = inner;
  return Status::Ok();
}

}  // namespace

std::vector<uint8_t> PageCodec::Encode(const PageBase* page, uint32_t iu_size) {
  std::vector<uint8_t> body;
  if (page->type == PageType::kLeafBase) {
    EncodeLeafBody(static_cast<const LeafBase*>(page), &body);
  } else {
    EncodeInnerBody(static_cast<const InnerBase*>(page), &body);
  }

  uint32_t logical_len = static_cast<uint32_t>(body.size());
  uint32_t crc = Crc32c(body.data(), body.size());

  std::vector<uint8_t> frame;
  frame.reserve(kPageFrameHeaderSize + body.size());
  PutU32(&frame, logical_len);
  PutU32(&frame, crc);
  frame.insert(frame.end(), body.begin(), body.end());

  if (iu_size > 1) {
    size_t rem = frame.size() % iu_size;
    if (rem != 0) frame.resize(frame.size() + (iu_size - rem), 0);
  }
  return frame;
}

Status PageCodec::Decode(const uint8_t* buf, size_t len, PageBase** out) {
  if (len < kPageFrameHeaderSize) return Status::InvalidArgument("short page frame");
  uint32_t logical_len = 0, crc = 0;
  for (int i = 0; i < 4; ++i) logical_len |= static_cast<uint32_t>(buf[i]) << (8 * i);
  for (int i = 0; i < 4; ++i) crc |= static_cast<uint32_t>(buf[4 + i]) << (8 * i);
  if (kPageFrameHeaderSize + logical_len > len) {
    return Status::Corruption("page logical_len exceeds frame");
  }
  const uint8_t* body = buf + kPageFrameHeaderSize;
  if (Crc32c(body, logical_len) != crc) return Status::Corruption("page CRC mismatch");

  Reader r(body, logical_len);
  uint8_t type = 0;
  uint64_t self_pid = 0;
  if (!r.U8(&type) || !r.U64(&self_pid)) return Status::Corruption("page body header");
  if (type == static_cast<uint8_t>(PageType::kLeafBase)) {
    return DecodeLeafBody(&r, self_pid, out);
  }
  if (type == static_cast<uint8_t>(PageType::kInnerBase)) {
    return DecodeInnerBody(&r, self_pid, out);
  }
  return Status::Corruption("unknown page type");
}

}  // namespace crowtree
