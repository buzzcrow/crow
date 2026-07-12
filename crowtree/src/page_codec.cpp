#include "crowtree/page_codec.h"

#include "crowtree/crc32c.h"

#include <cstring>

namespace crowtree {

namespace {

void put_u32(std::vector<uint8_t>* out, uint32_t v) {
  for (int i = 0; i < 4; ++i)
  {
    out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
  }
}
void put_u64(std::vector<uint8_t>* out, uint64_t v) {
  for (int i = 0; i < 8; ++i)
  {
    out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
  }
}

// Bounds-checked little-endian readers over a fixed buffer.
class Reader {
 public:
  Reader(const uint8_t* p, size_t n) : p_(p), n_(n) {}
  bool u32(uint32_t* v) {
    if (pos_ + 4 > n_)
    {
      return false;
    }
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i)
    {
      r |= static_cast<uint32_t>(p_[pos_ + i]) << (8 * i);
    }
    pos_ += 4;
    *v = r;
    return true;
  }
  bool u64(uint64_t* v) {
    if (pos_ + 8 > n_)
    {
      return false;
    }
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i)
    {
      r |= static_cast<uint64_t>(p_[pos_ + i]) << (8 * i);
    }
    pos_ += 8;
    *v = r;
    return true;
  }
  bool u8(uint8_t* v) {
    if (pos_ + 1 > n_)
    {
      return false;
    }
    *v = p_[pos_++];
    return true;
  }
  bool bytes(size_t len, std::string* out) {
    if (pos_ + len > n_)
    {
      return false;
    }
    out->assign(reinterpret_cast<const char*>(p_ + pos_), len);
    pos_ += len;
    return true;
  }
  bool bytes(size_t len, buffer* out) {
    if (pos_ + len > n_)
    {
      return false;
    }
    *out = buffer::alloc(len);
    if (len > 0)
    {
      std::memcpy(out->data(), p_ + pos_, len);
    }
    pos_ += len;
    return true;
  }

 private:
  const uint8_t* p_;
  size_t n_;
  size_t pos_ = 0;
};

void encode_leaf_body(const LeafBase* leaf, std::vector<uint8_t>* body) {
  body->push_back(static_cast<uint8_t>(page_type::kLeafBase));
  put_u64(body, leaf->page_id);
  put_u64(body, leaf->right_sibling());
  const auto& entries = leaf->entries();
  put_u32(body, static_cast<uint32_t>(entries.size()));
  for (const auto& e : entries)
  {
    put_u32(body, static_cast<uint32_t>(e.key.size()));
    put_u32(body, static_cast<uint32_t>(e.cell.size()));
  }
  for (const auto& e : entries)
  {
    body->insert(body->end(), e.key.begin(), e.key.end());
  }
  for (const auto& e : entries)
  {
    body->insert(body->end(), e.cell.data(), e.cell.data() + e.cell.size());
  }
}

void encode_inner_body(const InnerBase* inner, std::vector<uint8_t>* body) {
  body->push_back(static_cast<uint8_t>(page_type::kInnerBase));
  put_u64(body, inner->page_id);
  const auto& children = inner->children();
  const auto& seps = inner->separators();
  put_u32(body, static_cast<uint32_t>(children.size()));
  put_u32(body, static_cast<uint32_t>(seps.size()));
  for (uint64_t c : children)
  {
    put_u64(body, c);
  }
  for (const auto& s : seps)
  {
    put_u32(body, static_cast<uint32_t>(s.size()));
  }
  for (const auto& s : seps)
  {
    body->insert(body->end(), s.begin(), s.end());
  }
}

Status decode_leaf_body(Reader* r, uint64_t self_page_id, PageBase** out) {
  uint64_t right_sibling = 0;
  uint32_t count = 0;
  if (!r->u64(&right_sibling) || !r->u32(&count))
  {
    return Status::corruption("leaf header");
  }
  std::vector<uint32_t> klens(count), clens(count);
  for (uint32_t i = 0; i < count; ++i)
  {
    if (!r->u32(&klens[i]) || !r->u32(&clens[i]))
    {
      return Status::corruption("leaf sizes");
    }
  }
  std::vector<leaf_entry> entries(count);
  for (uint32_t i = 0; i < count; ++i)
  {
    if (!r->bytes(klens[i], &entries[i].key))
    {
      return Status::corruption("leaf key");
    }
  }
  for (uint32_t i = 0; i < count; ++i)
  {
    if (!r->bytes(clens[i], &entries[i].cell))
    {
      return Status::corruption("leaf cell");
    }
  }
  LeafBase* leaf = LeafBase::build(std::move(entries), right_sibling);
  leaf->page_id = self_page_id;
  *out = leaf;
  return Status::Ok();
}

Status decode_inner_body(Reader* r, uint64_t self_page_id, PageBase** out) {
  uint32_t nchild = 0, nsep = 0;
  if (!r->u32(&nchild) || !r->u32(&nsep))
  {
    return Status::corruption("inner header");
  }
  std::vector<uint64_t> children(nchild);
  for (uint32_t i = 0; i < nchild; ++i)
  {
    if (!r->u64(&children[i]))
    {
      return Status::corruption("inner child");
    }
  }
  std::vector<uint32_t> slens(nsep);
  for (uint32_t i = 0; i < nsep; ++i)
  {
    if (!r->u32(&slens[i]))
    {
      return Status::corruption("inner sep size");
    }
  }
  std::vector<std::string> seps(nsep);
  for (uint32_t i = 0; i < nsep; ++i)
  {
    if (!r->bytes(slens[i], &seps[i]))
    {
      return Status::corruption("inner sep");
    }
  }
  InnerBase* inner = InnerBase::build(std::move(seps), std::move(children));
  inner->page_id = self_page_id;
  *out = inner;
  return Status::Ok();
}

}  // namespace

std::vector<uint8_t> PageCodec::encode(const PageBase* page, uint32_t iu_size) {
  std::vector<uint8_t> body;
  if (page->type == page_type::kLeafBase)
  {
    encode_leaf_body(static_cast<const LeafBase*>(page), &body);
  } else
  {
    encode_inner_body(static_cast<const InnerBase*>(page), &body);
  }

  uint32_t logical_len = static_cast<uint32_t>(body.size());
  uint32_t crc = crc32c(body.data(), body.size());

  std::vector<uint8_t> frame;
  frame.reserve(kPageFrameHeaderSize + body.size());
  put_u32(&frame, logical_len);
  put_u32(&frame, crc);
  frame.insert(frame.end(), body.begin(), body.end());

  if (iu_size > 1)
  {
    size_t rem = frame.size() % iu_size;
    if (rem != 0)
    {
      frame.resize(frame.size() + (iu_size - rem), 0);
    }
  }
  return frame;
}

Status PageCodec::decode(const uint8_t* buf, size_t len, PageBase** out) {
  if (len < kPageFrameHeaderSize)
  {
    return Status::invalid_argument("short page frame");
  }
  uint32_t logical_len = 0, crc = 0;
  for (int i = 0; i < 4; ++i)
  {
    logical_len |= static_cast<uint32_t>(buf[i]) << (8 * i);
  }
  for (int i = 0; i < 4; ++i)
  {
    crc |= static_cast<uint32_t>(buf[4 + i]) << (8 * i);
  }
  if (kPageFrameHeaderSize + logical_len > len)
  {
    return Status::corruption("page logical_len exceeds frame");
  }
  const uint8_t* body = buf + kPageFrameHeaderSize;
  if (crc32c(body, logical_len) != crc)
  {
    return Status::corruption("page CRC mismatch");
  }

  Reader r(body, logical_len);
  uint8_t type = 0;
  uint64_t self_page_id = 0;
  if (!r.u8(&type) || !r.u64(&self_page_id))
  {
    return Status::corruption("page body header");
  }
  if (type == static_cast<uint8_t>(page_type::kLeafBase))
  {
    return decode_leaf_body(&r, self_page_id, out);
  }
  if (type == static_cast<uint8_t>(page_type::kInnerBase))
  {
    return decode_inner_body(&r, self_page_id, out);
  }
  return Status::corruption("unknown page type");
}

}  // namespace crowtree
