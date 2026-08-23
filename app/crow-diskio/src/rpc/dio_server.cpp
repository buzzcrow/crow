// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc/dio_server.h"

#include "crow-rpc/server/message.h"
#include "crow-rpc/server/server.h"
#include "disk/disk.h"

#include <diskio_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <msg_type_generated.h>

#include <cerrno>
#include <chrono>
#include <cstring>

namespace crow::diskio
{

// diskio_generated.h puts types in crow::diskio::proto.
// msg_type_generated.h puts FBMsgType in crow::rpc::proto.
namespace dproto = crow::diskio::proto;
namespace rproto = crow::rpc::proto;

DiskioServer::DiskioServer(std::shared_ptr<DiskSet> disk_set, crow::rpc::SocketTransport *transport)
    : disk_set_(std::move(disk_set)),
      transport_(transport)
{
}

// Build a diskio response control buffer (flatbuffer).
crow::rpc::Buffer *DiskioServer::build_response_ctrl(crow::rpc::BufferPool *pool, uint64_t request_id,
                                                     uint64_t rpc_create_nano, int16_t ret_code, uint16_t msg_type)
{
    auto                           fb_ret = static_cast<dproto::FBDiskIoRetCode>(ret_code);
    flatbuffers::FlatBufferBuilder fbb(64);
    if (msg_type == static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse)) {
        auto off = dproto::CreateFBDiskWriteResponse(fbb, request_id, rpc_create_nano, fb_ret);
        fbb.Finish(off);
    }
    else if (msg_type == static_cast<uint16_t>(rproto::FBMsgType_EDiskReadResponse)) {
        auto off = dproto::CreateFBDiskReadResponse(fbb, request_id, rpc_create_nano, fb_ret);
        fbb.Finish(off);
    }
    else { // fsync response
        auto off = dproto::CreateFBDiskFsyncResponse(fbb, request_id, rpc_create_nano, fb_ret);
        fbb.Finish(off);
    }
    uint32_t size = fbb.GetSize();
    auto    *buf  = pool->alloc(size);
    if (buf == nullptr) {
        return nullptr;
    }
    std::memcpy(buf->data, fbb.GetBufferPointer(), size);
    buf->write(buf->data, size);
    return buf;
}

void DiskioServer::send_error_response(crow::rpc::Connection *conn, uint64_t request_id, uint64_t rpc_create_nano,
                                       uint16_t msg_type, int16_t ret_code)
{
    auto *pool       = conn->pool();
    auto *ctrl       = build_response_ctrl(pool, request_id, rpc_create_nano, ret_code, msg_type);
    auto *out        = crow::rpc::build_out_frame(request_id, msg_type, ctrl, nullptr);
    out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    transport_->submit(conn, out);
}

static DiskId parse_disk_id(const crow::rpc::proto::FBInt128 *fb_id)
{
    if (fb_id == nullptr) {
        return {0, 0};
    }
    return {fb_id->high(), fb_id->low()};
}

crow::rpc::OutFrame *DiskioServer::handle_write(crow::rpc::Frame *request, crow::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskWriteRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId   did         = parse_disk_id(fb_req->disk_id());
    uint32_t zone_index  = fb_req->zone_index();
    uint64_t zone_offset = fb_req->zone_offset();
    uint32_t size        = fb_req->size();

    auto disk = disk_set_->find_disk(did);
    if (disk == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));
        return nullptr;
    }

    Zone *zone = disk->find_zone(zone_index);
    if (zone == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_ZoneNotExist));
        return nullptr;
    }
    off_t phys_offset = static_cast<off_t>(zone->base_offset + zone_offset);

    crow::rpc::Buffer *data_buf = nullptr;
    if (request->data_buf != nullptr && size > 0) {
        data_buf = request->data_buf->ref_clone();
    }
    delete request;

    if (data_buf == nullptr && size > 0) {
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }

    Disk *disk_ptr = disk.get();
    disk_ptr->engine()->submit_write(
        disk_ptr, phys_offset, data_buf ? data_buf->data : nullptr, size,
        [this, conn, req_id, create_nano, msg_type, data_buf, size](int res) {
            int16_t ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
            if (res < 0) {
                ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError);
            }
            else if (static_cast<uint32_t>(res) < size) {
                ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_PartialWrite);
            }
            auto *pool       = conn->pool();
            auto *ctrl       = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
            auto *out        = crow::rpc::build_out_frame(req_id, msg_type, ctrl, nullptr);
            out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            transport_->submit(conn, out);
            if (data_buf != nullptr) {
                data_buf->release();
            }
        });

    return nullptr;
}

crow::rpc::OutFrame *DiskioServer::handle_read(crow::rpc::Frame *request, crow::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskReadResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskReadRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId   did                 = parse_disk_id(fb_req->disk_id());
    uint32_t zone_index          = fb_req->zone_index();
    uint64_t zone_offset         = fb_req->zone_offset();
    uint32_t size                = fb_req->size();
    uint64_t test_pattern_offset = fb_req->test_pattern_offset();

    auto disk = disk_set_->find_disk(did);
    if (disk == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));
        return nullptr;
    }

    Zone *zone = disk->find_zone(zone_index);
    if (zone == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_ZoneNotExist));
        return nullptr;
    }
    off_t phys_offset = static_cast<off_t>(zone->base_offset + zone_offset);

    auto *pool     = conn->pool();
    auto *read_buf = pool->alloc(size);
    if (read_buf == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }

    delete request;

    Disk *disk_ptr = disk.get();
    disk_ptr->engine()->submit_read(disk_ptr, phys_offset, read_buf->data, size, test_pattern_offset,
                                    [this, conn, req_id, create_nano, msg_type, read_buf, size](int res) {
                                        int16_t ret_code        = static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
                                        crow::rpc::Buffer *data = nullptr;
                                        if (res < 0) {
                                            ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError);
                                        }
                                        else if (static_cast<uint32_t>(res) < size) {
                                            ret_code = static_cast<int16_t>(dproto::FBDiskIoRetCode_PartialWrite);
                                        }
                                        else {
                                            read_buf->len = static_cast<uint32_t>(res);
                                            data          = read_buf;
                                        }
                                        auto *pool = conn->pool();
                                        auto *ctrl = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
                                        auto *out  = crow::rpc::build_out_frame(req_id, msg_type, ctrl, data);
                                        out->create_nano = static_cast<uint64_t>(
                                            std::chrono::steady_clock::now().time_since_epoch().count());
                                        transport_->submit(conn, out);
                                        if (data == nullptr) {
                                            read_buf->release();
                                        }
                                    });

    return nullptr;
}

crow::rpc::OutFrame *DiskioServer::handle_fsync(crow::rpc::Frame *request, crow::rpc::Connection *conn)
{
    uint64_t req_id      = request->request_id;
    uint64_t create_nano = request->rpc_create_nano;
    uint16_t msg_type    = static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncResponse);

    auto *fb_req = ::flatbuffers::GetRoot<dproto::FBDiskFsyncRequest>(request->control.data());
    if (fb_req == nullptr || request->control.size() < 4) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type, static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError));
        return nullptr;
    }
    DiskId did = parse_disk_id(fb_req->disk_id());

    auto disk = disk_set_->find_disk(did);
    if (disk == nullptr) {
        delete request;
        send_error_response(conn, req_id, create_nano, msg_type,
                            static_cast<int16_t>(dproto::FBDiskIoRetCode_DiskNotExist));
        return nullptr;
    }

    delete request;

    Disk *disk_ptr = disk.get();
    disk_ptr->engine()->submit_fsync(disk_ptr, [this, conn, req_id, create_nano, msg_type](int res) {
        int16_t ret_code = (res < 0) ? static_cast<int16_t>(dproto::FBDiskIoRetCode_IoError)
                                     : static_cast<int16_t>(dproto::FBDiskIoRetCode_Success);
        auto   *pool     = conn->pool();
        auto   *ctrl     = build_response_ctrl(pool, req_id, create_nano, ret_code, msg_type);
        auto   *out      = crow::rpc::build_out_frame(req_id, msg_type, ctrl, nullptr);
        out->create_nano = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        transport_->submit(conn, out);
    });

    return nullptr;
}

void DiskioServer::register_handlers(crow::rpc::RpcServer &server)
{
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskWriteRequest),
        [this](crow::rpc::Frame *req, crow::rpc::Connection *conn) { return handle_write(req, conn); });
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskReadRequest),
        [this](crow::rpc::Frame *req, crow::rpc::Connection *conn) { return handle_read(req, conn); });
    server.register_handler(
        static_cast<uint16_t>(rproto::FBMsgType_EDiskFsyncRequest),
        [this](crow::rpc::Frame *req, crow::rpc::Connection *conn) { return handle_fsync(req, conn); });
}

} // namespace crow::diskio
