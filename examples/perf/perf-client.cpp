#include <random>
#include <gflags/gflags.h>
#include <photon/net/socket.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/rpc/rpc.h>
#include <photon/thread/thread11.h>

#include "protocol.h"

DEFINE_int32(port, 9527, "server port");
DEFINE_string(host, "127.0.0.1", "server ip");
DEFINE_string(type, "fill", "fill/get/put");
DEFINE_string(event_engine, "epoll", "Event engine: epoll, iouring");
DEFINE_int32(concurrency, 32, "concurrency");
DEFINE_int32(key_num, 100'000, "key num");
DEFINE_int32(value_size, 256 * 1024, "value size");

static uint64_t g_event_engine = photon::INIT_EVENT_EPOLL;

static uint64_t get_event_engine() {
    if (FLAGS_event_engine == "iouring") return photon::INIT_EVENT_IOURING;
    if (FLAGS_event_engine == "epoll") return photon::INIT_EVENT_EPOLL;
    return photon::INIT_EVENT_EPOLL;
}

static std::string random_value(size_t size) {
    static std::random_device rd;
    static thread_local std::mt19937_64 gen(rd());
    static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string s;
    s.resize(size);
    for (size_t i = 0; i < size; ++i) {
        s[i] = alphabet[gen() % (sizeof(alphabet) - 1)];
    }
    return s;
}

static std::string random_key() {
    static std::random_device rd;
    static thread_local std::mt19937_64 gen(rd());
    return std::to_string(gen() % FLAGS_key_num);
}

void run_put(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
    int ret;
    auto stub = pool->get_stub(ep, false);
    DEFER(pool->put_stub(ep, ret < 0));

    while (true) {
        KvPut::Request req;
        auto key = random_key();
        req.key.assign(key);
        auto val = random_value(FLAGS_value_size);
        req.value.assign(val);

        KvPut::Response resp;
        ret = stub->call<KvPut>(req, resp);
        if (ret < 0) {
            LOG_ERROR("rpc error: `", ret);
            continue;
        }
        if (resp.ret != 0) {
            LOG_ERROR("put failed: ret=`", resp.ret);
            continue;
        }
    }
}

void run_get(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
    int ret;
    auto stub = pool->get_stub(ep, false);
    DEFER(pool->put_stub(ep, ret < 0));

    while (true) {
        KvGet::Request req;
        std::string key = random_key();
        req.key.assign(key);

        KvGet::Response resp;
        ret = stub->call<KvGet>(req, resp);
        if (ret < 0) {
            LOG_ERROR("rpc error: `", ret);
            continue;
        }
        if (resp.ret != 0) {
            LOG_DEBUG("key not found or error: resp=`", resp.ret);
            continue;
        }
        if (resp.value.size() != (uint64_t) FLAGS_value_size) {
            LOG_ERROR("value size mismatch: expected `, got `", FLAGS_value_size, resp.value.size());
            continue;
        }
    }
}

void run_fill(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
    int ret;
    auto stub = pool->get_stub(ep, false);
    DEFER(pool->put_stub(ep, ret < 0));

    for (int i = 0; i < FLAGS_key_num; ++i) {
        KvPut::Request req;
        auto key = random_key();
        req.key.assign(key);
        auto val = random_value(FLAGS_value_size);
        req.value.assign(val);

        KvPut::Response resp;
        ret = stub->call<KvPut>(req, resp);
        if (ret < 0) {
            LOG_ERROR("rpc error: `", ret);
            continue;
        }
        if (resp.ret != 0) {
            LOG_ERROR("put failed: ret=`", resp.ret);
            continue;
        }
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    set_log_output_level(ALOG_INFO);
    g_event_engine = get_event_engine();
    if (photon::init(g_event_engine, photon::INIT_IO_NONE)) {
        LOG_ERROR_RETURN(0, -1, "fail to init photon");
    }
    DEFER(photon::fini());

    auto ep = photon::net::EndPoint(photon::net::IPAddr(FLAGS_host.c_str()),
                                    FLAGS_port);

    auto pool = photon::rpc::new_stub_pool(-1, -1, -1);
    DEFER(delete pool);

    if (FLAGS_type == "fill") {
        run_fill(ep, pool);
    } else if (FLAGS_type == "put") {
        for (int i = 0; i < FLAGS_concurrency; ++i) {
            photon::thread_create11(run_put, ep, pool);
        }
        photon::thread_sleep(-1);
    } else {
        for (int i = 0; i < FLAGS_concurrency; ++i) {
            photon::thread_create11(run_get, ep, pool);
        }
        photon::thread_sleep(-1);
    }
}