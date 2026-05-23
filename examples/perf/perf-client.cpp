#include <random>
#include <atomic>
#include <cstdio>
#include <gflags/gflags.h>
#include <photon/net/socket.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/rpc/rpc.h>
#include <photon/thread/thread11.h>
#include <photon/common/alog-stdstring.h>

#include "protocol.h"

DEFINE_int32(port, 9527, "server port");
DEFINE_string(host, "127.0.0.1", "server ip");
DEFINE_string(type, "fill", "fill/get/put");
DEFINE_string(event_engine, "epoll", "Event engine: epoll, iouring");
DEFINE_int32(concurrency, 32, "concurrency");
DEFINE_int32(key_num, 100'000, "key num");
DEFINE_int32(value_size, 256 * 1024, "value size");
DEFINE_bool(fill_full, false, "Fill all keys sequentially from 0 to key_num-1 instead of random");
DEFINE_int32(show_stats_interval, 1, "Interval seconds to show stats");

static uint64_t g_event_engine = photon::INIT_EVENT_EPOLL;

static std::atomic<uint64_t> g_total_ops{0};
static std::atomic<uint64_t> g_total_us{0};   // photon::now is in microseconds
static std::atomic<uint64_t> g_total_bytes{0};

static void stats_loop() {
    while (true) {
        photon::thread_sleep(FLAGS_show_stats_interval);
        auto ops = g_total_ops.exchange(0);
        auto us = g_total_us.exchange(0);
        auto bytes = g_total_bytes.exchange(0);
        uint64_t cur_qps = ops / FLAGS_show_stats_interval;
        double avg_lat_us = ops > 0 ? (double)us / (double)ops : 0.0;
        double mbps = (double)bytes / FLAGS_show_stats_interval / (1024.0 * 1024.0);
        char lat_buf[32], tp_buf[32];
        snprintf(lat_buf, sizeof(lat_buf), "%.2f", avg_lat_us);
        snprintf(tp_buf, sizeof(tp_buf), "%.2f", mbps);
        LOG_INFO("QPS: ` | Avg latency: ` us | Throughput: ` MB/s",
                 cur_qps, lat_buf, tp_buf);
    }
}

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
        uint64_t t0 = photon::now;
        ret = stub->call<KvPut>(req, resp);
        uint64_t dt = photon::now - t0;
        if (ret < 0) {
            LOG_ERROR("rpc error: `", ret);
            continue;
        }
        if (resp.ret != 0) {
            LOG_ERROR("put failed: ret=`", resp.ret);
            continue;
        }
        g_total_ops++;
        g_total_us += dt;
        g_total_bytes += req.key.size() + req.value.size();
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

        IOVector resp_iov;
        uint64_t t0 = photon::now;
        auto* resp = stub->call<KvGet>(req, resp_iov);
        uint64_t dt = photon::now - t0;
        if (!resp) {
            LOG_ERROR("rpc error: call returned null");
            ret = -1;
            continue;
        }
        ret = 0;
        g_total_ops++;
        g_total_us += dt;
        if (resp->ret == 0 && resp->value.size() > 0) {
            g_total_bytes += req.key.size() + resp->value.size();
        } else {
            g_total_bytes += req.key.size();
        }
    }
}

void run_fill(photon::net::EndPoint ep, photon::rpc::StubPool* pool) {
    int ret;
    auto stub = pool->get_stub(ep, false);
    DEFER(pool->put_stub(ep, ret < 0));

    uint64_t fill_ops = 0, fill_us = 0, fill_bytes = 0;

    for (int i = 0; i < FLAGS_key_num; ++i) {
        KvPut::Request req;
        std::string key = FLAGS_fill_full ? std::to_string(i) : random_key();
        req.key.assign(key);
        auto val = random_value(FLAGS_value_size);
        req.value.assign(val);

        KvPut::Response resp;
        uint64_t t0 = photon::now;
        ret = stub->call<KvPut>(req, resp);
        uint64_t dt = photon::now - t0;
        if (ret < 0) {
            LOG_ERROR("rpc error: `", ret);
            continue;
        }
        if (resp.ret != 0) {
            LOG_ERROR("put failed: ret=`", resp.ret);
            continue;
        }
        fill_ops++;
        fill_us += dt;
        fill_bytes += req.key.size() + req.value.size();
    }

    double elapsed_s = (double)fill_us / 1e6;
    double avg_lat_us = fill_ops > 0 ? (double)fill_us / (double)fill_ops : 0.0;
    double qps_val = elapsed_s > 0 ? (double)fill_ops / elapsed_s : 0.0;
    double mbps_val = elapsed_s > 0 ? (double)fill_bytes / elapsed_s / (1024.0 * 1024.0) : 0.0;

    char lat_buf[32], tp_buf[32], qps_buf[32];
    snprintf(qps_buf, sizeof(qps_buf), "%.2f", qps_val);
    snprintf(lat_buf, sizeof(lat_buf), "%.2f", avg_lat_us);
    snprintf(tp_buf, sizeof(tp_buf), "%.2f", mbps_val);

    LOG_INFO("Fill done: ops=` | QPS: ` | Avg latency: ` us | Throughput: ` MB/s",
             fill_ops, qps_buf, lat_buf, tp_buf);
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
        photon::thread_create11(stats_loop);
        for (int i = 0; i < FLAGS_concurrency; ++i) {
            photon::thread_create11(run_put, ep, pool);
        }
        photon::thread_sleep(-1);
    } else {
        photon::thread_create11(stats_loop);
        for (int i = 0; i < FLAGS_concurrency; ++i) {
            photon::thread_create11(run_get, ep, pool);
        }
        photon::thread_sleep(-1);
    }
}
