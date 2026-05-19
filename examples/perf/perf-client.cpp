#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/work_stealing.hpp>
#include <boost/fiber/io_uring.hpp>
#include <gflags/gflags.h>

// ── Protocol ──────────────────────────────────────────────────────────
enum OpCode : uint8_t { OP_PUT = 0x01, OP_GET = 0x02 };

// ── Flags ─────────────────────────────────────────────────────────────
DEFINE_int32(port, 9527, "Server port");
DEFINE_string(host, "127.0.0.1", "Server ip");
DEFINE_string(type, "fill", "fill/get/put");
DEFINE_int32(concurrency, 16, "Number of connections (= fibers)");
DEFINE_int32(key_num, 100000, "Key num");
DEFINE_int32(value_size, 256, "Value size");
DEFINE_int32(pipeline, 16, "Pipeline depth per connection");
DEFINE_int32(threads, 2, "Number of pthread I/O workers");
DEFINE_bool(sqpoll, false, "Use io_uring SQPOLL (kernel thread polls SQ)");

// ── Globals ───────────────────────────────────────────────────────────
static std::atomic<uint64_t> g_ops{0};
static std::atomic<int> g_fill_alive{0};   // number of fill fibers still running
static constexpr int KEY_POOL_SIZE = 65536;

// ── Key/value helpers ─────────────────────────────────────────────────
static thread_local std::string g_key_pool[KEY_POOL_SIZE];
static thread_local bool g_key_pool_ready = false;

static void init_key_pool() {
    if (g_key_pool_ready) return;
    std::mt19937_64 gen(42);
    for (int i = 0; i < KEY_POOL_SIZE; ++i)
        g_key_pool[i] = std::to_string(gen() % FLAGS_key_num);
    g_key_pool_ready = true;
}

static thread_local std::string g_value_buf;
static void init_value_buf() {
    if (!g_value_buf.empty()) return;
    g_value_buf.resize(FLAGS_value_size);
    std::mt19937 gen(12345);
    for (size_t i = 0; i < g_value_buf.size(); ++i)
        g_value_buf[i] = 'a' + (gen() % 26);
}

// ── Buffered I/O (io_uring) ─────────────────────────────────────────
class BufferedReader {
    static constexpr size_t BUF_CAP = 32 * 1024;
    int fd_;
    std::unique_ptr<char[]> buf_;
    size_t pos_ = 0, len_ = 0;

    bool refill() {
        pos_ = 0;
        int r = boost::fibers::io_uring::read(fd_, buf_.get(), BUF_CAP);
        if (r <= 0) { len_ = 0; return false; }
        len_ = static_cast<size_t>(r);
        return true;
    }
public:
    explicit BufferedReader(int fd) : fd_(fd), buf_(new char[BUF_CAP]) {}
    bool read(char* out, size_t n) {
        while (n > 0) {
            if (pos_ >= len_) { if (!refill()) return false; }
            size_t avail = len_ - pos_;
            size_t chunk = std::min(avail, n);
            ::memcpy(out, buf_.get() + pos_, chunk);
            pos_ += chunk; out += chunk; n -= chunk;
        }
        return true;
    }
};

class BufferedWriter {
    static constexpr size_t BUF_CAP = 64 * 1024;
    int fd_;
    std::unique_ptr<char[]> buf_;
    size_t pos_ = 0;
public:
    explicit BufferedWriter(int fd) : fd_(fd), buf_(new char[BUF_CAP]) {}
    ~BufferedWriter() { flush(); }
    bool write(const char* data, size_t n) {
        if (n + pos_ > BUF_CAP) {
            if (!flush()) return false;
            while (n > 0) {
                int w = boost::fibers::io_uring::write(fd_, data, n);
                if (w <= 0) return false;
                data += w; n -= w;
            }
            return true;
        }
        ::memcpy(buf_.get() + pos_, data, n);
        pos_ += n;
        return true;
    }
    bool flush() {
        if (!pos_) return true;
        const char* p = buf_.get(); size_t rem = pos_;
        while (rem > 0) {
            int w = boost::fibers::io_uring::write(fd_, p, rem);
            if (w <= 0) { pos_ = 0; return false; }
            p += w; rem -= w;
        }
        pos_ = 0;
        return true;
    }
};

// ── Connect via io_uring ─────────────────────────────────────────────
static int connect_server() {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    int flag = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(FLAGS_port);
    inet_pton(AF_INET, FLAGS_host.c_str(), &addr.sin_addr);

    int ret = boost::fibers::io_uring::connect(
        fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

// ── PUT fiber worker (runs forever) ──────────────────────────────────
static void run_put(long fiber_id) {
    init_key_pool();
    init_value_buf();
    uint32_t key_idx = fiber_id + 17;

    while (true) {
        int fd = connect_server();
        if (fd < 0) {
            boost::this_fiber::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        BufferedWriter wr(fd);
        BufferedReader rd(fd);

        while (true) {
            int sent = 0;
            for (int p = 0; p < FLAGS_pipeline; ++p) {
                const std::string& key = g_key_pool[key_idx++ % KEY_POOL_SIZE];
                uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
                uint32_t vlen = htonl(static_cast<uint32_t>(g_value_buf.size()));
                uint8_t op = OP_PUT;

                if (!wr.write(reinterpret_cast<const char*>(&op), 1)) goto conn_done;
                if (!wr.write(reinterpret_cast<const char*>(&klen), 4)) goto conn_done;
                if (!wr.write(key.data(), key.size())) goto conn_done;
                if (!wr.write(reinterpret_cast<const char*>(&vlen), 4)) goto conn_done;
                if (!wr.write(g_value_buf.data(), g_value_buf.size())) goto conn_done;
                sent++;
            }
            if (!wr.flush()) goto conn_done;

            for (int p = 0; p < sent; ++p) {
                int32_t ret_n;
                if (!rd.read(reinterpret_cast<char*>(&ret_n), 4)) goto conn_done;
                g_ops++;
            }
        }
conn_done:
        ::close(fd);
    }
}

// ── GET fiber worker (runs forever) ──────────────────────────────────
static void run_get(long fiber_id) {
    init_key_pool();
    uint32_t key_idx = fiber_id + 10000;

    while (true) {
        int fd = connect_server();
        if (fd < 0) {
            boost::this_fiber::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        BufferedWriter wr(fd);
        BufferedReader rd(fd);

        while (true) {
            int sent = 0;
            for (int p = 0; p < FLAGS_pipeline; ++p) {
                const std::string& key = g_key_pool[key_idx++ % KEY_POOL_SIZE];
                uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
                uint8_t op = OP_GET;

                if (!wr.write(reinterpret_cast<const char*>(&op), 1)) goto conn_done;
                if (!wr.write(reinterpret_cast<const char*>(&klen), 4)) goto conn_done;
                if (!wr.write(key.data(), key.size())) goto conn_done;
                sent++;
            }
            if (!wr.flush()) goto conn_done;

            for (int p = 0; p < sent; ++p) {
                int32_t ret_n;
                if (!rd.read(reinterpret_cast<char*>(&ret_n), 4)) goto conn_done;

                int32_t ret = ntohl(ret_n);
                if (ret == 0) {
                    uint32_t vlen_n;
                    if (!rd.read(reinterpret_cast<char*>(&vlen_n), 4)) goto conn_done;
                    uint32_t vlen = ntohl(vlen_n);
                    char dummy[4096];
                    while (vlen > 0) {
                        size_t chunk = std::min(static_cast<size_t>(vlen), sizeof(dummy));
                        if (!rd.read(dummy, chunk)) goto conn_done;
                        vlen -= chunk;
                    }
                }
                g_ops++;
            }
        }
conn_done:
        ::close(fd);
    }
}

// ── FILL fiber worker (runs once and exits) ─────────────────────────
static void run_fill(long fiber_id, int total_fibers) {
    init_key_pool();
    init_value_buf();

    int fd = connect_server();
    if (fd < 0) {
        fprintf(stderr, "[fill %ld] Connect failed\n", fiber_id);
        g_fill_alive--;
        return;
    }

    BufferedWriter wr(fd);
    BufferedReader rd(fd);
    uint32_t key_idx = fiber_id;
    int outstanding = 0;

    for (int i = fiber_id; i < FLAGS_key_num; i += total_fibers) {
        const std::string& key = g_key_pool[key_idx++ % KEY_POOL_SIZE];
        uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
        uint32_t vlen_n = htonl(static_cast<uint32_t>(g_value_buf.size()));
        uint8_t op = OP_PUT;

        if (!wr.write(reinterpret_cast<const char*>(&op), 1)) break;
        if (!wr.write(reinterpret_cast<const char*>(&klen), 4)) break;
        if (!wr.write(key.data(), key.size())) break;
        if (!wr.write(reinterpret_cast<const char*>(&vlen_n), 4)) break;
        if (!wr.write(g_value_buf.data(), g_value_buf.size())) break;

        outstanding++;
        if (outstanding >= FLAGS_pipeline || i + total_fibers >= FLAGS_key_num) {
            if (!wr.flush()) break;
            for (int p = 0; p < outstanding; ++p) {
                int32_t ret_n;
                if (!rd.read(reinterpret_cast<char*>(&ret_n), 4)) break;
                g_ops++;
            }
            outstanding = 0;
        }
    }

    wr.flush();
    ::close(fd);
    g_fill_alive--;
}

// ── I/O Worker Thread: sets up work_stealing + io_uring, then parks ──
static void io_worker_thread(int num_threads, bool sqpoll) {
    // Install work_stealing scheduler with io_uring on this thread.
    boost::fibers::use_scheduling_algorithm<
        boost::fibers::algo::work_stealing>(num_threads, false, sqpoll);

    // Park forever — the scheduler will run fibers that were spawned on any thread.
    boost::fibers::mutex mtx;
    boost::fibers::condition_variable cv;
    std::unique_lock<boost::fibers::mutex> lk(mtx);
    cv.wait(lk);
}

// ── Main ───────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    setbuf(stdout, nullptr);
    setbuf(stderr, nullptr);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    int num_threads = FLAGS_threads > 0 ? FLAGS_threads : 1;
    int total_fibers = FLAGS_concurrency > 0 ? FLAGS_concurrency : 16;

    // Start I/O worker threads first (they will share work_stealing)
    std::vector<std::thread> workers;
    for (int t = 1; t < num_threads; ++t) {
        workers.emplace_back(io_worker_thread, num_threads, FLAGS_sqpoll);
    }

    // Also install on main thread
    boost::fibers::use_scheduling_algorithm<
        boost::fibers::algo::work_stealing>(num_threads, false, FLAGS_sqpoll);

    if (FLAGS_type == "fill") {
        fprintf(stdout, "Starting fill with %d keys (value size %d, %d fibers × %d threads, pipeline %d)...\n",
                FLAGS_key_num, FLAGS_value_size, total_fibers, num_threads, FLAGS_pipeline);

        g_fill_alive.store(total_fibers);
        for (int i = 0; i < total_fibers; ++i) {
            boost::fibers::fiber(std::allocator_arg,
                                  boost::fibers::fixedsize_stack(128 * 1024),
                                  run_fill, (long)i, total_fibers).detach();
        }

        // Progress reporter (main fiber)
        uint64_t total = 0;
        while (g_fill_alive.load() > 0) {
            boost::this_fiber::sleep_for(std::chrono::seconds(1));
            uint64_t ops = g_ops.exchange(0);
            total += ops;
            fprintf(stdout, "Fill QPS: %lu\n", ops);
        }
        total += g_ops.exchange(0);
        fprintf(stdout, "Fill done, total ops: %lu\n", total);

    } else {
        fprintf(stdout, "Starting %s with %d fibers × %d threads, pipeline=%d...\n",
                FLAGS_type.c_str(), total_fibers, num_threads, FLAGS_pipeline);

        auto fn = (FLAGS_type == "put") ? run_put : run_get;

        for (int i = 0; i < total_fibers; ++i) {
            boost::fibers::fiber(std::allocator_arg,
                                  boost::fibers::fixedsize_stack(128 * 1024),
                                  fn, (long)i).detach();
        }

        // QPS reporter (main fiber)
        while (true) {
            boost::this_fiber::sleep_for(std::chrono::seconds(1));
            fprintf(stdout, "QPS: %lu\n", g_ops.exchange(0));
        }
    }

    for (auto& t : workers) t.join();
    return 0;
}
