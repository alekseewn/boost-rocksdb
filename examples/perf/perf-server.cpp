#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

#include <atomic>
#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/io_uring_stealing.hpp>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <memory>
#include <thread>
#include <vector>

#include "protocol.h"

DEFINE_int32(port, 9527, "Server listen port");
DEFINE_int32(show_qps_interval, 1, "Interval seconds to show qps");
DEFINE_int32(threads, 4, "Number of fiber/I/O worker threads");
DEFINE_string(db_dir, "perf-db", "DB dir");
DEFINE_bool(clean_db, false, "Clean db before tests");

static std::atomic<uint64_t> g_qps{0};
static std::atomic<uint64_t> g_qps_total{0};
static std::atomic<bool> g_shutdown{false};
static std::atomic<int> g_active_puts{0};
static std::atomic<int> g_active_reads{0};

class BufferedReader {
    static constexpr size_t BUF_SIZE = 128 * 1024;
    int fd_;
    std::unique_ptr<char[]> buf_;
    size_t pos_ = 0;
    size_t len_ = 0;

    bool refill() {
        pos_ = 0;
        g_active_reads++;
        int r = boost::fibers::io_uring::read(fd_, buf_.get(), BUF_SIZE);
        g_active_reads--;
        if (r <= 0) { len_ = 0; return false; }
        len_ = static_cast<size_t>(r);
        return true;
    }
public:
    explicit BufferedReader(int fd) : fd_(fd), buf_(new char[BUF_SIZE]) {}

    bool read(char* out, size_t n) {
        while (n > 0) {
            if (pos_ >= len_) {
                if (!refill()) return false;
            }
            size_t avail = len_ - pos_;
            size_t chunk = std::min(avail, n);
            ::memcpy(out, buf_.get() + pos_, chunk);
            pos_ += chunk;
            out += chunk;
            n -= chunk;
        }
        return true;
    }
};

class BufferedWriter {
    static constexpr size_t BUF_SIZE = 128 * 1024;
    int fd_;
    std::unique_ptr<char[]> buf_;
    size_t pos_ = 0;

public:
    explicit BufferedWriter(int fd) : fd_(fd), buf_(new char[BUF_SIZE]) {}
    ~BufferedWriter() { flush(); }

    bool write(const char* data, size_t n) {
        if (n + pos_ > BUF_SIZE) {
            if (!flush()) return false;
            const char* p = data;
            size_t rem = n;
            while (rem > 0) {
                int w = boost::fibers::io_uring::write(fd_, p, rem);
                if (w <= 0) return false;
                p += w;
                rem -= w;
            }
            return true;
        }
        ::memcpy(buf_.get() + pos_, data, n);
        pos_ += n;
        return true;
    }

    bool flush() {
        if (pos_ == 0) return true;
        const char* p = buf_.get();
        size_t remaining = pos_;
        while (remaining > 0) {
            int w = boost::fibers::io_uring::write(fd_, p, remaining);
            if (w <= 0) { pos_ = 0; return false; }
            p += w;
            remaining -= w;
        }
        pos_ = 0;
        return true;
    }
};

static void handle_client(int fd, rocksdb::DB* db,
                          rocksdb::WriteOptions* write_opts,
                          rocksdb::ReadOptions* read_opts) {
    fprintf(stderr, "[srv] handle_client fd=%d START\n", fd);
    BufferedReader rd(fd);
    BufferedWriter wr(fd);
    int put_count = 0;

    while (true) {
        uint8_t opcode;
        if (!rd.read(reinterpret_cast<char*>(&opcode), 1)) {
            fprintf(stderr, "[srv] fd=%d read opcode failed after %d puts\n", fd, put_count);
            break;
        }

        switch (opcode) {
        case OP_PUT: {
            g_active_puts++;
            uint32_t klen_n, vlen_n;
            if (!rd.read(reinterpret_cast<char*>(&klen_n), 4)) { g_active_puts--; goto done; }
            uint32_t klen = ntohl(klen_n);
            if (klen > (1 << 20)) { g_active_puts--; goto done; }

            std::string key_buf(klen, '\0');
            if (!rd.read(&key_buf[0], klen)) { g_active_puts--; goto done; }

            if (!rd.read(reinterpret_cast<char*>(&vlen_n), 4)) { g_active_puts--; goto done; }
            uint32_t vlen = ntohl(vlen_n);
            if (vlen > (16 << 20)) { g_active_puts--; goto done; }

            std::string val_buf(vlen, '\0');
            if (!rd.read(&val_buf[0], vlen)) { g_active_puts--; goto done; }

            rocksdb::Status s = db->Put(*write_opts,
                rocksdb::Slice(key_buf),
                rocksdb::Slice(val_buf));

            int32_t ret = s.ok() ? 0 : -1;
            if (!s.ok()) {
                fprintf(stderr, "Put FAILED: %s\n", s.ToString().c_str());
            }
            int32_t ret_n = htonl(ret);
            if (!wr.write(reinterpret_cast<const char*>(&ret_n), 4)) { g_active_puts--; goto done; }

            g_active_puts--;
            g_qps++;
            g_qps_total++;
            put_count++;

            if (put_count % 10000 == 0) {
                fprintf(stderr, "[fd=%d] PUT count: %d\n", fd, put_count);
            }

            if (!wr.flush()) goto done;
            break;
        }
        case OP_GET: {
            uint32_t klen_n;
            if (!rd.read(reinterpret_cast<char*>(&klen_n), 4)) goto done;
            uint32_t klen = ntohl(klen_n);
            if (klen > (1 << 20)) goto done;

            std::string key_buf(klen, '\0');
            if (!rd.read(&key_buf[0], klen)) goto done;

            std::string value;
            rocksdb::Status s = db->Get(*read_opts,
                rocksdb::Slice(key_buf), &value);

            int32_t ret_n;
            if (s.ok()) {
                ret_n = htonl(0);
                uint32_t vlen_n = htonl(static_cast<uint32_t>(value.size()));
                if (!wr.write(reinterpret_cast<const char*>(&ret_n), 4)) goto done;
                if (!wr.write(reinterpret_cast<const char*>(&vlen_n), 4)) goto done;
                if (!wr.write(value.data(), value.size())) goto done;
            } else {
                int32_t ret = s.IsNotFound() ? -1 : -2;
                ret_n = htonl(ret);
                if (!wr.write(reinterpret_cast<const char*>(&ret_n), 4)) goto done;
            }

            g_qps++;
            g_qps_total++;
            if (!wr.flush()) goto done;
            break;
        }
        default:
            goto done;
        }
    }
done:
    fprintf(stderr, "Client fd=%d disconnected, put_count=%d\n", fd, put_count);
    wr.flush();
    ::close(fd);
}

static void accept_loop(int listen_fd, rocksdb::DB* db,
                        rocksdb::WriteOptions* write_opts,
                        rocksdb::ReadOptions* read_opts) {
    while (!g_shutdown.load()) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        size_t addrlen = sizeof(addr);

        int client_fd = boost::fibers::io_uring::accept(
            listen_fd, reinterpret_cast<struct sockaddr*>(&addr), &addrlen);

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EINTR) continue;
            fprintf(stderr, "accept error: %m\n");
            break;
        }

        int flag = 1;
        ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        fprintf(stderr, "[srv] accepted fd=%d\n", client_fd);

        boost::fibers::fiber(
            std::allocator_arg,
            boost::fibers::fixedsize_stack(256 * 1024),
            [client_fd, db, write_opts, read_opts]() {
            handle_client(client_fd, db, write_opts, read_opts);
        }).detach();
    }
}

static void signal_handler(int) { g_shutdown.store(true); }

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    options.IncreaseParallelism(16);
    options.OptimizeLevelStyleCompaction();
    options.compression = rocksdb::CompressionType::kNoCompression;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 2;
    options.level0_file_num_compaction_trigger = 4;
    options.max_bytes_for_level_base = 256 * 1024 * 1024;
    options.max_background_compactions = 4;
    options.max_background_flushes = 2;

    std::string path;
    if (!FLAGS_db_dir.empty() && FLAGS_db_dir[0] == '/') {
        path = FLAGS_db_dir;
    } else {
        char cwd[4096];
        path = std::string(::getcwd(cwd, sizeof(cwd))) + "/" + FLAGS_db_dir;
    }
    if (FLAGS_clean_db) {
        int ret = system((std::string("rm -rf ") + path).c_str());
        (void)ret;
        fprintf(stdout, "Create new db at %s\n", path.c_str());
    } else {
        fprintf(stdout, "Open db at %s\n", path.c_str());
    }

    fprintf(stdout, "Opening RocksDB at %s ...\n", path.c_str());
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db);
    if (!s.ok()) {
        fprintf(stderr, "open db failed: %s\n", s.ToString().c_str());
        return -1;
    }
    fprintf(stdout, "RocksDB opened successfully\n");

    rocksdb::WriteOptions write_opts;
    write_opts.sync = false;
    write_opts.disableWAL = true;
    rocksdb::ReadOptions read_opts;
    read_opts.verify_checksums = false;
    read_opts.fill_cache = true;

    int listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) { perror("socket"); return -1; }
    int opt = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(FLAGS_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0
        || ::listen(listen_fd, 4096) < 0) {
        perror("bind/listen");
        return -1;
    }
    fprintf(stdout, "Listening on port %d\n", FLAGS_port);

    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    int num_threads = FLAGS_threads;
    std::vector<std::thread> workers;

    auto worker_fn = [&](int thread_id) {
        boost::fibers::use_scheduling_algorithm<
            boost::fibers::algo::io_uring_stealing>(num_threads);

        // Only thread 0 runs accept_loop
        if (thread_id == 0) {
            boost::fibers::fiber([&]() {
                accept_loop(listen_fd, db, &write_opts, &read_opts);
            }).detach();
        }

        if (thread_id == 0) {
            boost::fibers::fiber([&]() {
                uint64_t last_total = 0;
                while (!g_shutdown.load()) {
                    boost::this_fiber::sleep_for(
                        std::chrono::seconds(FLAGS_show_qps_interval));
                    uint64_t cur_total = g_qps_total.load();
                    uint64_t delta = cur_total - last_total;
                    last_total = cur_total;
                    uint64_t qps = delta / FLAGS_show_qps_interval;
                    fprintf(stdout, "QPS: %lu | total: %lu | active_puts=%d active_reads=%d\n",
                            qps, cur_total, g_active_puts.load(), g_active_reads.load());
                }
            }).detach();
        }

        boost::fibers::mutex m;
        boost::fibers::condition_variable cv;
        std::unique_lock<boost::fibers::mutex> lk(m);
        while (!g_shutdown.load()) {
            cv.wait_for(lk, std::chrono::milliseconds(500));
        }
    };

    for (int i = 1; i < num_threads; ++i) {
        workers.emplace_back(worker_fn, i);
    }
    worker_fn(0);

    for (auto&& t: workers) {
        t.join();
    }

    ::close(listen_fd);
    delete db;
    return 0;
}
