#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/io_uring_stealing.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <atomic>

static std::atomic<uint64_t> g_count{0};
static std::atomic<bool> g_stop{false};

void writer_fiber(rocksdb::DB* db, int id) {
    rocksdb::WriteOptions wo;
    wo.sync = false;
    wo.disableWAL = true;
    
    char key[64], val[256];
    uint64_t n = 0;
    while (!g_stop.load()) {
        snprintf(key, sizeof(key), "key_%d_%lu", id, (unsigned long)n);
        memset(val, 'x', sizeof(val));
        
        auto s = db->Put(wo, key, rocksdb::Slice(val, sizeof(val)));
        if (!s.ok()) {
            fprintf(stderr, "[F%d] Put FAILED at %lu: %s\n", id, (unsigned long)n, s.ToString().c_str());
            break;
        }
        n++;
        g_count.fetch_add(1);
        
        if (n % 10000 == 0) {
            fprintf(stderr, "[F%d] %lu puts done\n", id, (unsigned long)n);
        }
    }
    fprintf(stderr, "[F%d] stopped after %lu puts\n", id, (unsigned long)n);
}

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    int num_threads = argc > 1 ? atoi(argv[1]) : 4;
    int num_fibers = argc > 2 ? atoi(argv[2]) : 4;
    
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    options.IncreaseParallelism(num_threads > 16 ? 16 : num_threads);
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
    
    system("rm -rf /tmp/stress-db");
    auto s = rocksdb::DB::Open(options, "/tmp/stress-db", &db);
    if (!s.ok()) {
        fprintf(stderr, "DB open failed: %s\n", s.ToString().c_str());
        return 1;
    }
    fprintf(stderr, "DB opened. threads=%d fibers=%d\n", num_threads, num_fibers);
    
    std::vector<std::thread> threads;
    
    auto worker_fn = [&](int tid) {
        boost::fibers::use_scheduling_algorithm<
            boost::fibers::algo::io_uring_stealing>(num_threads);
        
        for (int i = 0; i < num_fibers / num_threads + 1; ++i) {
            int fid = tid * 100 + i;
            if (fid >= num_fibers) break;
            boost::fibers::fiber([db, fid]() {
                writer_fiber(db, fid);
            }).detach();
        }
        
        while (!g_stop.load()) {
            boost::this_fiber::sleep_for(std::chrono::seconds(1));
            uint64_t c = g_count.exchange(0);
            fprintf(stdout, "QPS: %lu (thread %d)\n", (unsigned long)c, tid);
            if (c == 0 && !g_stop.load()) {
                fprintf(stderr, "STALLED on thread %d!\n", tid);
            }
        }
    };
    
    for (int i = 1; i < num_threads; ++i) {
        threads.emplace_back(worker_fn, i);
    }
    worker_fn(0);
    
    for (auto& t : threads) t.join();
    
    delete db;
    return 0;
}
