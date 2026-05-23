#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/io_uring_stealing.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gflags/gflags.h>

#include "protocol.h"

DEFINE_int32(port, 9527, "Server port");
DEFINE_string(host, "127.0.0.1", "Server ip");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    boost::fibers::use_scheduling_algorithm<
        boost::fibers::algo::io_uring_stealing>(1, false, true);

    fprintf(stderr, "Creating fiber...\n"); fflush(stderr);

    boost::fibers::fiber([]() {
        fprintf(stderr, "Fiber started\n"); fflush(stderr);

        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            fprintf(stderr, "socket failed\n");
            return;
        }
        int flag = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(FLAGS_port);
        inet_pton(AF_INET, FLAGS_host.c_str(), &addr.sin_addr);

        fprintf(stderr, "About to io_uring::connect fd=%d...\n", fd); fflush(stderr);
        int rc = boost::fibers::io_uring::connect(
            fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        fprintf(stderr, "io_uring::connect returned %d\n", rc); fflush(stderr);

        if (rc < 0) {
            fprintf(stderr, "Connect failed\n");
            ::close(fd);
            return;
        }

        std::string key = "42";
        uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
        uint8_t op = OP_GET;

        fprintf(stderr, "Sending GET request for key=%s...\n", key.c_str()); fflush(stderr);

        char sendbuf[64];
        size_t pos = 0;
        memcpy(sendbuf + pos, &op, 1); pos += 1;
        memcpy(sendbuf + pos, &klen, 4); pos += 4;
        memcpy(sendbuf + pos, key.data(), key.size()); pos += key.size();

        const char* p = sendbuf;
        size_t rem = pos;
        while (rem > 0) {
            int w = boost::fibers::io_uring::write(fd, p, rem);
            if (w <= 0) {
                fprintf(stderr, "Write failed\n");
                ::close(fd);
                return;
            }
            p += w;
            rem -= w;
        }

        int32_t ret_n;
        char rbuf[4];
        char* rp = rbuf;
        size_t rr = 4;
        while (rr > 0) {
            int r = boost::fibers::io_uring::read(fd, rp, rr);
            fprintf(stderr, "io_uring::read returned %d\n", r); fflush(stderr);
            if (r <= 0) {
                fprintf(stderr, "Read response failed\n");
                ::close(fd);
                return;
            }
            rp += r;
            rr -= r;
        }

        int32_t ret = ntohl(*reinterpret_cast<int32_t*>(rbuf));
        fprintf(stderr, "GET response: ret=%d\n", ret); fflush(stderr);

        if (ret == 0) {
            char vbuf[4];
            if (4 != boost::fibers::io_uring::read(fd, vbuf, 4)) {
            }
            uint32_t vlen = ntohl(*reinterpret_cast<uint32_t*>(vbuf));
            fprintf(stderr, "Value length: %u\n", vlen);

            std::string val(vlen, '\0');
            char* vp = &val[0];
            size_t vr = vlen;
            while (vr > 0) {
                int r = boost::fibers::io_uring::read(fd, vp, vr);
                if (r <= 0) break;
                vp += r;
                vr -= r;
            }
            fprintf(stderr, "Got value of %zu bytes\n", val.size());
        }

        ::close(fd);
        fprintf(stderr, "Fiber done!\n"); fflush(stderr);
    }).detach();

    fprintf(stderr, "Waiting...\n"); fflush(stderr);

    boost::fibers::mutex mtx;
    boost::fibers::condition_variable cv;
    std::unique_lock<boost::fibers::mutex> lk(mtx);
    cv.wait(lk);

    return 0;
}
