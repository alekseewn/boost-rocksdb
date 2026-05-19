#pragma once

#include <arpa/inet.h>
#include <unistd.h>

#include <boost/fiber/io_uring.hpp>

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>

using ReadFunc = std::function<bool(int, char*, size_t)>;
using WriteFunc = std::function<bool(int, const char*, size_t)>;

static inline bool writen(int fd, const char* buf, size_t n) {
    while (n > 0) {
        int written = boost::fibers::io_uring::write(fd, buf, n);
        if (written <= 0) return false;
        buf += written;
        n -= written;
    }
    return true;
}

static inline bool readn(int fd, char* buf, size_t n) {
    while (n > 0) {
        int r = boost::fibers::io_uring::read(fd, buf, n);
        if (r <= 0) return false;
        buf += r;
        n -= r;
    }
    return true;
}

enum OpCode : uint8_t {
    OP_PUT = 0x01,
    OP_GET = 0x02,
};

struct KvPutRequest {
    std::string key;
    std::string value;

    bool write_to(int fd, WriteFunc wf = writen) const {
        uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
        uint32_t vlen = htonl(static_cast<uint32_t>(value.size()));
        uint8_t op = OP_PUT;
        return wf(fd, reinterpret_cast<const char*>(&op), 1) &&
               wf(fd, reinterpret_cast<const char*>(&klen), 4) &&
               wf(fd, key.data(), key.size()) &&
               wf(fd, reinterpret_cast<const char*>(&vlen), 4) &&
               wf(fd, value.data(), value.size());
    }

    bool read_from(int fd, ReadFunc rf = readn) {
        uint32_t klen, vlen;
        if (!rf(fd, reinterpret_cast<char*>(&klen), 4)) return false;
        klen = ntohl(klen);
        key.resize(klen);
        if (!rf(fd, &key[0], klen)) return false;
        if (!rf(fd, reinterpret_cast<char*>(&vlen), 4)) return false;
        vlen = ntohl(vlen);
        value.resize(vlen);
        return rf(fd, &value[0], vlen);
    }
};

struct KvPutResponse {
    int32_t ret;

    bool write_to(int fd, WriteFunc wf = writen) const {
        int32_t nret = htonl(ret);
        return wf(fd, reinterpret_cast<const char*>(&nret), 4);
    }

    bool read_from(int fd, ReadFunc rf = readn) {
        if (!rf(fd, reinterpret_cast<char*>(&ret), 4)) return false;
        ret = ntohl(ret);
        return true;
    }
};

struct KvGetRequest {
    std::string key;

    bool write_to(int fd, WriteFunc wf = writen) const {
        uint32_t klen = htonl(static_cast<uint32_t>(key.size()));
        uint8_t op = OP_GET;
        return wf(fd, reinterpret_cast<const char*>(&op), 1) &&
               wf(fd, reinterpret_cast<const char*>(&klen), 4) &&
               wf(fd, key.data(), key.size());
    }

    bool read_from(int fd, ReadFunc rf = readn) {
        uint32_t klen;
        if (!rf(fd, reinterpret_cast<char*>(&klen), 4)) return false;
        klen = ntohl(klen);
        key.resize(klen);
        return rf(fd, &key[0], klen);
    }
};

struct KvGetResponse {
    int32_t ret;
    std::string value;

    bool write_to(int fd, WriteFunc wf = writen) const {
        int32_t nret = htonl(ret);
        if (!wf(fd, reinterpret_cast<const char*>(&nret), 4)) return false;
        if (ret == 0) {
            uint32_t vlen = htonl(static_cast<uint32_t>(value.size()));
            if (!wf(fd, reinterpret_cast<const char*>(&vlen), 4)) return false;
            if (!wf(fd, value.data(), value.size())) return false;
        }
        return true;
    }

    bool read_from(int fd, ReadFunc rf = readn) {
        if (!rf(fd, reinterpret_cast<char*>(&ret), 4)) return false;
        ret = ntohl(ret);
        if (ret == 0) {
            uint32_t vlen;
            if (!rf(fd, reinterpret_cast<char*>(&vlen), 4)) return false;
            vlen = ntohl(vlen);
            value.resize(vlen);
            return rf(fd, &value[0], vlen);
        }
        return true;
    }
};
