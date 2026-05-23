FROM debian:stable-slim
COPY build/db_bench /root/
COPY build/benchmark.sh /root/
COPY build/run-bench.sh /root/
COPY build/librocksdb.so.6 /usr/lib/x86_64-linux-gnu/  
RUN apt update
RUN apt install -y libgflags-dev libsnappy-dev bc
COPY boost-libs/libboost_container.so.1.89.0 /usr/local/lib/libboost_container.so.1.89.0
COPY boost-libs/libboost_date_time.so.1.89.0 /usr/local/lib/libboost_date_time.so.1.89.0
COPY boost-libs/libboost_chrono.so.1.89.0 /usr/local/lib/libboost_chrono.so.1.89.0
COPY boost-libs/libboost_atomic.so.1.89.0 /usr/local/lib/libboost_atomic.so.1.89.0
COPY boost-libs/libboost_filesystem.so.1.89.0 /usr/local/lib/libboost_filesystem.so.1.89.0
COPY boost-libs/libboost_thread.so.1.89.0 /usr/local/lib/libboost_thread.so.1.89.0
COPY boost-libs/libboost_fiber.so.1.89.0 /usr/local/lib/libboost_fiber.so.1.89.0
COPY boost-libs/libboost_context.so.1.89.0 /usr/local/lib/libboost_context.so.1.89.0
