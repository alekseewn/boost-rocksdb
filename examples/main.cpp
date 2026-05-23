#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/io_uring_stealing.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

void print(std::string str) {
    std::cout << str << std::endl;
}

struct Env {
    boost::fibers::mutex mtx;
    boost::fibers::condition_variable_any cv;
    std::thread worker;

    Env() {
        print("Env ctor");
        worker = std::thread([this]() {
            boost::fibers::use_scheduling_algorithm<boost::fibers::algo::io_uring_stealing>(2);

            std::cout << "[Thread " << std::this_thread::get_id() << "] Scheduler initialized." << std::endl;

            mtx.lock();
            cv.wait(mtx);
            mtx.unlock();
            
            std::cout << "[Thread] Fiber joined. Thread exiting." << std::endl;
        });
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::io_uring_stealing>(2);
    }

    ~Env() {
        print("Env dtor");
        cv.notify_all();
        worker.join();
    }
};

// Env env;

void fiber_func(int id) {
    std::cout << "fiber id = " << id << std::endl;
}

int main() {
    std::vector<boost::fibers::fiber> fibers;

    boost::fibers::mutex mtx;
    boost::fibers::condition_variable_any cv;
    std::thread worker = std::thread([&]() {
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::io_uring_stealing>(2);

        std::cout << "[Thread " << std::this_thread::get_id() << "] Scheduler initialized." << std::endl;

        mtx.lock();
        cv.wait(mtx);
        mtx.unlock();
        
        std::cout << "[Thread] Fiber joined. Thread exiting." << std::endl;
    });
    
    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::io_uring_stealing>(2);

    for (int i = 1; i <= 3; ++i) {
        fibers.emplace_back(fiber_func, i);
    }

    for (auto& f : fibers) {
        f.join();
    }

    cv.notify_all();
    worker.join();

    std::cout << "All fibers finished.\n";
    return 0;
}