#include <boost/fiber/fss.hpp>

template<typename T>
struct FiberLocal {
    mutable boost::fibers::fiber_specific_ptr<T> value;
    T init_value;

    explicit FiberLocal(T init)
        : init_value(init)
    {}

    FiberLocal() : init_value{} {}  

    const T& operator*() const {
        return *get();
    }

    void reset(T* new_value) {
        value.reset(new_value);
    }

    T* release() {
        return value.release();
    }

    T& operator*() {
        return *get();
    }

    T* operator->() const {
        return get();
    }

private:
    T* get() const {
        if (value.get() == nullptr) {
            value.reset(new T(init_value));
        }
        return value.get();
    }
};