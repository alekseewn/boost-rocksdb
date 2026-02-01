#include <boost/thread/tss.hpp>
#include <utility>

template<typename T>
struct FiberLocal {
    boost::thread_specific_ptr<T> value;

    template<typename... Args>
    FiberLocal(Args&&... args) {
      value.reset(new T(std::forward<Args>(args)...));
    }

    FiberLocal() = default;

    T& operator*() {
        return *value;
    }

    T* operator->() {
        return value.get();
    }

    const T* operator->() const {
        return value.get();
    }

    void reset(T* new_value) {
      value.reset(new_value);
    }

    T* release() {
      return value.release();
    }

    T* get() const {
      return value.get();
    }
};