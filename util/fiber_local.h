#include <boost/thread/tss.hpp>
#include <utility>

template<typename T>
struct FiberLocal {
    boost::thread_specific_ptr<T> value;
    T* init_value;

    template<typename... Args>
    FiberLocal(Args&&... args) {
      init_value = new T(std::forward<Args>(args)...);
    }

    FiberLocal() = default;

    T* get_or_init() {
      if (value == nullptr) {
        value.reset(init_value);
      }
    }

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