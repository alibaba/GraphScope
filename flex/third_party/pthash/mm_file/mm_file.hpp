#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <type_traits>
#include <fcntl.h>
#include <unistd.h>  // close(fd)
#include <string>

namespace mm {

namespace advice {
static const int normal = POSIX_MADV_NORMAL;
static const int random = POSIX_MADV_RANDOM;
static const int sequential = POSIX_MADV_SEQUENTIAL;
}  // namespace advice

template <typename T>
struct file {
    file() {
        init();
    }

    ~file() {
        close();
    }

    file(file const&) = delete;             // non construction-copyable
    file& operator=(file const&) = delete;  // non copyable

    bool is_open() const {
        return m_fd != -1;
    }

    void close() {
        if (is_open()) {
            if (munmap((char*)m_data, m_size) == -1) {
                throw std::runtime_error("munmap failed when closing file");
            }
            ::close(m_fd);
            init();
        }
    }

    size_t bytes() const {
        return m_size;
    }

    size_t size() const {
        return m_size / sizeof(T);
    }

    T* data() const {
        return m_data;
    }

    struct iterator {
        iterator(T* addr, size_t offset = 0) : m_ptr(addr + offset) {}

        T operator*() {
            return *m_ptr;
        }

        void operator++() {
            ++m_ptr;
        }

        bool operator==(iterator const& rhs) const {
            return m_ptr == rhs.m_ptr;
        }

        bool operator!=(iterator const& rhs) const {
            return !((*this) == rhs);
        }

    private:
        T* m_ptr;
    };

    iterator begin() const {
        return iterator(m_data);
    }

    iterator end() const {
        return iterator(m_data, size());
    }

protected:
    int m_fd;
    size_t m_size;
    T* m_data;

    void init() {
        m_fd = -1;
        m_size = 0;
        m_data = nullptr;
    }

    void check_fd() {
        if (m_fd == -1) throw std::runtime_error("cannot open file");
    }
};

template <typename Pointer>
Pointer mmap(int fd, size_t size, int prot) {
    static const size_t offset = 0;
    Pointer p =
        static_cast<Pointer>(::mmap(NULL, size, prot, MAP_SHARED, fd, offset));
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
    return p;
}

template <typename T>
struct file_source : public file<T const> {
    typedef file<T const> base;

    file_source() {}

    file_source(std::string const& path, int adv = advice::normal) {
        open(path, adv);
    }

    void open(std::string const& path, int adv = advice::normal) {
        base::m_fd = ::open(path.c_str(), O_RDONLY);
        base::check_fd();
        struct stat fs;
        if (fstat(base::m_fd, &fs) == -1) {
            throw std::runtime_error("cannot stat file");
        }
        base::m_size = fs.st_size;
        base::m_data = mmap<T const*>(base::m_fd, base::m_size, PROT_READ);
        if (posix_madvise((void*)base::m_data, base::m_size, adv)) {
            throw std::runtime_error("madvise failed");
        }
    }
};

template <typename T>
struct file_sink : public file<T> {
    typedef file<T> base;

    file_sink() {}

    file_sink(std::string const& path) {
        open(path);
    }

    file_sink(std::string const& path, size_t n) {
        open(path, n);
    }

    void open(std::string const& path) {
        static const mode_t mode = 0600;  // read/write
        base::m_fd = ::open(path.c_str(), O_RDWR, mode);
        base::check_fd();
        struct stat fs;
        if (fstat(base::m_fd, &fs) == -1) {
            throw std::runtime_error("cannot stat file");
        }
        base::m_size = fs.st_size;
        base::m_data =
            mmap<T*>(base::m_fd, base::m_size, PROT_READ | PROT_WRITE);
    }

    void open(std::string const& path, size_t n) {
        static const mode_t mode = 0600;  // read/write
        base::m_fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, mode);
        base::check_fd();
        base::m_size = n * sizeof(T);
        ftruncate(base::m_fd,
                  base::m_size);  // truncate the file at the new size
        base::m_data =
            mmap<T*>(base::m_fd, base::m_size, PROT_READ | PROT_WRITE);
    }
};

}  // namespace mm