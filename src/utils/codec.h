#pragma once

#include <cstdint>
#include <string>

namespace raft
{
    class Writer
    {
    public:
        void u8(uint8_t v) { put(v); }
        void u32(uint32_t v) { put(v); }
        void u64(uint64_t v) { put(v); }

        void str(const std::string &s)
        {
            u32(static_cast<uint32_t>(s.size()));
            buf_.append(s);
        }

        std::string take() { return std::move(buf_); }

    private:
        template <typename T>
        void put(T v)
        {
            for (size_t i = 0; i < sizeof(T); ++i)
                buf_.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
        }

        std::string buf_;
    };

    class Reader
    {
    public:
        explicit Reader(const std::string &data) : data_(data) {}

        bool u8(uint8_t &v) { return get(v); }
        bool u32(uint32_t &v) { return get(v); }
        bool u64(uint64_t &v) { return get(v); }

        bool str(std::string &s)
        {
            uint32_t n;
            if (!u32(n) || data_.size() - pos_ < n)
                return false;
            s.assign(data_, pos_, n);
            pos_ += n;
            return true;
        }

        bool done() const { return pos_ == data_.size(); }

    private:
        template <typename T>
        bool get(T &v)
        {
            if (data_.size() - pos_ < sizeof(T))
                return false;
            v = 0;
            for (size_t i = 0; i < sizeof(T); ++i)
                v |= static_cast<T>(static_cast<T>(static_cast<uint8_t>(data_[pos_ + i])) << (8 * i));
            pos_ += sizeof(T);
            return true;
        }

        const std::string &data_;
        size_t pos_ = 0;
    };
}