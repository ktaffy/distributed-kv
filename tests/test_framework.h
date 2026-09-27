#pragma once

#include <cstdio>
#include <exception>
#include <sstream>
#include <string>
#include <vector>

namespace kvtest {
    struct TestCase {
        const char *name;
        void (*fn)();
    };

    inline std::vector<TestCase> &registry() {
        static std::vector<TestCase> tests;
        return tests;
    }

    inline int &current_failures() {
        static int failures = 0;
        return failures;
    }

    struct Registrar {
        Registrar(const char *name, void (*fn)()) { registry().push_back({name, fn}); }
    };

    inline void report_failure(const char *file, int line, const std::string &msg) {
        std::printf("   %s:%d: %s\n", file, line, msg.c_str());
        ++current_failures();
    }

    template <typename A, typename B>
    void check_eq(const A &a, const B &b, const char *a_expr, const char *b_expr, const char *file, int line) {
        if (!(a == b)) {
            std::ostringstream oss;
            oss << "CHECK_EQ(" << a_expr << ", " << b_expr << ") failed: "
                << a << " != " << b;
            report_failure(file, line, oss.str());
        }
    }

    inline int run_all() {
        int failed = 0;
        for (const auto &t : registry()) {
            current_failures() = 0;
            std::printf("[ RUN] %s\n", t.name);
            std::fflush(stdout);
            try {
                t.fn();
            } catch (const std::exception &e) {
                report_failure(t.name, 0, std::string("uncaught exception: ") + e.what());
            } catch (...) {
                report_failure(t.name, 0, "uncaught non-std exception");
            }

            bool ok = current_failures() == 0;
            std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", t.name);
            std::fflush(stdout);
            if (!ok)
                ++failed;
        }
        std::printf("\n%zu tests, %d failed\n", registry().size(), failed);
        return failed == 0 ? 0 : 1;
    }
} // namespace kvtest

#define TEST(name)                                                   \
    static void test_##name();                                       \
    static kvtest::Registrar registrar_##name(#name, &test_##name);  \
    static void test_##name()

    #define CHECK(cond)                                                              \
    do                                                                           \
    {                                                                            \
        if (!(cond))                                                             \
            kvtest::report_failure(__FILE__, __LINE__, "CHECK(" #cond ") failed"); \
    } while (0)

#define CHECK_EQ(a, b) kvtest::check_eq((a), (b), #a, #b, __FILE__, __LINE__)
