#include <stdio.h> // remove()
#include "catch.hpp"
#include "csv_parser.hpp"

using namespace csv;
using std::vector;
using std::string;

TEST_CASE("Allocator Test", "[test_allocator]") {
    using MyVec = vector<string, FreeAlloc<string>>;
    // FreeAlloc<string> alloc;

    MyVec test1, test2;
    for (int i = 1; i <= 10; i++) {
        std::cout << "Adding " << i * 100 << std::endl;
        test1.push_back(std::to_string(i * 100));
    }

    REQUIRE(test1.size() == 10);

    try {
        for (int i = 1; i <= 10000; i++) {
            test2.push_back(std::to_string(i));
        }

        REQUIRE(test2.size() == 10000);
    }
    catch (std::runtime_error& err) {
        std::cout << err.what() << std::endl;
    }
}