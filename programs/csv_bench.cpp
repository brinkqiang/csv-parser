// Calculate benchmarks for CSV parser

#include "csv_parser.hpp"
#include <chrono>
#include <iostream>

int main(int argc, char** argv) {
    using namespace csv;

    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " [file]" << std::endl;
        exit(1);
    }

    // Benchmark 1: File IO + Parsing
    std::string filename = argv[1];
    auto start1 = std::chrono::system_clock::now();
    auto info = get_file_info(filename);
    auto end1 = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end1 - start1;

    std::cout << "Parsing took (including disk IO): " << diff.count() << std::endl;

    // Benchmark 2: Parsing Only
    std::ifstream csv(filename, std::ios::binary);
    std::stringstream buffer;
    buffer << csv.rdbuf();

    std::string csv_string = buffer.str();

    auto start2 = std::chrono::system_clock::now();
    parse_to_string(csv_string);
    auto end2 = std::chrono::system_clock::now();
    diff = end2 - start2;

    std::cout << "Parsing took: " << diff.count() << std::endl;

    return 0;
}