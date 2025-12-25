# ipc0cp

Zero-copy (0CP) inter-process communication (IPC): A library for exchanging data between Python and C++ using shared memory.

## Overview

`ipc0cp` is a library that enables efficient data exchange between Python and C++ processes using shared memory for zero-copy inter-process communication. This approach minimizes overhead and maximizes performance when transferring data between processes written in different languages.

## Features

- **Zero-copy data transfer**: Use shared memory to avoid expensive data copying
- **Cross-language support**: Seamless integration between Python and C++
- **Efficient IPC**: Optimized for high-performance inter-process communication
- **Simple API**: Easy-to-use interfaces for both Python and C++

## Project Structure

```
ipc0cp/
├── ipc0cp/              # Python package
│   └── __init__.py      # Python module initialization
├── csrc/                # C++ source code
│   └── ipc0cp/          # C++ library
│       ├── ipc.hpp      # C++ header files
│       └── ipc.cpp      # C++ implementation
├── CMakeLists.txt       # CMake build configuration for C++
├── pyproject.toml       # Python project configuration
├── README.md            # This file
└── LICENSE              # Apache 2.0 License
```

## Building

### C++ Library

To build the C++ library, you'll need CMake 3.18 or higher and a C++17 compatible compiler.

```bash
# Create a build directory
mkdir build && cd build

# Configure the project
cmake ..

# Build the library
cmake --build .

# Install (optional)
cmake --install .
```

### Python Package

To install the Python package:

```bash
# Install in development mode
pip install -e .

# Or install normally
pip install .
```

## Usage

### Python

```python
import ipc0cp

# Your code here
```

### C++

```cpp
#include <ipc0cp/ipc.hpp>

int main() {
    // Your code here
    return 0;
}
```

## Requirements

### C++
- CMake 3.18 or higher
- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2017+)

### Python
- Python 3.8 or higher
- setuptools
- wheel

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

- Thamme Gowda

## Acknowledgments

This project aims to provide a simple yet efficient solution for inter-process communication between Python and C++ applications.
