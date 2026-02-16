cmake_minimum_required(VERSION 3.13)

# Pull in SDK (must be before project)
include(pico_sdk_import.cmake)

project(ebox_pico C CXX ASM)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# Initialize the SDK
pico_sdk_init()

# Add executable
add_executable(ebox_pico
    ebox_pico.c
)

# Pull in common dependencies
target_link_libraries(ebox_pico
    pico_stdlib
    pico_cyw43_arch_lwip_threadsafe_background
    hardware_uart
    hardware_gpio
    pico_lwip_mqtt
)

# Enable USB output, disable UART output on GPIO 0/1
pico_enable_stdio_usb(ebox_pico 1)
pico_enable_stdio_uart(ebox_pico 0)

# Create map/bin/hex/uf2 files
pico_add_extra_outputs(ebox_pico)

# Add compile options
target_compile_options(ebox_pico PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
)
