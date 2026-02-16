cmake_minimum_required(VERSION 3.13)

# Pull in SDK (must be before project)
include(pico_sdk_import.cmake)

project(pico_inverter_mgmt C CXX ASM)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# Initialize the SDK
pico_sdk_init()

# =============================================================================
# EBox Controller (original)
# =============================================================================

add_executable(ebox_pico
    ebox_pico.c
)

target_link_libraries(ebox_pico
    pico_stdlib
    pico_cyw43_arch_lwip_threadsafe_background
    hardware_uart
    hardware_gpio
    pico_lwip_mqtt
)

pico_enable_stdio_usb(ebox_pico 1)
pico_enable_stdio_uart(ebox_pico 0)
pico_add_extra_outputs(ebox_pico)

target_compile_options(ebox_pico PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
)

# =============================================================================
# Sofar Inverter Reader (RS232/Modbus RTU -> MQTT)
# =============================================================================

add_executable(read_inverter_pico
    read_inverter_pico.c
)

target_link_libraries(read_inverter_pico
    pico_stdlib
    pico_cyw43_arch_lwip_threadsafe_background
    hardware_uart
    hardware_gpio
    pico_lwip_mqtt
)

pico_enable_stdio_usb(read_inverter_pico 1)
pico_enable_stdio_uart(read_inverter_pico 0)
pico_add_extra_outputs(read_inverter_pico)

target_compile_options(read_inverter_pico PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
)

# =============================================================================
# Soyo Inverter Controller (MQTT -> RS485)
# =============================================================================

add_executable(soyo1min_pico
    soyo1min_pico.c
)

target_link_libraries(soyo1min_pico
    pico_stdlib
    pico_cyw43_arch_lwip_threadsafe_background
    hardware_uart
    hardware_gpio
    hardware_rtc
    pico_lwip_mqtt
)

pico_enable_stdio_usb(soyo1min_pico 1)
pico_enable_stdio_uart(soyo1min_pico 0)
pico_add_extra_outputs(soyo1min_pico)

target_compile_options(soyo1min_pico PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
)

# =============================================================================
# Fox2DB Power Management Controller (MQTT -> Relay Control)
# =============================================================================

add_executable(fox2db_pico
    fox2db_pico.c
)

target_link_libraries(fox2db_pico
    pico_stdlib
    pico_cyw43_arch_lwip_threadsafe_background
    hardware_gpio
    pico_lwip_mqtt
)

pico_enable_stdio_usb(fox2db_pico 1)
pico_enable_stdio_uart(fox2db_pico 0)
pico_add_extra_outputs(fox2db_pico)

target_compile_options(fox2db_pico PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
)
