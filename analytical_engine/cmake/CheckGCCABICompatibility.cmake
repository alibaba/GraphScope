macro(check_gcc_compatible)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -v
                        OUTPUT_VARIABLE GCC_VERSION_OUT
                        ERROR_VARIABLE GCC_VERSION_ERR)
        if(GCC_VERSION_OUT MATCHES ".*gcc4-compatible.*" OR GCC_VERSION_ERR MATCHES ".*gcc4-compatible.*")
            set(GCC_ABI_BACKWARDS_COMPATIBLE 1)
        else()
            set(GCC_ABI_BACKWARDS_COMPATIBLE 0)
        endif()
    else()
        set(GCC_ABI_BACKWARDS_COMPATIBLE 0)
    endif()
endmacro(check_gcc_compatible)
