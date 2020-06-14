set(CMAKE_C_FLAGS_INIT "")
set(CMAKE_C_FLAGS_DEBUG_INIT "-g -Wall -Wextra -Wno-missing-braces -Wno-unused-value -Wno-unused-parameter -O0 -pthread")
set(CMAKE_C_FLAGS_RELEASE_INIT " -Wall -Wextra -Wno-missing-braces -Wno-unused-value -Wno-unused-parameter -O3 -pthread -ftree-vectorize -msse2")
