FIND_PATH(LIBPMEM_INCLUDE_DIR NAME libpmem.h
    HINTS /scratch/pmem/pmdk/include/
    NO_DEFAULT_PATH NO_CMAKE_ENVIRONMENT_PATH NO_CMAKE_PATH NO_SYSTEM_ENVIRONMENT_PATH NO_CMAKE_SYSTEM_PATH 
)

FIND_LIBRARY(LIBPMEM_LIBRARY NAME pmem
    HINTS /scratch/pmem/pmdk/lib/ 
    )

	IF (LIBPMEM_INCLUDE_DIR)
	MESSAGE(STATUS "Found include")
	ENDIF ()
	IF (LIBPMEM_LIBRARY)
	MESSAGE(STATUS "Found lib")

	ENDIF()

IF (LIBPMEM_INCLUDE_DIR AND LIBPMEM_LIBRARY)
    SET(LIBPMEM_FOUND TRUE)
    MESSAGE(STATUS "Found libpmem library: inc=${LIBPMEM_INCLUDE_DIR}, lib=${LIBPMEM_LIBRARY}")
ELSE ()
    SET(LIBPMEM_FOUND FALSE)
    MESSAGE(STATUS "WARNING: libpmem library not found.")
ENDIF ()
