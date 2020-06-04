find_path( MPOOL_INCLUDE_DIR mpool/mpool.h )
find_library( MPOOL_LIBRARY mpool )
find_package_handle_standard_args(
  MPOOL
  REQUIRED_VARS
    MPOOL_INCLUDE_DIR
    MPOOL_LIBRARY
)
if( MPOOL_FOUND AND NOT ( TARGET mpool::mpool ) )
  add_library( mpool::mpool UNKNOWN IMPORTED )
  set_target_properties(
    mpool::mpool
    PROPERTIES
      IMPORTED_LOCATION ${MPOOL_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${MPOOL_INCLUDE_DIR}
  )
endif()

