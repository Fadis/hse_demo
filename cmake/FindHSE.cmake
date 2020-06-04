find_path(HSE_INCLUDE_DIR hse/hse.h)
find_library( HSE_LIBRARY hse_kvdb )
find_package_handle_standard_args(
  HSE
  REQUIRED_VARS
    HSE_INCLUDE_DIR
    HSE_LIBRARY
)
if( HSE_FOUND AND NOT ( TARGET hse::hse ) )
  add_library( hse::hse UNKNOWN IMPORTED )
  set_target_properties(
    hse::hse
    PROPERTIES
      IMPORTED_LOCATION ${HSE_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${HSE_INCLUDE_DIR}
  )
endif()

