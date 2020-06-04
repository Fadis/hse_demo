add_executable( mlog_demo mlog_demo.cpp )
target_link_libraries( mlog_demo
  mpool::mpool
  Boost::program_options
  Boost::system
  Threads::Threads
)
add_executable( mblock_demo mblock_demo.cpp )
target_link_libraries( mblock_demo
  mpool::mpool
  Boost::program_options
  Boost::system
  Threads::Threads
)
add_executable( mcache_demo mcache_demo.cpp )
target_link_libraries( mcache_demo
  mpool::mpool
  Boost::program_options
  Boost::system
  Threads::Threads
)
add_executable( mdc_demo mdc_demo.cpp )
target_link_libraries( mdc_demo
  mpool::mpool
  Boost::program_options
  Boost::system
  Threads::Threads
)
add_executable( hse_demo hse_demo.cpp )
target_link_libraries( hse_demo
  hse::hse
  mpool::mpool
  Boost::program_options
  Boost::system
  Threads::Threads
)
