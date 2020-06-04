/*
Copyright (c) 2020 Naomasa Matsubayashi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
#include <iostream>
#include <string>
#include <exception>
#include <boost/program_options.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
extern "C" {
#include <mpool/mpool.h>
}

#define PAGE_SIZE 4096

std::string get_mpool_error( uint64_t err, const char *file, int line ) {
  std::array< char, 1024 > buf;
  mpool_strinfo( err, buf.data(), buf.size() );
  std::string m( file );
  m += "(";
  m += std::to_string( line );
  m += "): ";
  m += buf.data();
  return m;
}

struct mpool_error : public std::exception {
  mpool_error() {}
};

#define SAFE_CALL( e ) \
  { \
    auto result = e ; \
    if( result ) { \
      std::cerr << get_mpool_error( result, __FILE__, __LINE__ ) << std::endl; \
      throw mpool_error() ; \
    } \
  }

struct free_deleter {
  template< typename T >
  void operator()( T *p ) { if( p ) free( p ); }
};

int main( int argc, char* argv[] ) {
  boost::program_options::options_description options("options");
  options.add_options()
    ( "help,h",    "display this message" )
    ( "pool,p", boost::program_options::value<std::string>(),  "pool name" )
    ( "object,o", boost::program_options::value<std::vector<uint64_t>>()->multitoken(),  "object id" );
  boost::program_options::variables_map params;
  boost::program_options::store( boost::program_options::parse_command_line( argc, argv, options ), params );
  boost::program_options::notify( params );
  if( params.count("help") ) {
    std::cout << options << std::endl;
    return 0;
  }
  if( !params.count( "pool" ) ) {
    std::cerr << "pool is required." << std::endl;
    std::cerr << options << std::endl;
    return 1;
  }
  mpool *raw_pool = nullptr;
  SAFE_CALL( mpool_open( params[ "pool" ].as< std::string >().c_str(), O_RDWR, &raw_pool, nullptr ) );
  std::shared_ptr< mpool > pool( raw_pool, []( mpool *p ) { if( p ) mpool_close( p ); } );
  std::vector< uint64_t > object_ids = params[ "object" ].as< std::vector< uint64_t > >();
  std::vector< mblock_props > props;
  props.reserve( object_ids.size() );
  std::transform( object_ids.begin(), object_ids.end(), std::back_inserter( props ), [&]( uint64_t object_id ){
    mblock_props props;
    memset( reinterpret_cast< void* >( &props ), 0, sizeof( props ) );
    uint64_t block_id = 0;
    SAFE_CALL( mpool_mblock_find_get( pool.get(), object_id, &block_id, &props ) )
    return props;
  } );
  {
    mpool_mcache_map *raw_map;
    SAFE_CALL( mpool_mcache_mmap( pool.get(), object_ids.size(), object_ids.data(), MPC_VMA_WARM, &raw_map ) );
    std::shared_ptr< mpool_mcache_map > map( raw_map, [pool]( mpool_mcache_map *p ) {
      if( p ) mpool_mcache_munmap( p );
    } );
    for( uint64_t cache_id = 0; cache_id != object_ids.size(); ++cache_id ) {
      SAFE_CALL( mpool_mcache_madvise( map.get(), cache_id, 0, props[ cache_id ].mpr_write_len, MADV_WILLNEED ) )
      size_t offset = 0u;
      void *page = nullptr;
      SAFE_CALL( mpool_mcache_getpages( map.get(), 1, cache_id, &offset, &page ) );
      char *data = reinterpret_cast< char* >( page );
      std::cout << "length: " << props[ cache_id ].mpr_write_len << std::endl;
      std::cout << "data: " << data << std::endl;
    }
  }
}
