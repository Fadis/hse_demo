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
extern "C" {
#include <mpool/mpool.h>
}
#include "common.h"

int main( int argc, char* argv[] ) {
  boost::program_options::options_description options("options");
  bool delete_block = false;
  bool abort_transaction = false;
  options.add_options()
    ("help,h",    "display this message")
    ("pool,p", boost::program_options::value<std::string>(),  "pool name")
    ("message,m", boost::program_options::value< std::string >()->default_value( "Hello, World!" ), "message" )
    ("object,o", boost::program_options::value<uint64_t>(),  "object id")
    ("delete,d", boost::program_options::bool_switch( &delete_block ),  "delete")
    ("abort,a", boost::program_options::bool_switch( &abort_transaction ),  "abort");
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

  uint64_t block_id = 0u;
  mblock_props props;
  size_t length = 0;
  if( !params.count( "object" ) ) {
    memset( reinterpret_cast< void* >( &props ), 0, sizeof( props ) );
    SAFE_CALL( mpool_mblock_alloc( pool.get(), MP_MED_CAPACITY, false, &block_id, &props ) )
    std::cout << "object id: " << props.mpr_objid << std::endl;
    std::string m = params[ "message" ].as< std::string >();
    size_t buf_size = ( m.size() / PAGE_SIZE + ( m.size() % PAGE_SIZE ? 1 : 0 ) ) * PAGE_SIZE;
    std::unique_ptr< char, free_deleter > buf( reinterpret_cast< char* >( aligned_alloc( PAGE_SIZE, buf_size ) ) );
    if( !buf ) throw std::bad_alloc();
    memset( buf.get(), 0, buf_size );
    std::copy( m.begin(), m.end(), buf.get() );
    iovec iov;
    iov.iov_base = buf.get();
    iov.iov_len = buf_size;
    length = buf_size;
    SAFE_CALL( mpool_mblock_write( pool.get(), block_id, &iov, 1 ) )
    if( abort_transaction )
      SAFE_CALL( mpool_mblock_abort( pool.get(), block_id ) )
    else
      SAFE_CALL( mpool_mblock_commit( pool.get(), block_id ) )
  }
  else {
    uint64_t object_id = params[ "object" ].as< uint64_t >();
    SAFE_CALL( mpool_mblock_find_get( pool.get(), object_id, &block_id, &props ) )
    length = props.mpr_write_len;
    std::cout << "object id: " << object_id << std::endl;
  }
  {
    size_t buf_size = length;
    std::unique_ptr< char, free_deleter > buf( reinterpret_cast< char* >( aligned_alloc( PAGE_SIZE, buf_size ) ) );
    if( !buf ) throw std::bad_alloc();
    memset( buf.get(), 0, buf_size );
    iovec iov;
    iov.iov_base = buf.get();
    iov.iov_len = buf_size;
    SAFE_CALL( mpool_mblock_read( pool.get(), block_id, &iov, 1, 0 ) )
    std::cout << "length: " << length << std::endl;
    std::cout << "data: " << buf.get() << std::endl;
  }
  if( delete_block )
    SAFE_CALL( mpool_mblock_delete( pool.get(), block_id ) )
}
