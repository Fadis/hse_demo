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
  bool delete_log = false;
  uint64_t erase_log = std::numeric_limits< uint64_t >::max();
  bool abort_transaction = false;
  options.add_options()
    ("help,h",    "display this message")
    ("pool,p", boost::program_options::value<std::string>(),  "pool name")
    ("message,m", boost::program_options::value< std::vector< std::string > >()->multitoken(), "message" )
    ("object,o", boost::program_options::value<uint64_t>(),  "object id")
    ("delete,d", boost::program_options::bool_switch( &delete_log ),  "delete")
    ("erase,e", boost::program_options::value< uint64_t >( &erase_log ),  "erase")
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
  SAFE_CALL( mpool_open( params[ "pool" ].as< std::string >().c_str(), O_RDWR|O_EXCL, &raw_pool, nullptr ) );
  std::shared_ptr< mpool > pool( raw_pool, []( mpool *p ) { if( p ) mpool_close( p ); } );
  mlog_capacity cap;
  memset( reinterpret_cast< void* >( &cap ), 0, sizeof( cap ) );
  std::shared_ptr< mpool_mlog > log;
  if( !params.count( "object" ) ) {
    cap.lcp_captgt = 4 * 1024 * 1024;
    mlog_props props;
    memset( reinterpret_cast< void* >( &props ), 0, sizeof( props ) );
    mpool_mlog *raw_log = nullptr;
    SAFE_CALL( mpool_mlog_alloc( pool.get(), &cap, MP_MED_CAPACITY, &props, &raw_log ) );
    log.reset( raw_log, [pool]( mpool_mlog *p ) { if( p ) mpool_mlog_close( pool.get(), p ); } );
    uint64_t object_id = props.lpr_objid;
    std::cout << "object id: " << object_id << std::endl;
    SAFE_CALL( mpool_mlog_commit( pool.get(), log.get() ) )
  }
  else {
    mlog_props props;
    mpool_mlog *raw_log = nullptr;
    SAFE_CALL( mpool_mlog_find_get( pool.get(), params[ "object" ].as<uint64_t>(), &props, &raw_log ) )
    log.reset( raw_log, [pool]( mpool_mlog *p ) { if( p ) mpool_mlog_close( pool.get(), p ); } );
    uint64_t object_id = props.lpr_objid;
    std::cout << "object id: " << object_id << std::endl;
  }
  uint64_t gen = 0;
  SAFE_CALL( mpool_mlog_open( pool.get(), log.get(), 0, &gen ) )
  if( params.count( "message" ) )
    for( const auto &a: params[ "message" ].as< std::vector< std::string > >() )
      SAFE_CALL( mpool_mlog_append_data( pool.get(), log.get(), const_cast< void* >( static_cast< const void* >( a.data() ) ), a.size(), 1 ) )
  if( abort_transaction )
    SAFE_CALL( mpool_mlog_abort( pool.get(), log.get() ) )
  else
    SAFE_CALL( mpool_mlog_commit( pool.get(), log.get() ) )
  if( erase_log != std::numeric_limits< uint64_t >::max() )
    SAFE_CALL( mpool_mlog_erase( pool.get(), log.get(), erase_log ) )
  bool empty = false;
  SAFE_CALL( mpool_mlog_empty( pool.get(), log.get(), &empty ) )
  std::cout << "empty: " << empty << std::endl;
  size_t len = 0;
  SAFE_CALL( mpool_mlog_len( pool.get(), log.get(), &len ) )
  std::cout << "length: " << len << std::endl;
  SAFE_CALL( mpool_mlog_read_data_init( pool.get(), log.get() ) )
  while( 1 ) {
    std::array< char, 1024u > buf;
    size_t length = 0u;
    SAFE_CALL( mpool_mlog_read_data_next( pool.get(), log.get(), buf.data(), buf.size() - 1, &length ) );
    if( !length ) break;
    buf[ length ] = '\0';
    std::cout << "data: " << buf.data() << std::endl;
  }
  SAFE_CALL( mpool_mlog_flush( pool.get(), log.get() ) )
  if( delete_log )
    SAFE_CALL( mpool_mlog_delete( pool.get(), log.get() ) )
}
