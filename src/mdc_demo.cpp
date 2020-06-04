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
#include <boost/spirit/include/qi.hpp>
#include <boost/fusion/include/vector.hpp>
#include <boost/fusion/include/at_c.hpp>
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
  options.add_options()
    ("help,h",    "display this message")
    ("pool,p", boost::program_options::value<std::string>(),  "pool name")
    ("message,m", boost::program_options::value< std::vector< std::string > >()->multitoken(), "message" )
    ("compact,c", boost::program_options::value< std::vector< std::string > >()->multitoken(), "compact" )
    ("object,o", boost::program_options::value<std::string>(),  "object id")
    ("delete,d", boost::program_options::bool_switch( &delete_log ),  "delete");
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
  uint64_t log1 = 0;
  uint64_t log2 = 0;
  if( !params.count( "object" ) ) {
    mdc_capacity cap;
    memset( reinterpret_cast< void* >( &cap ), 0, sizeof( cap ) );
    cap.mdt_captgt = 4 * 1024 * 1024;
    SAFE_CALL( mpool_mdc_alloc( pool.get(), &log1, &log2, MP_MED_CAPACITY, &cap, nullptr ) );
    std::cout << "object id: " << log1 << ":" << log2 << std::endl;
    SAFE_CALL( mpool_mdc_commit( pool.get(), log1, log2 ) )
  }
  else {
    auto v = params[ "object" ].as< std::string >();
    boost::fusion::vector< uint64_t, uint64_t > parsed;
    namespace qi = boost::spirit::qi;
    if( !qi::parse( v.begin(), v.end(), qi::ulong_long >> ':' >> qi::ulong_long, parsed ) ) {
      std::cerr << "invalid object id" << std::endl;
      return 1;
    }
    log1 = boost::fusion::at_c< 0 >( parsed );
    log2 = boost::fusion::at_c< 1 >( parsed );
  }
  mpool_mdc *raw_log = nullptr;
  SAFE_CALL( mpool_mdc_open( pool.get(), log1, log2, 0, &raw_log ) );
  std::shared_ptr< mpool_mdc > log( raw_log, [pool]( mpool_mdc *p ) { if( p ) mpool_mdc_close( p ); } );
  if( params.count( "message" ) )
    for( const auto &a: params[ "message" ].as< std::vector< std::string > >() )
      SAFE_CALL( mpool_mdc_append( log.get(), const_cast< void* >( static_cast< const void* >( a.data() ) ), a.size(), 1 ) )
  if( params.count( "compact" ) ) {
    auto v = params[ "compact" ].as< std::vector< std::string > >();
    std::sort( v.begin(), v.end() );
    std::vector< std::vector< char > > bufs;
    SAFE_CALL( mpool_mdc_rewind( log.get() ) )
    while( 1 ) {
      std::vector< char > buf( 4096, 0 );
      size_t size = 0;
      auto e = mpool_mdc_read( log.get(), buf.data(), buf.size() - 1, &size );
      if( mpool_errno( e ) == EOVERFLOW && size > buf.size() ) {
        buf.resize( size + 1, 0 );
        SAFE_CALL( mpool_mdc_read( log.get(), buf.data(), buf.size() - 1, &size ) );
      }
      else SAFE_CALL( e )
      if( !size ) break;
      if( std::binary_search( v.begin(), v.end(), std::string( buf.data() ) ) ) {
        buf.resize( size );
        bufs.emplace_back( std::move( buf ) );
      }
    }
    SAFE_CALL( mpool_mdc_cstart( log.get() ) )
    for( const auto &buf: bufs ) {
      SAFE_CALL( mpool_mdc_append( log.get(), const_cast< void* >( static_cast< const void* >( buf.data() ) ), buf.size(), 0 ) )
    }
    SAFE_CALL( mpool_mdc_cend( log.get() ) )
  }
  SAFE_CALL( mpool_mdc_rewind( log.get() ) )
  while( 1 ) {
    std::vector< char > buf( 4096, 0 );
    size_t size = 0;
    auto e = mpool_mdc_read( log.get(), buf.data(), buf.size() - 1, &size );
    if( mpool_errno( e ) == EOVERFLOW && size > buf.size() ) {
      buf.resize( size + 1, 0 );
      SAFE_CALL( mpool_mdc_read( log.get(), buf.data(), buf.size() - 1, &size ) );
    }
    else SAFE_CALL( e )
    if( !size ) break;
    std::cout << "data: " << buf.data() << std::endl;
  }
  if( delete_log ) {
    log.reset();
    SAFE_CALL( mpool_mdc_destroy( pool.get(), log1, log2 ) )
  }
}
