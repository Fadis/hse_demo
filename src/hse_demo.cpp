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
#include <hse/hse.h>
}
#include "common.h"

std::string get_hse_error( hse_err_t err, const char *file, int line ) {
  std::array< char, 1024 > buf{ 0 };
  size_t needed;
  hse_err_to_string( err, buf.data(), buf.size(), &needed );
  if( needed < buf.size() ) {
    std::string m( file );
    m += "(";
    m += std::to_string( line );
    m += "): ";
    m += buf.data();
    return m;
  }
  else {
    std::vector< char > buf( needed, 0 );
    hse_err_to_string( err, buf.data(), buf.size(), nullptr );
    std::string m( file );
    m += "(";
    m += std::to_string( line );
    m += "): ";
    m += buf.data();
    return m;
  }
}

#define HSE_SAFE_CALL( e ) \
  { \
    auto result = e ; \
    if( result ) { \
      std::cerr << get_hse_error( result, __FILE__, __LINE__ ) << std::endl; \
      throw mpool_error() ; \
    } \
  }


int main( int argc, char* argv[] ) {
  boost::program_options::options_description options("options");
  bool create_kvdb = false;
  bool create_kvs = false;
  bool delete_block = false;
  bool abort_transaction = false;
  options.add_options()
    ("help,h",    "display this message")
    ("pool,p", boost::program_options::value<std::string>(),  "pool name")
    ("kvs,k", boost::program_options::value<std::string>(),  "kvs name")
    ("message,m", boost::program_options::value< std::string >()->default_value( "Hello, World!" ), "message" )
    ("create-kvdb", boost::program_options::bool_switch( &create_kvdb ), "create kvdb")
    ("create-kvs", boost::program_options::bool_switch( &create_kvs ), "create kvs")
    ("object,o", boost::program_options::value<uint64_t>(),  "object id")
    ("delete,d", boost::program_options::bool_switch( &delete_block ),  "delete")
    ("abort,a", boost::program_options::bool_switch( &abort_transaction ),  "abort")
    ("put,P", boost::program_options::value<std::vector<std::string>>()->multitoken(),  "put")
    ("get,g", boost::program_options::value<std::vector<std::string>>()->multitoken(),  "get");
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
  std::vector< std::pair< std::string, std::string > > put_value;
  if( params.count( "put" ) ) {
    for( const auto &v: params[ "put" ].as< std::vector< std::string > >() ) {
      auto sep = std::find( v.begin(), v.end(), '=' );
      if( sep == v.end() ) {
        std::cerr << "invalid argument: " << v << std::endl;
        return 1;
      }
      put_value.push_back( std::make_pair( std::string( v.begin(), sep ), std::string( std::next( sep ), v.end() ) ) );
    }
  }
  std::vector< std::string > get_value;
  if( params.count( "get" ) )
    get_value = params[ "get" ].as< std::vector< std::string > >();
  HSE_SAFE_CALL( hse_kvdb_init() );
  std::shared_ptr< void > context( nullptr, []( void* ) { hse_kvdb_fini(); } );
  const std::string pool_name = params[ "pool" ].as< std::string >();
  if( create_kvdb )
    HSE_SAFE_CALL( hse_kvdb_make( pool_name.c_str(), nullptr ) );
  hse_kvdb *raw_kvdb = nullptr;
  HSE_SAFE_CALL( hse_kvdb_open( pool_name.c_str(), nullptr, &raw_kvdb ) );
  std::shared_ptr< hse_kvdb > kvdb( raw_kvdb, [context]( hse_kvdb *p ) { if( p ) hse_kvdb_close( p ); } );
  const std::string kvs_name = params[ "kvs" ].as< std::string >();
  if( create_kvs )
    HSE_SAFE_CALL( hse_kvdb_kvs_make( kvdb.get(), kvs_name.c_str(), nullptr ) );
  hse_kvs *raw_kvs;
  HSE_SAFE_CALL( hse_kvdb_kvs_open( kvdb.get(), kvs_name.c_str(), nullptr, &raw_kvs ) );
  std::shared_ptr< hse_kvs > kvs( raw_kvs, [kvdb]( hse_kvs *p ) { if( p ) hse_kvdb_kvs_close( p ); } );
  hse_kvdb_opspec os;
  HSE_KVDB_OPSPEC_INIT( &os );
  std::shared_ptr< hse_kvdb_txn > transaction( hse_kvdb_txn_alloc( kvdb.get() ), [kvdb]( hse_kvdb_txn *p ) { if( p ) hse_kvdb_txn_free( kvdb.get(), p ); } );
  os.kop_txn = transaction.get();
  HSE_SAFE_CALL( hse_kvdb_txn_begin( kvdb.get(), os.kop_txn ) );
  for( const auto &v: put_value ) {
    HSE_SAFE_CALL( hse_kvs_put( kvs.get(), &os, v.first.data(), v.first.size(), v.second.data(), v.second.size() ) );
  }
  for( const auto &v: get_value ) {
    std::array< char, 100 > data{ 0 };
    bool found = false;
    size_t length = 0;
    HSE_SAFE_CALL( hse_kvs_get( kvs.get(), &os, v.data(), v.size(), &found, data.data(), data.size(), &length ) );
    if( found )
      std::cout << v << "=" << data.data() << std::endl;
  }
  if( abort_transaction ) {
    HSE_SAFE_CALL( hse_kvdb_txn_abort( kvdb.get(), os.kop_txn ) );
  }
  else {
    HSE_SAFE_CALL( hse_kvdb_txn_commit( kvdb.get(), os.kop_txn ) );
  }
}
