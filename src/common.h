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

