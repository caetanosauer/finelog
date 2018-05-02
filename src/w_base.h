/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='W_BASE_H'>

 $Id: w_base.h,v 1.82 2010/12/08 17:37:37 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef W_BASE_H
#define W_BASE_H

#include <cstdint>

/**\file w_base.h
 *
 *\ingroup MACROS
 * Basic types.
 */

#if W_DEBUG_LEVEL>0
#define W_IFDEBUG1(x)    x
#define W_IFNDEBUG1(x)    /**/
#else
#define W_IFDEBUG1(x)    /**/
#define W_IFNDEBUG1(x)    x
#endif

#if W_DEBUG_LEVEL>1
#define W_IFDEBUG2(x)    x
#define W_IFNDEBUG2(x)    /**/
#else
#define W_IFDEBUG2(x)    /**/
#define W_IFNDEBUG2(x)    x
#endif

#if W_DEBUG_LEVEL>2
#define W_IFDEBUG3(x)    x
#define W_IFNDEBUG3(x)    /**/
#else
#define W_IFDEBUG3(x)    /**/
#define W_IFNDEBUG3(x)    x
#endif

#if W_DEBUG_LEVEL>3
#define W_IFDEBUG4(x)    x
#define W_IFNDEBUG4(x)    /**/
#else
#define W_IFDEBUG4(x)    /**/
#define W_IFNDEBUG4(x)    x
#endif

#if W_DEBUG_LEVEL>4
#define W_IFDEBUG5(x)    x
#define W_IFNDEBUG5(x)    /**/
#else
#define W_IFDEBUG5(x)    /**/
#define W_IFNDEBUG5(x)    x
#endif

#define W_IFDEBUG9(x)    /**/
#define W_IFNDEBUG9(x)    x

//////////////////////////////////////////////////////////
#undef  W_IFDEBUG
#undef  W_IFNDEBUG
#if W_DEBUG_LEVEL==1
#define W_IFDEBUG(x)    W_IFDEBUG1(x)
#define W_IFNDEBUG(x)    W_IFNDEBUG1(x)
#endif

#if W_DEBUG_LEVEL==2
#define W_IFDEBUG(x)    W_IFDEBUG2(x)
#define W_IFNDEBUG(x)    W_IFNDEBUG2(x)
#endif

#if W_DEBUG_LEVEL==3
#define W_IFDEBUG(x)    W_IFDEBUG3(x)
#define W_IFNDEBUG(x)    W_IFNDEBUG3(x)
#endif

#if W_DEBUG_LEVEL==4
#define W_IFDEBUG(x)    W_IFDEBUG4(x)
#define W_IFNDEBUG(x)    W_IFNDEBUG4(x)
#endif

#ifndef W_IFDEBUG
#define W_IFDEBUG(x) /**/
#endif
#ifndef W_IFNDEBUG
#define W_IFNDEBUG(x) x
#endif

//////////////////////////////////////////////////////////

#ifdef W_TRACE
#define    W_IFTRACE(x)    x
#define    W_IFNTRACE(x)    /**/
#else
#define    W_IFTRACE(x)    /**/
#define    W_IFNTRACE(x)    x
#endif

extern void global_assert_failed(
    const char*        desc,
    const char*        file,
    uint32_t        line);

/// Default assert/debug level is 0.
#define w_assert0(x)    do {                        \
    if (!(x)) global_assert_failed(#x, __FILE__, __LINE__);    \
} while(0)

#define w_assert0_msg(x, msg)                                           \
do {                                                                    \
    if(!(x)) {                                                          \
        std::stringstream s;                                                 \
        s << #x ;                                                       \
        s << " (detail: " << msg << ")";                                \
        w_base_t::assert_failed(s.str().c_str(), __FILE__, __LINE__);   \
 }                                                                      \
}while(0)                                                               \

#ifndef W_DEBUG_LEVEL
#define W_DEBUG_LEVEL 0
#endif

/// Level 1 should not add significant extra time.
#if W_DEBUG_LEVEL>=1
#define w_assert1(x)    w_assert0(x)
#else
//#define w_assert1(x)    /**/
#define w_assert1(x)    if (false) { (void)(x); }
#endif

/// Level 2 adds some time.
#if W_DEBUG_LEVEL>=2
#define w_assert2(x)    w_assert1(x)
#else
//#define w_assert2(x)    /**/
#define w_assert2(x)    if (false) { (void)(x); }
#endif

/// Level 3 definitely adds significant time.
#if W_DEBUG_LEVEL>=3
#define w_assert3(x)    w_assert1(x)
#else
//#define w_assert3(x)    /**/
#define w_assert3(x)    if (false) { (void)(x); }
#endif

#endif          /*</std-footer>*/
