/*<std-header orig-src='shore' incl-file-exclusion='BASICS_H'>

 $Id: basics.h,v 1.73 2010/07/26 23:37:06 nhall Exp $

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

#ifndef FINELOG_BASICS_H
#define FINELOG_BASICS_H

#include <type_traits>
#include <cstdint>
#include <cstddef>

/* For types of store, volumes, see stid_t.h and vid_t.h */

typedef uint32_t    PageID;
typedef uint32_t    StoreID;

// Used in log archive
typedef int32_t run_number_t;

/**
* \brief CPU Cache line size in bytes.
* \details
* Most modern CPU has 64 bytes cacheline.
* Some less popular CPU like Spark uses 128 bytes.
* This value is used for padding to keep lock objects in different cachelines.
* TODO: CMake script to automatically detect this and cmakedefine for it (JIRA ZERO-179).
*/
const size_t CACHELINE_SIZE = 64;

template<typename E>
constexpr auto enum_to_base(E e) -> typename std::underlying_type<E>::type
{
    return static_cast<typename std::underlying_type<E>::type>(e);
}

template<typename E>
constexpr E base_to_enum(typename std::underlying_type<E>::type e)
{
    return static_cast<E>(e);
}

void global_assert_failed(
    const char*        desc,
    const char*        file,
    uint32_t        line);

// CS TODO: use STL classes and exceptions
#define DO_PTHREAD(x) \
{   int res = x; \
    if(res) { global_assert_failed("PTHREAD error", __FILE__, __LINE__); }  \
}

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        std::stringstream ss; \
        ss << "Kernel errno code: " << errno; \
        throw std::runtime_error(ss.str()); \
    }

/**\enum timeout_t
 * \brief Special values for int.
 *
 * \details sthreads package recognizes 2 WAIT_* values:
 * == WAIT_IMMEDIATE
 * and != WAIT_IMMEDIATE.
 *
 * If it's not WAIT_IMMEDIATE, it's assumed to be
 * a positive integer (milliseconds) used for the
 * select timeout.
 * WAIT_IMMEDIATE: no wait
 * WAIT_FOREVER:   may block indefinitely
 * The user of the thread (e.g., sm) had better
 * convert timeout that are negative values (WAIT_* below)
 * to something >= 0 before calling block().
 *
 * All other WAIT_* values other than WAIT_IMMEDIATE
 * are handled by sm layer:
 * WAIT_SPECIFIED_BY_THREAD: pick up a int from the smthread.
 * WAIT_SPECIFIED_BY_XCT: pick up a int from the transaction.
 * Anything else: not legitimate.
 *
 * \sa int
 */
struct timeout_t {
    static constexpr int WAIT_IMMEDIATE     = 0;
    static constexpr int WAIT_FOREVER     = -1;
    static constexpr int WAIT_SPECIFIED_BY_THREAD     = -4; // used by lock manager
    static constexpr int WAIT_SPECIFIED_BY_XCT = -5; // used by lock manager
    // CS: I guess the NOT_USED value is only for threads that never acquire
    // any locks? And neither latches?
    static constexpr int WAIT_NOT_USED = -6; // indicates last negative number used by sthreads
};
/*<std-footer incl-file-exclusion='BASICS_H'>  -- do not edit anything below this line -- */

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

/// Default assert/debug level is 0.
#define w_assert0(x)    do {                        \
    if (!(x)) global_assert_failed(#x, __FILE__, __LINE__);    \
} while(0)

#define w_fatal(msg) global_assert_failed(msg, __FILE__, __LINE__);

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

/**\file w_debug.h
 *\ingroup MACROS
 *
*  This is a set of macros for use with C or C++. They give various
*  levels of debugging printing when compiled with --enable-trace.
*  With tracing, message printing is under the control of an environment
*  variable DEBUG_FLAGS (see debug.cpp).
*  If that variable is set, its value must
*  be  a string.  The string is searched for __FILE__ and the function name
*  in which the debugging message occurs.  If either one appears in the
*  string (value of the env variable), or if the string contains the
*  word "all", the message is printed.
*
*
*/
#include <cassert>
#include <pthread.h>
#include <sstream>
#include <iostream>

/* XXX missing type in vc++, hack around it here too, don't pollute
   global namespace too badly. */
typedef    std::ios::fmtflags    w_dbg_fmtflags;


// Turns full path from __FILE__ macro into just name of the file
// CS: This was not necessary in Shore-MT because gcc was invoked
// not on the full path, but on the file directly
#define _strip_filename(f) \
    (strrchr(f, '/') ? strrchr(f, '/') + 1 : f)

/* ************************************************************************  */

/* ************************************************************************
 *
 * Class debug_t, macros DBG, DBG_NONL, DBG1, DBG1_NONL:
 */


/**\brief An ErrLog used for tracing (configure --enable-trace)
 *
 * For tracing to be used, you must set the environment variable
 * DEBUG_FLAGS to a string containing the names of the files you
 * want traced, and
 *
 * DEBUG_FILE to the name of the output file to which the output
 * should be sent. If DEBUG_FILE is not set, the output goes to
 * stderr.
 */
class debug_t {
    private:
        char *_flags;
        enum { _all = 0x1, _none = 0x2 };
        unsigned int        mask;
        int            _trace_level;

        int            all(void) { return (mask & _all) ? 1 : 0; }
        int            none(void) { return (mask & _none) ? 1 : 0; }

    public:
        debug_t(const char *n, const char *f);
        ~debug_t();
        int flag_on(const char *fn, const char *file);
        const char *flags() { return _flags; }
        void setflags(const char *newflags);
        void memdump(void *p, int len); // hex dump of memory
        int trace_level() { return _trace_level; }
};
extern debug_t _debug;


// I wanted to use google-logging (glog), but changing all of the existing code
// takes time. So, currently it's just std::cout.
#define ERROUT(a) std::cerr << "[" << hex << pthread_self() << dec << "] " << __FILE__ << " (" << __LINE__ << ") " a << endl;
//#define DBGOUT(a) std::cout << "[" << pthread_self() << "] " << __FILE__ << " (" << __LINE__ << ") " a << endl;

// CS: reverted back to shore's old debug mechanism, which allows us
// to select only output from certain source files. The current mechanism
// dumps way to much debug information, which makes it hard to perform
// actual debugging focused only on certain components.
#define DBGPRINT(a, file, line) \
       std::stringstream ss; \
       ss << "[" << hex << pthread_self() << dec << "] " \
            << _strip_filename(file) << " (" << line << ") " a; \
       std::cerr << ss.str() << endl;

#define DBGOUT(a) do { \
    if(_debug.flag_on(__func__,_strip_filename(__FILE__))) { \
        DBGPRINT(a, __FILE__, __LINE__); \
    } \
 } while (0);

#define DBGOUT0(a) DBGOUT(a)

#if 1
#if W_DEBUG_LEVEL >= 1
#define DBGOUT1(a) DBGOUT(a)
#else
#define DBGOUT1(a)
#endif


#if W_DEBUG_LEVEL >= 2
#define DBGOUT2(a) DBGOUT(a)
#else
#define DBGOUT2(a)
#endif

#if W_DEBUG_LEVEL >= 3
#define DBGOUT3(a) DBGOUT(a)
#else
#define DBGOUT3(a)
#endif


#if W_DEBUG_LEVEL >= 4
#define DBGOUT4(a) DBGOUT(a)
#else
#define DBGOUT4(a)
#endif


#if W_DEBUG_LEVEL >= 5
#define DBGOUT5(a) DBGOUT(a)
#else
#define DBGOUT5(a)
#endif


#if W_DEBUG_LEVEL >= 6
#define DBGOUT6(a) DBGOUT(a)
#else
#define DBGOUT6(a)
#endif


#if W_DEBUG_LEVEL >= 7
#define DBGOUT7(a) DBGOUT(a)
#else
#define DBGOUT7(a)
#endif


#if W_DEBUG_LEVEL >= 8
#define DBGOUT8(a) DBGOUT(a)
#else
#define DBGOUT8(a)
#endif


#if W_DEBUG_LEVEL >= 9
#define DBGOUT9(a) DBGOUT(a)
#else
#define DBGOUT9(a)
#endif

#define DBG1(a) DBGOUT1(a)
#define DBG2(a) DBGOUT2(a)
#define DBG3(a) DBGOUT3(a)
#define DBG5(a) DBGOUT5(a)

#else

#define DBGOUT1(a)
#define DBGOUT2(a)
#define DBGOUT3(a)
#define DBGOUT4(a)
#define DBGOUT5(a)
#define DBGOUT6(a)
#define DBGOUT7(a)
#define DBGOUT8(a)
#define DBGOUT9(a)

#endif

// the old "DBG" idiom is level=3
#define DBG(a) DBGOUT3(a)
/*
#if defined(W_TRACE)

#    define DBG2(a,file,line) \
        w_dbg_fmtflags old = _debug.clog.setf(ios::dec, ios::basefield); \
        _debug.clog  << _strip_filename(file) << ":" << line << ":" ; \
        _debug.clog.setf(old, ios::basefield); \
        _debug.clog  a    << endl;

#    define DBG1(a) do {\
    if(_debug.flag_on(__func__,__FILE__)) {                \
        DBG2(a,__FILE__,__LINE__) \
    } } while(0)

#    define DBG(a) DBG1(a)

#else
#    define DBG(a)
#endif *//* defined(W_TRACE) */
/* ************************************************************************  */

// #define DBG2(a,f,l) DBGPRINT(a,f,l) // used by smthread.h

#include <thread>
#define DBGTHRD(arg) DBG(<<" th."<< std::this_thread::get_id() << " " arg)


#endif          /*</std-footer>*/
