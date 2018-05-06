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

#ifndef BASICS_H
#define BASICS_H

#include <type_traits>
#include <cstdint>
#include <cstddef>

/* For types of store, volumes, see stid_t.h and vid_t.h */

typedef uint32_t    PageID;
typedef uint32_t    StoreID;

// Used in log archive
typedef int32_t run_number_t;

/* Type of a record# on a page  in SM (sans page,store,volume info) */
typedef int16_t slotid_t;

/**
* \brief An integer to point to any record in B-tree pages.
* \details
* -1 if foster-child, 0 if pid0, 1 or larger if real child.
* Same as slotid_t, but used to avoid confusion.
*/
typedef int16_t general_recordid_t;
/**
 * \brief Defines constant values/methods for general_recordid_t.
 */
struct GeneralRecordIds {
    enum ConstantValues {
        /** "Record not found" etc. */
        INVALID = -2,
        /** Represents a foster child record. */
        FOSTER_CHILD = -1,
        /** Represents a PID0 record. */
        PID0 = 0,
        /** Represents the first real child. */
        REAL_CHILD_BEGIN = 1,
    };

    static slotid_t from_general_to_slot(general_recordid_t general) {
        return general - 1;
    }
    static general_recordid_t from_slot_to_general(slotid_t slot) {
        return slot + 1;
    }
};

/**
* \brief CPU Cache line size in bytes.
* \details
* Most modern CPU has 64 bytes cacheline.
* Some less popular CPU like Spark uses 128 bytes.
* This value is used for padding to keep lock objects in different cachelines.
* TODO: CMake script to automatically detect this and cmakedefine for it (JIRA ZERO-179).
*/
const size_t CACHELINE_SIZE = 64;

/* XXX duplicates w_base types. */
const int32_t    max_int4 = 0x7fffffff;         /*  (1 << 31) - 1;  */
const int32_t    max_int4_minus1 = max_int4 -1;
const int32_t    min_int4 = 0x80000000;         /* -(1 << 31);        */

const uint16_t    max_uint2 = 0xffff;
const uint16_t    min_uint2 = 0;
const uint32_t    max_uint4 = 0xffffffff;
const uint32_t    min_uint4 = 0;

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

#endif          /*</std-footer>*/
