/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

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

/*<std-header orig-src='shore' incl-file-exclusion='LOGREC_H'>

 $Id: logrec.h,v 1.73 2010/12/08 17:37:42 nhall Exp $

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

#ifndef FINELOG_LOGREC_H
#define FINELOG_LOGREC_H

#include <array>
#include <limits>
#include <cstring>
#include <cstdlib>
#include <ostream>

#include "finelog_basics.h"
#include "lsn.h"

// 1/4 of typical cache-line size (64B)
constexpr size_t LogrecAlignment = 16;

// CS TODO: disable alignment and measure tradeoff in space consumption vs CPU overhead
struct alignas(LogrecAlignment) baseLogHeader
{
   uint32_t _pid;
   uint32_t _page_version;
   uint16_t _len;
   uint8_t _type;

   bool is_valid() const;
};

static_assert(sizeof(baseLogHeader) == LogrecAlignment, "Wrong logrec header size");

class alignas(LogrecAlignment) logrec_t final {
public:
    static constexpr size_t MaxLogrecSize = 3 * 8192;
    static constexpr size_t MaxDataSize = MaxLogrecSize - sizeof(baseLogHeader);
    static constexpr uint8_t MaxLogrecType = std::numeric_limits<uint8_t>::max();

    friend struct baseLogHeader;

    using PageID = uint32_t;
    using StoreID = uint16_t;

    logrec_t() = default;

    logrec_t(uint8_t type)
    {
        header._type = type;
        header._pid = 0;
        header._page_version = 0;
        set_size(0);
    }

    enum flags_t {
        /** invalid log record type */
        t_bad   = 0,
        /** System log record: not transaction- or page-related; no undo/redo */
        t_system    = 1,
        /** log with UNDO action? */
        t_undo      = 1 << 1,
        /** log with REDO action? */
        t_redo      = 1 << 2,
        /** has page image */
        t_page_img  = 1  << 3,
        /** is an EOF log record, i.e., indicates end of log file */
        t_eof  = 1  << 4
    };

    // Initialize flags table
    template <typename Iter>
    static void initialize(Iter begin, Iter end)
    {
        flags.fill(t_bad);
        auto it = begin;
        unsigned count = 0;
        while (it != end) {
           w_assert0(count < MaxLogrecType);
           // EOF flag is for internal use
           w_assert0(*it < t_eof && *it > 0);
           flags[count++] = *it;
           it++;
        }
        assert(count > 0);
        // Max is reserved for EOF
        flags[MaxLogrecType] = t_eof;
    }

    void init_header(uint8_t type, PageID pid = 0, uint32_t version = 0)
    {
        header._type = type;
        header._pid = pid;
        header._page_version = version;
        // CS TODO: for most logrecs, set_size is called twice
        set_size(0);
        assert(valid_header());
    }

    void set_pid(PageID pid)
    {
        header._pid = pid;
    }

    bool valid_header() const
    {
        return header.is_valid();
    }


    void set_size(size_t size)
    {
        // Make sure log record length is always aligned
        constexpr auto bits = LogrecAlignment - 1;
        auto aligned_size = (size + bits) & ~bits;
        header._len = aligned_size + sizeof(baseLogHeader);
    }

    const char* data() const
    {
        return _data;
    }

    char* data()
    {
        return _data;
    }

    PageID pid() const
    {
       return header._pid;
    }

    uint32_t length() const
    {
        return header._len;
    }

    uint8_t type() const
    {
        return header._type;
    }

    uint32_t page_version() const
    {
        return header._page_version;
    }

    void set_page_version(uint32_t version)
    {
        header._page_version = version;
    }

    uint8_t get_flags() const
    {
        return flags[type()];
    }

    bool is_system() const
    {
        return (get_flags() & t_system) != 0;
    }

    bool is_redo() const
    {
        return (get_flags() & t_redo) != 0;
    }

    bool is_eof() const
    {
        return (get_flags() & t_eof) != 0;
    }

    bool is_undo() const
    {
        return (get_flags() & t_undo) != 0;
    }

    bool has_page_img() const
    {
        return (get_flags() & t_page_img) != 0;
    }

    static const logrec_t& get_eof_logrec()
    {
        static logrec_t lr{MaxLogrecType};
        return lr;
    }

private:
    baseLogHeader header;
    char _data[MaxDataSize];

    // Mapping table of log record types to their flags (one entry for each possible byte value)
    using FlagTable = std::array<uint8_t, std::numeric_limits<uint8_t>::max() + 1>;
    static FlagTable flags;
};

inline bool baseLogHeader::is_valid() const
{
   return (_len >= sizeof(baseLogHeader)
         && logrec_t::flags[_type] != logrec_t::t_bad
         && _len <= sizeof(logrec_t));
}

struct UndoEntry
{
    uint16_t offset;
    StoreID store;
    uint8_t type;
};

class UndoBuffer
{
    static constexpr size_t UndoBufferSize = 64 * 1024;
    static constexpr size_t MaxUndoRecords = UndoBufferSize / 128;
    std::array<char, UndoBufferSize> _buffer;
    std::array<UndoEntry, MaxUndoRecords+1> _entries;
    size_t _count;
    bool _abortable;

public:
    UndoBuffer()
    {
        reset();
    }

    void reset()
    {
        _count = 0;
        _abortable = true;
        _entries[0].offset = 0;
    }

    size_t get_count() { return _count; }

    bool is_abortable() { return _abortable; }

    char* get_buffer_end()
    {
        return &_buffer[_entries[_count].offset];
    }

    size_t get_free_space()
    {
        return UndoBufferSize - (get_buffer_end() - &_buffer[0]);
    }

    char* acquire()
    {
        if (!is_abortable()) { return nullptr; }
        // Conservative approach: make sure we can fit maximum logrec size
        if (get_free_space() < sizeof(logrec_t) || _count >= MaxUndoRecords) {
            // W_FATAL_MSG(eINTERNAL, <<
            //         "Transaction too large -- undo buffer full!");
            _abortable = false;
            return nullptr;
        }

        return get_buffer_end();
    }

    void release(size_t length, StoreID store, uint8_t type)
    {
        _entries[_count].store = store;
        _entries[_count].type = type;
        auto offset = _entries[_count].offset;
        _count++;
        _entries[_count].offset = offset + length;
    }

    char* get_data(size_t i)
    {
        if (i < _count) { return &_buffer[_entries[i].offset]; }
        return nullptr;
    }

    StoreID get_store_id(size_t i)
    {
        if (i < _count) { return _entries[i].store; }
        return StoreID{0};
    }

    uint8_t get_type(size_t i)
    {
        if (i < _count) { return _entries[i].type; }
        w_assert0(false);
        return 0;
    }
};

template <size_t BufferSize>
class alignas(LogrecAlignment) RedoBuffer
{
   // TODO: using heap allocation to circumvent TLS limits wiht large DataGen transactions
    // using BufferType = std::array<char, BufferSize>;
    char* _buffer;
    size_t _size;
    uint64_t _epoch;

public:
    RedoBuffer()
        : _size(0), _epoch(0)
    {
       // TODO aligned_alloc not working on my mac -- leaving posix_memalign for now
       // _buffer = reinterpret_cast<char*>(std::aligned_alloc(LogrecAlignment, BufferSize));
       auto res = posix_memalign(&_buffer, LogrecAlignment, BufferSize);
       w_assert0(res == 0);
    }

    ~RedoBuffer()
    {
       std::free(_buffer);
    }

    uint64_t get_epoch() const { return _epoch; }
    void set_epoch(uint64_t e) { _epoch = e; }

    char* get_buffer_end()
    {
        return &_buffer[_size];
    }

    char* get_buffer_begin()
    {
        return &_buffer[0];
    }

    size_t get_free_space()
    {
        return BufferSize - _size;
    }

    size_t get_size()
    {
        return _size;
    }

    void drop_suffix(size_t len)
    {
        _size -= len;
    }

    char* acquire()
    {
        // Conservative approach: make sure we can fit maximum logrec size
        if (get_free_space() < sizeof(logrec_t)) {
            return nullptr;
        }

        return get_buffer_end();
    }

    void release(size_t length)
    {
        _size += length;
    }

    void reset()
    {
        _size = 0;
    }
};

#endif          /*</std-footer>*/
