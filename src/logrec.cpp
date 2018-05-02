/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOGREC_C

#include "eventlog.h"

#include "basics.h"
#include <sstream>
#include "logrec_handler.h"

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

const logrec_t& logrec_t::get_skip_log()
{
    static logrec_t skip{kind_t::skip_log};
    return skip;
}

/*********************************************************************
 *
 *  logrec_t::type_str()
 *
 *  Return a string describing the type of the log record.
 *
 *********************************************************************/
const char*
logrec_t::get_type_str(kind_t type)
{
    switch (type)  {
        case kind_t::comment_log : return "comment";
	case kind_t::skip_log : return "skip";
	case kind_t::chkpt_begin_log : return "chkpt_begin";
	case kind_t::add_backup_log : return "add_backup";
	case kind_t::evict_page_log : return "evict_page";
	case kind_t::fetch_page_log : return "fetch_page";
	case kind_t::xct_end_log : return "xct_end";
	case kind_t::xct_latency_dump_log : return "xct_latency_dump";
	case kind_t::alloc_page_log : return "alloc_page";
	case kind_t::dealloc_page_log : return "dealloc_page";
	case kind_t::create_store_log : return "create_store";
	case kind_t::alloc_format_log : return "alloc_format";
	case kind_t::stnode_format_log : return "stnode_format";
	case kind_t::append_extent_log : return "append_extent";
	case kind_t::loganalysis_begin_log : return "loganalysis_begin";
	case kind_t::loganalysis_end_log : return "loganalysis_end";
	case kind_t::redo_done_log : return "redo_done";
	case kind_t::undo_done_log : return "undo_done";
	case kind_t::restore_begin_log : return "restore_begin";
	case kind_t::restore_segment_log : return "restore_segment";
	case kind_t::restore_end_log : return "restore_end";
	case kind_t::warmup_done_log : return "warmup_done";
	case kind_t::page_img_format_log : return "page_img_format";
	case kind_t::update_emlsn_log : return "update_emlsn";
	case kind_t::btree_insert_log : return "btree_insert";
	case kind_t::btree_insert_nonghost_log : return "btree_insert_nonghost";
	case kind_t::btree_update_log : return "btree_update";
	case kind_t::btree_overwrite_log : return "btree_overwrite";
	case kind_t::btree_ghost_mark_log : return "btree_ghost_mark";
	case kind_t::btree_ghost_reclaim_log : return "btree_ghost_reclaim";
	case kind_t::btree_ghost_reserve_log : return "btree_ghost_reserve";
	case kind_t::btree_foster_adopt_log : return "btree_foster_adopt";
	case kind_t::btree_unset_foster_log : return "btree_unset_foster";
	case kind_t::btree_bulk_delete_log : return "btree_bulk_delete";
	case kind_t::btree_compress_page_log : return "btree_compress_page";
	case kind_t::tick_sec_log : return "tick_sec";
	case kind_t::tick_msec_log : return "tick_msec";
	case kind_t::benchmark_start_log : return "benchmark_start";
	case kind_t::page_write_log : return "page_write";
	case kind_t::page_read_log : return "page_read";
    }

    /*
     *  Not reached.
     */
    w_assert0(false);
}

logrec_t::logrec_t(kind_t kind)
{
    header._type = enum_to_base(kind);
    header._pid = 0;
    header._page_version = 0;
    set_size(0);
}

void logrec_t::init_header(kind_t type, PageID pid)
{
    header._type = enum_to_base(type);
    header._pid = pid;
    header._page_version = 0;
    // CS TODO: for most logrecs, set_size is called twice
    set_size(0);
}

void logrec_t::set_size(size_t l)
{
    char *dat = data();
    if (l != ALIGN_BYTE(l)) {
        // zero out extra space to keep purify happy
        memset(dat+l, 0, ALIGN_BYTE(l)-l);
    }
    unsigned int tmp = ALIGN_BYTE(l) + (hdr_sz);
    tmp = (tmp + 7) & unsigned(-8); // force 8-byte alignment
    w_assert1(tmp <= sizeof(*this));
    header._len = tmp;
}

/*
 * Determine whether the log record header looks valid
 */
bool
logrec_t::valid_header() const
{
    return header.is_valid();
}


/*********************************************************************
 *  Invoke the redo method of the log record.
 *********************************************************************/
template <class PagePtr>
void logrec_t::redo(PagePtr page)
{
    DBG( << "Redo  log rec: " << *this << " size: " << header._len);

    switch (base_to_enum<kind_t>(header._type))  {
        case kind_t::alloc_page_log : LogrecHandler<kind_t::alloc_page_log, PagePtr>::redo(this, page); break;
        case kind_t::dealloc_page_log : LogrecHandler<kind_t::dealloc_page_log, PagePtr>::redo(this, page); break;
        case kind_t::alloc_format_log : LogrecHandler<kind_t::alloc_format_log, PagePtr>::redo(this, page); break;
        case kind_t::stnode_format_log : LogrecHandler<kind_t::stnode_format_log, PagePtr>::redo(this, page); break;
        case kind_t::create_store_log : LogrecHandler<kind_t::create_store_log, PagePtr>::redo(this, page); break;
        case kind_t::append_extent_log : LogrecHandler<kind_t::append_extent_log, PagePtr>::redo(this, page); break;
        case kind_t::page_img_format_log : LogrecHandler<kind_t::page_img_format_log, PagePtr>::redo(this, page); break;
        case kind_t::update_emlsn_log : LogrecHandler<kind_t::update_emlsn_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_insert_log : LogrecHandler<kind_t::btree_insert_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_insert_nonghost_log : LogrecHandler<kind_t::btree_insert_nonghost_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_update_log : LogrecHandler<kind_t::btree_update_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_overwrite_log : LogrecHandler<kind_t::btree_overwrite_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_ghost_mark_log : LogrecHandler<kind_t::btree_ghost_mark_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_ghost_reclaim_log : LogrecHandler<kind_t::btree_ghost_reclaim_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_ghost_reserve_log : LogrecHandler<kind_t::btree_ghost_reserve_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_foster_adopt_log : LogrecHandler<kind_t::btree_foster_adopt_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_unset_foster_log : LogrecHandler<kind_t::btree_unset_foster_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_bulk_delete_log : LogrecHandler<kind_t::btree_bulk_delete_log, PagePtr>::redo(this, page); break;
        case kind_t::btree_compress_page_log : LogrecHandler<kind_t::btree_compress_page_log, PagePtr>::redo(this, page); break;
    }

    page->set_version(page_version());
}

void logrec_t::redo()
{
    // CS TODO fix this
    // redo<btree_page_h*>(nullptr);
}


/*********************************************************************
 *
 *  logrec_t::undo(page)
 *
 *  Invoke the undo method of the log record. Automatically tag
 *  a compensation lsn to the last log record generated for the
 *  undo operation.
 *
 *********************************************************************/
void logrec_t::undo(kind_t type, StoreID stid, const char* data)
{
    // CS TODO fix this
    // using PagePtr = fixable_page_h*;
    // switch (type) {
	// case btree_insert_log :
    //             LogrecHandler<btree_insert_log, PagePtr>::undo(stid, data);
		// break;
	// case btree_insert_nonghost_log :
    //             LogrecHandler<btree_insert_nonghost_log, PagePtr>::undo(stid, data);
		// break;
	// case btree_update_log :
    //             LogrecHandler<btree_update_log, PagePtr>::undo(stid, data);
		// break;
	// case btree_overwrite_log :
    //             LogrecHandler<btree_overwrite_log, PagePtr>::undo(stid, data);
		// break;
	// case btree_ghost_mark_log :
    //             LogrecHandler<btree_ghost_mark_log, PagePtr>::undo(stid, data);
		// break;
	// default :
		// W_FATAL(eINTERNAL);
		// break;
    // }
}

/*********************************************************************
 *
 *  logrec_t::corrupt()
 *
 *  Zero out most of log record to make it look corrupt.
 *  This is for recovery testing.
 *
 *********************************************************************/
void
logrec_t::corrupt()
{
    char* end_of_corruption = ((char*)this)+length();
    char* start_of_corruption = (char*)&header._type;
    size_t bytes_to_corrupt = end_of_corruption - start_of_corruption;
    memset(start_of_corruption, 0, bytes_to_corrupt);
}

/*********************************************************************
 *
 *  operator<<(ostream, logrec)
 *
 *  Pretty print a log record to ostream.
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, logrec_t& l)
{
    ios_fmtflags        f = o.flags();
    o.setf(ios::left, ios::left);

    o << l.type_str();
    o << " len=" << l.length();
    o << " pid=" << l.pid();
    o << " pversion=" << l.page_version();

    // CS TODO: fix this
    switch(l.type()) {
        // case comment_log :
        //     {
        //         o << " " << (const char *)l._data;
        //         break;
        //     }
        // case update_emlsn_log:
        //     {
        //         general_recordid_t slot;
        //         lsn_t lsn;
        //         deserialize_log_fields(&l, slot, lsn);
        //         o << " slot: " << slot << " emlsn: " << lsn;
        //         break;
        //     }
        // case evict_page_log:
        //     {
        //         PageID pid;
        //         uint32_t version;
        //         deserialize_log_fields(&l, pid, version);
        //         o << " pid: " << pid << " version: " << version;
        //         break;
        //     }
        // case fetch_page_log:
        //     {
        //         PageID pid;
        //         uint32_t version;
        //         StoreID store;
        //         deserialize_log_fields(&l, pid, version, store);
        //         o << " pid: " << pid << " version: " << version << " store: " << store;
        //         break;
        //     }
        // case alloc_page_log:
        // case dealloc_page_log:
        //     {
        //         PageID pid;
        //         deserialize_log_fields(&l, pid);
        //         o << " page: " << pid;
        //         break;
        //     }
        // case create_store_log:
        //     {
        //         StoreID stid;
        //         PageID root_pid;
        //         deserialize_log_fields(&l, stid, root_pid);
        //         o << " stid: " <<  stid;
        //         o << " root_pid: " << root_pid;
        //         break;
        //     }
        // case page_read_log:
        //     {
        //         PageID pid;
        //         uint32_t count;
        //         PageID end = pid + count - 1;
        //         deserialize_log_fields(&l, pid, count);
        //         o << " pids: " << pid << "-" << end;
        //         break;
        //     }
        // case page_write_log:
        //     {
        //         PageID pid;
        //         lsn_t clean_lsn;
        //         uint32_t count;
        //         deserialize_log_fields(&l, pid, clean_lsn, count);
        //         PageID end = pid + count - 1;
        //         o << " pids: " << pid << "-" << end << " clean_lsn: " << clean_lsn;
        //         break;
        //     }
        // case restore_segment_log:
        //     {
        //         uint32_t segment;
        //         deserialize_log_fields(&l, segment);
        //         o << " segment: " << segment;
        //         break;
        //     }
        // case append_extent_log:
        //     {
        //         extent_id_t ext;
        //         StoreID snum;
        //         deserialize_log_fields(&l, snum, ext);
        //         o << " extent: " << ext << " store: " << snum;
        //         break;
        //     }


        default: /* nothing */
                break;
    }

    o.flags(f);
    return o;
}

// template void logrec_t::template redo<btree_page_h*>(btree_page_h*);
// template void logrec_t::template redo<fixable_page_h*>(fixable_page_h*);
