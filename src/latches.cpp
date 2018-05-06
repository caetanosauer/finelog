#include "latches.h"

#include <cstring>
#include <iostream>
#include <list>
#include <algorithm>

using namespace std;

const char* const  latch_t::latch_mode_str[4] = { "NL", "Q", "SH", "EX" };

latch_t::latch_t() :
    _total_count(0)
{
}

latch_t::~latch_t()
{
#if W_DEBUG_LEVEL > 1
    int t = _total_count;
    // do this just to get the symbol to remain
    if(t) {
        fprintf(stderr, "t=%d\n", t);
    }

    // Should only check if the system is not doing dirty shutdown (ss_m::shutdown_clean)
    // But we don't have a way to check this flag from latch object
////////////////////////////////////////
// TODO(Restart)... comment out the assertion in debug mode for 'instant restart' testing purpose
//                     if we are using simulated crash shutdown, the following assertion might fire if
//                     we are in the middle of taking a checkpoint
//                    need a way to ignore latch count checking if using simulated system crash
//
//                    For now, comment out the assertion, although we might miss other
//                    bugs by comment out the assertion
////////////////////////////////////////

//    w_assert2(t == 0);// BUG_SEMANTICS_FIX

//    w_assert2(mode() == LATCH_NL);
//    w_assert2(num_holders() == 0);

#endif
}


/**\var static __thread latch_holder_t* latch_holder_t::thread_local_holders;
 * \brief Linked list of all latches held by this thread.
 * \ingroup TLS
 *
 * \details
 * Every time we want to grab a latch,
 * we have to create a latch_holder_t; we do that with the
 * holder_search class, which searches the per-thread list below
 * to make sure we `(this thread) don't already hold the latch and
 * if not, it creates a new latch_holder_t for the new latch acquisition,
 * and stuffs the latch_holder_t in this list.
 * If we do already have hold the latch in some capacity, the
 * holder_search returns that existing latch_holder_t.
 * So we can tell if this thread holds a given latch, and we can
 * find all latches held by this thread, but we can't find
 * all the holders of a given latch.
 *
 * \sa latch_holder_t
 */
__thread latch_holder_t* latch_holder_t::thread_local_holders(NULL);

/**\var static __thread latch_holder_t* latch_holder_t::thread_local_freelist;
 * \brief Pool of unused latch_holder_t instances.
 * \ingroup TLS
 *
 * \details
 * Ready for recycling.  These structures are first taken from the global heap
 * but put on this list for reuse rather than ::free-ed.
 * When the thread is destroyed, the items on this list are returned
 * to the global heap.
 *
 * \sa latch_holder_t
 */
__thread latch_holder_t* latch_holder_t::thread_local_freelist(NULL);

/**\brief The list-handling class for latch_holder_t instances.
 *
 * \details
 * Really, all this does is provide an iterator and a means to
 * insert (push_front)  and remove (unlink) these things.
 *
 * The list contents are always instances of latch_holder_t, which
 * have an internal link for creating the list.
 */
class holder_list
{
    latch_holder_t* &_first;
public:
    holder_list(latch_holder_t* &first) : _first(first) { }

    /**\brief Iterator over a list of latch_holder_t structures */
    struct iterator {
        latch_holder_t* _cur;
        public:

        /// Construct an iterator starting with the given latch_holder_t.
        explicit iterator(latch_holder_t* cur) : _cur(cur) { }

        /// Get current.
        operator latch_holder_t*() const { return _cur; }

        /// Get current.
        latch_holder_t* operator->() const { return *this; }

        ///  Make iterator point to next.
        iterator &operator++() { _cur = _cur->_next; return *this; }

        ///  Make iterator point to next.
        iterator operator++(int) { return ++iterator(*this); }
    };

    /// Dereferencing this iterator brings us to the first item in the list.
    iterator begin() { return iterator(_first); }

    /// Dereferencing this iterator brings us past the last item in any list.
    iterator end() { return iterator(NULL); }

    /// Insert h at the front of this list.
    void push_front(latch_holder_t* h) {
        h->_next = _first;
        if(_first) _first->_prev = h;
        h->_prev = NULL;
        _first = h;
    }

    /// Remove whatever is the current item for the given iterator.
    latch_holder_t* unlink(iterator const &it) {
        if(it->_next)
            it->_next->_prev = it->_prev;

        if(it->_prev)
            it->_prev->_next = it->_next;
        else
            _first = it->_next;

        // now it's orphaned...
        return it;
    }
};

/**\class holders_print
 * \brief For debugging only.
 *
 * \details
 *
 * Constructor looks through all the holders in the
 * implied list starting with the latch_holder_t passed in as the sole
 * constructor argument.
 *
 * It prints info about each latch_holder_t in the list.
 *
 * \sa latch_holder_t.
 */
class  holders_print
{
private:
    holder_list _holders;
    void print(holder_list holders)
    {
        holder_list::iterator it=holders.begin();
        for(; it!=holders.end() && it->_latch;  ++it)
        {
            it->print(cerr);
        }
    }
public:
    holders_print(latch_holder_t *list)
    : _holders(list)
    {
        print(_holders);
    }
};

/**\class holder_search
 * \brief Finds all latches held by this thread.
 *
 * \details
 * Searches a thread-local list for a latch_holder_t that is a
 * reference to the given latch_t.
 *
 * \sa latch_holder_t.
 */
class holder_search
{
public:
    /// find holder of given latch in given list
    static holder_list::iterator find(holder_list holders, latch_t const* l)
    {
        holder_list::iterator it=holders.begin();
        for(; it!=holders.end() && it->_latch != l; ++it) ;
        return it;
    }

    /// count # times we find a given latch in the list. For debugging, asserts.
    static int count(holder_list holders, latch_t const* l)
    {
        holder_list::iterator it=holders.begin();
        int c=0;
        for(; it!=holders.end(); ++it) if(it->_latch == l) c++;
        return c;
    }

private:
    holder_list _holders;
    latch_holder_t* &_freelist;
    holder_list::iterator _end;
    holder_list::iterator _it;

public:
    /// Insert latch_holder_t for given latch if not already there.
    holder_search(latch_t const* l)
        : _holders(latch_holder_t::thread_local_holders),
          _freelist(latch_holder_t::thread_local_freelist),
          _end(_holders.end()),
          _it(find(_holders, l))
    {
        // if we didn't find the latch in the list,
        // create a new latch_holder_t (with mode LATCH_NL)
        // to return, just so that the value() method always
        // returns a non-null ptr.  It might be used, might not.
        if(_it == _end) {
            latch_holder_t* h = _freelist;
            if(h) _freelist = h->_next;
            // need to clear out the latch either way
            if(h)
                // h->latch_holder_t(); // reinit
                h = new(h) latch_holder_t();
            else
                h = new latch_holder_t;
            _holders.push_front(h);
            _it = _holders.begin();
        }
        w_assert2(count(_holders, l) <= 1);
    }

    ~holder_search()
    {
        if(_it == _end || _it->_mode != LATCH_NL)
            return;

        // don't hang onto it in the holders list  if it's not latched.
        latch_holder_t* h = _holders.unlink(_it);
        h->_next = _freelist;
        _freelist = h;
    }

    latch_holder_t* operator->() { return this->value(); }

    latch_holder_t* value() { return (_it == _end)?
        (latch_holder_t *)(NULL) : &(*_it); }
}; // holder_search

AcquireResult latch_t::latch_acquire(latch_mode_t mode, int timeout_in_ms)
{
    w_assert1(mode != LATCH_NL);
    holder_search me(this);
    return _acquire(mode, timeout_in_ms, me.value());
}

void latch_t::upgrade_if_not_block(bool& would_block)
{
    DBGTHRD(<< " want to upgrade " << *this );
    holder_search me(this);

    // should already hold the latch
    w_assert3(me.value() != NULL);

    // already hold EX? DON'T INCREMENT THE COUNT!
    if(me->_mode == LATCH_EX) {
        would_block = false;
        return;
    }

    AcquireResult rc = _acquire(LATCH_EX, timeout_t::WAIT_IMMEDIATE, me.value());
    if(rc != AcquireResult::OK) {
        // it never should have tried to block
        w_assert1(rc == AcquireResult::WOULD_BLOCK);
        would_block = true;
    }
    else {
        // upgrade should not increase the lock count
        lintel::unsafe::atomic_fetch_sub(&_total_count, 1);
        me->_count--;
        would_block = false;
    }
}

int latch_t::latch_release()
{
    holder_search me(this);
    // we should already hold the latch!
    w_assert2(me.value() != NULL);
    return _release(me.value());
}

AcquireResult latch_t::_acquire(latch_mode_t new_mode,
    int timeout,
    latch_holder_t* me)
{
    DBGTHRD( << "want to acquire in mode "
            << W_ENUM(new_mode) << " " << *this
            );
    w_assert2(new_mode != LATCH_NL);
    w_assert2(me);

    bool is_upgrade = false;
    if(me->_latch == this)
    {
        // we already hold the latch
        w_assert2(me->_mode != LATCH_NL);
        w_assert2(mode() == me->_mode);
        // note: _mode can't change while we hold the latch!
        if(mode() == LATCH_EX) {
            w_assert2(num_holders() == 1);
            // once we hold it in EX all later acquires default to EX as well
            new_mode = LATCH_EX;
        } else {
            w_assert2(num_holders() >= 1);
        }
        if(me->_mode == new_mode) {
            DBGTHRD(<< "we already held latch in desired mode " << *this);
            lintel::unsafe::atomic_fetch_add(&_total_count, 1);// BUG_SEMANTICS_FIX
            me->_count++; // thread-local
            // fprintf(stderr, "acquire latch %p %dx in mode %s\n",
            //        this, me->_count, latch_mode_str[new_mode]);
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
            // These are counted in bf statistics.
            // but if we don't count them here, we will get
            // a misleading impression of the wait counts
            // is_upgrade is figured w/o consideraton whether request is
            // conditional/unconditional, but we consider it
            // uncondl because the unconditional case is
            // the one we're trying to understand in the callers
            // (bf find, bf scan, btree latch
            // INC_TSTAT(latch_uncondl_nowait);
#endif
            return AcquireResult::OK;
        } else if(new_mode == LATCH_EX && me->_mode == LATCH_SH) {
            is_upgrade = true;
        }
    } else {
        // init 'me' (latch holder) for the critical section
        me->_latch = this;
        me->_mode = LATCH_NL;
        me->_count = 0;
    }

    // have to acquire for real

    if(is_upgrade) {
        // to avoid deadlock,
        // never block on upgrade
        if(!_lock.attempt_upgrade()) {
            return AcquireResult::WOULD_BLOCK;
        }

        w_assert2(me->_count > 0);
        w_assert2(new_mode == LATCH_EX);
        me->_mode = new_mode;
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
        // These are counted in bf statistics.
        // but if we don't count them here, we will get
        // a misleading impression of the wait counts
        // is_upgrade is figured w/o consideraton whether request is
        // conditional/unconditional, but we consider it
        // uncondl because the unconditional case is
        // the one we're trying to understand in the callers
        // (bf find, bf scan, btree latch
        // INC_TSTAT(latch_uncondl_nowait);
#endif
    } else {
        if(timeout == timeout_t::WAIT_IMMEDIATE) {
            // INC_TSTAT(needs_latch_condl);
            bool success = (new_mode == LATCH_SH)?
                _lock.attempt_read() : _lock.attempt_write();
            if(!success)
                return AcquireResult::TIMEOUT;
            // INC_TSTAT(latch_condl_nowait);
        }
        else {
            // forever timeout
            // INC_TSTAT(needs_latch_uncondl);
            if(new_mode == LATCH_SH) {
// NOTE: These stats are questionable in their
// heiseneffect as well as in the fact that we might
// not wait in the _lock.acquire_{read,write} call
// after the attempt- call. Nevertheless, they might
// help us in some instances to understand where the
// contention is, and are under compiler control for
// this reason.
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
                if(_lock.attempt_read()) {
                    // INC_TSTAT(latch_uncondl_nowait);
                } else
#endif
                _lock.acquire_read();
            }
            else {
                w_assert2(new_mode == LATCH_EX);
                w_assert2(me->_count == 0);
#if defined(EXPENSIVE_LATCH_COUNTS) && EXPENSIVE_LATCH_COUNTS>0
                if(_lock.attempt_write()) {
                    // INC_TSTAT(latch_uncondl_nowait);
                } else
#endif
                _lock.acquire_write();
            }
        }
        w_assert2(me->_count == 0);
        me->_mode = new_mode;
    }
    lintel::unsafe::atomic_fetch_add(&_total_count, 1);// BUG_SEMANTICS_FIX
    me->_count++;// BUG_SEMANTICS_FIX
    DBGTHRD(<< "acquired " << *this );
    return AcquireResult::OK;
}


int
latch_t::_release(latch_holder_t* me)
{
    DBGTHRD(<< "want to release " << *this );

    w_assert2(me->_latch == this);

    w_assert2(me->_mode != LATCH_NL);
    w_assert2(me->_count > 0);

    lintel::unsafe::atomic_fetch_sub(&_total_count, 1);
    if(--me->_count) {
        DBGTHRD(<< "was held multiple times -- still " << me->_count << " " << *this );
        return me->_count;
    }

    if(me->_mode == LATCH_SH) {
        w_assert2(_lock.has_reader());
        if (_lock.has_reader())
            _lock.release_read();
    }
    else {
        w_assert2(_lock.has_writer());
        if (_lock.has_writer())
            _lock.release_write();
    }
    me->_mode = LATCH_NL;
    return 0;
}

void latch_t::downgrade() {
    holder_search me(this);
    // we should already hold the latch!
    w_assert3(me.value() != NULL);
    _downgrade(me.value());
}

void
latch_t::_downgrade(latch_holder_t* me)
{
    DBGTHRD(<< "want to downgrade " << *this );

    w_assert3(me->_latch == this);
    w_assert3(me->_mode == LATCH_EX);
    w_assert3(me->_count > 0);

    _lock.downgrade();
    me->_mode = LATCH_SH;

}

void latch_holder_t::print(ostream &o) const
{
    o << "Holder " << latch_t::latch_mode_str[int(_mode)]
        << " cnt=" << _count
    << " latch:";
    if(_latch) {
        o  << *_latch << endl;
    } else {
        o  << "NULL" << endl;
    }
}

// return the number of times the latch is held by this thread
// or 0 if I do not hold the latch
// There should never be more than one holder structure for a single
// latch.
int
latch_t::held_by_me() const
{
    holder_search me(this);
    return me.value()? me->_count : 0;
}

bool
latch_t::is_mine() const {
    holder_search me(this);
    return me.value()? (me->_mode == LATCH_EX) : false;
}

// NOTE: this is not safe, but it can be used by unit tests
// and for debugging
std::ostream &latch_t::print(std::ostream &out) const
{
    out <<    "latch(" << this << ") ";
    out << " held in " << latch_mode_str[int(mode())] << " mode ";
    out << "by " << num_holders() << " threads " ;
    out << "total " << latch_cnt() << " times " ;
    out << endl;
    return out;
}


ostream& operator<<(ostream& out, const latch_t& l)
{
    return l.print(out);
}

// For use in debugger:
void print_latch(const latch_t *l)
{
    if(l != NULL) l->print(cerr);
}

// For use in debugger:
void print_my_latches()
{
    holders_print all(latch_holder_t::thread_local_holders);
}

occ_rwlock::occ_rwlock()
    : _active_count(0)
{
    _write_lock._lock = _read_lock._lock = this;
    DO_PTHREAD(pthread_mutex_init(&_read_write_mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&_read_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_write_cond, NULL));
}

occ_rwlock::~occ_rwlock()
{
    DO_PTHREAD(pthread_mutex_destroy(&_read_write_mutex));
    DO_PTHREAD(pthread_cond_destroy(&_read_cond));
    DO_PTHREAD(pthread_cond_destroy(&_write_cond));
    _write_lock._lock = _read_lock._lock = NULL;
}

void occ_rwlock::release_read()
{
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(READER <= (int) _active_count);
    unsigned count = lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
    if(count == WRITER) {
        // wake it up
        CRITICAL_SECTION(cs, _read_write_mutex);
        DO_PTHREAD(pthread_cond_signal(&_write_cond));
    }
}

void occ_rwlock::acquire_read()
{
    unsigned count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)READER) + READER;
    while(count & WRITER) {
        // block
        count = lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
        {
            CRITICAL_SECTION(cs, _read_write_mutex);

            // nasty race: we could have fooled a writer into sleeping...
            if(count == WRITER) {
                DO_PTHREAD(pthread_cond_signal(&_write_cond));
            }

            while(*&_active_count & WRITER) {
                DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
            }
        }
        count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
    }
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

void occ_rwlock::release_write()
{
    w_assert3(_active_count & WRITER);
    CRITICAL_SECTION(cs, _read_write_mutex);
    lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)WRITER);
    DO_PTHREAD(pthread_cond_broadcast(&_read_cond));
}

void occ_rwlock::acquire_write()
{
    // only one writer allowed in at a time...
    CRITICAL_SECTION(cs, _read_write_mutex);
    while(*&_active_count & WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
    }

    // any lurking writers are waiting on the cond var
    unsigned count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)WRITER) + WRITER;
    w_assert1(count & WRITER);

    // drain readers
    while(count != WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_write_cond, &_read_write_mutex));
        count = *&_active_count;
    }
}


/************************************************************************************
 * mcs_rwlock implementation; cheaper but problematic when we get os preemptions
 */

// CC mangles this as __1cKmcs_rwlockOspin_on_writer6M_v_
// private
int mcs_rwlock::_spin_on_writer()
{
    int cnt=0;
    while(has_writer()) cnt=1;
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return cnt;
}
// CC mangles this as __1cKmcs_rwlockPspin_on_readers6M_v_
// private
void mcs_rwlock::_spin_on_readers()
{
    while(has_reader()) { };
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

// private
void mcs_rwlock::_add_when_writer_leaves(int delta)
{
    // we always have the parent lock to do this
    int cnt = _spin_on_writer();
    lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_holders), delta);
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
    // if(cnt  && (delta == WRITER)) {
    //     INC_TSTAT(rwlock_w_wait);
    // }
}

bool mcs_rwlock::attempt_read()
{
    unsigned int old_value = *&_holders;
    if(old_value & WRITER ||
       !lintel::unsafe::atomic_compare_exchange_strong(const_cast<unsigned int*>(&_holders), &old_value, old_value+READER))
        return false;

    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return true;
}

void mcs_rwlock::acquire_read()
{
    /* attempt to CAS first. If no writers around, or no intervening
     * add'l readers, we're done
     */
    if(!attempt_read()) {
        // INC_TSTAT(rwlock_r_wait);
        /* There seem to be writers around, or other readers intervened in our
         * attempt_read() above.
         * Join the queue and wait for them to leave
         */
        {
            CRITICAL_SECTION(cs, (parent_lock*) this);
            _add_when_writer_leaves(READER);
        }
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
    }
}

void mcs_rwlock::release_read()
{
    w_assert2(has_reader());
    lintel::atomic_thread_fence(lintel::memory_order_release); // flush protected modified data before releasing lock;
    // update and complete any loads by others before I do this write
    lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_holders), READER);
}

bool mcs_rwlock::_attempt_write(unsigned int expected)
{
    /* succeeds iff we are the only reader (if expected==READER)
     * or if there are no readers or writers (if expected==0)
     *
     * How do we know if the only reader is us?
     * A:  we rely on these facts:
     * this is called with expected==READER only from attempt_upgrade(),
     *   which is called from latch only in the case
     *   in which we hold the latch in LATCH_SH mode and
     *   are requesting it in LATCH_EX mode.
     *
     * If there is a writer waiting we have to get in line
     * like everyone else.
     * No need for a memfence because we already hold the latch
     */

// USE_PTHREAD_MUTEX is determined by configure option and
// thus defined in config/shore-config.h
#ifdef USE_PTHREAD_MUTEX
    ext_qnode me = QUEUE_EXT_QNODE_INITIALIZER;
#else
    ext_qnode me;
    QUEUE_EXT_QNODE_INITIALIZE(me);
#endif

    if(*&_holders != expected || !attempt(&me))
        return false;
    // at this point, we've called mcs_lock::attempt(&me), and
    // have acquired the parent/mcs lock
    // The following line replaces our reader bit with a writer bit.
    bool result = lintel::unsafe::atomic_compare_exchange_strong(const_cast<unsigned int*>(&_holders), &expected, WRITER);
    release(me); // parent/mcs lock
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return result;
}

bool mcs_rwlock::attempt_write()
{
    if(!_attempt_write(0))
        return false;

    // moved to end of _attempt_write() lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return true;
}

void mcs_rwlock::acquire_write()
{
    /* always join the queue first.
     *
     * 1. We don't want to race with other writers
     *
     * 2. We don't want to make readers deal with the gap between
     * us updating _holders and actually acquiring the MCS lock.
     */
    CRITICAL_SECTION(cs, (parent_lock*) this);
    _add_when_writer_leaves(WRITER);
    w_assert1(has_writer()); // me!

    // now wait for existing readers to clear out
    if(has_reader()) {
        // INC_TSTAT(rwlock_w_wait);
        _spin_on_readers();
    }

    // done!
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

void mcs_rwlock::release_write() {
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(*&_holders == WRITER);
    *&_holders = 0;
}

bool mcs_rwlock::attempt_upgrade()
{
    w_assert1(has_reader());
    return _attempt_write(READER);
}

void mcs_rwlock::downgrade()
{
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(*&_holders == WRITER);
    *&_holders = READER;
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

