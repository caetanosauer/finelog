#ifndef FINELOG_LATCHES_H
#define FINELOG_LATCHES_H

#include "finelog_basics.h"
#include "AtomicCounter.hpp"
#include <list>
#include <thread>
#include <pthread.h>

/**
 * \enum latch_mode_t
 *
 * Increasing values indicate increasing degrees of privilege; that is lock mode l, l>k,
 * allows at least as many operations as mode k.  I.e., EX > SH > Q > NL.
 *
 * If you alter this, also change the corresponding interface and definition of
 * latch_mode_str.
 */
enum latch_mode_t { LATCH_NL = 0, LATCH_SH = 2, LATCH_EX = 3 };

/// type of a Q mode ticket; exact type and location of definition TBD
typedef int64_t q_ticket_t;

/// Possible results of an acquire call
enum class AcquireResult {
    OK = 0,
    WOULD_BLOCK, // Only valid with timeout_t::WAIT_IMMEDIATE
    TIMEOUT
};

class latch_t;
extern std::ostream &operator<<(std::ostream &, const latch_t &);

/** \brief Indicates a latch is held by this thread.
 *
 *\details Every time we want to grab a latch,
 * we have to create a latch_holder_t.
 * We do that with the holder_search class,
 * which searches a TLS list to make sure  we(this thread)
 * doesn't already hold the latch, and, if not,
 * it creates a new latch_holder_t for the new latch acquisition.
 * It then stuffs the latch_holder_t in the TLS list.
 * If we do already have hold the latch in some capacity,
 * the holder_search returns that existing latch_holder_t.
 * \sa holder_search
 */
class latch_holder_t
{
public:

    static __thread latch_holder_t* thread_local_holders;
    static __thread latch_holder_t* thread_local_freelist;

    latch_t*     _latch;
    latch_mode_t _mode;
    int          _count;
private:
    std::thread::id   _threadid; // REMOVE ME (for debuging)

    // disabled
    latch_holder_t &operator=(latch_holder_t const &other);
public:
    // internal freelist use only!
    latch_holder_t* _prev;
    latch_holder_t* _next;

    latch_holder_t()
    : _latch(NULL), _mode(LATCH_NL), _count(0)
    {
        _threadid = std::this_thread::get_id();
    }

    bool operator==(latch_holder_t const &other) const {
        if(_threadid != other._threadid) return false;
        return _latch == other._latch &&
              _mode == other._mode && _count == other._count;
    }

    void print(std::ostream &o) const;
};

/**\brief Wrapper for pthread mutexes, with a queue-based lock API.
 *
 * When the storage manager is configured with the default,
 * --enable-pthread-mutex, this lock uses a Pthreads mutex for the lock.
 * In this case, it is not a true queue-based lock, since
 * release doesn't inform the next node in the queue, and in fact the
 * nodes aren't kept in a queue.
 * It just gives pthread mutexes the same API as the other
 * queue-based locks so that we use the same idioms for
 * critical sections based on different kinds of locks.
 * By configuring with pthreads mutexes implementing this class, the
 * server can spawn any number of threads, regardless of the number
 * of hardware contexts available; threads will block as necessary.
 *
 * When the storage manager is configured with
 * --disable-pthread-mutex, this lock uses an MCS (\ref MCS1) queue-based
 * lock for the lock.
 * In this case, it is a true queue-based lock.
 * By configuring with MCS locks implementing this class, if the
 * server spawn many more threads than hardware contexts, time can be wasted
 * spinning; threads will not block until the operating system (or underlying
 * thread scheduler) determines to block the thread.
 *
 * The idiom for using these locks is
 * that the qnode is on a threads's stack, so the qnode
 * implicitly identifies the owning thread.
 *
 * This allows us to add an is_mine() capability that otherwise
 * the pthread mutexen don't have.
 *
 * Finally, using this class ensures that the pthread_mutex_init/destroy
 * is done (in the --enable-pthread-mutex case).
 *
 *  See also: \ref REFSYNC
 *
 */
struct w_pthread_lock_t
{
    /**\cond skip */
    struct ext_qnode {
        w_pthread_lock_t* _held;
    };
#define PTHREAD_EXT_QNODE_INITIALIZER { NULL }
#define PTHREAD_EXT_QNODE_INITIALIZE(x) (x)._held =  NULL

    typedef ext_qnode volatile* ext_qnode_ptr;
    /**\endcond skip */

private:
    pthread_mutex_t     _mutex; // w_pthread_lock_t blocks on this
    /// Holder is this struct if acquire is successful.
    w_pthread_lock_t *  _holder;

public:
    w_pthread_lock_t() :_holder(0) { pthread_mutex_init(&_mutex, 0); }

    ~w_pthread_lock_t()
    {
////////////////////////////////////////
// TODO(Restart)... comment out the assertion in debug mode for 'instant restart' testing purpose
//                    if we are using simulated crash shutdown, this assertion might fire if
//                    we are in the middle of taking a checkpoint
//                    this is for mutex chkpt_serial_m::write_release();
//                    need a way to ignore _holder checking if using simulated system crash
//
//                    For now, comment out the assertion, although we might miss other
//                    bugs by comment out the assertion
////////////////////////////////////////

//        w_assert1(!_holder);

        pthread_mutex_destroy(&_mutex);
    }

    /// Returns true if success.
    bool attempt(ext_qnode* me) {
        if(attempt( *me)) {
            me->_held = this;
            _holder = this;
            return true;
        }
        return false;
    }

private:
    /// Returns true if success. Helper for attempt(ext_qnode *).
    bool attempt(ext_qnode & me) {
        w_assert1(!is_mine(&me));
        w_assert0( me._held == 0 );  // had better not
        // be using this qnode for another lock!
        return pthread_mutex_trylock(&_mutex) == 0;
    }

public:
    /// Acquire the lock and set the qnode to refer to this lock.
    void* acquire(ext_qnode* me) {
        w_assert1(!is_mine(me));
        w_assert1( me->_held == 0 );  // had better not
        // be using this qnode for another lock!
        pthread_mutex_lock(&_mutex);
        me->_held = this;
        _holder = this;
#if W_DEBUG_LEVEL > 0
        {
            lintel::atomic_thread_fence(lintel::memory_order_acquire); // needed for the assert
            w_assert1(is_mine(me)); // TODO: change to assert2
        }
#endif
        return 0;
    }

    /// Release the lock and clear the qnode.
    void release(ext_qnode &me) { release(&me); }

    /// Release the lock and clear the qnode.
    void release(ext_qnode_ptr me) {
        // assert is_mine:
        w_assert1( _holder == me->_held );
        w_assert1(me->_held == this);
         me->_held = 0;
        _holder = 0;
        pthread_mutex_unlock(&_mutex);
#if W_DEBUG_LEVEL > 10
        // This is racy since the containing structure could
        // have been freed by the time we do this check.  Thus,
        // we'll remove it.
        {
            lintel::atomic_thread_fence(lintel::memory_order_acquire);// needed for the assertions?
            w_pthread_lock_t *h =  _holder;
            w_pthread_lock_t *m =  me->_held;
            w_assert1( (h==NULL && m==NULL)
                || (h  != m) );
        }
#endif
    }

    /**\brief Return true if this thread holds the lock.
     *
     * This method doesn't actually check for this pthread
     * holding the lock, but it checks that the qnode reference
     * is to this lock.
     * The idiom for using these locks is
     * that the qnode is on a threads's stack, so the qnode
     * implicitly identifies the owning thread.
     */

    bool is_mine(ext_qnode* me) const {
       if( me->_held == this ) {
           // only valid if is_mine
          w_assert1( _holder == me->_held );
          return true;
       }
       return false;
    }
};

/**\def USE_PTHREAD_MUTEX
 * \brief If defined and value is 1, use pthread-based mutex for queue_based_lock_t
 *
 * \details
 * The Shore-MT release contained alternatives for scalable locks in
 * certain places in the storage manager; it was released with
 * these locks replaced by pthreads-based mutexes.
 *
 * You can disable the use of pthreads-based mutexes and use the
 * mcs-based locks by configuring with --disable-pthread-mutex.
 */

/**\defgroup SYNCPRIM Synchronization Primitives
 *\ingroup UNUSED
 *
 * sthread/sthread.h: As distributed, a queue-based lock
 * is a w_pthread_lock_t,
 * which is a wrapper around a pthread lock to give it a queue-based-lock API.
 * True queue-based locks are not used, nor are time-published
 * locks.
 * Code for these implementations is included for future
 * experimentation, along with typedefs that should allow
 * easy substitution, as they all should have the same API.
 *
 * We don't offer the spin implementations at the moment.
 */
/*
 * These typedefs are included to allow substitution at some  point.
 * Where there is a preference, the code should use the appropriate typedef.
 */

typedef w_pthread_lock_t queue_based_block_lock_t; // blocking impl always ok
#define QUEUE_BLOCK_EXT_QNODE_INITIALIZER PTHREAD_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_BLOCK_EXT_QNODE_INITIALIZE(x) x._held = NULL

// csauer: was defined statically in CMakeLists.txt
#define USE_PTHREAD_MUTEX 1

#ifdef USE_PTHREAD_MUTEX
typedef w_pthread_lock_t queue_based_spin_lock_t; // spin impl preferred
typedef w_pthread_lock_t queue_based_lock_t; // might want to use spin impl
#define QUEUE_SPIN_EXT_QNODE_INITIALIZER PTHREAD_EXT_QNODE_INITIALIZER
#define QUEUE_EXT_QNODE_INITIALIZER      PTHREAD_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_EXT_QNODE_INITIALIZE(x) x._held = NULL;
#else
typedef mcs_lock queue_based_spin_lock_t; // spin preferred
typedef mcs_lock queue_based_lock_t;
#define QUEUE_SPIN_EXT_QNODE_INITIALIZER MCS_EXT_QNODE_INITIALIZER
#define QUEUE_EXT_QNODE_INITIALIZER      MCS_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_EXT_QNODE_INITIALIZE(x) MCS_EXT_QNODE_INITIALIZE(x)
#endif


/**\brief Shore read-write lock:: many-reader/one-writer spin lock
 *
 * This read-write lock is implemented around a queue-based lock. It is
 * the basis for latches in the storage manager.
 *
 * Use this to protect data structures that get constantly hammered by
 * short reads, and less frequently (but still often) by short writes.
 *
 * "Short" is the key word here, since this is spin-based.
 */
class mcs_rwlock : protected queue_based_lock_t
{
    typedef queue_based_lock_t parent_lock;

    /* \todo  TODO: Add support for blocking if any of the spins takes too long.
     *
       There are three spins to worry about: spin_on_writer,
       spin_on_reader, and spin_on_waiting

       The overall idea is that threads which decide to block lose
       their place in line to avoid forming convoys. To make this work
       we need to modify the spin_on_waiting so that it blocks
       eventually; the mcs_lock's preemption resistance will take care
       of booting it from the queue as necessary.

       Whenever the last reader leaves it signals a cond var; when a
       writer leaves it broadcasts.
       END TODO
     */
    unsigned int volatile _holders; // 2*readers + writer

public:
    enum rwmode_t { NONE=0, WRITER=0x1, READER=0x2 };
    mcs_rwlock() : _holders(0) { }
    ~mcs_rwlock() {}

    /// Return the mode in which this lock is held by anyone.
    rwmode_t mode() const { int holders = *&_holders;
        return (holders == WRITER)? WRITER : (holders > 0) ? READER : NONE; }

    /// True if locked in any mode.
    bool is_locked() const { return (*&_holders)==0?false:true; }

    /// 1 if held in write mode, else it's the number of readers
    int num_holders() const { int holders = *&_holders;
                              return (holders == WRITER)? 1 : holders/2; }

    /// True iff has one or more readers.
    bool has_reader() const { return *&_holders & ~WRITER; }
    /// True iff has a writer (never more than 1)
    bool has_writer() const { return *&_holders & WRITER; }

    /// True if success.
    bool attempt_read();
    /// Wait (spin) until acquired.
    void acquire_read();
    /// This thread had better hold the lock in read mode.
    void release_read();

    /// True if success.
    bool attempt_write();
    /// Wait (spin) until acquired.
    void acquire_write();
    /// This thread had better hold the lock in write mode.
    void release_write();
    /// Try to upgrade from READ to WRITE mode. Fail if any other threads are waiting.
    bool attempt_upgrade();
    /// Atomically downgrade the lock from WRITE to READ mode.
    void downgrade();

private:
    // CC mangles this as __1cKmcs_rwlockO_spin_on_writer6M_v_
    int  _spin_on_writer();
    // CC mangles this as __1cKmcs_rwlockP_spin_on_readers6M_v_
    void _spin_on_readers();
    bool _attempt_write(unsigned int expected);
    void _add_when_writer_leaves(int delta);
};

typedef mcs_rwlock srwlock_t;

#include <iosfwd>

/**\brief A short-term hold (exclusive or shared) on a page.
 *
 * \details
 * A latch may be acquire()d multiple times by a single thread.
 * The mode of subsequent acquire()s must be at or above the level
 * of the currently held latch.
 * Each of these individual locks must be released.
 * \sa latch_holder_t
 */
class latch_t /*: public sthread_named_base_t*/ {

public:
    /// Create a latch
    latch_t();
    ~latch_t();

    // Dump latch info to the ostream. Not thread-safe.
    std::ostream&                print(std::ostream &) const;

    // Return a unique id for the latch.For debugging.
    inline const void *     id() const { return &_lock; }

    /// Change the name of the latch.
    inline void             setname(const char *const desc);

    /// Acquire the latch in given mode. \sa  timeout_t.
    AcquireResult                  latch_acquire(
                                latch_mode_t             m,
                                int timeout = timeout_t::WAIT_FOREVER);
    /**\brief Upgrade from SH to EX if it can be done w/o blocking.
     * \details Returns bool indicating if it would have blocked, in which
     * case the upgrade did not occur. If it didn't have to block, the
     * upgrade did occur.
     * \note Does \b not increment the count.
     */
    void                  upgrade_if_not_block(bool& would_block);

    /**\brief Convert atomically an EX latch into an SH latch.
     * \details
     * Does not decrement the latch count.
     */
    void                    downgrade();

    /**\brief release the latch.
     * \details
     * Decrements the latch count and releases only when
	 * it hits 0.
	 * Returns the resulting latch count.
     */
    int                     latch_release();
    /**\brief Unreliable, but helpful for some debugging.
     */
    bool                    is_latched() const;

    /*
     * GNATS 30 fix: changes lock_cnt name to latch_cnt,
     * and adds _total_cnt to the latch structure itself so it can
     * keep track of the total #holders
     * This is an additional cost, but it is a great debugging aid.
     * \todo TODO: get rid of BUG_LATCH_SEMANTICS_FIX: replace with gnats #
     */

    /// Number of acquires.  A thread may hold more than once.
    int                     latch_cnt() const {return _total_count;}

    /// How many threads hold the R/W lock.
    int                     num_holders() const;
    ///  True iff held in EX mode.
    bool                    is_mine() const; // only if ex
    ///  True iff held in EX or SH mode.  Actually, it returns the
	//latch count (# times this thread holds the latch).
    int                     held_by_me() const; // sh or ex
    ///  EX,  SH, or NL (if not held at all).
    latch_mode_t            mode() const;

    /// string names of modes.
    static const char* const    latch_mode_str[4];

private:
    // found, iterator
    AcquireResult                _acquire(latch_mode_t m,
                                 int timeout_in_ms,
                                 latch_holder_t* me);
	// return #times this thread holds the latch after this release
    int                   _release(latch_holder_t* me);
    void                  _downgrade(latch_holder_t* me);

/*
 * Note: the problem with #threads and #cpus and os preemption is real.
 * And it causes things to hang; and it's hard to debug, in the sense that
 * using pthread facilities gives thread-analysis tools and debuggers
 * understood-things with which to work.
 * Consequently, we use w_pthread_rwlock for our lock.
 */
    mutable srwlock_t           _lock;

    // disabled
    latch_t(const latch_t&);
    latch_t&                     operator=(const latch_t&);

    uint32_t            _total_count;
};

inline bool
latch_t::is_latched() const
{
    /* NOTE: Benign race -- this function is naturally unreliable, as
     * its return value may become invalid as soon as it is
     * generated. The only way to reliably know if the lock is held at
     * a particular moment is to hold it yourself, which defeats the
     * purpose of asking in the first place...
     * ... except for assertions / debugging... since there are bugs
     * in acquire/release of latches
     */
    return _lock.is_locked();
}

inline int
latch_t::num_holders() const
{
    return _lock.num_holders();
}


inline latch_mode_t
latch_t::mode() const
{
    switch(_lock.mode()) {
    case mcs_rwlock::NONE: return LATCH_NL;
    case mcs_rwlock::WRITER: return LATCH_EX;
    case mcs_rwlock::READER: return LATCH_SH;
    default: w_assert1(0); // shouldn't ever happen
             return LATCH_SH; // keep compiler happy
    }
}

// unsafe: for use in debugger:
extern "C" void print_my_latches();
extern "C" void print_all_latches();
extern "C" void print_latch(const latch_t *l);

/**\brief A multiple-reader/single-writer lock based on pthreads (blocking)
 *
 * Use this to protect data structures that get hammered by
 *  reads and where updates are very rare.
 * It is used in the storage manager by the histograms (histo.cpp),
 * and in place of some mutexen, where strict exclusion isn't required.
 *
 * This lock is used in the storage manager by the checkpoint thread
 * (the only acquire-writer) and other threads to be sure they don't
 * do certain nasty things when a checkpoint is going on.
 *
 * The idiom for using these locks is
 * that the qnode is on a threads's stack, so the qnode
 * implicitly identifies the owning thread.
 *
 *  See also: \ref REFSYNC
 *
 */
struct occ_rwlock {
    occ_rwlock();
    ~occ_rwlock();
    /// The normal way to acquire a read lock.
    void acquire_read();
    /// The normal way to release a read lock.
    void release_read();
    /// The normal way to acquire a write lock.
    void acquire_write();
    /// The normal way to release a write lock.
    void release_write();

    /**\cond skip */
    /// Exposed for critical_section<>. Do not use directly.
    struct occ_rlock {
        occ_rwlock* _lock;
        void acquire() { _lock->acquire_read(); }
        void release() { _lock->release_read(); }
    };
    /// Exposed for critical_section<>. Do not use directly.
    struct occ_wlock {
        occ_rwlock* _lock;
        void acquire() { _lock->acquire_write(); }
        void release() { _lock->release_write(); }
    };

    /// Exposed for the latch manager.. Do not use directly.
    occ_rlock *read_lock() { return &_read_lock; }
    /// Exposed for the latch manager.. Do not use directly.
    occ_wlock *write_lock() { return &_write_lock; }
    /**\endcond skip */
private:
    enum { WRITER=1, READER=2 };
    unsigned int volatile _active_count;
    occ_rlock _read_lock;
    occ_wlock _write_lock;

    pthread_mutex_t _read_write_mutex; // paired w/ _read_cond, _write_cond
    pthread_cond_t _read_cond; // paired w/ _read_write_mutex
    pthread_cond_t _write_cond; // paired w/ _read_write_mutex
};

#define MUTEX_ACQUIRE(mutex)    W_COERCE((mutex).acquire());
#define MUTEX_RELEASE(mutex)    (mutex).release();
#define MUTEX_IS_MINE(mutex)    (mutex).is_mine()

/** Scoped objects to automatically acquire srwlock_t/mcs_rwlock. */
class spinlock_read_critical_section {
public:
    spinlock_read_critical_section(srwlock_t *lock) : _lock(lock) {
        _lock->acquire_read();
    }
    ~spinlock_read_critical_section() {
        _lock->release_read();
    }
private:
    srwlock_t *_lock;
};

class spinlock_write_critical_section {
public:
    spinlock_write_critical_section(srwlock_t *lock) : _lock(lock) {
        _lock->acquire_write();
    }
    ~spinlock_write_critical_section() {
        _lock->release_write();
    }
private:
    srwlock_t *_lock;
};

/**
 * Used to access qnode's _waiting and _delegated together
 * \b regardless \b of \b endianness.
 */
union qnode_status {
    struct {
        int32_t _waiting;
        int32_t _delegated;
    } individual;
    int64_t _combined;
};
const qnode_status QNODE_IDLE = {{0, 0}};
const qnode_status QNODE_WAITING = {{1, 0}};
const qnode_status QNODE_DELEGATED = {{1, 1}};

/**\brief An MCS queuing spinlock.
 *
 * Useful for short, contended critical sections.
 * If contention is expected to be rare, use a
 * tatas_lock;
 * if critical sections are long, use pthread_mutex_t so
 * the thread can block instead of spinning.
 *
 * Tradeoffs are:
   - test-and-test-and-set locks: low-overhead but not scalable
   - queue-based locks: higher overhead but scalable
   - pthread mutexes : high overhead and blocks, but frees up cpu for other threads
*/
struct mcs_lock {
    struct qnode;
    struct qnode {
        qnode*  _next;
        qnode_status _status;
        // int32_t _waiting;
        // int32_t _delegated;
        qnode volatile* vthis() { return this; }
    };
    struct ext_qnode {
        qnode _node;
        mcs_lock* _held;
        operator qnode*() { return &_node; }
    };
#define MCS_EXT_QNODE_INITIALIZER {{NULL,false},NULL}
#define MCS_EXT_QNODE_INITIALIZE(x) \
{ (x)._node._next = NULL; (x)._node._waiting = 0; (x)._node._delegated = 0; (x)._held = NULL; }
    qnode* _tail;
    mcs_lock() : _tail(NULL) { }

    /* This spinning occurs whenever there are critical sections ahead
       of us.
    */
    void spin_on_waiting(qnode* me) {
        while(me->vthis()->_status.individual._waiting);
    }
    /* Only acquire the lock if it is free...
     */
    bool attempt(ext_qnode* me) {
        if(attempt((qnode*) me)) {
            me->_held = this;
            return true;
        }
        return false;
    }
    bool attempt(qnode* me) {
        me->_next = NULL;
        me->_status.individual._waiting = 1;
        // lock held?
        qnode* null_cas_tmp = NULL;
        if(!lintel::unsafe::atomic_compare_exchange_strong<qnode*>(
            &_tail, &null_cas_tmp, (qnode*) me))
            return false;
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return true;
    }
    // return true if the lock was free
    void* acquire(ext_qnode* me) {
        me->_held = this;
        return acquire((qnode*) me);
    }
    void* acquire(qnode* me) {
        return __unsafe_end_acquire(me, __unsafe_begin_acquire(me));
    }

    qnode* __unsafe_begin_acquire(qnode* me) {
        me->_next = NULL;
        me->_status.individual._waiting = 1;
        qnode* pred = lintel::unsafe::atomic_exchange<qnode*>(&_tail, me);
        if(pred) {
            pred->_next = me;
        }
        return pred;
    }
    void* __unsafe_end_acquire(qnode* me, qnode* pred) {
        if(pred) {
            spin_on_waiting(me);
        }
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        return (void*) pred;
    }

    /* This spinning only occurs when we are at _tail and catch a
       thread trying to enqueue itself.

       CC mangles this as __1cImcs_lockMspin_on_next6Mpon0AFqnode__3_
    */
    qnode* spin_on_next(qnode* me) {
        qnode* next;
        while(!(next=me->vthis()->_next));
        return next;
    }
    void release(ext_qnode *me) {
        w_assert1(is_mine(me));
        me->_held = 0; release((qnode*) me);
    }
    void release(ext_qnode &me) { w_assert1(is_mine(&me)); release(&me); }
    void release(qnode &me) { release(&me); }
    void release(qnode* me) {
        lintel::atomic_thread_fence(lintel::memory_order_release);

        qnode* next;
        if(!(next=me->_next)) {
            qnode* me_cas_tmp = me;
            if(me == _tail &&
                lintel::unsafe::atomic_compare_exchange_strong<qnode*>(&_tail, &me_cas_tmp, (qnode*) NULL)) {
                return;
            }
            next = spin_on_next(me);
        }
        next->_status.individual._waiting = 0;
    }
    // bool is_mine(qnode* me) { return me->_held == this; }
    bool is_mine(ext_qnode* me) { return me->_held == this; }
};

/** Used to keep mcs_lock in its own cacheline. */
const size_t CACHELINE_MCS_PADDING = CACHELINE_SIZE - sizeof(mcs_lock);


#if MUTRACE_ENABLED_H
#include <MUTrace/mutrace.h>
#endif // MUTRACE_ENABLED_H

// csauer: replace old SIZEOF_PTHREAD_T
static_assert(sizeof(pthread_t) == 8, "Wrong pthread_t size");

/**\brief A test-and-test-and-set spinlock.
 *
 * This lock is good for short, uncontended critical sections.
 * If contention is high, use an mcs_lock.
 * Long critical sections should use pthread_mutex_t.
 *
 * Tradeoffs are:
 *  - test-and-test-and-set locks: low-overhead but not scalable
 *  - queue-based locks: higher overhead but scalable
 *  - pthread mutexes : very high overhead and blocks, but frees up
 *  cpu for other threads when number of cpus is fewer than number of threads
 *
 *  \sa REFSYNC
 */
struct tatas_lock {
    /**\cond skip */
    enum { NOBODY=0 };
    typedef union  {
        pthread_t         handle;
        uint64_t           bits;
    } holder_type_t;
    volatile holder_type_t _holder;
    /**\endcond skip */

#ifdef MUTRACE_ENABLED_H
    MUTRACE_PROFILE_MUTEX_CONSTRUCTOR(tatas_lock) { _holder.bits=NOBODY; }
#else
    tatas_lock() { _holder.bits=NOBODY; }
#endif

private:
    // CC mangles this as __1cKtatas_lockEspin6M_v_
    /// spin until lock is free
    void spin() { while(*&(_holder.handle)) ; }

public:
    /// Try to acquire the lock immediately.
    bool try_lock()
    {
        holder_type_t tid = { pthread_self() };
        bool success = false;
        uint64_t old_holder = NOBODY;
        if(lintel::unsafe::atomic_compare_exchange_strong(const_cast<uint64_t*>(&_holder.bits), &old_holder, tid.bits)) {
            lintel::atomic_thread_fence(lintel::memory_order_acquire);
            success = true;
        }
        return success;
    }

    /// Acquire the lock, spinning as long as necessary.
#ifdef MUTRACE_ENABLED_H
    MUTRACE_PROFILE_MUTEX_LOCK_VOID(tatas_lock, void, acquire, try_lock)
#else
    void acquire()
#endif
    {
        w_assert1(!is_mine());
        holder_type_t tid = { pthread_self() };
        uint64_t old_holder = NOBODY;
        do {
            spin();
	        old_holder = NOBODY; // a CAS that fails overwrites old_holder with the current holder
        } while(!lintel::unsafe::atomic_compare_exchange_strong(const_cast<uint64_t*>(&_holder.bits), &old_holder, tid.bits));
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
        w_assert1(is_mine());
    }

    /// Release the lock
#ifdef MUTRACE_ENABLED_H
    MUTRACE_PROFILE_MUTEX_UNLOCK_VOID(tatas_lock, void, release)
#else
    void release()
#endif
    {
        lintel::atomic_thread_fence(lintel::memory_order_release);
        w_assert1(is_mine()); // moved after the fence
        _holder.bits= NOBODY;
#if W_DEBUG_LEVEL > 0
        {
            lintel::atomic_thread_fence(lintel::memory_order_acquire); // needed for the assert?
            w_assert1(!is_mine());
        }
#endif
    }

    /// True if this thread is the lock holder
    bool is_mine() const { return
        pthread_equal(_holder.handle, pthread_self()) ? true : false; }
#undef CASFUNC
};
/** Scoped objects to automatically acquire tatas_lock. */
class tataslock_critical_section {
public:
    tataslock_critical_section(tatas_lock *lock) : _lock(lock) {
        _lock->acquire();
    }
    ~tataslock_critical_section() {
        _lock->release();
    }
private:
    tatas_lock *_lock;
};

/** Used to keep tatas_lock in its own cacheline. */
const size_t CACHELINE_TATAS_PADDING = CACHELINE_SIZE - sizeof(tatas_lock);

/**\def CRITICAL_SECTION(name, lock)
 *
 * This macro starts a critical section protected by the given lock
 * (2nd argument).  The critical_section structure it creates is
 * named by the 1st argument.
 * The rest of the scope (in which this macro is used) becomes the
 * scope of the critical section, since it is the destruction of this
 * critical_section structure that releases the lock.
 *
 * The programmer can release the lock early by calling \<name\>.pause()
 * or \<name\>.exit().
 * The programmer can reacquire the lock by calling \<name\>.resume() if
 * \<name\>.pause() was called, but not after \<name\>.exit().
 *
 * \sa critical_section
 */
#define CRITICAL_SECTION(name, lock) critical_section<__typeof__(lock)&> name(lock)

template<class Lock>
struct critical_section;

/**\brief Helper class for CRITICAL_SECTION idiom (macro).
 *
 * This templated class does nothing; its various specializations
 * do the work of acquiring the given lock upon construction and
 * releasing it upon destruction.
 * See the macros:
 * - SPECIALIZE_CS(Lock, Extra, ExtraInit, Acquire, Release)
 * - CRITICAL_SECTION(name, lock)
 */
template<class Lock>
struct critical_section<Lock*&> : public critical_section<Lock&> {
    critical_section<Lock*&>(Lock* mutex) : critical_section<Lock&>(*mutex) { }
};

/*
 * NOTE: I added ExtraInit to make the initialization happen so that
 * assertions about holding the mutex don't fail.
 * At the same time, I added a holder to the w_pthread_lock_t
 * implementation so I could make assertions about the holder outside
 * the lock implementation itself.  This might seem like doubly
 * asserting, but in the cases where the critical section isn't
 * based on a pthread mutex, we really should have this clean
 * initialization and the check the assertions.
 */

/**\def SPECIALIZE_CS(Lock, Extra, ExtraInit, Acquire, Release)
 * \brief Macro that enables use of CRITICAL_SECTION(name,lock)
 *\addindex SPECIALIZE_CS
 *
 * \details
 * Create a templated class that holds
 *   - a reference to the given lock and
 *   - the Extra (2nd macro argument)
 *
 *  and it
 *   - applies the ExtraInit and Acquire commands upon construction,
 *   - applies the Release command upon destruction.
 *
 */
#define SPECIALIZE_CS(Lock,Extra,ExtraInit,Acquire,Release) \
template<>  struct critical_section<Lock&> { \
critical_section(Lock &mutex) \
    : _mutex(&mutex)          \
    {   ExtraInit; Acquire; } \
    ~critical_section() {     \
        if(_mutex)            \
            Release;          \
            _mutex = NULL;    \
        }                     \
    void pause() { Release; } \
    void resume() { Acquire; }\
    void exit() { Release; _mutex = NULL; } \
    Lock &hand_off() {        \
        Lock* rval = _mutex;  \
        _mutex = NULL;        \
        return *rval;         \
    }                         \
private:                      \
    Lock* _mutex;             \
    Extra;                    \
    void operator=(critical_section const &);   \
    critical_section(critical_section const &); \
}


// I undef-ed this and found all occurrances of CRITICAL_SECTION with this.
// and hand-checked them.
SPECIALIZE_CS(pthread_mutex_t, int _dummy,  (_dummy=0),
    pthread_mutex_lock(_mutex), pthread_mutex_unlock(_mutex));

// tatas_lock doesn't have is_mine, but I changed its release()
// to Release and through compiling saw everywhere that uses release,
// and fixed those places
SPECIALIZE_CS(tatas_lock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

// queue_based_lock_t asserts is_mine() in release()
SPECIALIZE_CS(w_pthread_lock_t, w_pthread_lock_t::ext_qnode _me, (_me._held=0),
    _mutex->acquire(&_me), _mutex->release(&_me));
#ifndef USE_PTHREAD_MUTEX
SPECIALIZE_CS(mcs_lock, mcs_lock::ext_qnode _me, (_me._held=0),
    _mutex->acquire(&_me), _mutex->release(&_me));
#endif

SPECIALIZE_CS(occ_rwlock::occ_rlock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

SPECIALIZE_CS(occ_rwlock::occ_wlock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

#endif
