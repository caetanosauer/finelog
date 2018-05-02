#ifndef RINGBUFFER_H
#define RINGBUFFER_H

#include "w_defines.h"
#include "basics.h"
#include "w_debug.h"
#include "latches.h"

/**
 * Simple implementation of a circular IO buffer for the archiver reader,
 * which reads log records from the recovery log, and the archiver writer,
 * which writes sorted runs to output files. Despite being designed for
 * log archiving, it should support more general use cases.
 *
 * The buffer supports only one consumer and one producer, which makes
 * synchronization much simpler. In the case of a read buffer, the producer
 * is the reader thread and the consumer is the sorting thread (with the
 * log scanner), which inserts log records into the heap for replacement
 * selection. In a write buffer, the producer is the sorting thread, while
 * the consumer writes blocks to the current run.
 *
 * The allocation of buffer blocks (by both producers and consumers)
 * must be done in two stages:
 * 1) Request a block, waiting on a condvar if buffer is empty/full
 * 2) Once done, release it to the other producer/consumer thread
 *
 * Requests could be implemented in a single step if we copy
 * the block to the caller's memory, but we want to avoid that.
 * Again, this assumes that only one thread is consuming and one is
 * releasing, and that they request and release blocks in an ordered
 * behavior.
 *
 * Author: Caetano Sauer
 *
 */
class AsyncRingBuffer {
public:
    char* producerRequest();
    void producerRelease();
    char* consumerRequest();
    void consumerRelease();

    bool isFull() { return begin == end && bparity != eparity; }
    bool isEmpty() { return begin == end && bparity == eparity; }
    size_t getBlockSize() { return blockSize; }
    size_t getBlockCount() { return blockCount; }
    void set_finished(bool f = true) { finished = f; }
    bool* get_finished() { return &finished; } // not thread-safe
    bool isFinished(); // thread-safe


    AsyncRingBuffer(size_t bsize, size_t bcount)
        : begin(0), end(0), bparity(true), eparity(true),
        finished(false), blockSize(bsize), blockCount(bcount)
    {
        buf = new char[blockCount * blockSize];
        // CS TODO replace with <mutex>
        DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
        DO_PTHREAD(pthread_cond_init(&notEmpty, NULL));
        DO_PTHREAD(pthread_cond_init(&notFull, NULL));
    }

    ~AsyncRingBuffer()
    {
        delete buf;
        DO_PTHREAD(pthread_mutex_destroy(&mutex));
        DO_PTHREAD(pthread_cond_destroy(&notEmpty));
        DO_PTHREAD(pthread_cond_destroy(&notFull));
    }

private:
    char * buf;
    int begin;
    int end;
    bool bparity;
    bool eparity;
    bool finished;

    const size_t blockSize;
    const size_t blockCount;

    pthread_mutex_t mutex;
    pthread_cond_t notEmpty;
    pthread_cond_t notFull;

    bool wait(pthread_cond_t*, bool isProducer);

    void increment(int& p, bool& parity) {
        p = (p + 1) % blockCount;
        if (p == 0) {
            parity = !parity;
        }
    }
};

// timeout given in ms (csauer: copied from smthread.cpp)
void timeout_to_timespec(int timeout, struct timespec &when)
{
    w_assert1(timeout != timeout_t::WAIT_IMMEDIATE);
    w_assert1(timeout != timeout_t::WAIT_FOREVER);
    if(timeout > 0) {
	::clock_gettime(CLOCK_REALTIME, &when);
	when.tv_nsec += (uint64_t) timeout * 1000000;
	when.tv_sec += when.tv_nsec / 1000000000;
	when.tv_nsec = when.tv_nsec % 1000000000;
    }
}

inline bool AsyncRingBuffer::wait(pthread_cond_t* cond, bool isProducer)
{
    struct timespec timeout;
    timeout_to_timespec(100, timeout); // 100ms
    // caller must have locked mutex!
    int code = pthread_cond_timedwait(cond, &mutex, &timeout);
    if (code == ETIMEDOUT) {
        //DBGTHRD(<< "Wait timed out -- try again");
        if (finished && (isEmpty() || isProducer)) {
            DBGTHRD(<< "Wait aborted: finished flag is set");
            return false;
        }
    }
    if (code != ETIMEDOUT) {
        DO_PTHREAD(code);
    }
    return true;
}

inline char* AsyncRingBuffer::producerRequest()
{
    // CS TODO: use unique_lock
    CRITICAL_SECTION(cs, mutex);
    while (isFull()) {
        DBGTHRD(<< "Waiting for condition notFull ...");
        constexpr bool isProducer = true;
        if (!wait(&notFull, isProducer)) {
            DBGTHRD(<< "Produce request failed!");
            return NULL;
        }
    }
    DBGTHRD(<< "Producer request: block " << end);
    return buf + (end * blockSize);
}

inline void AsyncRingBuffer::producerRelease()
{
    CRITICAL_SECTION(cs, mutex);
    bool wasEmpty = isEmpty();
    increment(end, eparity);
    DBGTHRD(<< "Producer release, new end is " << end);

    if (wasEmpty) {
        DBGTHRD(<< "Signal buffer not empty");
        DO_PTHREAD(pthread_cond_signal(&notEmpty));
    }
}

inline char* AsyncRingBuffer::consumerRequest()
{
    CRITICAL_SECTION(cs, mutex);
    while (isEmpty()) {
        DBGTHRD(<< "Waiting for condition notEmpty ...");
        constexpr bool isProducer = false;
        if (!wait(&notEmpty, isProducer)) {
            DBGTHRD(<< "Consume request failed!");
            return NULL;
        }
    }
    DBGTHRD(<< "Consumer request: block " << begin);
    return buf + (begin * blockSize);
}

inline void AsyncRingBuffer::consumerRelease()
{
    CRITICAL_SECTION(cs, mutex);
    bool wasFull = isFull();
    increment(begin, bparity);
    DBGTHRD(<< "Consumer release, new begin is " << begin);

    if (wasFull) {
        DBGTHRD(<< "Signal buffer not full");
        DO_PTHREAD(pthread_cond_signal(&notFull));
    }
}

inline bool AsyncRingBuffer::isFinished()
{
    /*
     * Acquiring the mutex here is done to ensure consistency
     * of modifications to the finished flag. It creates a memory
     * fence to ensure the correct order of operations in the case
     * where one thread reading the flag after another one has set it.
     *
     * Caution! This does not mean that there are no blocks left for
     * consumption---just that someone set the finished flag. The former
     * case must be checked by calling producerRequest()
     */
    CRITICAL_SECTION(cs, mutex);
    return finished;
}

#endif
