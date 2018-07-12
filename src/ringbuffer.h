#ifndef FINELOG_RINGBUFFER_H
#define FINELOG_RINGBUFFER_H

#include "finelog_basics.h"

#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

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
    bool isFinished() { return finished.load(); }

    AsyncRingBuffer(size_t bsize, size_t bcount)
        : begin(0), end(0), bparity(true), eparity(true),
        blockSize(bsize), blockCount(bcount)
    {
        buf = new char[blockCount * blockSize];
    }

    ~AsyncRingBuffer()
    {
        delete[] buf;
    }

private:
    char * buf;
    int begin;
    int end;
    bool bparity;
    bool eparity;
    std::atomic<bool> finished{false};

    const size_t blockSize;
    const size_t blockCount;

    std::mutex mtx;
    std::condition_variable cond;

    bool wait(pthread_cond_t*, bool isProducer);

    void increment(int& p, bool& parity) {
        p = (p + 1) % blockCount;
        if (p == 0) {
            parity = !parity;
        }
    }
};

inline char* AsyncRingBuffer::producerRequest()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lck{mtx};
    while (isFull() && !finished) {
        cond.wait_for(lck, 100ms, [this] { return finished || !isFull(); });
    }
    if (finished) {
        return NULL;
    }
    return buf + (end * blockSize);
}

inline void AsyncRingBuffer::producerRelease()
{
    std::unique_lock<std::mutex> lck{mtx};
    bool wasEmpty = isEmpty();
    increment(end, eparity);

    if (wasEmpty) {
        cond.notify_one();
    }
}

inline char* AsyncRingBuffer::consumerRequest()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lck{mtx};
    while (isEmpty() && !finished) {
        cond.wait_for(lck, 100ms, [this] { return finished || !isEmpty(); });
    }
    // Consumer doesn't finish until the queue is empty
    if (finished && isEmpty()) {
        return NULL;
    }
    return buf + (begin * blockSize);
}

inline void AsyncRingBuffer::consumerRelease()
{
    std::unique_lock<std::mutex> lck{mtx};
    bool wasFull = isFull();
    increment(begin, bparity);

    if (wasFull) {
        cond.notify_one();
    }
}

#endif
