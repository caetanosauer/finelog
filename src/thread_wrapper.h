#ifndef THREAD_WRAPPER_H
#define THREAD_WRAPPER_H

#include <thread>
#include <memory>

#include "w_rc.h"
#include "latch.h"

/*
 * The sole purpose of this class is to replace sthread_t with as little code impact as
 * possible -- new code should use the C++11 thread library directly (as soon as the TODO
 * below about tls_manager is fixed...)
 */
class thread_wrapper_t
{
public:

    thread_wrapper_t()
    {
    }

    virtual ~thread_wrapper_t()
    {
        thread_ptr.reset();
    }

    /*
     * Virtual methods to be overridden by sub-classes.
     */
    virtual void run() = 0;
    virtual void before_run() {};
    virtual void after_run() {};

    void spawn()
    {
        // csauer: Thread list was used for TSTATS only
        // smthread_t::add_me_to_thread_list();

        before_run();
        run();
        after_run();

        // save my stats before leaving
        // csauer: TSTATS not used in finelog
        // smlevel_0::add_to_global_stats(smthread_t::TL_stats()); // before detaching them
        // smthread_t::remove_me_from_thread_list();

        // latch_t maintains some static data structures that must be deleted manually
        latch_t::on_thread_destroy();
    }

    void fork()
    {
        thread_ptr.reset(new std::thread ([this] { spawn(); }));
    }

    void join()
    {
        if (thread_ptr) {
            thread_ptr->join();
            thread_ptr = nullptr;
        }
    }

private:
    std::unique_ptr<std::thread> thread_ptr;
};

#endif
