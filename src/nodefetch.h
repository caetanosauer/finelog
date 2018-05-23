#pragma once
//--------------------------------------------------------------------------------
#include <memory>
#include "logarchive_scanner.h"
//--------------------------------------------------------------------------------
/*
 * A log-record iterator that encapsulates a log archive scan and a recovery
 * log scan. It reads from the former until it runs out, after which it reads
 * from the latter, which is collected by following the per-page chain in the
 * recovery log.
 */
template <typename Redoer>
class NodeFetch
{
public:
   NodeFetch(std::shared_ptr<ArchiveIndex> archIndex)
      : archive_scan{archIndex}, img_consumed{false}
   {
   }

   ~NodeFetch() = default;

   template <typename NodeID>
   void open(NodeID id)
   {
       archive_scan.open(id, id+1);
       img_consumed = false;
   }

   template <typename Node>
   void apply(Node& node)
   {
       logrec_t* lr;
       unsigned replayed = 0; // used for debugging

       while (archive_scan.next(lr)) {
           redo(node, lr);
           replayed++;
       }
   }

   // Required for eviction of pages with updates not yet archived (FineLine)
   template <typename NodeID>
   void reopen(NodeID id)
   {
       archive_scan.open(id, id+1, getLastProbedRun()+1);
   }

   run_number_t getLastProbedRun() const { return archive_scan.getLastProbedRun(); }

private:

   bool shouldRedo(const logrec_t* lr)
   {
       assert(lr->valid_header());
       assert(lr->is_redo());
       assert(lr->page_version() > 0);
       // TODO: only assert this if Node type is actually a page
       // assert(p.pid() == lr->pid());
       // assert(lr->has_page_img() || p.version() > 0);
       // assert(p.version() < lr->page_version());

       // This is a hack to circumvent a problem with page-img compression. Since it is an SSX,
       // it may appear in the log before a page update with lower version. Usually, that's not a
       // problem, because the updates will be ordered by version when scanning. But, in the
       // special case where the lower update ends up in the next log file, it will not be
       // eliminated by page-img compression, and thus NodeFetch will not see the page image
       // as the first log record. This is fixed with the check below.
       if (lr->has_page_img()) {
           img_consumed = true;
       }
       else if (!img_consumed) {
           return false;
       }
       return true;
   }

   template <typename Node>
   void redo(Node& node, logrec_t* lr)
   {
       if (!shouldRedo(lr)) {
          return;
       }
       Redoer::redo(lr, &node);
   }

   ArchiveScan archive_scan;
   bool img_consumed; // Workaround for page-img compression (see comments in cpp)
};
//--------------------------------------------------------------------------------
