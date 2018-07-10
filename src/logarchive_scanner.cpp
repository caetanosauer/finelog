#include "logarchive_scanner.h"

#include <vector>

#include "stopwatch.h"
#include "log_consumer.h" // for LogScanner

thread_local std::vector<MergeInput> ArchiveScan::_mergeInputVector;

bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b)
{
    if (a.keyPID != b.keyPID) { return a.keyPID > b.keyPID; }
    return a.keyVersion > b.keyVersion;
}

ArchiveScan::ArchiveScan(std::shared_ptr<ArchiveIndex> archIndex)
    : archIndex(archIndex), prevVersion(0), currentPID(0), singlePage(false), lastProbedRun(0)
{
    clear();
}

void ArchiveScan::open(PageID startPID, PageID endPID, run_number_t runBegin, run_number_t runEnd)
{
    w_assert0(archIndex);
    clear();
    auto& inputs = _mergeInputVector;

    archIndex->probe(inputs, startPID, endPID, runBegin, runEnd);
    lastProbedRun = runEnd;
    singlePage = (endPID == startPID+1);
    heapBegin = inputs.begin();

    // Iterate over inputs in reverse order in order to prune them if a page-image logrec is found
    auto it = inputs.rbegin();
    while (it != inputs.rend())
    {
        if (it->open(startPID)) {
            auto lr = it->logrec();
            it++;
            if (singlePage && lr->has_page_img()) {
                // Any entries beyond it (including it are ignored)
                heapBegin = it.base();
                ADD_TSTAT(la_img_trimmed, heapBegin - inputs.begin());
                break;
            }
        }
        else {
            std::advance(it, 1);
            inputs.erase(it.base());
        }
    }

    heapEnd = inputs.end();
    std::make_heap(heapBegin, heapEnd, mergeInputCmpGt);
}

void ArchiveScan::openByPage()
{
   open(0, 0);
}

bool ArchiveScan::finished()
{
    return heapBegin == heapEnd;
}

void ArchiveScan::clear()
{
    auto& inputs = _mergeInputVector;
    for (auto it : inputs) {
        archIndex->closeScan(it.runFile->runid);
    }
    inputs.clear();
    heapBegin = inputs.end();
    heapEnd = inputs.end();
    prevVersion = 0;
    currentPID = 0;
}

bool ArchiveScan::next(logrec_t*& lr)
{
retry:
    if (finished()) { return false; }

    // CS: This optimization does not work with FineLine, because the mapping
    // of individual page updates to the run they end up in the log archive
    // is not a monotonic function (e.g., update 1 might be on run 2, update 2
    // on run 1, and update 3 on run 1 again).
    // if (singlePage) {
    //     if (!heapBegin->finished()) {
    //         lr = heapBegin->logrec();
    //         heapBegin->next();
    //     }
    //     else {
    //         heapBegin++;
    //         return next(lr);
    //     }
    // }
    // else
    std::pop_heap(heapBegin, heapEnd, mergeInputCmpGt);
    auto top = std::prev(heapEnd);
    if (!top->finished()) {
        lr = top->logrec();
        w_assert1(lr->page_version() == top->keyVersion && lr->pid() == top->keyPID);
        if (archIndex->shouldSkipPages()) {
           auto& runInfo = runInfo[top->runFile->runid];
           top->nextByPage(runInfo);
        } else {
           top->next();
        }
        std::push_heap(heapBegin, heapEnd, mergeInputCmpGt);
    }
    else {
        heapEnd--;
        return next(lr);
    }

    // Check if we're moving to a new PID
    if (lr->pid() != currentPID && ) {
        currentPID = lr->pid();
        // Check if any of the other runs on higher epochs has an image on that page
        for (it = heapBegin; it != heapEnd; it++) {
           if (it == top) { continue; }
           const RunId& runid = it->runFile->runid;
        }
    }

    prevVersion = lr->page_version();
    currentPID = lr->pid();

    return true;
}

bool ArchiveScan::nextByPage(logrec_t*& lr)
{
   return true;
}

ArchiveScan::~ArchiveScan()
{
    clear();
}

void ArchiveScan::dumpHeap()
{
    // CS TODO: implement operator<< in interpreter
    // for (auto it = heapBegin; it != heapEnd; it++) {
    //     std::cout << *(it->logrec()) << std::endl;
    // }
}

logrec_t* MergeInput::logrec()
{
    return reinterpret_cast<logrec_t*>(runFile->getOffset(pos));
}

bool MergeInput::open(PageID startPID)
{
    if (!finished()) {
        auto lr = logrec();
        keyVersion = lr->page_version();
        keyPID = lr->pid();

        // advance index until firstPID is reached
        if (keyPID < startPID) {
            while (!finished() && lr->pid() < startPID) {
                ADD_TSTAT(la_skipped_bytes, lr->length());
                next();
                lr = logrec();
            }
            if (finished()) {
                INC_TSTAT(la_wasted_read);
                return false;
            }
        }
    }
    else {
        INC_TSTAT(la_wasted_read);
        return false;
    }

    w_assert1(keyVersion == logrec()->page_version());
    return true;
}

bool MergeInput::finished()
{
    if (!runFile || runFile->length == 0) { return true; }
    auto lr = logrec();
    return lr->is_eof() || (endPID != 0 && lr->pid() >= endPID);
}

void MergeInput::next()
{
    w_assert1(!finished());
    pos += logrec()->length();
    w_assert1(logrec()->valid_header());
    keyPID = logrec()->pid();
    keyVersion = logrec()->page_version();
}

// This type of iteration is optimized for whole-file merges.
// It jumps offsets according to the page information available in a RunInfo object.
void MergeInput::nextByPage(RunInfo& runInfo)
{
    w_assert1(!finished());
    pos += logrec()->length();
    w_assert1(logrec()->valid_header());

    while (true) {
       // WARNING: In this iterator, endPID is reused to store RunInfo entry slot of current keyPID
       auto& currentSlot = endPID;
       PageID nextPid = runInfo.getPid(currentSlot+1);
       if (nextPid == keyPID) {
          // getPid returns the last pid if currentSlot went out of bounds
          break;
       }
       auto nextPidPos = runInfo.getOffset(currentSlot+1);
       if (pos >= nextPidPos) {
          // We already scanning the next pid/slot
          currentSlot++;
          w_assert1(pos == nextPidPos);
          if (pidsToIgnore.count(nextPid)) {
             // Current PID should be ignored -- jump to next slot
             currentSlot++;
             if (nextPid == runInfo.getLastPid()) {
                // We're done scanning, but there's no easy way to skip the last pid, so we just keep going
             } else {
                pos = runInfo.getOffset(currentSlot);
             }
             continue;
          };
       }
       break;
    }

    // Access mmap memory to read log record
    keyPID = logrec()->pid();
    keyVersion = logrec()->page_version();
}

// endPID is not used for this iterator variant!
bool MergeInput::finishedByPage()
{
    if (!runFile || runFile->length == 0) { return true; }
    return logrec()->is_eof();
}

bool MergeInput::openByPage()
{
   // Just makes sure that endPID (which is interpreted as current slot in RunInfo) is initialized to 0
   endPID = 0;
   constexpr PageID startPID = 0;
   return open(startPID);
}
