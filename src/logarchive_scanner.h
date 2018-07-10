#ifndef FINELOG_LOGARCHIVE_SCANNER_H
#define FINELOG_LOGARCHIVE_SCANNER_H

#include <iostream>
#include <memory>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include "logarchive_index.h"

class ArchiveIndex;
class logrec_t;

struct alignas(32) MergeInput
{
    RunFile* runFile;
    size_t pos;
    uint32_t keyVersion;
    PageID keyPID;
    PageID endPID;


    logrec_t* logrec();
    bool open(PageID startPID);
    bool finished();
    void next();

    // By-page iteration; used by whole-file merges
    bool openByPage();
    void nextByPage();
    bool finishedByPage();

    friend bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);
};


// Merge input should be exactly 1/2 of a cacheline
static_assert(sizeof(MergeInput) == 32, "Misaligned MergeInput");

class ArchiveScan {
public:
    ArchiveScan(std::shared_ptr<ArchiveIndex>);
    ~ArchiveScan();

    void open(PageID startPID, PageID endPID, run_number_t runBegin = 0, run_number_t runEnd = 0);
    bool next(logrec_t*&);
    bool finished();

    // By-page iteration; used by whole-file merges
    bool openByPage();
    void nextByPage();
    bool finishedByPage();

    template <class Iter> void openForMerge(Iter begin, Iter end);
    run_number_t getLastProbedRun() const { return lastProbedRun; }
    void dumpHeap();

private:
    // Thread-local storage for merge inputs
    static thread_local std::vector<MergeInput> _mergeInputVector;

    std::vector<MergeInput>::iterator heapBegin;
    std::vector<MergeInput>::iterator heapEnd;

    std::shared_ptr<ArchiveIndex> archIndex;
    uint32_t prevVersion;
    PageID currentPID;
    bool singlePage;
    run_number_t lastProbedRun;
    // Used for whole-file merges with page skipping
    std::unordered_map<RunId, std::unique_ptr<RunInfo>> runInfos;

    void clear();
};

bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);

template <class Iter>
void ArchiveScan::openForMerge(Iter begin, Iter end)
{
    w_assert0(archIndex);
    clear();
    auto& inputs = _mergeInputVector;
    runInfos.clear();

    for (Iter it = begin; it != end; it++) {
        MergeInput input;
        input.pos = 0;
        input.runFile = archIndex->openForScan(*it);
        inputs.push_back(input);
    }

    heapBegin = inputs.begin();

    // Iterate backwards to remove empty inputs
    auto it = inputs.rbegin();
    while (it != inputs.rend())
    {
        constexpr PageID startPID = 0;
        if (it->open(startPID)) {
            if (archIndex->shouldSkipPages()) {
               runInfos[*it] = std::make_unique<RunInfo>();
               runInfos[*it].loadFromFile(input.runFile, *it);
            }
            it++;
        }
        else {
            std::advance(it, 1);
            inputs.erase(it.base());
        }
    }

    heapEnd = inputs.end();
    std::make_heap(heapBegin, heapEnd, mergeInputCmpGt);
    currentPID = 0;
}

#endif
