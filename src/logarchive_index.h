#ifndef FINELOG_LOGARCHIVE_INDEX_H
#define FINELOG_LOGARCHIVE_INDEX_H

#include <vector>
#include <list>
#include <unordered_map>
#include <map>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "latches.h"
#include "lsn.h"

class RunRecycler;
class log_storage;

struct RunId {
    run_number_t begin;
    run_number_t end;
    unsigned level;

    bool operator==(const RunId& other) const
    {
        return begin == other.begin && end == other.end
            && level == other.level;
    }
};

// Controls access to a single run file through mmap
struct RunFile {
    RunId runid;
    int fd;
    int refcount;
    char* data;
    size_t length;

    RunFile() : fd(-1), refcount(0), data(nullptr), length(0)
    {
    }

    char* getOffset(off_t offset) const { return data + offset; }
};

// Temporary structure used to add blocks from BlockAssembly into the ArchiveIndex
struct BucketInfo {
    PageID pid;
    uint64_t offset;
    bool hasPageImage;
};

namespace std
{
    /// Hash function for RunId objects
    /// http://stackoverflow.com/q/17016175/1268568
    template<> struct hash<RunId>
    {
        using argument_type = RunId;
        using result_type = std::size_t;
        result_type operator()(argument_type const& a) const
        {
            result_type const h1 ( std::hash<lsn_t>()(a.begin) );
            result_type const h2 ( std::hash<lsn_t>()(a.end) );
            result_type const h3 ( std::hash<unsigned>()(a.level) );
            return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
        }
    };
}

// Comparator for map of open files
struct CmpOpenFiles {
    bool operator()(const RunId& a, const RunId& b) const
    {
	if (a.level != b.level) { return a.level < b.level; }
	return a.begin < b.begin;
    }
};

/** \brief Encapsulates all file and I/O operations on the log archive
 *
 * The directory object serves the following purposes:
 * - Inspecting the existing archive files at startup in order to determine
 *   the last LSN persisted (i.e., from where to resume archiving) and to
 *   delete incomplete or already merged (TODO) files that can result from
 *   a system crash.
 * - Support run generation by providing operations to open a new run,
 *   append blocks of data to the current run, and closing the current run
 *   by renaming its file with the given LSN boundaries.
 * - Support scans by opening files given their LSN boundaries (which are
 *   determined by the archive index), reading arbitrary blocks of data
 *   from them, and closing them.
 * - In the near future, it should also support the new (i.e.,
 *   instant-restore-enabled) asynchronous merge daemon (TODO).
 * - Support auxiliary file-related operations that are used, e.g., in
 *   tests and experiments.  Currently, the only such operation is
 *   parseLSN.
 *
 * \author Caetano Sauer
 */
class ArchiveIndex {
public:
    ArchiveIndex(const std::string& archdir, log_storage* logStorage, bool reformat, size_t max_open_files = 20);
    virtual ~ArchiveIndex();

    class RunInfo {
    public:
        run_number_t begin;
        run_number_t end;

        std::vector<PageID> pids;

        bool operator<(const RunInfo& other) const
        {
            return begin < other.begin;
        }

        // Set offset as given, without masking (used by loadRunInfo)
        void addRawEntry(PageID pid, uint64_t rawOffset)
        {
            pids.push_back(pid);
            offsets.push_back(rawOffset);
        }

        void addEntry(PageID pid, uint64_t offset, bool hasImage)
        {
            addRawEntry(pid, offset | (hasImage ? Mask : 0));
        }

        uint64_t getOffset(int i)
        {
            return offsets[i] & ~Mask;
        }

        void serialize(int fd, off_t offset);

    private:
        std::vector<uint64_t> offsets;
        static constexpr uint64_t Mask = ~(~0ul >> 1); // most significant bit set to 1
    };

    std::string getArchDir() const { return archdir; }

    run_number_t getLastRun();
    run_number_t getLastRun(unsigned level);
    run_number_t getFirstRun(unsigned level);

    // run generation methods
    void openNewRun(unsigned level);
    void append(char* data, size_t length, unsigned level);
    void fsync(unsigned level);
    void closeCurrentRun(run_number_t currentRun, unsigned level);

    // run scanning methods
    RunFile* openForScan(const RunId& runid);
    void closeScan(const RunId& runid);

    void listFiles(std::vector<std::string>& list, int level = -1);
    void listFileStats(std::list<RunId>& list, int level = -1);
    void deleteRuns(unsigned replicationFactor = 0);

    static bool parseRunFileName(std::string fname, RunId& fstats);
    static size_t getFileSize(int fd);

    void newBlock(const std::vector<BucketInfo>& buckets, unsigned level);

    void finishRun(run_number_t first, run_number_t last, int fd, off_t offset, unsigned level);

    template <class Input>
    void probe(std::vector<Input>&, PageID, PageID, run_number_t runBegin,
            run_number_t& runEnd);

    void loadRunInfo(RunFile*, const RunId&);
    void startNewRun(unsigned level);

    unsigned getMaxLevel() const { return maxLevel; }
    size_t getRunCount(unsigned level) {
        if (level > maxLevel) { return 0; }
        return runs[level].size();
    }

    void dumpIndex(std::ostream& out);
    void dumpIndex(std::ostream& out, const RunId& runid);

    template <class OutputIter>
    void listRunsNonOverlapping(OutputIter out)
    {
        auto level = maxLevel;
        run_number_t nextRun = 1;

        // Start collecting runs on the max level, which has the largest runs
        // and therefore requires the least random reads
        while (level > 0) {
            auto index = findRun(nextRun, level);

            while ((int) index <= lastFinished[level]) {
                auto& run = runs[level][index];
                out = RunId{run.begin, run.end, level};
                nextRun = run.end + 1;
                index++;
            }

            level--;
        }
    }

private:

    void appendNewRun(unsigned level);
    size_t findRun(run_number_t run, unsigned level);
    // binary search
    size_t findEntry(RunInfo* run, PageID pid,
            int from = -1, int to = -1);
    void serializeRunInfo(RunInfo&, int fd, off_t);

private:
    std::string archdir;
    std::vector<int> appendFd;
    std::vector<off_t> appendPos;

    fs::path archpath;

    // Run information for each level of the index
    std::vector<std::vector<RunInfo>> runs;

    // Last finished run on each level -- this is required because runs are
    // generated asynchronously, so that a new one may be appended to the
    // index before the last one is finished. Thus, when calling finishRun(),
    // we cannot simply take the last run in the vector.
    std::vector<int> lastFinished;

    unsigned maxLevel;

    std::unique_ptr<RunRecycler> runRecycler;

    mutable srwlock_t _mutex;

    /// Cache for open files (for scans only)
    std::map<RunId, RunFile, CmpOpenFiles> _open_files;
    mutable srwlock_t _open_file_mutex;
    size_t _max_open_files;
    bool directIO;

    fs::path make_run_path(run_number_t begin, run_number_t end, unsigned level = 1) const;
    fs::path make_current_run_path(unsigned level) const;

public:
    const static std::string RUN_PREFIX;
    const static std::string CURR_RUN_PREFIX;
    const static std::string run_regex;
    const static std::string current_regex;
};

template <class Input>
void ArchiveIndex::probe(std::vector<Input>& inputs,
        PageID startPID, PageID endPID, run_number_t runBegin, run_number_t& runEnd)
{
    spinlock_read_critical_section cs(&_mutex);

    Input input;
    input.endPID = endPID;
    unsigned level = maxLevel;
    inputs.clear();
    run_number_t nextRun = runBegin;

    while (level > 0) {
        if (runEnd > 0 && nextRun > runEnd) { break; }

        size_t index = findRun(nextRun, level);
        while ((int) index <= lastFinished[level]) {
            auto& run = runs[level][index];
            index++;
            nextRun = run.end;

            if (run.pids.size() > 0) {
                if (startPID > run.pids[run.pids.size()-1]) {
                    // Prune this run if PID is larger than maximum found in run (skips binary search; should happen frequently)
                    INC_TSTAT(la_avoided_probes);
                    continue;
                }

                size_t entryBegin = findEntry(&run, startPID);

                if ((run.pids[entryBegin] >= endPID) && (endPID > 0)) {
                    INC_TSTAT(la_avoided_probes);
                    continue;
                }

                input.pos = run.getOffset(entryBegin);
                input.runFile =
                    openForScan(RunId{run.begin, run.end, level});
                w_assert1(input.pos < input.runFile->length);
                inputs.push_back(input);
            }
        }

        level--;
    }

    // Return last probed run as out-parameter
    runEnd = nextRun;
}

#endif
