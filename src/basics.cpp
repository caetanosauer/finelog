#include "basics.h"
#include <sstream>
#include <iostream>

using namespace std;

void global_assert_failed(
    const char*        desc,
    const char*        file,
    uint32_t        line)
{
    stringstream os;
    /* make the error look something like an RC in the meantime. */
    os << "assertion failure: " << desc << endl
        << "1. error in "
        << file << ':' << line
        << " Assertion failed" << endl
        << "\tcalled from:" << endl
        << "\t0) " << file << ':' << line
        << endl << ends;
    fprintf(stderr, "%s", os.str().c_str());
    cout.flush();
    cerr.flush();
    ::abort();
}
