#include "finelog_basics.h"
#include <sstream>
#include <cstring>
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

#ifdef W_TRACE
debug_t _debug("debug", getenv("DEBUG_FILE"));
#endif

debug_t::debug_t(const char* /*n*/, const char* /*f*/)
{
#ifdef USE_REGEX
    //re_ready = false;
    //re_error_str = "Bad regular expression";
    re_syntax_options = RE_NO_BK_VBAR;
#endif /* USE_REGEX */

    mask = 0;
    const char *temp_flags = getenv("DEBUG_FLAGS");
    // malloc the flags so it can be freed
    if(!temp_flags) {
        temp_flags = "";
        mask = _none;
    }

    // make a copy of the flags so we can delete it later
    _flags = new char[strlen(temp_flags)+1];
    strcpy(_flags, temp_flags);
    assert(_flags != NULL);

    if(!strcmp(_flags,"all")) {
    mask |= _all;
#ifdef USE_REGEX
    } else if(!none()) {
    char *s;
    if((s=re_comp_debug(_flags)) != 0) {
        if(strstr(s, "No previous regular expression")) {
        // this is ok
        } else {
        cerr << "Error in regex, flags not set: " <<  s << endl;
        }
        mask = _none;
    }
#endif /* USE_REGEX */
    }

    assert( !( none() && all() ) );
}

debug_t::~debug_t()
{
    if(_flags) delete [] _flags;
    _flags = NULL;

}

void
debug_t::setflags(const char *newflags)
{
    if(!newflags) return;
#ifdef USE_REGEX
    {
        char *s;
        if((s=re_comp_debug(newflags)) != 0) {
            cerr << "Error in regex, flags not set: " <<  s << endl;
            mask = _none;
            return;
        }
    }
#endif /* USE_REGEX */

    mask = 0;
    if(_flags) delete []  _flags;
    _flags = new char[strlen(newflags)+1];
    strcpy(_flags, newflags);
    if(strlen(_flags)==0) {
        mask |= _none;
    } else if(!strcmp(_flags,"all")) {
        mask |= _all;
    }
    assert( !( none() && all() ) );
}

int
debug_t::flag_on(
    const char *fn,
    const char *file
)
{
    int res = 0;
    assert( !( none() && all() ) );
    if(_flags==NULL) {
    res = 0; //  another constructor called this
            // before this's constructor got called.
    } else if(none())     {
    res = 0;
    } else if(all())     {
    res = 1;
#ifdef USE_REGEX
    } else  if(file && re_exec_debug(file)) {
    res = 1;
    } else if(fn && re_exec_debug(fn)) {
    res = 1;
#endif /* USE_REGEX */
    } else
    // if the regular expression didn't match,
    // try searching the strings
    if(file && strstr(_flags,file)) {
    res = 1;
    } else if(fn && strstr(_flags,fn)) {
    res = 1;
    }
    return res;
}

/* This function prints a hex dump of (len) bytes at address (p) */
// void
// w_debug::memdump(void *p, int len)
// {
//     register int i;
//     char *c = (char *)p;

//     clog << "x";
//     for(i=0; i< len; i++) {
//         W_FORM2(clog,("%2.2x", (*(c+i))&0xff));
//         if(i%32 == 31) {
//             clog << endl << "x";
//         } else if(i%4 == 3) {
//             clog <<  " x";
//         }
//     }
//     clog << "--done" << endl;
// }

