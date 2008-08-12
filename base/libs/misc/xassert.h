#ifdef XASSERT_H_DEF

#undef XASSERT_H_DEF
#undef assert_or_hang
#undef HANG_HOURS 

#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#define XASSERT_H_DEF

// Make the calling process sleep for a specified number of hours so
// it can be attached to with a debugger. Print out information
// of where hang was called and how to attach before going to sleep.
#define HANG_HOURS 12
#define hang(hours)   do { { int __i; int __h = (hours), __pid = (int)getpid(); \
                       for (__i=0; __i<__h; __i++) {  \
                         fprintf(stderr, "PID %d: Waiting %d hours before " \
                            "exiting. Run \"gdb; attach %d\" to attach debugger.\n", \
                            __pid, __h-__i, __pid); \
                         sleep(3600); \
                       } \
                     } } while(0)

#ifndef NDEBUG
#define assert_or_hang(val) do { if (!(val)) { \
                              fprintf(stderr, "%s, line %d: Assertion \"" \
                                      #val "\" failed.\n", __FILE__, __LINE__); \
                              hang(HANG_HOURS); \
                              exit(1);		\
                            } } while(0)
#else
#define assert_or_hang(val) val
#endif

#define XASSERT(code) assert_or_hang((code))

