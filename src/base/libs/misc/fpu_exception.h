#ifndef H_fpu_exception
#define H_fpu_exception 1

#if defined(__cplusplus)
extern "C" {
#endif

//
// Enable or disable exceptions, remembering the old state.
// Arguments:
// 1) True if you want to enable exceptions.
//
void fpu_exception_push_and_set(int enable_flag);

//
// Restore the last exception state (i.e., turn off exceptions if you just
// enabled them, or vice versa).
//
void fpu_exception_pop(void);

#if defined(__cplusplus)
}
#endif

#endif // H_fpu_exception
