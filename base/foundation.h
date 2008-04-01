#ifndef _FOUNDATION_H
#define _FOUNDATION_H

#ifdef __attribute__used__
static union { uint32_t rep; float val; } __f_nan__ __attribute_used__ = {0xffc00000};
static union { uint64_t rep; double val; } __d_nan__ __attribute_used__ = {0xfff8000000000000};
#else 
static union { uint32_t rep; float val; } __f_nan__ __attribute__((used)) = {0xffc00000};
static union { uint64_t rep; double val; } __d_nan__ __attribute__((used)) = {0xfff8000000000000};
#endif /* __attribute__used__ */
#define F_NAN (__f_nan__.val)
#define D_NAN (__d_nan__.val)

#endif // _FOUNDATION_H
