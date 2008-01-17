#ifndef _FOUNDATION_H
#define _FOUNDATION_H

static union { uint32_t rep; float val; } __f_nan__ __attribute_used__ = {0xffc00000};
#define F_NAN (__f_nan__.val)
static union { uint64_t rep; double val; } __d_nan__ __attribute_used__ = {0xfff8000000000000};
#define D_NAN (__d_nan__.val)

#endif // _FOUNDATION_H
