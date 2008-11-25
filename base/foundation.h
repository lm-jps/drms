#ifndef _FOUNDATION_H
#define _FOUNDATION_H

#ifdef __attribute__used__
static union { uint32_t rep; float val; } __f_inf__ __attribute_used__ = {0x7f800000};
static union { uint32_t rep; float val; } __f_neg_inf__ __attribute_used__ = {0xff800000};
static union { uint32_t rep; float val; } __f_nan__ __attribute_used__ = {0xffc00000};
static union { uint64_t rep; double val; } __d_inf__ __attribute_used__ = {0x7ff0000000000000};
static union { uint64_t rep; double val; } __d_neg_inf__ __attribute_used__ = {0xfff0000000000000};
static union { uint64_t rep; double val; } __d_nan__ __attribute_used__ = {0xfff8000000000000};
#else 
static union { uint32_t rep; float val; } __f_inf__ __attribute__((used)) = {0x7f800000};
static union { uint32_t rep; float val; } __f_neg_inf__ __attribute__((used)) = {0xff800000};
static union { uint32_t rep; float val; } __f_nan__ __attribute__((used)) = {0xffc00000};
static union { uint64_t rep; double val; } __d_inf__ __attribute__((used)) = {0x7ff0000000000000};
static union { uint64_t rep; double val; } __d_neg_inf__ __attribute__((used)) = {0xfff0000000000000};
static union { uint64_t rep; double val; } __d_nan__ __attribute__((used)) = {0xfff8000000000000};
#endif /* __attribute__used__ */
#define F_INF (__f_inf__.val)
#define F_NEG_INF (__f_neg_inf__.val)
#define F_NAN (__f_nan__.val)
#define D_INF (__d_inf__.val)
#define D_NEG_INF (__d_neg_inf__.val)
#define D_NAN (__d_nan__.val)

#endif // _FOUNDATION_H
