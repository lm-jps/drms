#ifndef UTIL_H
#define UTIL_H

#include <math.h>

typedef enum
{
   kExact = 0,
   kInexact
} FltCnvDisp_t;

char *ns(const char *name);
void strtolower(char *str);
void strtoupper(char *str);
void copy_string(char **dst, char *src); /* like strdup with assert on error. */
size_t base_strlcat(char *dst, const char *src, size_t size);
void *base_strcatalloc(char *dst, const char *src, size_t *sizedst);
int convert_int_field(char *field, int len);
long convert_long_field(char *field, int len);
float convert_float_field(char *field, int len);
double convert_double_field(char *field, int len);
void convert_string_field(char *field, int len, char *output, int maxlen);
int copyfile(const char *inputfile, const char *outputfile);
#undef likely
#undef unlikely
#define unlikely(a) __builtin_expect((a), 0)
#define likely(a) __builtin_expect((a), 1)

int IsValidFitsKeyName(const char *name);
int GenerateFitsKeyName(const char *kwName, char *fitsName, int size);
int IsValidDRMSKeyName(const char *drmsName);
int GenerateDRMSKeyName(const char *fitsName, char *drmsName, int size);


#ifdef ICCCOMP
#pragma warning (disable : 1572)
#endif

static inline long long FloatToLongLong(float f, int *stat)
{
   long long result = (long long)(f + 0.5);

   if (f == (float)result)
   {
      *stat = kExact;
   }
   else
   {
      *stat = kInexact;
   }

   return result;
}

static inline long long DoubleToLongLong(double d, int *stat)
{
   long long result = (long long)(d + 0.5);

   if (d == (double)result)
   {
      *stat = kExact;
   }
   else
   {
      *stat = kInexact;
   }

   return result;
}

static inline int IsZeroF(float f)
{
   return f == (float)0.0;
}

static inline int IsZero(double d)
{
   return d == (double)0.0;
}

static inline int IsPosHugeValF(float f)
{
   return f == HUGE_VALF;
}

static inline int IsNegHugeValF(float f)
{
   return f == -HUGE_VALF;
}

static inline int IsPosHugeVal(double d)
{
   return d == HUGE_VAL;
}

static inline int IsNegHugeVal(double d)
{
   return d == -HUGE_VAL;
}

#ifdef ICCCOMP
#pragma warning (default : 1572)
#endif


#endif
