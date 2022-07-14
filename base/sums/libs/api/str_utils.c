/* string handling utility functions
 * These functions can be considered as extensions to strings.3
 *
 * strdup - exists in some versions of unix.  It is included
 *		here to allow portability.
 *
 * string - takes same arguments as printf but leaves the result
 *		in a "malloc"ed string.
 *
 * String - takes same arguments as printf but leaves the result
 *		in a static array.
 *
 * stindex - find substring index with specific termination 
 *
 * sindex - find substring index 
 *
 * strlow - returns lower case temp copy of string 
 *
 * strup - returns upper case temp copy of string 
 *
 * mprefix - extracts multiplier prefix from ascii number 
 *
 * Strcmp - case insensitive version of strcmp(3)
 *
 * Strncmp - case insensitive version of strncmp(3)
 *
 */

#include	<stdlib.h>
#include        <stdio.h>
#include <stdarg.h>
#include	<ctype.h>
#include	<string.h>
#include	<strings.h>

#define VERSION_NUM     (0.8)

/* return duplicate of string */

char *string(const char *fmt, ...)
{
  char tmp[8192];
  va_list vargs;
  va_start(vargs,fmt);
  vsnprintf(tmp, 8192, fmt, vargs);
  va_end(vargs);
  return strdup(tmp);
}

char *String(const char *fmt, ...)
{
  static char tmp[8192];
  va_list vargs;
  va_start(vargs,fmt);
  vsnprintf(tmp, 8192, fmt, vargs);
  va_end(vargs);
  return(tmp);
}


/* stindex - find substring index with specific termination */

char *stindex(char *str, char *pat, char *term)
{
  char *pc;
  int len;

  if (term==NULL)
    return strstr(str,pat); /* Why reinvent libc? */
  else
  {
    len = strlen(pat);
    pc = str-1;
    while ((pc = strstr(++pc,pat)))
      if (strchr(term,*(pc+len)))
	return pc;
    return NULL;
  }
}

char *sindex(char *str, char *pat)
{
  return strstr(str,pat);
}

/* strlow - returns lower case temp copy of string */

char *strlow(char *str)
{
  static char str2[8192];
  char *s = str2;
  int n = 0;

  while (*str && ++n < 8190)
    *s++ = tolower(*str++);
  *s = '\0';
  return(str2);
}

/* strup - returns upper case temp copy of string */

char *strup(char *str)
{
  static char str2[8192];
  char *s = str2;
  int n = 0;

  while (*str && ++n < 8190)
    *s++ = toupper(*str++);
  *s = '\0';
  return(str2);
}


/* mprefix - extracts multiplier prefix from ascii number */

struct nmval {
  char *name;
  double value;
};

static struct nmval pre[] = {
  {"exa",	1e18},
  {"pecta",	1e15},
  {"tera",	1e12},
  {"giga",	1e9},
  {"mega",	1e6},
  {"kilo",	1e3},
  {"hecto",	1e2},
  {"deca",     	1e1},
  {"deci",	1e-1},
  {"centi",	1e-2},
  {"milli",	1e-3},
  {"micro",	1e-6},
  {"mu",	1e-6},
  {"nano",	1e-9},
  {"pico",	1e-12},
  {"femto",	1e-15},
  {"atto",	1e-18},
};

char *mprefix(char *str, double *mult)
{
  int i;
  char *pc;
  char *s = str;
  const int n_prefixes = sizeof(pre)/sizeof(struct nmval);

  for (i=0; i < n_prefixes; ++i)
    if ((pc = sindex(s, pre[i].name)))
    {
      s = pc + strlen(pre[i].name);
      *mult = pre[i].value;
      return(s);
    }
  *mult = 1.0;
  return(str);
}

/*
 * Compare strings ignore case:  s1>s2: >0  s1==s2: 0  s1<s2: <0
 */

int Strcmp(char *s1, char *s2)
{
  return strcasecmp(s1,s2); /* Why reinvent libc? */
}

/*
 * Compare strings ignore case (at most n bytes):
 *	 s1>s2: >0  s1==s2: 0  s1<s2: <0
 */

int Strncmp(char *s1, char *s2, int n)
{
  return strncasecmp(s1,s2,n); /* Why reinvent libc? */
}
