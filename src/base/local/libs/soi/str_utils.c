/* COPIED from SOI's libast.d, then modified.  This wouldn't compile as is with gcc.
 * Specifically, there is no <varargs.h>.  Changed to <stdarg.h>, and modified
 * string() and String() to use new va_args syntax.
 *
 * arta 5/30/2007
 */

static char rcsid[] = "$Header";

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
 * Strncmp - case insensitive version of Strncmp(3)
 *
 */

#include	<stdlib.h>
#include        <stdio.h>
#include	<ctype.h>
#include	<string.h>

#define VERSION_NUM     (0.8)

/* return duplicate of string */

#ifndef __linux__
char *strdup(const char *str)
{
int n;
char *s;
if (!str)
	return(NULL);
n = strlen(str) + 1;
s = (char *)malloc(n);
strcpy(s,str);
return(s);
}
#endif

/* string - return formatted and malloced string */

#ifdef vax

#define _IOSTRG         00100
        char *string(fmt, args)
        char *fmt;
        {
        char tmp[8192];
        FILE _strbuf;
        _strbuf._flag = _IOWRT+_IOSTRG;
        _strbuf._ptr = (unsigned char *)tmp;
        _strbuf._cnt = 32767;
        _doprnt(fmt, &args, &_strbuf);
        putc('\0', &_strbuf);
        return(strdup(tmp));
        }

	char *String(fmt, args)
	char *fmt;
	{
        static char tmp[8192];
        FILE _strbuf;
        _strbuf._flag = _IOWRT+_IOSTRG;
        _strbuf._ptr = (unsigned char *)tmp;
        _strbuf._cnt = 32767;
        _doprnt(fmt, &args, &_strbuf);
        putc('\0', &_strbuf);
        return(tmp);
        }
#else

#include <stdarg.h>

        char *string(const char *fmt, ...)
        {
        char tmp[8192];
        va_list vargs;
        va_start(vargs, fmt);
        (void)vsprintf(tmp, fmt, vargs);
        va_end(vargs);
        return(strdup(tmp));
        }

        char *String(const char *fmt, ...)
        {
        static char tmp[8192];
        va_list vargs;
        va_start(vargs, fmt);
        (void)vsprintf(tmp, fmt, vargs);
        va_end(vargs);
        return(tmp);
        }

#endif


/* stindex - find substring index with specific termination */

char *stindex(char *str, char *pat, char *term)
{
char *pc = str-1;
int len = strlen(pat);

while (pc = strchr(++pc,*pat))
	if (!strncmp(pc,pat,len) && (term==NULL||strchr(term,*(pc+len))))
		break;
return(pc);
}

/* sindex - find substring index */

char *sindex(char *str, char *pat)
{
return(stindex(str,pat,NULL));
}

/* strlow - returns lower case temp copy of string */

char *strlow(char *str)
{
static char str2[8192];
char *s = str2;
int n = 0;

while (*str && ++n < 8190)
	*s++ = isupper(*str)?tolower(*str++):*str++;
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
	*s++ = islower(*str)?toupper(*str++):*str++;
*s = '\0';
return(str2);
}


/* mprefix - extracts multiplier prefix from ascii number */
char *mprefix(char *str, double *mult) {

  struct nmval {
    char *name;
    double value;
  };

  static struct nmval pre[] = {
	"exa",		1e18,
	"pecta",	1e15,
	"tera",		1e12,
	"giga",		1e9,
	"mega",		1e6,
	"kilo",		1e3,
	"hecto",	1e2,
	"deca",		1e1,
	"deci",		1e-1,
	"centi",	1e-2,
	"milli",	1e-3,
	"micro",	1e-6,
	"mu",		1e-6,
	"nano",		1e-9,
	"pico",		1e-12,
	"femto",	1e-15,
	"atto",		1e-18,
  };

  int i;
  char *pc;
  char *s = str;
  int n_prefixes = sizeof(pre)/sizeof(struct nmval);

  for (i=0; i < n_prefixes; ++i)
	if (pc = sindex(s, pre[i].name))
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

Strcmp(char *s1, char *s2)
{
char S1,S2;

	while ((S1=(isupper(*s1)?tolower(*s1):*s1)) ==
		(S2=(isupper(*s2)?tolower(*s2++):*s2++)))
		if (*s1++=='\0')
			return(0);
	return(S1 - S2);
}

/*
 * Compare strings ignore case (at most n bytes):
 *	 s1>s2: >0  s1==s2: 0  s1<s2: <0
 */

Strncmp(char *s1, char *s2, int n)
{
char S1,S2;

	while (--n >= 0 &&
		(S1=(isupper(*s1) ? tolower(*s1) : *s1)) ==
		(S2=(isupper(*s2) ? tolower(*s2++) : *s2++)) )
		if (*s1++ == '\0')
			return(0);
	return (n<0 ? 0 : S1-S2);
}

/*
 * V.0.8	93.02.17	P Scherrer	Copy from existing versions
 */

/*
$Id: str_utils.c,v 1.1.1.1 2007/10/02 00:12:20 arta Exp $
$Source: /home/akoufos/Development/Testing/jsoc-4-repos-0914/JSOC-mirror/JSOC/src/base/local/libs/soi/Attic/str_utils.c,v $
$Author: arta $
 * $Log: str_utils.c,v $
 * Revision 1.1.1.1  2007/10/02 00:12:20  arta
 * First new, reorganized JSOC tree
 *
 * Revision 1.1  2007/06/21 19:45:40  arta
 * Rechecking these in.  CVS checked them into su_interal, not su_internal last time.
 *
 * Revision 1.1  2007/06/21 16:42:34  arta
 * SOI library plug-in.  Called from libdsds.so to open DSDS records and read data.  Not to be exported outside of Stanford.
 *
 * Revision 1.14  2007/05/07  19:09:39  rick
 * fixed erroneous storage declaration for mprefix()
 *
 * Revision 1.13  1999/08/02 18:07:49  jim
 * add __linux__
 *
 * Revision 1.12  1999/07/30  22:23:04  kehcheng
 * backed out of last change
 *
 * Revision 1.11  1999/07/29 21:01:40  CM
 * use __ultrix__
 *
 * Revision 1.10  1999/06/22  00:19:49  kehcheng
 * *** empty log message ***
 *
 * Revision 1.9  1999/06/22 00:18:17  kehcheng
 * removed strdup
 *
 * Revision 1.8  1995/09/21 00:36:36  phil
 * added String
 *
 * Revision 1.7  1995/06/09  20:54:16  phil
 * made strdup return NULL on NULL passed to it.
 *
 * Revision 1.6  1995/02/07  18:11:36  phil
 * merged more WSO string utilities
 *
 * Revision 1.5  1994/12/14  18:06:00  kay
 * changed argument to strdup to const
 *
 * Revision 1.4  1994/11/05  01:39:34  CM
 * auto rcsfix by cm
 * */
