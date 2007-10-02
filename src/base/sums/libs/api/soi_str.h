/* soi_str.h - prototypes for string utility functions in libast */

#ifndef SOI_STR_INCL

#ifndef SOI_VERSION_INCL
#include <soi_version.h>
#endif

#include	<string.h>
#ifndef SOI_TIME_INCL
#include	"timeio.h"
#endif

#ifndef __linux__
extern char *strdup();
#endif

extern char *string(char *format, ... );
extern char *String(char *format, ... );

extern char *mprefix(char *str, double *mult);
extern char *stindex(char *str, char *pat, char *term);
extern char *sindex(char *str, char *pat);
extern char *strlow(char *str);
extern char *strup(char *str);
extern int *Strcmp(char *s1, char *s2);
extern int *Strncmp(char *s1, char *s2);
						  /*  from libast.d/atoinc.c  */
extern TIME atoinc (char *str);
extern int fprint_inc (FILE *fp, TIME inc);
extern int print_inc (TIME inc);
extern char *sprint_inc (char *str, TIME inc);
#ifdef NEVER
extern char *getinc (TIME a, TIME b, double *dt);
#endif

#define SOI_STR_INCL
#endif

/*
 *  Revision History
 *  03.02.07	Rick Bogart		added prototypes for sprint_inc()
 */

/*
$Id: soi_str.h,v 1.1.1.1 2007/10/02 00:12:19 arta Exp $
$Source: /home/akoufos/Development/Testing/jsoc-4-repos-0914/JSOC-mirror/JSOC/src/base/sums/libs/api/Attic/soi_str.h,v $
$Author: arta $
 * $Log: soi_str.h,v $
 * Revision 1.1.1.1  2007/10/02 00:12:19  arta
 * First new, reorganized JSOC tree
 *
 * Revision 1.3  2007/05/11 23:27:02  karen
 * New timeio lib
 *
 * Revision 1.2  2006/01/27 21:22:03  jim
 * *** empty log message ***
 *
 * Revision 1.1.1.1  2005/01/31 19:18:13  cvsuser
 * initial
 *
 * Revision 1.10  2003/02/07  19:31:20  rick
 * see above
 *
 * Revision 1.9  1999/08/02 17:59:06  CM
 * put in __linux__
 *
 * Revision 1.8  1999/07/30  22:23:29  kehcheng
 * backed out of last change
 *
 * Revision 1.7  1999/07/29 21:04:58  CM
 * add __ultrix__
 *
 * Revision 1.6  1999/06/03  21:11:10  jim
 * elim strdup
 *
 * Revision 1.5  1997/04/16  21:56:53  kehcheng
 * added #include <soi_version.h>
 *
 * Revision 1.4  1995/09/21  00:35:38  phil
 * added String
 *
 * Revision 1.3  1995/02/07  18:13:55  phil
 * *** empty log message ***
 *
 * Revision 1.2  1995/02/07  17:52:07  phil
 * *** empty log message ***
 * */
