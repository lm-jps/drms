/* COPIED from SOI's liberrstk.d, then modified.  This wouldn't compile as is with gcc.
 * Specifically, there is no <varargs.h>.  Changed to <stdarg.h>, and modified
 * errstk() to use new va_args syntax.
 *
 * arta 5/30/2007
 */

static char rcsid[] = "$Header: /home/akoufos/Development/Testing/jsoc-4-repos-0914/JSOC-mirror/JSOC/src/base/local/libs/soi/Attic/errstk.c,v 1.1 2007/10/02 00:12:20 arta Exp $";
/* errstk.c
 *
 * This is a library of routines to maintain an error stack. Each time a 
 * called procedure gets an error, it can push a message onto the error stack.
 * The top level procedure can then print the error stack to view the 
 * errors reported in the calling sequence.
 *
 *	errstk("vararg type string");
 *
 * Add error onto the error stack. This routine mallocs storage and puts 
 * the error string in it and links this string to the 
 * head of any previous error strings, thereby forming an error stack. 
 * If errstk() itself gets an error, like can't malloc, 
 * it will put a pre-assigned string such as "errstk() can't malloc" on top 
 * of the error stack and return. The calling error string will be lost, but 
 * any previous error stack will be intact. Any further calls to errstk() 
 * will also be lost, until the error stack is re-initialized.
 *
 *      perrstk(int (*function)());
 *
 * This routine prints all strings in the current error stack in LIFO order
 * by calling the function pointed to. The function must be one that takes
 * a vararg sting and prints it to where you want, e.g. printf() or write_log(). * Normally this will just be the errlog() arg passed to a strategy module.
 * perrstk then initializes the error stack.
 *
 *	initerrstk();
 *
 * Lets you explicitly clear the error stack and free any storage.
*/

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

struct estack {
  struct estack *next;
  char *msg;
};
typedef struct estack ESTK;

static ESTK *estkhead = NULL;
static ESTK efix = {NULL, "errstk() can't malloc. err msg(s) lost\n"};

/* Put vararg stuff on the error stack */
void errstk(const char *fmt, ...)
{
  va_list args;
  char string[512];
  ESTK *estk;

  if(estkhead == &efix)			/* a malloc err has already occured */
    return;				/* nop until initerrstk() is called */
  va_start(args, fmt);
  vsprintf(string, fmt, args);
  va_end(args);
  if((estk = (ESTK *)malloc(sizeof(ESTK))) == NULL) {
    efix.next = estkhead;
    estkhead = &efix;
    return;
  }
  if((estk->msg = (char *)malloc(strlen(string)+1)) == NULL) {
    efix.next = estkhead;
    estkhead = &efix;
    return;
  }
  strcpy(estk->msg, string);
  estk->next = estkhead;		/* put at top of error stack */
  estkhead = estk;
}

/* Initialize the error stack */
void initerrstk()
{
  ESTK *eptr;
  ESTK *nptr;

  for(eptr=estkhead; eptr != NULL; eptr=nptr) {
    if(eptr == &efix) continue;
    free(eptr->msg);
    nptr=eptr->next;			/* save the next before free it */
    free(eptr);
  }
  estkhead = NULL;
}

/* Print the error stack and initialize it */
void perrstk(int (*errfunc)())
{
  ESTK *eptr;

  for(eptr=estkhead; eptr != NULL; eptr=eptr->next) {
    (*errfunc)(eptr->msg);
  }
  initerrstk();
}

/*
 *  Revision History
 * 
 *  V 1.0   95.06.01	Rick Bogart	    removed unused variable msgtxt
 */

/*
$Id: errstk.c,v 1.1 2007/10/02 00:12:20 arta Exp $
$Source: /home/akoufos/Development/Testing/jsoc-4-repos-0914/JSOC-mirror/JSOC/src/base/local/libs/soi/Attic/errstk.c,v $
$Author: arta $
*/
/* $Log: errstk.c,v $
 * Revision 1.1  2007/10/02 00:12:20  arta
 * *** empty log message ***
 *
/* Revision 1.1  2007/06/21 19:45:40  arta
/* Rechecking these in.  CVS checked them into su_interal, not su_internal last time.
/*
/* Revision 1.1  2007/06/21 16:42:34  arta
/* SOI library plug-in.  Called from libdsds.so to open DSDS records and read data.  Not to be exported outside of Stanford.
/*
 * Revision 1.6  1997/11/06  18:06:19  kay
 * delete "static" from struct definition to eliminate compiler warning
 *
 * Revision 1.5  1995/08/21  22:24:06  jim
 * fix initerrstk to pick up next before free eptr
 *
 * Revision 1.4  1995/06/01  23:03:26  rick
 * removed unused variable
 *
 * Revision 1.3  1994/03/31  20:02:30  jim
 * changed perrstk() to take a function pointer
 *
 * Revision 1.2  1994/03/30  18:14:55  jim
 * initial version
 * */
