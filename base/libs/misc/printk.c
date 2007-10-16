#include <stdio.h>
#include <stdarg.h>
#include "xmem.h"

/********** default wrapper doing I/O to stderr *************/
static int fprintf_wrap(const char *fmt, ...);
static int printf_wrap(const char *fmt, ...);

/************ global function pointers for I/O redirection ***************/
int (*printk)(const char *fmt, ...) = printf_wrap; // default = output to stdout.
int (*printkerr)(const char *fmt, ...) = fprintf_wrap;  //default = output to stderr.


/* Set printk output functions. */
void printk_set(int (*std)(const char *fmt, ...),
		int (*err)(const char *fmt, ...))
{
  printk = std;
  printkerr = err;
}

/* Default error message output function. */
static int fprintf_wrap(const char *fmt, ...)
{
  int val;
  va_list args;

  va_start(args,fmt);
  val = vfprintf(stderr,fmt,args);
  va_end(args);
  return val;
}

/* Default error message output function. */
static int printf_wrap(const char *fmt, ...)
{
  int val;
  va_list args;

  va_start(args,fmt);
  val = vprintf(fmt,args);
  va_end(args);
  return val;
}
