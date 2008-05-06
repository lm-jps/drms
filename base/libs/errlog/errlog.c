#include <stdio.h>
#include <stdarg.h>
#include "errlog.h"
#include "cmdparams.h"

#define kERRLOG "errlog"
#define kHSTLOG "hstlog"

/* default logfile */
FILE *errlog_errlogfile = NULL;
FILE *errlog_hstlogfile = NULL;

/* default error logging */
void errlog_errwrite (char *fmt, ...) {
   va_list ap;
   int m, n;
   char c, string[4096], newstr[5120];

   va_start (ap, fmt);
   vsprintf (string, fmt, ap);

   newstr[n = m = 0] = '\0';
   while (c = string[n++]) {
      newstr[m++] = c;
      if (c == '%') newstr[m++] = c;
      newstr[m] = '\0';
   }

   if (errlog_errlogfile) {
      fprintf (errlog_errlogfile, newstr);
      fflush (errlog_errlogfile);
   } else {
      fprintf (stderr, newstr);
      fflush (stderr);
   }
   va_end (ap);
}

/* default error logging */
void errlog_hstwrite (char *fmt, ...) {
   va_list ap;
   int m, n;
   char c, string[4096], newstr[5120];

   va_start (ap, fmt);
   vsprintf (string, fmt, ap);

   newstr[n = m = 0] = '\0';
   while (c = string[n++]) {
      newstr[m++] = c;
      if (c == '%') newstr[m++] = c;
      newstr[m] = '\0';
   }

   if (errlog_hstlogfile) {
      fprintf (errlog_hstlogfile, newstr);
      fflush (errlog_hstlogfile);
   } else {
      fprintf (stdout, newstr);
      fflush (stdout);
   }
   va_end (ap);
}

int errlog_paramerr(LogPrint_t log, char *pname, ErrNo_t error, const char *series, long long recnum)
{
   (*log)("** error '%d' in parameter '%s', series '%s', recnum '%lld' **\n",
	  error, 
	  pname, 
	  series, 
	  recnum);
   return error;
}

int errlog_paramdef(LogPrint_t log, 
		     char *pname, 
		     WarnNo_t warn, 
		     double defaultp, 
		     double *p, 
		     const char *series, 
		     long long recnum)
{
   /* logs warning and sets parameter to default value */
   *p = defaultp;
   (*log)(" ** warning '%d' - default value '%g' used for parameter '%s' in series '%s', recnum '%lld' **\n",
	  defaultp, 
	  pname, 
	  series, 
	  recnum);
   return warn;
}

int errlog_staterr(LogPrint_t log, char *msg, ErrNo_t error, const char *series, long long recnum)
{
   /* logs error */
   (*log)("** error '%d' (%s), series '%s', recnum '%lld'**\n", 
	  error, 
	  msg, 
	  series, 
	  recnum);
   return error;
}

void errlog_seterrlog(CmdParams_t *cmdparams)
{
   char *lf = cmdparams_get_str(cmdparams, kERRLOG, NULL);
   {
      if (lf)
      {
	 errlog_errlogfile = fopen(lf, "a");
      }
   }
}

void errlog_sethstlog(CmdParams_t *cmdparams)
{
   char *lf = cmdparams_get_str(cmdparams, kHSTLOG, NULL);
   {
      if (lf)
      {
	 errlog_hstlogfile = fopen(lf, "a");
      }
   }
}
