#ifndef _ERRLOG_H
#define _ERRLOG_H

typedef int ErrNo_t;
typedef int WarnNo_t;
typedef void (*LogPrint_t)(char *fmt, ...);

int errlog_paramerr(LogPrint_t log, char *pname, ErrNo_t error, const char *series, long long recnum);
int errlog_paramdef(LogPrint_t log, 
		       char *pname, 
		       WarnNo_t warn, 
		       double defaultp, 
		       double *p, 
		       const char *series, 
		       long long recnum);
int errlog_staterr(LogPrint_t log, char *fnn, ErrNo_t error, const char *series, long long recnum);
void errlog_errwrite (char *fmt, ...);
void errlog_hstwrite (char *fmt, ...);

#endif /* _ERRLOG_H */
