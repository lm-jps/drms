#include <stdio.h>
#include <signal.h>
#include <execinfo.h>
#include "backtrace.h"

void show_stackframe(FILE *fp) {
  void *trace[32];
  char **messages = (char **)NULL;
  int i, trace_size = 0;

  trace_size = backtrace(trace, 32);
  messages = backtrace_symbols(trace, trace_size);
  fprintf(fp,"BACKTRACE: Execution path:\n");
  for (i=0; i<trace_size; ++i)
	fprintf(fp,"BACKTRACE: %s\n", messages[i]);
}
