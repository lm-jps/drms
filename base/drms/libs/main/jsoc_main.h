#ifndef __MAIN_H
#define __MAIN_H
#include "timeio.h"
#include "jsoc.h"
#include "timer.h"
#include "xassert.h"
#include "drms_types.h"
#include "cmdparams.h"
#include "drms_env.h"
#include "drms_series.h"
#include "drms_keyword.h"
#include "drms_keymap.h"
#include "drms_keymap_priv.h"
#include "drms_link.h"
#include "drms_record.h"
#include "drms_segment.h"
#include "drms_array.h"
#include "drms_protocol.h"
#include "drms_statuscodes.h"
#include "drms_parser.h"
#include "drms_network.h"
#include "drms_names.h"
#include "drms_fits.h"
#include "util.h"
#include "drms_dsdsapi.h"

extern CmdParams_t cmdparams;
/* Global DRMS Environment handle. */
extern DRMS_Env_t *drms_env;

/*  Default module arguments  */
extern ModuleArgs_t module_args[];
extern ModuleArgs_t *gModArgs;

/*  Module name  */
extern char *module_name;

/*  DoIt() Module entry point - defined in <modulename>.c */
extern int DoIt (void);

CmdParams_t *GetGlobalCmdParams(void);

int JSOCMAIN_Main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void));
int JSOCMAIN_Init(int argc, 
		  char **argv, 
		  const char *module_name, 
		  int *dolog,
		  int *verbose,
		  pid_t *drms_server_pid, 
		  pid_t *tee_pid,
		  int *cont);
int JSOCMAIN_Term(int dolog, int verbose, pid_t drms_server_pid, pid_t tee_pid, int abort_flag);

#ifdef FLIB
void f_cmdparams_get_handle(pFHandleCmdParams handle);
char *f_cmdparams_gethandle2();
#endif

#endif
