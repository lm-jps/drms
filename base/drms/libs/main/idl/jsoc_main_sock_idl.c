/* jsoc_main_idl.c 
 * 
 * IDL interface to DRMS structures and DRMS session management.
 *
 * --Art Amezcua 8/16/2007
 *
 */

#ifdef IDLLIB
#ifdef DEBUG_MEM
#include "xmem.h"
#endif
#include "jsoc_main.h"
#include "inthandles.h"

typedef const char *IDLHandleDRMS;
typedef IDLHandleDRMS *pIDLHandleDRMS;

CmdParams_t cmdparams; /* Even though we don't need to process a cmd-line 
			* when using the idl interface, CmdParams_t is used 
			* for session management. */
static HContainer_t *idlHandlesDRMS = NULL; /* Handle/Structure container */

const const char modulename[] = "idlinterface";
ModuleArgs_t module_args[] =
{     
   {ARG_END}
};
/* Top-level function that initializes all containers holding structures used within 
 * the IDL interface wrappers.  Each class of structure is contained within a single
 * container. 
 */
static void InitializeIDLInterface()
{
   InthandlesInit();
   if (!idlHandlesDRMS)
   {
      idlHandlesDRMS = hcon_create(sizeof(void *), 
				 kMaxIDLHandleKey, 
				 NULL, /* Shouldn't ever let hcon free, APIs should free 
					* underlying DRMS data structures. */
				 NULL, 
				 NULL, 
				 NULL, 
				 0);
      XASSERT(idlHandlesDRMS);
   }
}

/* Top-level function that releases all memory allocated during initialization of 
 * the IDL interface. 
 */
static void TerminateIDLInterface()
{
   InthandlesTerm();
   if (idlHandlesDRMS)
   {
      hcon_destroy(&idlHandlesDRMS);
   }
}

IDLError_t StartUp(int argc, 
		   char **argv, 
		   int *dolog, 
		   int *verbose, 
		   pid_t *drms_server_pid, 
		   pid_t *tee_pid) 
{
   IDLError_t ret = kIDLRet_Success;
   int jsocmainRet = 0;
   int cont;

   XASSERT(dolog && verbose && drms_server_pid && tee_pid);
   *dolog = 0;
   *verbose = 0;
   *drms_server_pid = 0;
   *tee_pid = 0;

   InitializeIDLInterface();
  
   jsocmainRet = JSOCMAIN_Init(argc, 
			       argv, 
			       modulename,
			       dolog,
			       verbose,
			       drms_server_pid, 
			       tee_pid,
			       &cont);

   if (!cont)
   {
      /* could look at jsocmainRet for more detail */
      ret = kIDLRet_CantContinue;
   }

   return ret;
}

IDLError_t ShutDown(int abort, int dolog, int verbose, pid_t drms_server_pid, pid_t tee_pid)
{
   IDLError_t ret = kIDLRet_Success;
   int status;

   status = JSOCMAIN_Term(dolog, verbose, drms_server_pid, tee_pid, abort);
   printf("  shutdown status %d\n", status);
   if (status != 256)  /* xxx - where did 256 come from? */
   {
      /* problem shutting down drms_server */
      ret = kIDLRet_ShutdownFailed;
   }

   TerminateIDLInterface();

   return ret;
}

#endif /* IDLLIB */
