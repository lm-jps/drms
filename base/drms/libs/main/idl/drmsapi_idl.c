/*****************************************************************************/
/* Database connection routines for the HMI jsoc database.                   */
/* This module must be precompiled into straight C code and thence into a    */
/* shared object library.                                                    */
/*                                                                           */
/* Last Update - 10 Sept 2007 by Art A                                       */
/*****************************************************************************/

#include "jsoc_main.h"
#include "inthandles.h"

/* Jennifer, you'll need to create APIs for accessing individual records
 * from a DRMS_RecordSet_t *.  To do that, you'll need to add new functions to 
 * drms_record.c.  Something like drms_record_getrec(DRMS_RecordSet_t *rs, int indx);
 */

#define ITEMCNT 7000    /* Maximum number of telemetry items       */
#define NAMECNT 3000    /* Maximum number of items per subsystem   */
#define NAMELEN   17    /* Maximum length of identifier strings    */
#define NAME32    33    /* Maximum length of 32 character name.    */
#define NAME30    31    /* Maximum length of 30 character name.    */
#define STRLEN    81    /* Maximum length of descriptive strings   */
#define STR60     61    /* Maximum length of 60 character strings  */
#define STR40     41    /* Maximum length of 40 character strings  */
#define STR16     17    /* Maximum length of 16 character strings  */
#define STR08      9    /* Maximum length of  8 character strings  */
#define STR06      7    /* Maximum length of  6 character strings  */
#define STR04      5    /* Maximum length of  4 character strings  */
#define STATECNT 256    /* Maximum number of states for a discrete */
#define SYSCNT    64    /* Maximum number of subsystems            */
#define NCALSEGS  20    /* Maximum number of calibration segments  */
#define BUFLEN  4000    /*String buffer for mult-selects*/
#define BUFMEAN   10    /* Mean # of buffers for handling TMIDS    */


#define IDLAPI_BEGIN(X) \
   int idl_##X(int argc, void **argv) \
   {                                \
      char *fnName = #X;

#define IDLAPI_END      \
   }

typedef enum kDRMSObjType_enum
{
   kDRMSObjType_Start = 0,
   kDRMSObjType_RecSet,
   kDRMSObjType_NumObjsPlusOne,
} kDRMSObjType_t;

static const char *gObjTypeStrings[] = 
{
   NULL,
   "DRMS_RecordSet",
   NULL,
};

int gDoLog = 0;
int gVerbose = 0;
pid_t gDRMSServerPID = 0;
pid_t gTeePID = 0;

/*****************************************************************************/
/* Set the environment for jsoc                                              */
/****************************************************************************/

/******************************************************
 * static helper functions
 ******************************************************/

static int IsValidDRMSObj(kDRMSObjType_t t)
{
   return (t > kDRMSObjType_Start && t < kDRMSObjType_NumObjsPlusOne);
}

/* Returns a pointer to the handle if <type> is valid and inserting <ptr> with 
 * the generated handle succeeds (the handle must be unique, for example).
 */
static const char *GetHandle(const void *ptr, kDRMSObjType_t type, const char *func)
{
   const char *ret_str = NULL;

   char buf[kMaxIDLHandleKey];
   char *pBuf = buf;
   const char *typeStr = NULL;

   if (IsValidDRMSObj(type))
   {
      typeStr = gObjTypeStrings[type];
      snprintf(buf, sizeof(buf), "%p;%s;%s", ptr, typeStr, func);
      InsertJSOCStructure(&pBuf, (void *)ptr, kIDLHandleTypeDRMS, &ret_str);
   }

   return ret_str;
}

/* Checks validity of <handle>, returns NULL if not valid, otherwise returns
 * the requested DRMS object.
 */
static void *GetDRMSObj(const char *handle)
{
   const char **pHandle = &handle;
   return GetJSOCStructure((void *)pHandle, kIDLHandleTypeDRMS);
}

static void AddArg(char *argv, int *iarg, const char *argname, const char *argvalue)
{
   snprintf(&(argv[*iarg * DRMS_MAXPATHLEN]), DRMS_MAXPATHLEN, "%s=%s", argname, argvalue);
   *iarg = *iarg + 1;
}

/***********************************************************
 *  API functions
 *    Every function uses the IDLAPI_BEGIN macro.  To use
 *    a function declared as IDLAPI_BEGIN(FXN), call
 *    idl_FXN.
 *
 *    Functions return 0 upon faliure and 1 upon success.
 ***********************************************************/

IDLAPI_BEGIN(drms_open_records)
{
   const char *seriesin = (char *)argv[0];
   char *handleout = (char *)argv[1];
   const int *bufsize = (char *)argv[2];
   char si[DRMS_MAXNAMELEN];
   const char *keyout = NULL;
   long success = 0;
   int stat;
   DRMS_RecordSet_t *recs = NULL;

   fprintf(stdout, "Entering idl_drms_open_records\n");
   fprintf(stdout, "  series: %s\n", seriesin);
   fprintf(stdout, "  bufsize: %d\n", *bufsize);

   strncpy(si, seriesin, sizeof(si));

   recs = drms_open_records(drms_env, si, &stat);
   printf("  status %d\n", stat);

   if (stat == DRMS_SUCCESS)
   {
      printf("got records okay\n");
      keyout = GetHandle(recs, kDRMSObjType_RecSet, fnName);
      if (keyout)
      {
	 printf("created handle okay\n");
	 snprintf(handleout, *bufsize, "%s", keyout);
	 success = 1;
      }
      else
      {
	 drms_close_records(recs, DRMS_FREE_RECORD);
      }
   }
  
   return success; 
}
IDLAPI_END

/* Sample using previously assigned handle. */
IDLAPI_BEGIN(drms_process_records)
{
   /* Uses a DRMS_RecordSet_t handle previously assigned by another api function. */
   const char *handlein = argv[0];

   DRMS_RecordSet_t *rs = GetDRMSObj(handlein);
   return 0;
}
IDLAPI_END

/*****************************************************************************/
/* Routine for opening jsoc records.                                          */
/*****************************************************************************/
IDLAPI_BEGIN(db_connect)
{ 
   void error_handler ();
   void warning_handler ();

   long success = 0;

   char *name_in;
   char *password_in;
   char *server_in;
   char *dbname_in;
   char *session_in;

   fprintf(stdout, "Entering idl_db_connect\n");

   if (gDRMSServerPID == 0)
   {
      /* dolog and verbose should be parameters to this function. */
      int dolog = 0;
      int verbose = 1;
      pid_t drms_server = 0;
      pid_t tee = 0;

      char argvTemp[8 * DRMS_MAXPATHLEN];
      char **argvDRMS = NULL;
      int argcDRMS = 0;
      int iarg;

      name_in     = (char  *)argv[0]; /* JSOC_DBUSER */
      password_in = (char  *)argv[1]; /* JSOC_DBPASSWD */
      server_in   = (char  *)argv[2]; /* JSOC_DBHOST */
      dbname_in   = (char  *)argv[3]; /* JSOC_DBNAME */
      session_in  = (char  *)argv[4]; /* DRMSSESSION */

      if (strlen(name_in) > 0)
      {
	 AddArg(argvTemp, &argcDRMS, "JSOC_DBUSER", name_in);
      }

      if (strlen(password_in) > 0)
      {
	 AddArg(argvTemp, &argcDRMS, "JSOC_DBPASSWD", password_in);
      }

      if (strlen(server_in) > 0)
      {
	 AddArg(argvTemp, &argcDRMS, "JSOC_DBHOST", server_in);
      }

      if (strlen(dbname_in) > 0)
      {
	 AddArg(argvTemp, &argcDRMS, "JSOC_DBNAME", dbname_in);
      }

      if (strlen(session_in) > 0)
      {
	 AddArg(argvTemp, &argcDRMS, "DRMSSESSION", session_in); /* host:port */
      }

      /* first parameter will be module name - cmdparams expects this */
      argcDRMS++;

      if (argcDRMS > 0)
      {
	 argvDRMS = (char **)malloc(argcDRMS * sizeof(char *));

	 if (argvDRMS != 0)
	 {
	    iarg = 0;

	    argvDRMS[iarg++] = strdup("idlclient"); /* cmdparams code expect the module name to be
						     * first parameter
						     */

	    while (iarg < argcDRMS)
	    {
	       argvDRMS[iarg] = strdup(&(argvTemp[(iarg - 1) * DRMS_MAXPATHLEN]));
	       fprintf(stdout, argvDRMS[iarg]);
	       fprintf(stdout, "\n");
	       iarg++;
	    }
	 }
      }

      if (StartUp(argcDRMS, argvDRMS, &dolog, &verbose, &drms_server, &tee) == kIDLRet_Success)
      {
	 printf("drms_server pid: %d, tee pid: %d\n", drms_server, tee);
	 gDoLog = dolog;
	 gVerbose = verbose;
	 gDRMSServerPID = drms_server;
	 gTeePID = tee;
	 success = 1;
      }
      else
      {
	 fprintf(stdout, "DRMS Error: couldn't create drms session.\n");
      }

      if (argvDRMS)
      {
	 free(argvDRMS);
      }
   }

   return success;
}
IDLAPI_END

/*****************************************************************************/
/* Routine for disconnecting from the postgres database.                     */ 
/* This routine closes the connection to the database.                       */
/*****************************************************************************/

IDLAPI_BEGIN(db_disconnect)
{
   long success = 0;

   fprintf(stdout, "Entering idl_db_disconnect\n");

   if (gDRMSServerPID != 0)
   {
      int abortFlag = *((int *)(argv[0])); /* IDL's long is C's int */

      success = 
	(ShutDown(abortFlag, gDoLog, gVerbose, gDRMSServerPID, gTeePID) == kIDLRet_Success);
      
      if (success)
      {
	 gDRMSServerPID = 0;
      }
   }

   return success;
}
IDLAPI_END
