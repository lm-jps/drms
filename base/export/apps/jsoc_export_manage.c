#include "list.h"

// #define DEBUG 1
#define DEBUG 0

// insubstantial change to test make

// TESTMODE=1 means look for status=12 in jsoc.export_new, used for testing jsoc_export_manage
// #define TESTMODE 1
#define TESTMODE 0

/* 
 * jsoc_fetch writes the size that SUM_info returns into the Size keyword of the jsoc.export_new series, 
 * which is then copied to jsoc.export.  But this value is meaningless since the export hasn't even
 * been performed yet, and I'm not even sure exactly what the value returned by SUM_info means.
 * This module overwrites jsoc.export:Size by calling extractexpsize.pl, which extracts the 
 * true download size from the index.json file.
 */

/*
 *  jsoc_export_manage - program to manage jsoc export requests posted to jsoc.export
  
  This program provides some service functions to manage export requests using
  the jsoc.export, jsoc.export_new, and jsoc.export_user series.

  In the default mode the program watches for new requests and submits them to
  the cluster for processing.  The submission is handled via a pair of scripts.
  the first is a qsub script that executes a drms_run call of the second.
  The steps are as follows:

    1.  foreach record with status==2 in jsoc.export_new do: 
        a. create new record in jsoc.export and fill with contents of
           the jsoc.export_new record.
        b. perhaps later handle the contact info better to work with
           a list of users and addresses.  For now just leave jsoc_export_user
           alone.
        c. get record directory of new jsoc.export record, REQDIR.
        c. get the Processing keyword and check for valid operation.
        d. Get the DataSet keyword
        e. Build processing command 
        f. get RequestID
        g. get other keywords such as Notify.
        g. create a script file at: REQDIR/<RequestID>.qsub that contains:
                # /bin/csh -f
		cd REQDIR
                drms_run <RequestID>.drmsrun
                if ($status) then
                  set_keys ds="jsoc.export[<RequestID>] status=4
		  set subject="JSOC export <RequestID> FAILED"
                  set msg="Error status returned from DRMS session, see log files at http://jsoc.stanford.edu/REQDIR"
                  mail -s "JSOC export <RequestID> FAILED" <NOTIFY> <<!
                  Error status returned from DRMS session.
		  See log files at http://jsoc.stanford.edu/REQDIR
                  !
                endif
                
        h. create a script file at: REQDIR/<RequestID>.drmsrun
            1. write preamble for running in cluster via qsub.
            2. add processing command with stdout and stderr to local files in REQDIR
            3. add generation of index.html, index.txt, index.json, index.xml(?)
            4. set status=0 in jsoc.export[<RequestID>]
        i  perhaps add initial versions of index.html, etc. 
        j. execute the program: qsub -j.q REQDIR/<RequestID>.qsub
        k. set status=1 in jsoc.export[<RequestID>] in a (maybe)transient record.
     
  Later additions to this program may provide monitoring services to watch the status.

  Command line args of JSOC_DBHOST and JSOC_DBMAINHOST are now used to direct queries to
  one database server for export processing (JSOC_DBHOST) and another for access to science
  data to be exported (JSOC_DBMAINHOST).

 *
 */
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "json.h"
#include "serverdefs.h"

#define EXPORT_SERIES "jsoc.export"
#define EXPORT_SERIES_NEW "jsoc.export_new"
#define EXPORT_USER "jsoc.export_user"

#define PACKLIST_VER "0.5"

#define kArgTestmode    "t"
#define kArgProcSeries  "procser"

#define kMaxProcNameLen 128
#define kMaxIntVar 64
#define kMaxShVar  32
#define kMaxArgVar 64

#define DIE(msg) { fprintf(stderr,"XXXX jsoc_exports_manager failure: %s\nstatus=%d",msg,status); exit(1); }

#define kHgPatchLog "hg_patch.log"

enum Protocol_enum
{
   kProto_AsIs = 0,
   kProto_FITS = 1,
   kProto_MPEG = 2,
   kProto_JPEG = 3,
   kProto_PNG  = 4,
   kProto_MP4  = 5
};

typedef enum Protocol_enum Protocol_t;

const char *protos[] =
{
   "as-is",
   "fits",
   "mpg", 
   "jpg",
   "png",
   "mp4"
};

struct ExpProcArg_struct
{
    char *name;
    char *def;
};

typedef struct ExpProcArg_struct ExpProcArg_t;

struct ProcStep_struct
{
    char *name; /* Name of program. */
    char *path; /* Path to program. */
    char *args; /* Program argument key=value pairs, comma-separated. */
    char *input; /* data-set input to processing */
    char *output; /* data-set output by processing */
};

typedef struct ProcStep_struct ProcStep_t;

/* Proc series keyword names. */
#define kProcSerCOLproc     "proc"
#define kProcSerCOLpath     "path"
#define kProcSerCOLreq      "required"
#define kProcSerCOLopt      "optional"
#define kProcSerCOLmap      "map"
#define kProcSerCOLout      "out"

struct ProcStepInfo_struct
{
    char *name;
    char *path;
    LinkedList_t *req;
    LinkedList_t *opt;
    HContainer_t *namemap;
    char *suffix;
    /* TBD */
};

typedef struct ProcStepInfo_struct ProcStepInfo_t;

ModuleArgs_t module_args[] =
{ 
    {ARG_STRING, "op", "process", "<Operation>"},
    {ARG_STRING, kArgProcSeries, "jsoc.export_procs", "The series containing the list of available processing steps. There is one such series on hmidb and one on hmidb2."},
    {ARG_FLAG, kArgTestmode, NULL, "if set, then operates on new requests with status 12 (not 2)"},
    {ARG_FLAG, "h", "0", "help - show usage"},
    {ARG_END}
};

char *module_name = "jsoc_export_manage";

/* The internal (to jsoc_export_manage) variables available for use as export program
 * argument values. */
HContainer_t *gIntVars = NULL;
HContainer_t *gShVars = NULL;

static void FreeIntVars(void *data)
{
    char **pstr = (char **)data;
    
    if (pstr && *pstr)
    {
        free(*pstr);
        *pstr = NULL;
    }
}

static void FreeShVars(void *data)
{
    FreeIntVars(data);
}

static int RegisterIntVar(const char *key, char type, void *val)
{
    char numbuf[64];
    char *strval = NULL;
    int err;
    
    err = 0;
    
    if (key && val)
    {
        switch (type)
        {
            case 's':
                strval = strdup((char *)val);
                break;
            case 'i':
                snprintf(numbuf, sizeof(numbuf), "%d", *((int *)val));
                strval = strdup(numbuf);
                break;
            case 'f':
                snprintf(numbuf, sizeof(numbuf), "%f", *((double *)val));
                strval = strdup(numbuf);
                break;
            default:
                err = 1;
        }
        
        if (!gIntVars)
        {
            gIntVars = hcon_create(sizeof(char *), 
                                   kMaxIntVar, 
                                   (void (*)(const void *))FreeIntVars,
                                   NULL,
                                   NULL,
                                   NULL,
                                   0);
        }
        
        if (!gIntVars)
        {
            fprintf(stderr, "Out of memory.\n");
            err = 1;
        }
        
        /* Remove previous value, if it exists, otherwise hcon_insert() will be a noop. */
        hcon_remove(gIntVars, key);
        
        /* Assumes ownership of string value. */
        hcon_insert(gIntVars, key, &strval);
    }
    else
    {
        err = 1;
    }
    
    return err;
}

static int RegisterShVar(const char *key, const char *val)
{
    char *strval = NULL;
    int err;
    
    err = 0;
    
    if (key && val)
    {
        if (!gShVars)
        {
            gShVars = hcon_create(sizeof(char *), 
                                   kMaxShVar, 
                                   (void (*)(const void *))FreeShVars,
                                   NULL,
                                   NULL,
                                   NULL,
                                   0);
        }
        
        if (!gShVars)
        {
            fprintf(stderr, "Out of memory.\n");
            err = 1;
        }
        
        strval = strdup(val);
        
        /* Assumes ownership of string value. */
        hcon_insert(gShVars, key, &strval);
        
    }
    else
    {
        err = 1;
    }
    
    return err;
}

/* The caller wants to connect to the db on the host specified by dbhost. The current context, 
 * env, may hanve a handle to the desired connection. If so, contextOKout is set to 1. If not, 
 * the desired connection is made, then contextOKout is set to 0. */
static DB_Handle_t *GetDBHandle(DRMS_Env_t *env, const char *dbhost, int *contextOKout, int close)
{
   static DB_Handle_t *dbh = NULL;
   static int contextOK = -1;
   int forceconn = 0;
   char contexthost[DRMS_MAXHOSTNAME];

   if (dbh != NULL)
   {
      if (close && contextOK == 0)
      {
         /* Do not close the context db connection. */
         db_disconnect(&dbh);
         return NULL;
      }

      if (contextOKout)
      {
         *contextOKout = contextOK;
      }

      return dbh;
   }
    else if (close)
    {
        /* There was no db connection - do nothing. */
        return NULL;
    }

   XASSERT(env && dbhost);

#ifdef DRMS_CLIENT
   /* For a sock module, the db host to which it has access is the db host that the 
    * serving drms_server is connected to. And I don't think there is a way to 
    * determine to which db host drms_server is connected, so we'll HAVE TO 
    * connect to dbhost here, regardless of the existing connection between 
    * drms_server and a db. */
   forceconn = 1;
#else
   /* For a server module, hostname is in drms_env->session->db_handle->dbhost. */
   snprintf(contexthost, sizeof(contexthost), "%s", env->session->db_handle->dbhost);
   forceconn = (strcasecmp(dbhost, contexthost) != 0);
#endif

   if (forceconn)
   {
      /* The caller wants to check for series existence in a db on host to which 
       * this module has no connection */

      /* Use db_connect to connect to the jsoc db on dbhost. Steal dbuser from the existing, 
       * irrelevant db connection. */
      if ((dbh = db_connect(dbhost, env->session->db_handle->dbuser, NULL, "jsoc", 1)) == NULL)
      {
         fprintf(stderr,"Couldn't connect to jsoc database on %s.\n", dbhost);
      }
      else
      {
         /* Successful connection. */
         if (contextOK == -1)
         {
            contextOK = 0;
         }
      }
   }
   else
   {
      dbh = env->session->db_handle;
      if (contextOK == -1)
      {
         contextOK = 1;
      }
   }

   if (contextOKout)
   {
      *contextOKout = contextOK;
   }

   return dbh;
}

static void CloseDBHandle()
{
   GetDBHandle(NULL, NULL, NULL, 1);
}

static void FreeRecSpecParts(char ***snames, char ***filts, int nitems)
{
   if (snames)
   {
      int iname;
      char **snameArr = *snames;

      if (snameArr)
      {
	 for (iname = 0; iname < nitems; iname++)
	 {
	    char *oneSname = snameArr[iname];

	    if (oneSname)
	    {
	       free(oneSname);
	    }
	 }

	 free(snameArr);
      }

      *snames = NULL;
   }

   if (filts)
   {
      int ifilt;
      char **filtArr = *filts;

      if (filtArr)
      {
         for (ifilt = 0; ifilt < nitems; ifilt++)
         {
            char *onefilt = filtArr[ifilt];

            if (onefilt)
            {
               free(onefilt);
            }
         }

         free(filtArr);
      }

      *filts = NULL;
   }
}

/* returns by reference an array of series names determined by parsing the rsquery record-set query. */
/* returns 1 if an error occurred, 0 otherwise. */
static int ParseRecSetSpec(DRMS_Env_t *env, 
                           const char *dbhost, 
                           const char *rsquery, 
                           char ***snamesout, 
                           char ***filtsout,
                           int *nsetsout, 
                           DRMS_RecQueryInfo_t *infoout)
{
   int err = 0;
   char *allvers = NULL;
   char **sets = NULL;
   DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
   char **snames = NULL;
   int nsets = 0;
   DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
   int iset;
   DRMS_Record_t *template = NULL;
   int drmsstat = 0;
   char *filter = NULL;
   DB_Handle_t *dbh = NULL;
   int contextOK = 0;

   if (drms_record_parserecsetspec(rsquery, &allvers, &sets, &settypes, &snames, &nsets, &rsinfo) == DRMS_SUCCESS)
   {     
      *infoout = rsinfo;
      *nsetsout = nsets;

      if (nsets > 0)
      {
         *snamesout = (char **)calloc(nsets, sizeof(char *));
         *filtsout = (char **)calloc(nsets, sizeof(char *));

         if (snamesout && filtsout)
         {
            dbh = GetDBHandle(env, dbhost, &contextOK, 0);

            if (!dbh)
            {
               fprintf(stderr, "jsoc_export_manage: Unable to connect to database.\n");
               err = 1;
            }
            else
            {
               for (iset = 0; iset < nsets; iset++)
               {
                  (*snamesout)[iset] = strdup(snames[iset]);
               
                  if (contextOK)
                  {
                     /* Get template record. */
                     template = drms_template_record(env, snames[iset], &drmsstat);
                     if (DRMS_ERROR_UNKNOWNSERIES == drmsstat)
                     {
                        fprintf(stderr, "Unable to open template record for series '%s'; this series does not exist.\n", snames[iset]);
                        err = 1;
                        break;
                     }
                     else
                     {
                        filter = drms_recordset_extractfilter(template, sets[iset], &err);
                     }
                  }
                  else
                  {
                     filter = drms_recordset_extractfilter_ext(dbh, sets[iset], &err);
                  }

                  if (!err && filter)
                  {
                     (*filtsout)[iset] = filter; /* transfer ownership to caller. */
                  }
               }
            }
         }
         else
         {
            fprintf(stderr, "jsoc_export_manage FAILURE: out of memory.\n");
            err = 1;
         }
      }
   }
   else
   {
      fprintf(stderr, "jsoc_export_manage FAILURE: invalid record-set query %s.\n", rsquery);
      err = 1;
   }
     
   drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, nsets);

   if (err == 1)
   {
      /* free up stuff */
      if (nsets > 0)
      {
         FreeRecSpecParts(snamesout, filtsout, nsets);
      }
   }

   return err;
}

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\njsoc_info {-h} "
        "  details are:\n"
	"op=<command> tell which function to execute\n"
        "   choices are:\n"
        "       process - get new export requests and submit scripts.\n"
        "       TBD\n"
	"h=help - show usage\n"
	);
    return(1);
    }
  return (0);
  }

// generate qsub script
void make_qsub_call(char *requestid, char *reqdir, int requestorid, const char *dbname, 
               const char *dbuser, const char *dbids, const char *dbexporthost)
  {
  FILE *fp;
  char qsubscript[DRMS_MAXPATHLEN];
  // Note that the reqdir where the script is put and is run from will not
  // be the record dir that is visible when the drms_run run is finished.
  sprintf(qsubscript, "%s/%s.qsub", reqdir, requestid);
  fp = fopen(qsubscript, "w");
  fprintf(fp, "#! /bin/csh -f\n");
  fprintf(fp, "set echo\n");
  // fprintf(fp, "setenv SUMSERVER d02\n");
  // fprintf(fp, "unsetenv DRMSSESSION\n");
  // fprintf(fp, "unsetenv SETJSOCENV\n");
  // fprintf(fp, "unsetenv SET_SOLAR_ENV\n");
  // fprintf(fp, "source /home/phil/.sunrc\n");
  // fprintf(fp, "source /home/jsoc/.setJSOCenv\n");
  // fprintf(fp, "printenv\n");

  /* Set path based on export root (if set) */
  fprintf(fp, "if (${?JSOCROOT_EXPORT}) then\n");
  fprintf(fp, "  set path = ($JSOCROOT_EXPORT/bin/$JSOC_MACHINE $JSOCROOT_EXPORT/scripts $path)\n");
  fprintf(fp,"endif\n");

  fprintf(fp, "while (`show_info JSOC_DBHOST=%s -q 'jsoc.export_new[%s]' key=Status %s` == 2)\n", dbexporthost, requestid, dbids);
  fprintf(fp, "  echo waiting for jsocdb commit >> /home/jsoc/exports/tmp/%s.runlog \n",requestid);
  fprintf(fp, "  sleep 1\nend \n");
  if (dbname)
  {
     fprintf(fp,   "setenv JSOC_DBNAME %s\n", dbname);
  }
  if (dbuser)
  {
     fprintf(fp,   "setenv JSOC_DBUSER %s\n", dbuser);
  }
// TEST
//fprintf(fp,   "setenv JSOC_DBHOST %s\n", dbmainhost);
  fprintf(fp,   "setenv JSOC_DBHOST %s\n", dbexporthost);
  fprintf(fp,   "setenv JSOC_DBEXPORTHOST %s\n", dbexporthost);
  fprintf(fp, "drms_run JSOC_DBHOST=%s %s/%s.drmsrun >>& /home/jsoc/exports/tmp/%s.runlog \n", dbexporthost, reqdir, requestid, requestid);
  fprintf(fp, "set DRMS_ERROR=$status\n");
  fprintf(fp, "set NewRecnum=`cat /home/jsoc/exports/tmp/%s.recnum` \n", requestid);
  fprintf(fp, "set WAITCOUNT = 60\n");
  fprintf(fp, "while (`show_info JSOC_DBHOST=%s -q -r 'jsoc.export[%s]' %s` < $NewRecnum)\n", dbexporthost, requestid, dbids);
  fprintf(fp, "  echo waiting for jsocdb drms_run commit >> /home/jsoc/exports/tmp/%s.runlog \n",requestid);
  fprintf(fp, "  @ WAITCOUNT = $WAITCOUNT - 1\n");
  fprintf(fp, "  if ($WAITCOUNT <= 0) then\n    set DRMS_ERROR = -1\n    break\n  endif\n");
  fprintf(fp, "  sleep 1\nend \n");
  if (requestorid)
     fprintf(fp, "set Notify=`show_info JSOC_DBHOST=%s -q 'jsoc.export_user[? RequestorID=%d ?]' key=Notify %s` \n", dbexporthost, requestorid, dbids);
  fprintf(fp, "set REQDIR = `show_info JSOC_DBHOST=%s -q -p 'jsoc.export[%s]' %s`\n", dbexporthost, requestid, dbids);
  fprintf(fp, "if ($DRMS_ERROR) then\n");
  fprintf(fp, "  set_keys -C JSOC_DBHOST=%s  ds='jsoc.export[%s]' Status=4 %s\n", dbexporthost, requestid, dbids);
  if (requestorid)
     {
     fprintf(fp, "  mail -n -s 'JSOC export FAILED - %s' $Notify <<!\n", requestid);
     fprintf(fp, "Error status returned from DRMS session.\n");
     fprintf(fp, "See log files at http://jsoc.stanford.edu/$REQDIR\n");
     fprintf(fp, "Also complete log file at /home/jsoc/exports/tmp/%s.runlog\n", requestid);
     fprintf(fp, "!\n");
     }
  fprintf(fp, "else\n");
  if (requestorid)
     {
     fprintf(fp, "  mail -n -s 'JSOC export complete - %s' $Notify <<!\n", requestid);
     fprintf(fp, "JSOC export request %s is complete.\n", requestid);
     fprintf(fp, "Results at http://jsoc.stanford.edu/$REQDIR\n");
     fprintf(fp, "!\n");
     }
  fprintf(fp, "  rm /home/jsoc/exports/tmp/%s.recnum\n", requestid);
  fprintf(fp, "  mv /home/jsoc/exports/tmp/%s.reqlog /home/jsoc/exports/tmp/done \n", requestid);
  /* The log after the call to drms_run gets lost because of the following line - should preserve this
   * somehow (but it can't go in the new inner REQDIR because that is read-only after drms_run returns. */
  fprintf(fp, "  mv /home/jsoc/exports/tmp/%s.runlog /home/jsoc/exports/tmp/done \n", requestid);
  fprintf(fp, "endif\n");
  fclose(fp);
  chmod(qsubscript, 0555);
  }

// Security testing.  Make sure DataSet does not contain attempt to run program
//   example: look out for chars that end dataset spec and give command.
int isbadDataSet()
  {
  return(0);
  }

// Security testing.  Make sure Processing does not contain attempt to run illegal program
int isbadProcessing()
  {
  return(0);
  }

#include <time.h>
TIME timenow()
  {
  TIME UNIX_epoch = -220924792.000; /* 1970.01.01_00:00:00_UTC */
  TIME now = (double)time(NULL) + UNIX_epoch;
  return(now);
  }

enum PParseState_enum
{
   kPPStError,
   kPPStBeginProc,
   kPPStOneProc,
   kPPStEndProc,
   kPPStEnd
};

typedef enum PParseState_enum PParseState_t;

static void FreeProcStep(void *ps)
{
    ProcStep_t *data = (ProcStep_t *)ps;
    
    if (data && data->name)
    {
        free(data->name);
    }
    
    if (data && data->args)
    {
        free(data->args);
    }
    
    if (data && data->input)
    {
        free(data->input);
    }
    
    if (data && data->output)
    {
        free(data->output);
    }
}

static void FreeProcStepInfo(void *val)
{
    ProcStepInfo_t *info = (ProcStepInfo_t *)val;
    
    if (info->name)
    {
        free(info->name);
    }
    
    if (info->path)
    {
        free(info->path);
    }
    
    if (info->req)
    {
        list_llfree(&info->req);
    }
    
    if (info->opt)
    {
        list_llfree(&info->opt);
    }
    
    if (info->namemap)
    {
        hcon_destroy(&info->namemap);
    }
    
    if (info->suffix)
    {
        free(info->suffix);
    }
}

static void FreeProcArg(void *data)
{
    ExpProcArg_t *parg = (ExpProcArg_t *)data;
    
    if (parg)
    {
        if (parg->name)
        {
            free(parg->name);
        }
        
        if (parg->def)
        {
            free(parg->def);
        }
        
        memset(parg, 0, sizeof(ExpProcArg_t));
    }
}

static LinkedList_t *ParseArgs(const char *list, int dodef, int *status)
{
    char *arg = NULL;
    char *name = NULL;
    char *def = NULL; /* for options only */
    char *cpy = NULL;
    char *pc = NULL;
    char *eq = NULL;
    char *co = NULL;
    int intstat;
    int done;
    LinkedList_t *lst = NULL;
    ExpProcArg_t argnode;
    
    if (list)
    {
        cpy = strdup(list);
        
        intstat = DRMS_SUCCESS;
        for (pc = cpy, done = 0; !done;)
        {
            arg = pc;
            co = strchr(pc, ',');
            if (co)
            {
                *co = '\0';
                pc = co + 1;
            }
            else
            {
                /* done */
                done = 1;
            } 
            
            name = arg;
            argnode.def = NULL;
            
            if (dodef)
            {
                eq = strchr(arg, '=');
                if (eq)
                {
                    *eq = '\0';
                    def = eq + 1;
                    argnode.def = strdup(def);
                }
            }
            
            argnode.name = strdup(name);
            
            if (!lst)
            {
                lst = list_llcreate(sizeof(ExpProcArg_t), (void (*)(const void *))FreeProcArg);
            }
            
            if (lst)
            {
                list_llinserttail(lst, &argnode);
            }
            else
            {
                intstat = DRMS_ERROR_OUTOFMEMORY;
            }
        }
    }
    else
    {
        intstat = DRMS_ERROR_INVALIDDATA;
    }
    
    if (status)
    {
        *status = intstat;
    }
    
    return lst;
}

static void FreeProcMap(void *data)
{
    char **pname = (char **)data;
    
    if (pname && *pname)
    {
        free(*pname);
        *pname = NULL;
    }
}

static HContainer_t *ParseMap(const char *list, int *status)
{
    HContainer_t *map = NULL;
    char *cpy = NULL;
    int intstat;
    int done;
    char *pc = NULL;
    char *co = NULL;
    char *eq = NULL;
    char *mapping = NULL;
    char *argname = NULL;
    char *intname = NULL;
    
    intstat = 0;
    if (list && *list)
    {
        cpy = strdup(list);
        if (cpy)
        {
            for (pc = cpy, done = 0; !done; )
            {
                mapping = pc;
                co = strchr(pc, ',');
                if (co)
                {
                    *co = '\0';
                    pc = co + 1;
                }
                else
                {
                    /* done */
                    done = 1;
                }
                
                argname = mapping;
                
                eq = strchr(mapping, ':');
                if (!eq)
                {
                    fprintf(stderr, "Invalid argument - no internal name supplied.\n");
                    intstat = DRMS_ERROR_INVALIDDATA;
                    break;
                }
                else
                {
                    *eq = '\0';
                    intname = strdup(eq + 1);
                    
                    if (!intname)
                    {
                        intstat = DRMS_ERROR_OUTOFMEMORY;
                        break;
                    }
                }
                
                if (!map)
                {
                    map = hcon_create(sizeof(char *), 
                                      kMaxProcNameLen, 
                                      (void (*)(const void *))FreeProcMap,
                                      NULL,
                                      NULL,
                                      NULL,
                                      0);
                }
                
                hcon_insert(map, argname, &intname);
            }
            
            free(cpy);
        }
        else
        {
            intstat = DRMS_ERROR_OUTOFMEMORY;
        }
    }
    
    if (status)
    {
        *status = intstat;
    }
    
    return map;
}

static int SuckInProcInfo(DRMS_Env_t *env, const char *procser, HContainer_t *info)
{
    /* Must support these 
     ProcStepInfo_t pi;
     
     pi.name = strdup("Not Specified");
     pi.suffix = strdup("");
     hcon_insert(pinfo, "Not Specified", &pi);
     
     pi.name = strdup("no_op");
     pi.suffix = strdup("");
     hcon_insert(pinfo, "no_op", &pi);
     
     pi.name = strdup("hg_patch");
     pi.suffix = strdup("hgpatch");
     hcon_insert(pinfo, "hg_patch", &pi);
     
     pi.name = strdup("su_export");
     pi.suffix = strdup("");
     hcon_insert(pinfo, "su_export", &pi);
     
     //Eventually we will have to have a real suffix for this - right now we special case: 
     //aia.lev1 --> aia_test.lev1p5. 
     pi.name = strdup("aia_scale");
     pi.suffix = strdup("");
     hcon_insert(pinfo, "aia_scale", &pi);
     */
    
    int status;
    DRMS_RecordSet_t *prs = NULL;
    DRMS_Record_t *rec = NULL;
    DRMS_RecChunking_t chunkstat;
    int newchunk;
    
    char *proc = NULL;
    char *path = NULL;
    char *req = NULL;
    char *opt = NULL;    
    char *map = NULL;
    char *out = NULL;
    
    ProcStepInfo_t pi;
    
    status = DRMS_SUCCESS;
    
    if (info)
    {
        prs = drms_open_records(env, procser, &status);
        if (status == DRMS_SUCCESS && prs)
        {
            if (prs->n > 0)
            {
                while ((rec = drms_recordset_fetchnext(env, prs, &status, &chunkstat, &newchunk)) 
                       != NULL)
                {
                    proc = drms_getkey_string(rec, kProcSerCOLproc, &status);
                    if (status) break;
                    path = drms_getkey_string(rec, kProcSerCOLpath, &status);
                    if (status) break;
                    req = drms_getkey_string(rec, kProcSerCOLreq, &status);
                    if (status) break;
                    opt = drms_getkey_string(rec, kProcSerCOLopt, &status);
                    if (status) break;
                    map = drms_getkey_string(rec, kProcSerCOLmap, &status);
                    if (status) break;
                    out = drms_getkey_string(rec, kProcSerCOLout, &status);
                    if (status) break;
                    
                    /* No need to dupe strings - drms_getkey_string() already does that. */
                    /* The objects in the db have already passed a syntax test. */
                    pi.name = proc;
                    pi.path = path;
                    pi.req = ParseArgs(req, 0, &status);
                    if (status) break;
                    pi.opt = ParseArgs(opt, 1, &status);
                    if (status) break;
                    pi.namemap = ParseMap(map, &status);
                    if (status) break;
                    pi.suffix = out;
                    
                    /* container takes ownership of all strings, lists, and containers. */
                    hcon_insert(info, proc, &pi);
                }
            }
            
            drms_close_records(prs, DRMS_FREE_RECORD);
        }
    }
    
    return status;
}

static int NumPKeyKeys(DRMS_Env_t *env, const char *dbhost, const char *series)
{
    int rv = -1;
    DB_Handle_t *dbh = NULL;
    int contextOK = 0;
    int istat = 0;
    
    /* returns NULL on error. */
    dbh = GetDBHandle(env, dbhost, &contextOK, 0);
    
    if (!dbh)
    {
        fprintf(stderr, "jsoc_export_manage: Unable to connect to database.\n");
    }
    else
    {
        if (!contextOK)
        {
            /* The caller wants to check for series existence in a db on host to which 
             * this module has no connection. Use dbh, not env. */
            
            /* Successfully connected to dbhost; can't use DRMS calls since we don't have a 
             * functioning environment for this ad hoc connection. */
            char query[512];
            char *schema = NULL;
            DB_Text_Result_t *res = NULL;
            
            if (get_namespace(series, &schema, NULL))
            {
                fprintf(stderr, "Invalid series name %s.\n", series);
            }
            else
            {
                snprintf (query, 
                          sizeof(query), 
                          "SELECT primary_idx FROM %s.drms_series WHERE lower(seriesname) = lower(\'%s\')", 
                          schema, 
                          series);
                
                res = db_query_txt(dbh, query);
                
                if (res && res->num_rows == 1) 
                {
                    const char *pidx = res->field[0][0];
                    int nkeys = 0;
                    const char *pc = pidx;
                    
                    if (*pc == '\0')
                    {
                        /* No prime keys. */
                        rv = 0;
                    }
                    else
                    {    
                        nkeys++;
                        
                        /* bah, gotta parse pidx to get number of keys. */
                        while (*pc)
                        {
                            if (*pc == ',')
                            {
                                nkeys++;
                            }
                            
                            pc++;
                        }
                        
                        rv = nkeys;
                    }
                    
                    db_free_text_result(res);
                    res = NULL;
                }
                else
                {
                    fprintf(stderr, "Invalid SQL query: %s.\n", query);
                }
                
                free(ns);
            }
        }
        else
        {
            /* The caller wants to get the number of prime keys of a series in the database to which 
             * this modue is currently connected. */
            DRMS_Record_t *template = drms_template_record(env, series, &istat);
            
            if (istat != DRMS_SUCCESS || !template)
            {
                fprintf(stderr, "Problems obtaining template record for %s on %s.\n", series, dbhost);
            }
            else
            {
                rv = template->seriesinfo->pidx_num;
            }
        }
    }

    return rv;
}

void FreeArgInfo(const void *data)
{
   char **info = (char **)data;

   if (info && *info)
   {
      free(*info);
      *info = NULL;
   }
}

/* pvarsargs - arguments/values from jsoc.export_new. */
static int InitVarConts(const char *args, 
                        HContainer_t **pvarsargs /* both req and opt args. */)
{
    char *cpy = NULL;
    char *pc = NULL;
    char *onename = NULL;
    char *oneval = NULL;
    int err;
    
    err = 0;
    
    /* Optional parameters in the processing field of jsoc.export_new (in opt). */
    /* Must parse comma-separated list. */
    if (pvarsargs && !*pvarsargs && args)
    {
        const char *valcpy = NULL;
        *pvarsargs = hcon_create(sizeof(char *),
                                 kMaxArgVar, 
                                 (void (*)(const void *))FreeArgInfo,
                                 NULL,
                                 NULL,
                                 NULL,
                                 0);
        
        if (*args)
        {
            cpy = strdup(args);
            pc = cpy;
            onename = pc;
            while (*pc)
            {
                if (*pc == '=')
                {
                    *pc = '\0';
                    oneval = ++pc; /* for varsargs, RHS of '=' is actual value. */
                }
                else if (*pc == ',')
                {
                    *pc = '\0';
                    
                    if (oneval)
                    {
                        valcpy = strdup(oneval);
                        hcon_insert(*pvarsargs, onename, &valcpy);
                    }
                    else
                    {
                        valcpy = strdup("");
                        hcon_insert(*pvarsargs, onename, &valcpy);
                    }
                    pc++;
                    onename = pc;
                    oneval = NULL;
                }
                
                pc++;
            }
            
            if (!err)
            {
                /* do last one. */
                if (oneval)
                {
                    valcpy = strdup(oneval);
                    hcon_insert(*pvarsargs, onename, &valcpy);
                }
                else
                {
                    valcpy = strdup("");
                    hcon_insert(*pvarsargs, onename, &valcpy);
                }
            }
            
            free(cpy);
        }
    }
    
    return err;
}

/* Returns 1 on success, 0 on failure. */
/*   pinfo - information from the processing-step series specific to the current procsessing step. 
*    args - argument values from the processing keyword of jsoc.export_new. 
*    argsout - final argument string (comma-separated list of arguments/values).
*    stepdata - has program name, input record-set, output record-set.
*/
static int GenProgArgs(ProcStepInfo_t *pinfo, 
                       HContainer_t *args, 
                       ProcStep_t *stepdata,
                       const char *reclim,
                       char **argsout)
{
    /*
     *   1. Grab an argument from pinfo->req. Map the argument name, using 
     *      pinfo->namemap, to an "internal" name. 
     *   2. If the internal name is in args, then use the corresponding value in args 
     *      as the value of this argument. 
     *   3. If the internal name is NOT in args, then look for it in gIntVars. If it 
     *      is there, use the associated value as the value of this argument. 
     *   4. If the internal name is NOT in gIntVars, then look for it in gShVars. If it 
     *      is there, use the associated value as the value of this argument.
     *   5. If the internal name is NOT in gShVars, bail. A required argument was 
     *      not specified in any way. 
     *   6. Repeat for all remaining required arguments. 
     *   7. Grab an option from pinfo->opt. Proceed as before with required arguments, 
     *      through step 5. 
     *   8. If no value for the option can be found in args, gIntVars, or gShVars, then use 
     *      the default value in pinfo->opt. 
     *   9. If there is no default value in pinfo->opt, then bail. 
     */
    
    int err;
    HIterator_t *hit = NULL;
    ListNode_t *node = NULL;
    ExpProcArg_t *data = NULL;
    const char *intname = NULL;
    const char **pintname = NULL;
    size_t sz = 128;
    const char **pval = NULL;
    const char *val = NULL;
    char *finalargs = NULL;
    int first;
    int isflag;
    
    err = 0;
    finalargs = malloc(sz);
    memset(finalargs, 0, sz);
    
    if (pinfo)
    {
        if (pinfo->req)
        {
            list_llreset(pinfo->req);
            
            /* loop through required arguments. */
            first = 1;
            while ((node = list_llnext(pinfo->req)) != NULL)
            {
                data = (ExpProcArg_t *)node->data;
                if (pinfo->namemap)
                {
                    pintname = (const char **)hcon_lookup(pinfo->namemap, data->name);
                }
                else
                {
                    pintname = NULL;
                }
                
                if (pintname == NULL)
                {
                    /* There is NO mapping from program argument to internal variable name - use 
                     * original name. */
                    intname = data->name;
                }
                else
                {
                    intname = *pintname; 
                }
                
                /* Look for intname first in args, then in gIntVars, then in gShVars. */
                if ((args && (pval = (const char **)hcon_lookup(args, intname)) != NULL) ||
                    (gIntVars && (pval = (const char **)hcon_lookup(gIntVars, intname)) != NULL) ||
                    (gShVars && (pval = (const char **)hcon_lookup(gShVars, intname)) != NULL))
                {
                    val = *pval;
                }
                else if (strcasecmp(intname, "in") == 0)
                {
                    /* Special case for the input and output record-set specification arguments. 
                     * The values for these come from stepdata. There MUST be a namemap entry 
                     * for <input arg>=in and <output arg>=out in the processing series map
                     * field. */
                    val = stepdata->input;
                }
                else if (strcasecmp(intname, "out") == 0)
                {
                    /* Special case for the input and output record-set specification arguments. 
                     * The values for these come from stepdata. There MUST be a namemap entry 
                     * for <input arg>=in and <output arg>=out in the processing series map
                     * field. */
                    val = stepdata->output;
                }
                else
                {
                    /* Couldn't find required arg - bail. */
                    val = NULL;
                    fprintf(stderr, "Unable to locate value for program argument %s.\n", data->name);
                    err = 1;
                    break;
                }
                
                if (val)
                {
                    if (!first)
                    {
                        finalargs = base_strcatalloc(finalargs, " ", &sz);
                    }
                    else
                    {
                        first = 0;
                    }
                    
                    finalargs = base_strcatalloc(finalargs, data->name, &sz);
                    finalargs = base_strcatalloc(finalargs, "=", &sz);
                    finalargs = base_strcatalloc(finalargs, "'", &sz);
                    finalargs = base_strcatalloc(finalargs, val, &sz);
                    finalargs = base_strcatalloc(finalargs, "'", &sz);
                }
            }
        }
        
        if (!err)
        {
            /* loop through optional arguments. */
            if (pinfo->opt)
            {
                list_llreset(pinfo->opt);
                
                first = 1;
                while ((node = list_llnext(pinfo->opt)) != NULL)
                {
                    data = (ExpProcArg_t *)node->data;
                    if (pinfo->namemap)
                    {
                        pintname = (const char **)hcon_lookup(pinfo->namemap, data->name);
                    }
                    else
                    {
                        pintname = NULL;
                    }
                    
                    if (pintname == NULL)
                    {
                        /* There is NO mapping from program argument to internal variable name - use 
                         * original name. */
                        intname = data->name;
                    }
                    else
                    {
                        intname = *pintname;
                    }
                    
                    /* Look for intname first in args, then in gIntVars, then in gShVars. */
                    if ((pval = (const char **)hcon_lookup(args, intname)) != NULL ||
                        (pval = (const char **)hcon_lookup(gIntVars, intname)) != NULL ||
                        (pval = (const char **)hcon_lookup(gShVars, intname)) != NULL
                        )
                    {
                        val = *pval;
                    }
                    else if (strcasecmp(intname, "reclim") == 0)
                    {
                        /* Special case for the record-limit argument. There MUST be a 
                         * namemap entry for <record-limit arg>=reclim in the processing series map
                         * field IF we want to apply a record-limit. For example, there must 
                         * be an entry with n=reclim for hg_patch processing. 
                         */
                        val = reclim;
                    }
                    else
                    {
                        /* Couldn't find optional arg. See if it is in pinfo->opt, default values
                         * from the processing-step dataseries. */
                        val = NULL;
                        if (data->def && *data->def)
                        {
                            /* default value exists, use it. */
                            val = data->def;
                        }
                        else
                        {
                            /* No default value for this optional argument, so do not add the 
                             * argument to the cmd-line. */
                            val = NULL;
                        }
                    }
                    
                    if (val)
                    {
                        if (!first)
                        {
                            finalargs = base_strcatalloc(finalargs, " ", &sz);
                        }
                        else
                        {
                            if (*finalargs)
                            {
                                /* Need space between req args and options. */
                                finalargs = base_strcatalloc(finalargs, " ", &sz);
                            }
                            first = 0;
                        }
                        
                        finalargs = base_strcatalloc(finalargs, data->name, &sz);
                        
                        /* Argument names that begin with '-' are flags and have no value. */
                        if (!(*data->name == '-'))
                        {
                            finalargs = base_strcatalloc(finalargs, "=", &sz);
                            finalargs = base_strcatalloc(finalargs, val, &sz);
                        }
                    }
                }
            }
        }
    }
    
    if (!err && finalargs && *finalargs)
    {
        *argsout = finalargs;
    }
    else
    {
        *argsout = NULL;
    }
    
    return err;
}

static int GenOutRSSpec(DRMS_Env_t *env, 
                        const char *dbhost, 
                        ProcStepInfo_t *cpinfo, 
                        ProcStepInfo_t *ppinfo,
                        ProcStep_t *data,
                        const char *reqid)
{
    int err = 0;
    
    if (cpinfo)
    {
        const char *suffix = cpinfo->suffix;
        const char *psuffix = NULL;
        char **snames = NULL;
        char **filts = NULL;
        int nsets;
        int iset;
        DRMS_RecQueryInfo_t info;
        char *outseries = NULL;
        char *newoutseries = NULL;
        char *newfilter = NULL;
        int npkeys;
        size_t sz;
        int len;
        char *repl = NULL;
        
        while (1)
        {
            if (ppinfo)
            {
                /* The input to the current processing step, which is identical to ppinfo->output,
                 * might have a suffix. If it has a suffix, then it is ppinfo->suffix. */
                psuffix = ppinfo->suffix;
            }
            else
            {
                /* Since there was no previous processing step, there is no suffix 
                 * attached to the input of this processing step. */
            }
            
            /* Parse input record-set query parts. */
            
            /* ART - env is not necessarily the correct environment for talking 
             * to the database about DRMS objects (like records, keywords, etc.). 
             * It is connected to the db on dbexporthost. dbmainhost is the host
             * of the correct jsoc database. This function will ensure that
             * it talks to dbmainhost. */
            if (ParseRecSetSpec(env, dbhost, data->input, &snames, &filts, &nsets, &info))
            {
                err = 1;
                break;
            }
            
            /* The suffix column's value is a true suffix only if the string begins with '_'. If
             * not, then the value is actually the output series name. So if *psuffix != '_', 
             * then there was really no suffix for the previous processing step, and if *suffix != '_',
             * then we need to replace the input series name with the output series name. */
            if (psuffix && *psuffix == '_' && suffix && *suffix == '_')
            {
                /* The input series and output series both have suffixes. Replace the 
                 * input series' suffixes with the output series' suffixes. */
                for (iset = 0; iset < nsets; iset++)
                {
                    outseries = base_strreplace(data->input, psuffix, suffix);
                }   
            }
            else if (suffix && *suffix == '_')
            {
                /* No suffix on input series names, but suffix on output series names. Append 
                 * the output series' suffix onto the input series' names. */
                char replname[DRMS_MAXSERIESNAMELEN];
                
                outseries = strdup(data->input);
                for (iset = 0; iset < nsets; iset++)
                {
                    snprintf(replname, sizeof(replname), "%s%s", snames[iset], suffix);
                    newoutseries = base_strreplace(outseries, snames[iset], replname);
                    free(outseries);
                    outseries = newoutseries;
                }   
            }
            else if (suffix && *suffix)
            {
                /* No suffix on either the input series or the output series, but there 
                 * is a new output series name. Replace input series names with the name
                 * in suffix. */
                for (iset = 0; iset < nsets; iset++)
                {
                    outseries = base_strreplace(data->input, snames[iset], suffix);
                }
                
                /* ART - This is the case for aia_scale processing (so far). This code will 
                 * take whatever the input series is, and replace the series name with 
                 * aia_test.lev1p5. Because we no longer have processing-step specific code, 
                 * we cannot ensure that the input series is aia.lev1 only. If it isn't, then
                 * this processing step is invalid. */
            }
            else
            {
                /* No suffix on input series names, and no suffix on output series names. */
                outseries = strdup(data->input);
            }
            
            /* Remove input series' filters. */
            for (iset = 0; iset < nsets; iset++)
            {
                newoutseries = base_strreplace(outseries, filts[iset], "");
                free(outseries);
                outseries = newoutseries;
            }
            
            FreeRecSpecParts(&snames, &filts, nsets);
            
            /* Add filters to output series names. */
            if (ParseRecSetSpec(env, dbhost, outseries, &snames, &filts, &nsets, &info))
            {
                err = 1;
                break;
            }
            
            sz = 32;
            newfilter = malloc(sz);
            memset(newfilter, 0, sz);
            for (iset = 0; iset < nsets; iset++)
            {  
                /* snames now contains output series names. */
                
                /* Get number of prime-key keywords for current series. */
                
                /* Must talk to db that has actual series (i.e., hmidb), despite the fact
                 * that the env might contain a connection to the wrong db. NumPKeyKeys()
                 * will talk to the correct db. */
                npkeys = NumPKeyKeys(env, dbhost, snames[iset]);
                
                if (npkeys < 1)
                {
                    /* There must be at least one prime key constituent - the reqid keyword. */
                    err = 1;
                    break;
                }
                
                /* create npkeys - 1 "[]"*/
                while (--npkeys)
                {
                    newfilter = base_strcatalloc(newfilter, "[]", &sz);
                }
                
                newfilter = base_strcatalloc(newfilter, "[", &sz);
                newfilter = base_strcatalloc(newfilter, reqid, &sz);
                newfilter = base_strcatalloc(newfilter, "]", &sz);
                
                len = strlen(snames[iset]) + strlen(newfilter) + 16;
                repl = malloc(len);
                
                if (repl)
                {
                    snprintf(repl, len, "%s%s", snames[iset], newfilter);
                    newoutseries = base_strreplace(outseries, snames[iset], repl);
                    free(repl);
                    repl = NULL;
                }
                else
                {
                    fprintf(stderr, "Out of memory.\n");
                    err = 1;
                    break;
                }
                
                free(outseries);
                outseries = newoutseries;
                
                *newfilter = '\0';
            }
            
            if (err == 1)
            {
                break;
            }
            
            free(newfilter);
            newfilter = NULL;
            
            FreeRecSpecParts(&snames, &filts, nsets);
            /* Done! outseries has record-set specifications that have the proper 
             * suffix and that have the proper filters. */
            data->output = outseries;
            
            break; /* one-time through */
        }  
        /* The output series name is the input series name, stripped of the suffix, 
         * with the output suffix appended. The output filter is a string consisting 
         * of [] for every prime-key constituent, except for the requestid keyword, 
         * in which case the subfilter is [reqid]. */
    }
    
    return err;
}

/* returns 1 on good parse, 0 otherwise */
/* Ignore the kProc_Reclimit set when obtaining the corresponding input/output datasets from
 * the dataset field. */
/* The DataSet column will actually have only a single field - the input record-set. In this
 * function, we want to create a list of record-sets, starting with the input record-set. As we
 * parse processing step commands, we will insert into the HEAD of this list the output record-set
 * that will result when the processing step has completed. In the calling function, we 
 * will write this list of record-sets into the DataSet column of jsoc.export. */
/* val - The process field of jsoc.export_new.
 * dset - The dataset field of jsoc.export_new
 * status - boy, I wonder what this field is for.
 */
static LinkedList_t *ParseFields(DRMS_Env_t *env, /* dbhost of jsoc.export_new. */
                                 const char *procser,
                                 const char *dbhost, /* dbhost of db containing series. */
                                 const char *val, 
                                 const char *dset, 
                                 const char *reqid, 
                                 char **reclim, /* returned by reference */
                                 int *status)
{
    LinkedList_t *rv = NULL;
    char *activestr = NULL;
    char *pc = NULL;
    PParseState_t state = kPPStBeginProc;
    char *onecmd = NULL;
    char *args = NULL;
    ProcStep_t data; /* current step's data. */
    int procnum; /* The number of the proc-step being currently processed. */
    char *adataset = NULL;
    char *end = NULL;
    HContainer_t *pinfo = NULL;
    ProcStepInfo_t *cpinfo = NULL;
    ProcStepInfo_t *ppinfo = NULL; /* previous step's data */
    int intstat;
    int gettingargs;
    char *reclimint = NULL;
    int bar;
    
    HContainer_t *varsargs = NULL;
    
    intstat = 0;
    procnum = 0;
    activestr = malloc(strlen(val) + 2);
    pc = activestr;
    snprintf(pc, strlen(val) + 2, "%s|", val); /* to make parsing easier */
    end = pc + strlen(pc);
    
    /* point to data-set */
    adataset = strdup(dset);
    
    /* Set-up proc-step name associative array. The value of each element of this container is
     * a struct that has the contents of the processing step's record in this db table. There 
     * is one hash element per processing step, keyed by processing-step name. */
    pinfo = hcon_create(sizeof(ProcStepInfo_t), kMaxProcNameLen, (void (*)(const void *))FreeProcStepInfo, NULL, NULL, NULL, 0);
    
    /* Collect proc info from jsoc.export_procs */
    SuckInProcInfo(env, procser, pinfo);
    
    while (1)
    {
        if (state == kPPStError)
        {
            /* Must free stuff in data. */
            break;
        }
        
        if (state == kPPStBeginProc)
        {
            onecmd = pc;
            args = NULL;
            procnum++;
            state = kPPStOneProc;
            data.name = NULL;
            data.path = NULL;
            data.args = NULL; /* complete set of arguments with which to call processing step. */
            data.input = adataset; 
            data.output = NULL;
            cpinfo = NULL;
            gettingargs = 0;
            bar = 0;
        }
        else if (state == kPPStOneProc)
        {
            if ((*pc == '|' || *pc == ',') && !gettingargs)
            {
                /* We may be at the end of a current processing step, but maybe not. If the current 
                 * char is a comma, then this could be the separator between the program name
                 * and the arguments to the program. */
                if (*pc == '|')
                {
                    bar = 1;
                }
                
                *pc = '\0'; 
                
                /* We have a complete processing-step name in onecmd. It will be in pinfo, unless it is n=XX, which 
                 * was used before we standardized the processing column. */
                cpinfo = hcon_lookup(pinfo, onecmd);
                
                /* Check for old-style comma delimiter between the first proc step (which
                 * must be the reclimit step), and the real proc steps. */
                if (!cpinfo && strncasecmp(onecmd, "n=", 2) == 0 && procnum == 1)
                {
                    args = onecmd + 2;
                    
                    /* This step is not a real processing step, so it will not modify any 
                     * image data --> input == output. */
                    data.name = strdup("n=xx");
                    data.output = strdup(data.input);
                    
                    /* There could be two '|' between n=XX and the next cmd. */
                    if (bar && *(pc + 1) == '|')
                    {
                        pc++;
                    }
                    
                    state = kPPStEndProc;
                }
                else if (cpinfo)
                {
                    /* These are all the real processing steps. There may or may not be arguments 
                     * associated with these steps. Start collecting chars in the input val as 
                     * potential argument chars. */
                    data.name = strdup(onecmd);
                    args = pc + 1;
                    if (bar)
                    {
                        /* Done with this proc-step. There may or may not be proc-steps that follow. */
                        state = kPPStEndProc;
                    }
                    else
                    {
                        gettingargs = 1;
                    }
                }
                else
                {
                    /* Unknown type - processing error */
                    fprintf(stderr, "Unknown processing step %s.\n", onecmd);
                    state = kPPStError;
                }
            }
            else if (*pc == '|')
            {
                /* This is the end of the processing step when that step's program takes arguments. */
                *pc = '\0';
                state = kPPStEndProc;
            }
            else
            {
                /* Still looking for the end of the processing step. */
                pc++;
            }
        }
        else if (state == kPPStEndProc)
        {
            /* These are all the real processing steps. There is now a single function 
             * that generates the final arguments from the processing step's 
             * information from the processing series (cpinfo), 
             * the argument values in the processing keyword of the jsoc.export_new series
             * (args), the list of available jsoc_export_manage internal variables (gIntVars), 
             * and the shell variables (gShVars). 
             * 
             * This is the only state where a valid input string can result in pc reaching end.
             */
            char *finalargs = NULL;
            
            /* If this is the record-limit "processing step", then simply set reclim, 
             * and go to the next processing step. */
            if (strcasecmp(data.name, "n=xx") == 0)
            {
                state = kPPStBeginProc;
                reclimint = strdup(args);
                pc++; /* Advance to first char after the proc-step delimiter. */
            }
            else if (cpinfo->path && *cpinfo->path)
            {
                /* If there is no program path for this processing step, then it is not 
                 * possible for this processing step to modify the series' data, and there
                 * will not be the need for any program cmd-line in the drms run script. */
                if (!varsargs)
                {
                    if (InitVarConts(args, &varsargs))
                    {
                        state = kPPStError;
                        continue;
                    }
                }
                
                if (!varsargs)
                {
                    state = kPPStError;
                    continue;
                }
                
                /* Generate output name. The plan is to take the input name, strip its suffix (if one
                 * exists), then add the new suffix (if one exists). The output series will have 
                 * requestid as a keyword. So the output record-set will be <series>[][][reqid]. */
                if (GenOutRSSpec(env, dbhost, cpinfo, ppinfo, &data, reqid))
                {
                    state = kPPStError;
                    continue;
                }
                
                if (GenProgArgs(cpinfo, varsargs, &data, reclimint, &finalargs))
                {
                    state = kPPStError;
                    continue;
                }
                
                data.path = strdup(cpinfo->path);
                
                /* Finalize args. */
                if (finalargs)
                {
                    data.args = finalargs;
                }
                
                if (!rv)
                {
                    rv = list_llcreate(sizeof(ProcStep_t), (ListFreeFn_t)FreeProcStep);
                }
                
                list_llinserttail(rv, &data);
                
                ppinfo = cpinfo;
                
                /* The output record-set now becomes the input record-set of the next processing step. */
                if (data.output)
                {
                    adataset = strdup(data.output);
                    state = kPPStBeginProc;
                    pc++; /* Advance to first char after the proc-step delimiter. */
                }
                else
                {
                    state = kPPStError;
                }
            }
            else
            {
                /* Go to the next processing step, if one exists. */
                state = kPPStBeginProc;
                pc++; /* Advance to first char after the proc-step delimiter. */
            }
        }
        
        if (pc == end)
        {
            if (state == kPPStBeginProc)
            {
                state = kPPStEnd;
            }
            
            if (state != kPPStEnd)
            {
                state = kPPStError;
            }
            
            break;
        }
        
    } /* while loop */
    
    hcon_destroy(&pinfo);
    
    if (activestr)
    {
        free(activestr);
    }
    
    if (adataset)
    {
        /* There will always be an unused adataset when parsing completes, or when an error occurs. */
        free(adataset);
    }
    
    if (state == kPPStEnd)
    {
        intstat = 1;
    }
    
    if (reclim)
    {
        *reclim = reclimint;
    }
    
    if (status)
    {
        *status = intstat;
    }
    
    return rv;
}

enum PSeqState_enum
{
   kPSeqBegin,
   kPSeqReclim,
   kPSeqNoMore,
   kPSeqError,
   kPSeqMoreOK
};

typedef enum PSeqState_enum PSeqState_t;

static int IsBadProcSequence(LinkedList_t *procs)
{
#if 0
   /* There is an acceptable order to proc steps. */
   ListNode_t *node = NULL;
   Processing_t type = kProc_Unk;
   PSeqState_t state = kPSeqBegin;
   int quit;
   int rv;

   quit = 0;
   list_llreset(procs);
   while (!quit && (node = list_llnext(procs)) != 0)
   {
      type = ((ProcStep_t *)node->data)->type;

      switch (state)
      {
         case kPSeqError:
           {
              quit = 1;
           }
           break;
         case kPSeqBegin:
           {
              if (type == kProc_Noop || type == kProc_SuExport)
              {
                 state = kPSeqNoMore;
              }
              else if (type == kProc_Reclimit)
              {
                 state = kPSeqReclim;
              }
              else if (type == kProc_NotSpec || type == kProc_HgPatch || type == kProc_AiaScale)
              {
                 state = kPSeqMoreOK;
              }
              else
              {
                 fprintf(stderr, "Unknown processing type %d.\n", (int)type);
                 state = kPSeqError;
              }
           }
           break;
         case kPSeqReclim:
           {
              if (type == kProc_Noop || type == kProc_SuExport)
              {
                 state = kPSeqNoMore;
              }
              else if (type == kProc_Reclimit)
              {
                 fprintf(stderr, "Multiple record-limit statements.\n");
                 state = kPSeqError;
              }
              else if (type == kProc_NotSpec || type == kProc_HgPatch || type == kProc_AiaScale)
              {
                 state = kPSeqMoreOK;
              }
              else
              {
                 fprintf(stderr, "Unknown processing type %d.\n", (int)type);
                 state = kPSeqError;
              }
           }
           break;
         case kPSeqNoMore:
           {
              fprintf(stderr, "No additional processing allowed.\n");
              state = kPSeqError;
           }
           break;
         case kPSeqMoreOK:
           {
              if (type == kProc_Reclimit || type == kProc_Noop)
              {
                 fprintf(stderr, "Multiple record-limit statements, or a processing step combined with a noop processing step.\n");
                 state = kPSeqError;
              }
              else if (type == kProc_NotSpec || type == kProc_HgPatch || type == kProc_AiaScale)
              {
                 state = kPSeqMoreOK;
              }
              else if (kProc_SuExport)
              {
                 state = kPSeqNoMore;
              }
              else
              {
                 fprintf(stderr, "Unknown processing type %d.\n", (int)type);
                 state = kPSeqError;
              }
           }
           break;
      }
   }
   
   if (state == kPSeqError)
   {
      rv = 1;
   }
   else
   {
      rv = 0;
   }

   return rv;
#endif
    
    return 0;
}

static void GenErrChkCmd(FILE *fptr)
{
   fprintf(fptr, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
}

/* returns 1 on error, 0 on success */
static int GenExpFitsCmd(FILE *fptr, 
                         const char *proto,
                         const char *dbmainhost,
                         const char *requestid,
                         const char *dataset,
                         const char *RecordLimit,
                         const char *filenamefmt,
                         const char *method,
                         const char *dbids)
{
   char *protocol = strdup(proto);
   int rv = 0;

   if (protocol)
   {
      char *cparms, *p = index(protocol, ',');

      /* The "Protocol" field of jsoc.export has been overloaded. It contains not only the export protocol 
       * (i.e., FITS, as-is, JPEG, MOVIE), but for the FITS protocol, the comma-separated compression 
       * parameters follow. */

      if (p)
      {
         *p = '\0';
         cparms = p+1;
      }
      else
        cparms = "**NONE**";

      /* ART - multiple processing steps
       * Always use record limit, since we can no longer make the export commands processing-specific. */
      fprintf(fptr, "jsoc_export_as_fits JSOC_DBHOST=%s reqid='%s' expversion=%s rsquery='%s' n=%s path=$REQDIR ffmt='%s' "
              "method='%s' protocol='%s' cparms='%s' %s\n",
              dbmainhost, requestid, PACKLIST_VER, dataset, RecordLimit, filenamefmt, method, protos[kProto_FITS], cparms, dbids);

      GenErrChkCmd(fptr);
   }
   else
   {
      fprintf(stderr, "XX jsoc_export_manage FAIL - out of memory.\n");
      rv = 1;
   }

   return rv;
}

/* returns 1 on error, 0 on success */
static int GenPreProcessCmd(FILE *fptr, 
                            const char *progpath,
                            const char *args,
                            const char *dbhost,
                            const char *dbids) /* JSOC_DBNAME and JSOC_DBUSER */
{
    int rv = 0;

    if (progpath)
    {
        /* There are some legacy processing steps - "Not Specified", "no_op", */
        
        /* All processing-step shell cmds can now be created with the same code. */
        if (dbhost)
        {
            fprintf(fptr, "%s %s JSOC_DBHOST=%s %s\n", progpath, args, dbhost, dbids);
        }
        else
        {
            fprintf(fptr, "%s %s %s\n", progpath, args, dbids);
        }
    }
    
    GenErrChkCmd(fptr);
    
    return rv;
}

static int GenProtoExpCmd(FILE *fptr, 
                          const char *protocol,
                          const char *dbmainhost,
                          const char *dataset,
                          const char *RecordLimit,
                          const char *requestid,
                          const char *method,
                          const char *filenamefmt,
                          const char *dbids)
{
   int rv = 0;

   if (strncasecmp(protocol, protos[kProto_FITS], strlen(protos[kProto_FITS])) == 0)
   {
      rv = (GenExpFitsCmd(fptr, protocol, dbmainhost, requestid, dataset, RecordLimit, filenamefmt, method, dbids) != 0);
   }
   else if (strncasecmp(protocol, protos[kProto_MPEG], strlen(protos[kProto_MPEG])) == 0 ||
            strncasecmp(protocol, protos[kProto_JPEG], strlen(protos[kProto_JPEG])) == 0 ||
            strncasecmp(protocol, protos[kProto_PNG], strlen(protos[kProto_PNG])) == 0 ||
            strncasecmp(protocol, protos[kProto_MP4], strlen(protos[kProto_MP4])) == 0)
   {
      char *newproto = strdup(protocol);
      char *origproto = newproto;
      char *pcomma=index(newproto,',');

      if (pcomma)
        *pcomma = '\0';

      if (strcasecmp(newproto,"mpg")==0 || strcasecmp(protocol, "mp4")==0)
        fprintf(fptr, "%s ", (TESTMODE ? "/home/phil/jsoc/base/export/scripts/jsoc_export_as_movie_test" : "jsoc_export_as_movie"));
      else
        fprintf(fptr, "%s ", (TESTMODE ? "/home/phil/jsoc/base/export/scripts/jsoc_export_as_images_test" : "jsoc_export_as_images"));

      fprintf(fptr, "in='%s' reqid='%s' expversion='%s' method='%s' outpath=$REQDIR ffmt='%s' cparms='%s'",
              dataset, requestid, PACKLIST_VER, method, filenamefmt, "cparms is not needed");

      fprintf(fptr, " protocol='%s'", newproto);
      while(pcomma)
      {
         newproto = pcomma+1;
         pcomma=index(newproto,',');
         if (pcomma)
           *pcomma = '\0';
         fprintf(fptr, " '%s'", newproto);
      }
      fprintf(fptr, "\n");

      if (origproto)
      {
         free(origproto);
      }
   }
   else if (strncasecmp(protocol, protos[kProto_AsIs], strlen(protos[kProto_AsIs])) == 0)
   {
      /* ART - multiple processing steps
       * Always use record limit, since we can no longer make the export commands processing-specific. */
      fprintf(fptr, "jsoc_export_as_is JSOC_DBHOST=%s ds='%s' n=%s requestid='%s' method='%s' protocol='%s' filenamefmt='%s'\n",
              dbmainhost, dataset, RecordLimit, requestid, method, protos[kProto_AsIs], filenamefmt);

      GenErrChkCmd(fptr);

      /* print keyword values for as-is processing */
      fprintf(fptr, "show_info JSOC_DBHOST=%s -ait ds='%s' n=%s > %s.keywords.txt\n",
              dbmainhost, dataset, RecordLimit, requestid);
      GenErrChkCmd(fptr);
   }
   else
   {
      /* Unknown protocol */
      fprintf(stderr,
              "XX jsoc_export_manage FAILURE; invalid protocol, requestid=%s, protocol=%s, method=%s\n",
              requestid, protocol, method);
      rv = 1;
   }

   return rv;
}

static void FreeDataSetKw(void *val)
{
   char **str = (char **)val;

   if (str && *str)
   {
      free(*str);
      *str = NULL;
   }
}

/* Check for the existence of series series in the db on host dbhost. 
 * Returns 1 on error, 0 on success. */
static int SeriesExists(DRMS_Env_t *env, const char *series, const char *dbhost, int *status)
{
   int rv = 0;
   int istat = 0;
   DB_Handle_t *dbh = NULL;
   int contextOK = 0;

   /* returns NULL on error. */
   dbh = GetDBHandle(env, dbhost, &contextOK, 0);

   if (!dbh)
   {
      fprintf(stderr, "jsoc_export_manage: Unable to connect to database.\n");
      istat = 1;
   }
   else
   {
      if (!contextOK)
      {
         /* The caller wants to check for series existence in a db on host to which 
          * this module has no connection. Use dbh, not env. */
      
         /* Successfully connected to dbhost; can't use DRMS calls since we don't have a 
          * functioning environment for this ad hoc connection. */
         char query[512];
         char *schema = NULL;
         char *table = NULL;
         DB_Text_Result_t *res = NULL;
         
         if (get_namespace(series, &schema, &table))
         {
            fprintf(stderr, "Invalid series name %s.\n", series);
         }
         else
         {
            snprintf (query, sizeof(query), 
                      "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = lower(\'%s\') AND tablename = lower(\'%s\')", 
                      schema, 
                      table);

            res = db_query_txt(dbh, query);

            if (res) 
            {
               rv = (res->num_rows != 0);
               db_free_text_result(res);
               res = NULL;
            }
            else
            {
               fprintf(stderr, "Invalid SQL query: %s.\n", query);
            }

            free(schema);
            free(table);
         }
      }
      else
      {
         /* The caller wants to check for the existence of a series in the database to which 
          * this modue is currently connected. */
         rv = drms_series_exists(env, series, &istat);
         if (istat != DRMS_SUCCESS)
         {
            fprintf(stderr, "Problems checking for series '%s' existence on %s.\n", series, dbhost);
            rv = 0;
         }
      }
   }

   if (status)
   {
      *status = istat;
   }

   return rv;
}

/* Module main function. */
int DoIt(void)
  {
						/* Get command line arguments */
  const char *op;
      const char *procser = NULL;
  char *dataset;
  char *requestid;
  char *process;
  char *requestor;
  long requestorid;
  char *notify;
  char *format;
  char *shipto;
  char *method;
  char *protocol;
  char *filenamefmt;
  char *errorreply;
  char reqdir[DRMS_MAXPATHLEN];
  char command[DRMS_MAXPATHLEN];
  char *RecordLimit = NULL;
  long long size;
  TIME reqtime;
  TIME esttime;
  TIME exptime;
  TIME now;
  double waittime;
  DRMS_RecordSet_t *exports, *exports_new_orig, *exports_new;
  DRMS_RecordSet_t *requestor_rs;
  DRMS_Record_t *export_rec, *export_log;
  char requestorquery[DRMS_MAXQUERYLEN];
  int status = 0;
  FILE *fp;
  char runscript[DRMS_MAXPATHLEN];
  char *dashp;
  int testmode = 0;
  LinkedList_t *proccmds = NULL;

  const char *dbname = NULL;
  const char *dbexporthost = NULL;
  const char *dbmainhost = NULL;
  const char *dbuser = NULL;
  const char *jsocroot = NULL;
  char dbids[128];
  char jsocrootstr[128] = {0};
  int pc = 0;

  int procerr;
  ListNode_t *node = NULL;
  int quit;
  char *now_at = NULL;
      char *progpath = NULL;
  char *args = NULL;
  const char *cdataset = NULL;
  const char *datasetout = NULL;
  char *exprecspec = NULL;
  char **snames = NULL;
  char **filts = NULL;
  char seriesin[DRMS_MAXSERIESNAMELEN];
  char seriesout[DRMS_MAXSERIESNAMELEN];
  int nsets;
  char *series = NULL;
  ProcStep_t *ndata = NULL;
  DRMS_RecQueryInfo_t info;
  char csname[DRMS_MAXSERIESNAMELEN];
  int iset;

  if (nice_intro ()) return (0);

  testmode = (TESTMODE || cmdparams_isflagset(&cmdparams, kArgTestmode));

  if ((dbmainhost = cmdparams_get_str(&cmdparams, "JSOC_DBMAINHOST", NULL)) == NULL)
  {
     dbmainhost = SERVER;
  }

  if ((dbexporthost = cmdparams_get_str(&cmdparams, "JSOC_DBHOST", NULL)) == NULL)
  {
     dbexporthost = SERVER;
  }

// TEST
// dbmainhost = dbexporthost;

  if ((dbname = cmdparams_get_str(&cmdparams, "JSOC_DBNAME", NULL)) == NULL)
  {
     dbname = DBNAME;
  }

  if ((dbuser = cmdparams_get_str(&cmdparams, "JSOC_DBUSER", NULL)) == NULL)
  {
     dbuser = USER;
  }

  if ((jsocroot = cmdparams_get_str(&cmdparams, "JSOCROOT", NULL)) != NULL)
  {
     snprintf(jsocrootstr, sizeof(jsocrootstr), "JSOCROOT_EXPORT=%s", jsocroot);
  }

  if (dbname)
  {
     pc += snprintf(dbids + pc, sizeof(dbids) - pc, "JSOC_DBNAME=%s ", dbname);
  }

  if (dbuser)
  {
     pc += snprintf(dbids + pc, sizeof(dbids) - pc, "JSOC_DBUSER=%s ", dbuser);
  }

  op = cmdparams_get_str (&cmdparams, "op", NULL);
      procser = cmdparams_get_str(&cmdparams, kArgProcSeries, NULL);

  /*  op == process, this is export_manage cmd line, NOT for request being managed */
  if (strcmp(op,"process") == 0) 
    {
    int irec;
    if (testmode)
      exports_new_orig = drms_open_records(drms_env, EXPORT_SERIES_NEW"[][? Status=12 ?]", &status);
    else
      exports_new_orig = drms_open_records(drms_env, EXPORT_SERIES_NEW"[][? Status=2 ?]", &status);
    if (!exports_new_orig)
	DIE("Can not open RecordSet");
    if (exports_new_orig->n < 1)  // No new exports to process.
        {
        drms_close_records(exports_new_orig, DRMS_FREE_RECORD);
        return(0);
        }
    exports_new = drms_clone_records(exports_new_orig, DRMS_PERMANENT, DRMS_SHARE_SEGMENTS, &status);
    if (!exports_new)
	DIE("Can not clone RecordSet");
    drms_close_records(exports_new_orig, DRMS_FREE_RECORD);

    for (irec=0; irec < exports_new->n; irec++) 
      {
      now = timenow();
      export_log = exports_new->records[irec];
      // Get user provided new export request
      status     = drms_getkey_int(export_log, "Status", NULL);
      requestid    = drms_getkey_string(export_log, "RequestID", NULL);
      dataset    = drms_getkey_string(export_log, "DataSet", NULL);
      process    = drms_getkey_string(export_log, "Processing", NULL);
      protocol   = drms_getkey_string(export_log, "Protocol", NULL);
      filenamefmt= drms_getkey_string(export_log, "FilenameFmt", NULL);
      method     = drms_getkey_string(export_log, "Method", NULL);
      format     = drms_getkey_string(export_log, "Format", NULL);
      reqtime    = drms_getkey_time(export_log, "ReqTime", NULL);
      esttime    = drms_getkey_time(export_log, "EstTime", NULL); // Crude guess for now
      size       = drms_getkey_longlong(export_log, "Size", NULL);
      requestorid = drms_getkey_int(export_log, "Requestor", NULL);
      printf("New Request #%d/%d: %s, Status=%d, Processing=%s, DataSet=%s, Protocol=%s, Method=%s\n",
	irec, exports_new->n, requestid, status, process, dataset, protocol, method);
      fflush(stdout);
          
          RegisterIntVar("requestid", 's', requestid);

      // Get user notification email address
      sprintf(requestorquery, "%s[? RequestorID = %ld ?]", EXPORT_USER, requestorid);
      requestor_rs = drms_open_records(drms_env, requestorquery, &status);
      if (!requestor_rs)
        DIE("Cant find requestor info series");
      if (requestor_rs->n > 0)
        {
        DRMS_Record_t *rec = requestor_rs->records[0];
        notify = drms_getkey_string(rec, "Notify", NULL);
        if (*notify == '\0')
           notify = NULL;
        }
      else
        notify = NULL;
      drms_close_records(requestor_rs, DRMS_FREE_RECORD);

      // Create new record in export control series, this one must be DRMS_PERMANENT
      // It will contain the scripts to do the export and set the status to processing
      /* EXPORT_SERIES is jsoc.export. */
      export_rec = drms_create_record(drms_env, EXPORT_SERIES, DRMS_PERMANENT, &status);
      if (!export_rec)
        DIE("Cant create export control record");
  
      drms_setkey_string(export_rec, "RequestID", requestid);
      drms_setkey_string(export_rec, "DataSet", dataset);
      drms_setkey_string(export_rec, "Processing", process);
      drms_setkey_string(export_rec, "Protocol", protocol);
      drms_setkey_string(export_rec, "FilenameFmt", filenamefmt);
      drms_setkey_string(export_rec, "Method", method);
      drms_setkey_string(export_rec, "Format", format);
      drms_setkey_time(export_rec, "ReqTime", now);
      drms_setkey_time(export_rec, "EstTime", now+10); // Crude guess for now
      drms_setkey_longlong(export_rec, "Size", size);
      drms_setkey_int(export_rec, "Requestor", requestorid);
  
      // check  security risk dataset spec or processing request
      if (isbadDataSet() || isbadProcessing())
        { 
        fprintf(stderr," Illegal format detected - security risk!\n"
  		     "RequestID= %s\n"
                       " Processing = %s\n, DataSet=%s\n",
  		     requestid, process, dataset);
        drms_setkey_int(export_rec, "Status", 4);
        drms_close_record(export_rec, DRMS_INSERT_RECORD);
        continue;
        }
  
      drms_record_directory(export_rec, reqdir, 1);

      // Insert qsub command to execute processing script into SU
      make_qsub_call(requestid, reqdir, (notify ? requestorid : 0), dbname, dbuser, dbids, dbexporthost);
  
      // Insert export processing drms_run script into export record SU
      // The script components must clone the export record with COPY_SEGMENTS in the first usage
      // and with SHARE_SEGMENTS in subsequent modules in the script.  All but the last module
      // in the script may clone as DRMS_TRANSIENT.
      // Remember all modules in this script that deal with the export record must be _sock modules.
      // But modules that need the main database for processing or extracting data should run as
      // direct-connect modules.  They run with the export SU as current directory and must pass all
      // results into that directory.

      // First, prepare initial part of script, same for all processing.
      sprintf(runscript, "%s/%s.drmsrun", reqdir, requestid);
      fp = fopen(runscript, "w");
      fprintf(fp, "#! /bin/csh -f\n");
      fprintf(fp, "set echo\n");
      fprintf(fp, "set histchars\n");
      // force clone with copy segment. 
      fprintf(fp, "set_keys_sock -C JSOC_DBHOST=%s ds='jsoc.export[%s]' Status=1\n", dbexporthost, requestid);
      fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
      // Get new SU for the export record
      fprintf(fp, "set REQDIR = `show_info_sock JSOC_DBHOST=%s -q -p 'jsoc.export[%s]'`\n", dbexporthost, requestid);
      fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
      // cd to SU in export record
      fprintf(fp, "cd $REQDIR\n");
      fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
      // save some diagnostic info
      //fprintf(fp, "printenv > %s.env\n", requestid);
      fprintf(fp, "echo Node = $HOSTNAME\n");
      fprintf(fp, "echo JSOC_DBHOST = %s, Processing DBHOST = %s\n", dbexporthost, dbmainhost);
      fprintf(fp, "echo SUdir = $REQDIR\n");

      // Now generate specific processing related commands
          
          
          /* Register shell variables available for the drms_run script. The value must match the 
           * shell variable name. The key does not necessarily need to match the shell variable, but
           * it does need to match the RHS of the namemap entry. */
          RegisterShVar("$REQDIR", "$REQDIR");
          RegisterShVar("$HOSTNAME", "$HOSTNAME");
          RegisterShVar("$EXPSIZE", "$EXPSIZE");

      // extract optional record count limit from process field.
      /* 'process' contains the contents of the 'Processing' column in jsoc.export. Originally, it 
       * used to contain strings like "no_op" or "hg_patch". But now it optionally starts with a 
       * "n=XX" string that is used to limit the number of records returned (in a way completely 
       * analogous to the n=XX parameter to show_info). So the "Processing" field got overloaded.
       * The following block of code checks for the presence of a "n=XX" prefix and strips it off
       * if it is present (setting RecordLimit in the process). */

      int ppstat = 0;

      /* Parse the Dataset and Process fields to create the processing step struct. */

      /* ART - env is not necessarily the correct environment for talking 
       * to the database about DRMS objects (like records, keywords, etc.). 
       * It is connected to the db on dbexporthost. dbmainhost is the host
       * of the correct jsoc database. This function will ensure that
       * it talks to dbmainhost. */
      proccmds = ParseFields(drms_env, procser, dbmainhost, process, dataset, requestid, &RecordLimit, &ppstat);
      if (ppstat == 0)
      {
         fprintf(stderr, "Invalid process field value: %s.\n", process);
         drms_setkey_int(export_log, "Status", 4);
         drms_close_record(export_rec, DRMS_INSERT_RECORD);
         fclose(fp);
         fp = NULL;

         /* mem leak - need to free all strings obtained with drms_getkey_string(). */
         continue; /* next export record */
      }

      /* PRE-PROCESSING */

      /* For now, this does nothing. Since jsoc_export_manage has no knowledge of specific 
       * processing steps, this function cannot assess whether the sequence is appropriate. 
       * If we want to enforce a proper sequence, we'll have to put that information in
       * the processing-series table. */
      if (IsBadProcSequence(proccmds))
      {
         fprintf(stderr, "Bad sequence of processing steps, skipping recnum %lld.\n", export_rec->recnum);
         list_llfree(&proccmds);
         drms_setkey_int(export_log, "Status", 4);
         drms_close_record(export_rec, DRMS_FREE_RECORD);
         fclose(fp);
         fp = NULL;

         /* mem leak - need to free all strings obtained with drms_getkey_string(). */
         continue; /* next export record */
      }

      /* First do the pre-processing of one dataseries into another (if requested). For example, the 
       * user may have requested region extraction. The pre-processing will first make a series of 
       * extracted regions (if it doesn't already exist). The export code will then export from the series 
       * of extracted regions, not from the original full images. */
      procerr = 0;
      quit = 0;
          
          if (proccmds)
          {
              list_llreset(proccmds);
          }
              
      datasetout = NULL;

      LinkedList_t *datasetkwlist = NULL;
      char *rsstr = NULL;

      while (!quit && (node = list_llnext(proccmds)) != 0)
      {
         ndata = (ProcStep_t *)node->data;

         /* Put the pipeline of record-sets into a string that gets saved to the dataset column of 
          * jsoc.export. If a processing step doesn't change the record-set (like no_op), then 
          * skip that processing step's record-set. */
         if (!datasetkwlist)
         {
            datasetkwlist = list_llcreate(sizeof(char *), (ListFreeFn_t)FreeDataSetKw);
            rsstr = strdup(ndata->input);
            list_llinserthead(datasetkwlist, &rsstr);
         }

         /* If this processing step will change the record-set specification, then save the new 
          * specification in a list. The contents of the list will be placed in the DataSet
          * column of jsoc.export. */
         if (strcmp(ndata->input, ndata->output) != 0)
         {
            rsstr = strdup(ndata->output);
            list_llinserthead(datasetkwlist, &rsstr);
         }

         cdataset = ndata->input;
         datasetout = ndata->output;

         /* Ensure that only a single input series is being exported; ensure that the input series exists. */
         
         /* ART - env is not necessarily the correct environment for talking 
          * to the database about DRMS objects (like records, keywords, etc.). 
          * It is connected to the db on dbexporthost. dbmainhost is the host
          * of the correct jsoc database. This function will ensure that
          * it talks to dbmainhost. */
         if (ParseRecSetSpec(drms_env, dbmainhost, cdataset, &snames, &filts, &nsets, &info))
         {
            fprintf(stderr, "Invalid input series record-set query %s.\n", cdataset);
            quit = 1;
            break;
         }

         /* If the record-set query is an @file or contains multiple sub-record-set queries. We currently
          * support exports from a single series. */
         for (iset = 0, *csname = '\0'; iset < nsets; iset++)
         {
            series = snames[iset];
            if (*csname != '\0') 
            {
               if (strcmp(series, csname) != 0)
               {
                  fprintf(stderr, "jsoc_export_manage FAILURE: attempt to export a recordset containing multiple input series.\n");
                  quit = 1;
                  break;
               }
            }
            else
            {
               snprintf(csname, sizeof(csname), "%s", series);
            }
         } // end series-name loop

         FreeRecSpecParts(&snames, &filts, nsets);

         snprintf(seriesin, sizeof(seriesin), "%s", csname);

         if (!SeriesExists(drms_env, seriesin, dbmainhost, &status) || status)
         {
            fprintf(stderr, "Input series %s does not exist.\n", csname);
            quit = 1;
            break;
         }

         /* Ensure that only a single output series is being written to; ensure that the output series exists. */
         
         /* ART - env is not necessarily the correct environment for talking 
          * to the database about DRMS objects (like records, keywords, etc.). 
          * It is connected to the db on dbexporthost. dbmainhost is the host
          * of the correct jsoc database. This function will ensure that
          * it talks to dbmainhost. */
         if (ParseRecSetSpec(drms_env, dbmainhost, datasetout, &snames, &filts, &nsets, &info))
         {
            fprintf(stderr, "Invalid output series record-set query %s.\n", datasetout);
            quit = 1;
            break;
         }
         
         /* If the record-set query is an @file or contains multiple sub-record-set queries. We currently
          * support exports to only a single series. */
         for (iset = 0, *csname = '\0'; iset < nsets; iset++)
         {
            series = snames[iset];
            if (*csname != '\0') 
            {
               if (strcmp(series, csname) != 0)
               {
                  fprintf(stderr, "jsoc_export_manage FAILURE: attempt to export a recordset to multiple output series.\n");
                  quit = 1;
                  break;
               }
            }
            else
            {
               snprintf(csname, sizeof(csname), "%s", series);
            }
         } // end series-name loop

         FreeRecSpecParts(&snames, &filts, nsets);

         snprintf(seriesout, sizeof(seriesout), "%s", csname);

         if (!SeriesExists(drms_env, seriesout, dbmainhost, &status) || status)
         {
            fprintf(stderr, "Output series %s does not exist.\n", csname);
            quit = 1;
            break;
         }

          progpath = ((ProcStep_t *)node->data)->path;
         args = ((ProcStep_t *)node->data)->args;
          

         procerr = GenPreProcessCmd(fp,
                                    progpath,
                                    args,
                                    dbmainhost,
                                    dbids);

         if (procerr)
         {
            quit = 1;
            break;
         }
      } /* loop over processing steps */

      /* Before freeing proccmds, copy datasetout. datasetout is a pointer to a string in proccmds, and
       * it is needed below. This record-set specification is the final one the processing pipeline
       * has generated. */
      if (!quit)
      {
         if (datasetout)
         {
            exprecspec = strdup(datasetout);
         }
         else
         {
            exprecspec = strdup(dataset);
         }
      }

          if (proccmds)
          {
              list_llfree(&proccmds);
          }

      if (quit)
      {
         drms_setkey_int(export_log, "Status", 4);
         drms_close_record(export_rec, DRMS_FREE_RECORD);
         fclose(fp);
         fp = NULL;

         /* mem leak - need to free all strings obtained with drms_getkey_string(). */
         continue; /* next export record */
      }

      /* We have finished generating the DataSet column value. */
      if (datasetkwlist)
      {
         char **recsetstr = NULL;
         char *datasetkw = NULL;
         size_t datasetkwsz = 128;
         int ft = 1;

         list_llreset(datasetkwlist);
         datasetkw = malloc(datasetkwsz);
         *datasetkw = '\0';

         /* The order in the list is the reverse pipeline order. */
         while ((node = list_llnext(datasetkwlist)) != 0)
         {
            recsetstr = (char **)node->data;
            
            if (recsetstr && *recsetstr)
            {
               if (ft)
               {
                  datasetkw = base_strcatalloc(datasetkw, *recsetstr, &datasetkwsz);
                  ft = 0;
               }
               else
               {
                  datasetkw = base_strcatalloc(datasetkw, "|", &datasetkwsz);
                  datasetkw = base_strcatalloc(datasetkw, *recsetstr, &datasetkwsz);
               }
            }
            else
            {
               fprintf(stderr, "Problem obtaining a record-set specification string.\n");
            }
         }

         drms_setkey_string(export_rec, "DataSet", datasetkw);
         free(datasetkw);
         datasetkw = NULL;

         list_llfree(&datasetkwlist);
      }

      /* PROTOCOL-SPECIFIC EXPORT */
      /* Use the dataset output by the last processing step (if processing was performed). */
      procerr = GenProtoExpCmd(fp, 
                               protocol,
                               dbmainhost, 
                               exprecspec,
                               RecordLimit, 
                               requestid, 
                               method, 
                               filenamefmt, 
                               dbids);

      if (procerr)
      {
         drms_setkey_int(export_log, "Status", 4);
         drms_close_record(export_rec, DRMS_FREE_RECORD);
         fclose(fp);
         fp = NULL;

         /* mem leak - need to free all strings obtained with drms_getkey_string(). */
         if (dataset)
         {
            free(dataset); /* this was malloc'd in this loop */
         }

         if (exprecspec)
         {
            free(exprecspec);
         }

         continue;
      }
      
      if (exprecspec)
      {
         free(exprecspec);
      }

      CloseDBHandle();

      // Finally, add tail of script used for all processing types.
      // convert index.txt list into index.json and index.html packing list files. 
      fprintf(fp, "jsoc_export_make_index\n");
      fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");

      // Parse out the size property from the index.json JSON.
      // Hard-code index.json - this probably will never change
      fprintf(fp, "set EXPSIZE = `extractexpsize.pl $REQDIR/index.json`\n");
      GenErrChkCmd(fp);
      fprintf(fp, "set_info_sock JSOC_DBHOST=%s ds='jsoc.export[%s]' Size=$EXPSIZE\n", dbexporthost, requestid);

      // create tar file if '-tar' suffix on method
      dashp = rindex(method, '-');
      if (dashp && strcmp(dashp, "-tar") == 0)
        {
        fprintf(fp, "cp %s.* index.* ..\n", requestid);
        fprintf(fp, "tar  chf ../%s.tar --remove-files ./\n", requestid);
        fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
        fprintf(fp, "mv ../%s.* ../index.* .\n", requestid);
        fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
        }

      // DONE, Standard exit here only if no errors above
      // set status=done and mark this version of the export record permanent
      fprintf(fp, "set_keys_sock JSOC_DBHOST=%s ds='jsoc.export[%s]' Status=0\n", dbexporthost, requestid);
                  // copy the drms_run log file
      fprintf(fp, "cp /home/jsoc/exports/tmp/%s.runlog ./%s.runlog \n", requestid, requestid);

      // DONE, Standard exit here if errors, falls through to here if OK
      fprintf(fp, "EXITPLACE:\n");
      fprintf(fp, "if ($RUNSTAT) then\n");
      fprintf(fp, "  echo XXXXXXXXXXXXXXXXXXXXXXX ERROR EXIT XXXXXXXXXXXXXXXXXXXXXXXXXXX\nprintenv\n");
      fprintf(fp, "endif\n");
                  // make drms_run completion lock file (always do this)
      fprintf(fp, "show_info_sock JSOC_DBHOST=%s -q -r 'jsoc.export[%s]' > /home/jsoc/exports/tmp/%s.recnum \n", dbexporthost, requestid, requestid);
      fprintf(fp, "set LOCKSTAT = $status\n");
      fprintf(fp, "if ($LOCKSTAT) then\n");
                  // make drms_run completion lock file (always do this) - drms_server died, so don't use sock version here
      fprintf(fp, "  show_info JSOC_DBHOST=%s -q -r 'jsoc.export[%s]' > /home/jsoc/exports/tmp/%s.recnum \n", dbexporthost, requestid, requestid);
      fprintf(fp, "endif\n");
      fprintf(fp, "exit $RUNSTAT\n");
      fclose(fp);
      chmod(runscript, 0555);
  
      // close the current (first) version of the record in jsoc.export
// fprintf(stderr,"export_manage closing new record for %s\n",requestid);
      drms_setkey_int(export_rec, "Status", 1);
      drms_close_record(export_rec, DRMS_INSERT_RECORD);
// fprintf(stderr,"export_manage closed new record for %s, starting qsub\n",requestid);
  
      // SU now contains both qsub script and drms_run script, ready to execute and lock the record.
      //sprintf(command,"qsub -q x.q,o.q,j.q -v %s "
      sprintf(command,"qsub -q j.q -v %s "
  	" -o /home/jsoc/exports/tmp/%s.runlog "
  	" -e /home/jsoc/exports/tmp/%s.runlog "
  	"  %s/%s.qsub ",
        jsocrootstr, requestid, requestid, reqdir, requestid);
  /*
  	"  >>& /home/jsoc/exports/tmp/%s.runlog",
  */
// fprintf(stderr,"export_manage for %s, qsub=%s\n",requestid, command);
      if (system(command))
      {
        DIE("Submission of qsub command failed");
      }
      // Mark the record in jsoc.export_new as accepted for processing.
      drms_setkey_int(export_log, "Status", 1);
      printf("Request %s submitted\n",requestid);

      /* mem leak - need to free all strings obtained with drms_getkey_string(). */
      } // end looping on new export requests (looping over records in jsoc.export_new)

    drms_close_records(exports_new, DRMS_INSERT_RECORD);
    return(0);
    } // End process new requests.
  else if (strcmp(op, "SOMETHINGELSE") == 0)
    {
    }
  else 
    DIE("Operation not allowed");
  return(1);
  }

