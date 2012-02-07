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

#define kMaxProcNameLen 128

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

enum Processing_enum
{
   kProc_Unk = 0,
   kProc_Reclimit = 1,
   kProc_Noop = 2,
   kProc_NotSpec = 3,
   kProc_HgPatch = 4,
   kProc_SuExport = 5,
   kProc_AiaScale = 6
};

typedef enum Processing_enum Processing_t;

const char *procs[] =
{
   "uknown",
   "n==",
   "no_op",
   "Not Specified",
   "hg_patch",
   "su_export",
   "aia_scale"
};

struct ProcStep_struct
{
  Processing_t type;
  char *args;
  char *input; /* data-set input to processing */
  char *output; /* data-set output by processing */
};

typedef struct ProcStep_struct ProcStep_t;

struct ProcStepInfo_struct
{
  char *name;
  char *suffix;
  /* TBD */
};

typedef struct ProcStepInfo_struct ProcStepInfo_t;

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "process", "<Operation>"},
  {ARG_FLAG, kArgTestmode, NULL, "if set, then operates on new requests with status 12 (not 2)"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_END}
};

char *module_name = "jsoc_export_manage";

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
               const char *dbuser, const char *dbids, const char *dbexporthost, const char *dbmainhost)
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
int isbadDataSet(char *in)
  {
  return(0);
  }

// Security testing.  Make sure Processing does not contain attempt to run illegal program
int isbadProcessing(char *process)
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

#if 0
/* action == 0 => return current data-set, don't advance to next 
 * action == 1 => return current data-set, advance to next 
 * action == 2 => free allocated memory
 *
 * field number of current field.
 * 
 * If field == NULL, then this is a continuation of previous parsing, 
 * otherwise, this is the beginning of parsing a new field.
 */
static const char *GetDataSet(const char *field, int action, int *fieldn)
{
   static char *ds = NULL;
   static char *pc = NULL;
   static int fieldno = 0;
   char *delim = NULL;

   if ((!field && !ds))
   {
      return NULL;
   }

   if (field)
   {
      if (ds)
      {
         free(ds);
      }

      ds = NULL;
      pc = NULL;
      fieldno = 0;
   }

   if (!ds)
   {
      ds = strdup(field);
   }

   /* point to first data-set */
   if (!pc)
   {
      pc = ds;
      fieldno = 1;
   }

   if (pc == ds + strlen(ds))
   {
      /* end of the line */
      *fieldn = fieldno;
      return NULL;
   }
   else if (action == 0)
   {
      /* just return current data-set */
      *fieldn = fieldno;
      return pc;
   }
   else if (action == 1)
   {
      char *rv = NULL;

      rv = pc;

      /* advance to next data-set */
      if ((delim = strchr(pc, '|')) != NULL)
      {
         *delim = '\0';
         pc = delim + 1;
         fieldno++;
      }
      else
      {
         /* no more data-sets */
         pc = pc + strlen(pc);
      }

      /* return current dataset */
      *fieldn = fieldno;
      return rv;
   }
   else if (action == 2 && ds)
   {
      free(ds);
      ds = NULL;
      pc = NULL;
      fieldno = 0;
      *fieldn = fieldno;
      return NULL;
   }

   *fieldn = fieldno;
   return NULL;
}
#endif

static void FreeProcStep(void *ps)
{
   ProcStep_t *data = (ProcStep_t *)ps;

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

   if (info->suffix)
   {
      free(info->suffix);
   }
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
static LinkedList_t *ParseFields(DRMS_Env_t *env, 
                                 const char *dbhost, 
                                 const char *val, 
                                 const char *dset, 
                                 const char *reqid, 
                                 int *status)
{
   LinkedList_t *rv = NULL;
   char *activestr = NULL;
   char *pc = NULL;
   PParseState_t state = kPPStBeginProc;
   char *onecmd = NULL;
   char *args = NULL;
   ProcStep_t data;
   int procnum;
   char *adataset = NULL;
   char *end = NULL;
   HContainer_t *pinfo = NULL;
   char onamebuf[2048];
   char spec[2048];
   char **snames = NULL;
   char **filts = NULL;
   int nsets;
   int iset;
   DRMS_RecQueryInfo_t info;
   ProcStepInfo_t *cpinfo = NULL;
   const char *suffix = NULL;
   char *outputname = NULL;
   char *newoutputname = NULL;

   *status = 0;
   procnum = 0;
   activestr = malloc(strlen(val) + 2);
   pc = activestr;
   snprintf(pc, strlen(val) + 2, "%s|", val); /* to make parsing easier */
   end = pc + strlen(pc);

   /* point to data-set */
   adataset = strdup(dset);

   /* Set-up proc-step name associative array. Soon, these names will come from a db table, but
    * for now they are hard-coded in this module. The value of each element of this container will
    * be a struct that has the contents of the processing step's record in this db table. */
   pinfo = hcon_create(sizeof(ProcStepInfo_t), kMaxProcNameLen, (void (*)(const void *))FreeProcStepInfo, NULL, NULL, NULL, 0);

   /* Hacky-hacky. Manually add the processing steps here. In the future, the step info will come from
    * the db table. */

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

   /* Eventually we will have to have a real suffix for this - right now we special case: 
    * aia.lev1 --> aia_test.lev1p5. */
   pi.name = strdup("aia_scale");
   pi.suffix = strdup("");
   hcon_insert(pinfo, "aia_scale", &pi);

   while (1)
   {
      if (state == kPPStError || state == kPPStEnd || pc == end)
      {
         /* Must free stuff in data. */
         break;
      }
      else if (state == kPPStBeginProc)
      {
         onecmd = pc;
         args = NULL;
         procnum++;
         state = kPPStOneProc;
         data.type = kProc_Unk;
         data.args = NULL;
         data.input = adataset; 
         data.output = NULL;
      }
      else if (state == kPPStOneProc)
      {
         if ((*pc == '|' || *pc == ',') && data.type == kProc_Unk)
         {
            *pc = '\0';

            /* We have a complete processing-step name in onecmd. It will be in pinfo, unless it is n=XX, which 
             * was used before we standardized the processing column. */
            cpinfo = hcon_lookup(pinfo, onecmd);

            if (cpinfo)
            {
               suffix = cpinfo->suffix;

               if (suffix && *suffix)
               {
                  /* output is input with the suffix appended. */
                  /* ART - env is not necessarily the correct environment for talking 
                   * to the database about DRMS objects (like records, keywords, etc.). 
                   * It is connected to the db on dbexporthost. dbmainhost is the host
                   * of the correct jsoc database. This function will ensure that
                   * it talks to dbmainhost. */
                  if (ParseRecSetSpec(env, dbhost, data.input, &snames, &filts, &nsets, &info))
                  {
                     state = kPPStError;
                     continue;
                  }

                  outputname = strdup(data.input);

                  for (iset = 0; iset < nsets; iset++)
                  {
                     snprintf(spec, sizeof(spec), "%s%s", snames[iset], filts[iset]);
                     snprintf(onamebuf, sizeof(onamebuf), "%s_%s%s", snames[iset], suffix, filts[iset]);
                     newoutputname = base_strreplace(outputname, spec, onamebuf);
                     free(outputname);
                     outputname = newoutputname;
                  }

                  data.output = outputname;

                  FreeRecSpecParts(&snames, &filts, nsets);
               }
               else
               {
                  data.output = strdup(data.input);
               }
            }

            /* Check for old-style comma delimiter between the first proc step (which
             * must be the reclimit step), and the real proc steps. */
            if (!cpinfo && strncasecmp(onecmd, "n=", 2) == 0 && procnum == 1)
            {
               args = onecmd + 2;
               data.type = kProc_Reclimit;
               
               /* This step is not a real processing step, so it will not modify any 
                * image data --> input == output. */
               data.output = strdup(data.input);

               /* There could be two '|' between n=XX and the next cmd. */
               if (*pc == '|' && *(pc + 1) == '|')
               {
                  pc++;
                  *pc = '\0';
               }

               state = kPPStEndProc;
            }
            else if (strncasecmp(onecmd, procs[kProc_Noop], strlen(procs[kProc_Noop])) == 0)
            {
               /* noop takes no args */
               data.type = kProc_Noop;
               state = kPPStEndProc;
            }
            else if (strncasecmp(onecmd, procs[kProc_NotSpec], strlen(procs[kProc_NotSpec])) == 0)
            {
               /* notspec takes no args */
               data.type = kProc_NotSpec;
               state = kPPStEndProc;
            }
            else if (strncasecmp(onecmd, procs[kProc_HgPatch], strlen(procs[kProc_HgPatch])) == 0)
            {
               /* Stuff following comma are args to hg_patch */
               /* These args will also have to be fetched from cpinfo. */
               args = pc + 1;
               data.type = kProc_HgPatch;

               /* ACK - somehow we have to figure out how to specify in the db table that 
                * the output record-set filter isn't just adding the _hgpatch suffix to 
                * the input series name. You have to go from an input of hmi.lev1[2012.1.5] to 
                * an output of hmi.lev1_hgpatch[][][JSOC_20111104_037_IN]. data.output has
                * hmi.lev1_hgpatch[2012.1.5] at this point (it won't after figure out how
                * to specify in the db table that we use a different output series filter than
                * the input series filter). Blah! */

               /* ART - env is not necessarily the correct environment for talking 
                * to the database about DRMS objects (like records, keywords, etc.). 
                * It is connected to the db on dbexporthost. dbmainhost is the host
                * of the correct jsoc database. This function will ensure that
                * it talks to dbmainhost. */
               if (ParseRecSetSpec(env, dbhost, data.output, &snames, &filts, &nsets, &info))
               {
                  state = kPPStError;
                  continue;
               }

               outputname = strdup(data.output);

               for (iset = 0; iset < nsets; iset++)
               {
                  snprintf(spec, sizeof(spec), "%s%s", snames[iset], filts[iset]);
                  snprintf(onamebuf, sizeof(onamebuf), "%s[][][%s]", snames[iset], reqid);
                  newoutputname = base_strreplace(outputname, spec, onamebuf);
                  free(outputname);
                  outputname = newoutputname;
               }

               data.output = outputname;
            }
            else if (strncasecmp(onecmd, procs[kProc_SuExport], strlen(procs[kProc_SuExport])) == 0)
            {
               data.type = kProc_SuExport;
               state = kPPStEndProc;
            }
            else if (strncasecmp(onecmd, procs[kProc_AiaScale], strlen(procs[kProc_AiaScale])) == 0)
            {
               data.type = kProc_AiaScale;
               state = kPPStEndProc;

               /* HACK!! For now, the only series to which aiascale processing can be applied is 
                * aia.lev1, producing the output aia_test.lev1p5. */
               outputname = strdup(data.output);

                /* ART - env is not necessarily the correct environment for talking 
                * to the database about DRMS objects (like records, keywords, etc.). 
                * It is connected to the db on dbexporthost. dbmainhost is the host
                * of the correct jsoc database. This function will ensure that
                * it talks to dbmainhost. */
               if (ParseRecSetSpec(env, dbhost, data.input, &snames, &filts, &nsets, &info))
               {
                  state = kPPStError;
                  free(outputname);
                  continue;
               }

               for (iset = 0; iset < nsets; iset++)
               {
                  if (strcasecmp(snames[iset], "aia.lev1") != 0)
                  {
                     state = kPPStError;
                     break;
                  }
                  else
                  {
                     /* Force to aia.lev1p5 */
                     if (outputname)
                     {
                        snprintf(spec, sizeof(spec), "%s%s", snames[iset], filts[iset]);
                        snprintf(onamebuf, sizeof(onamebuf), "aia.lev1p5%s", filts[iset]);
                        newoutputname = base_strreplace(outputname, spec, onamebuf);
                        free(outputname);
                        outputname = newoutputname;
                     }

                     data.output = outputname;
                  }
               }
               
               FreeRecSpecParts(&snames, &filts, nsets);
            }
            else
            {
               /* Unknown type - processing error */
               fprintf(stderr, "Unknown processing step %s.\n", onecmd);
               state = kPPStError;
            }
         }
         else if (*pc == '|' && data.type == kProc_HgPatch)
         {
            *pc = '\0';
            state = kPPStEndProc;
         }
         else
         {
            pc++;
         }
      }
      else if (state == kPPStEndProc)
      {
         if (args)
         {
            data.args = strdup(args);
         }

         if (!rv)
         {
            rv = list_llcreate(sizeof(ProcStep_t), (ListFreeFn_t)FreeProcStep);
         }

         list_llinserttail(rv, &data);

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

      if (state != kPPStError)
      {
         if (pc == end && state == kPPStBeginProc)
         {
            state = kPPStEnd;
         }
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
      *status = 1;
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

int IsBadProcSequence(LinkedList_t *procs)
{
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
}

static void GenErrChkCmd(FILE *fptr)
{
   fprintf(fptr, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
}

/* returns 1 on error, 0 on success */
static int GenHgPatchCmd(FILE *fptr, 
                         const char *hgparms,
                         const char *dbmainhost,
                         const char *dataset,
                         int RecordLimit,
                         const char *requestid,
                         const char *dbids)
{
   int rv = 0;
   char *p = NULL;
   char *hgparams = strdup(hgparms);

   if (hgparams)
   {
      for (p=hgparams; *p; p++)
        if (*p == ',') *p = ' ';
      
      /* Phil says it is okay to remove the hard-coding of his home dir */
      //      fprintf(fptr, "/home/phil/cvs/JSOC/bin/linux_x86_64/hg_patch JSOC_DBHOST=%s %s in='%s' n=%d requestid='%s' log=hg_patch.log %s\n",
      fprintf(fptr, "hg_patch JSOC_DBHOST=%s %s in='%s' n=%d requestid='%s' log=hg_patch.log %s\n",
              dbmainhost, hgparams, dataset, RecordLimit, requestid,  dbids);
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
static int GenExpFitsCmd(FILE *fptr, 
                         const char *proto,
                         const char *dbmainhost,
                         const char *requestid,
                         const char *dataset,
                         int RecordLimit,
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
      fprintf(fptr, "jsoc_export_as_fits JSOC_DBHOST=%s reqid='%s' expversion=%s rsquery='%s' n=%d path=$REQDIR ffmt='%s' "
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
                            const char *protocol,
                            Processing_t process,
                            const char * args,
                            const char *dbmainhost,
                            const char *datasetin,
                            const char *seriesin,
                            const char *seriesout,
                            int RecordLimit,
                            const char *requestid,
                            const char *method,
                            const char *dbids)
{
   int rv = 0;

   if (process == kProc_HgPatch)
   {
      /* hg_patch compatible with all protocols (if not fits, then assume as-is). */

      /* hg_patch requires additional arguments */
      if (args)
      {
         rv = (GenHgPatchCmd(fptr, args, dbmainhost, datasetin, RecordLimit, requestid, dbids) != 0);
      }
      else
      {
         /* arguments missing */
         fprintf(stderr,
                 "XX jsoc_export_manage FAIL NOT implemented yet, requestid=%s, process=%s, protocol=%s, method=%s\n",
                 requestid, procs[process], protocol, method);
         rv = 1;
      }
   }
   else if (process == kProc_SuExport)
   {
      fprintf(fptr, "jsoc_export_SU_as_is_sock ds='%s' requestid=%s\n", datasetin, requestid); 
      GenErrChkCmd(fptr);
      /* rv = 0 */
   }
   else if (process == kProc_Noop)
   {
      /* rv = 0 */
   }
   else if (process == kProc_NotSpec)
   {
      /* rv = 0 */
   }
   else if (process == kProc_AiaScale)
   {
      /* This will currently work ONLY with aia.lev1 as input and aia.lev1p5 as output. Eventually,
       * this should work with other series. I'm not sure how this is going to work when the output
       * series does not exist. For now, fail.
       *
       * -Art */
      if (strcasecmp(seriesin, "aia.lev1") !=0 || strcasecmp(seriesout, "aia.lev1p5") != 0)
      {
         fprintf(stderr, "Currently, aia_scale processing can be applied to aia.lev1 only, generating records in aia.lev1p5.\n");
         rv = 1;
      }
      else
      {
         fprintf(fptr, "aia_lev1p5 dsinp='%s' dsout='%s'\n", datasetin, seriesout);
      }
   }
   else
   {
      /* Unknown processing */
      fprintf(stderr,
              "XX jsoc_export_manage FAILURE; invalid processing request, requestid=%s, process=%s, protocol=%s, method=%s\n",
              requestid, procs[process], protocol, method);
      rv = 1;
   }

   return rv;
}

static int GenProtoExpCmd(FILE *fptr, 
                          const char *protocol,
                          const char *dbmainhost,
                          const char *dataset,
                          int RecordLimit,
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
      fprintf(fptr, "jsoc_export_as_is JSOC_DBHOST=%s ds='%s' n=%d requestid='%s' method='%s' protocol='%s' filenamefmt='%s'\n",
              dbmainhost, dataset, RecordLimit, requestid, method, protos[kProto_AsIs], filenamefmt);

      GenErrChkCmd(fptr);

      /* print keyword values for as-is processing */
      fprintf(fptr, "show_info JSOC_DBHOST=%s -ait ds='%s' n=%d > %s.keywords.txt\n",
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
  char *op;
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
  int RecordLimit;
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
  DRMS_Type_Value_t val;

  const char *dbname = NULL;
  const char *dbexporthost = NULL;
  const char *dbmainhost = NULL;
  const char *dbuser = NULL;
  const char *jsocroot = NULL;
  char dbids[128];
  char jsocrootstr[128] = {0};
  int pc = 0;

  int procerr;
  Processing_t proctype;
  ListNode_t *node = NULL;
  int quit;
  char *now_at = NULL;
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

  op = (char *)cmdparams_get_str (&cmdparams, "op", NULL);

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
      if (isbadDataSet(dataset) || isbadProcessing(process))
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
      make_qsub_call(requestid, reqdir, (notify ? requestorid : 0), dbname, dbuser, dbids, dbexporthost, dbmainhost);
  
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

      // extract optional record count limit from process field.
      /* 'process' contains the contents of the 'Processing' column in jsoc.export. Originally, it 
       * used to contain strings like "no_op" or "hg_patch". But now it optionally starts with a 
       * "n=XX" string that is used to limit the number of records returned (in a way completely 
       * analogous to the n=XX parameter to show_info). So the "Processing" field got overloaded.
       * The following block of code checks for the presence of a "n=XX" prefix and strips it off
       * if it is present (setting RecordLimit in the process). */

      int ppstat = 0;
      int morestat = 0;

      /* Parse the Dataset and Process fields to create the processing step struct. 
       * HACK!! For now, if hgpatch is present, just put '@hgpatchlog.txt' in the ds output field
       * of the hgpatch processing step. */

      /* ART - env is not necessarily the correct environment for talking 
       * to the database about DRMS objects (like records, keywords, etc.). 
       * It is connected to the db on dbexporthost. dbmainhost is the host
       * of the correct jsoc database. This function will ensure that
       * it talks to dbmainhost. */
      proccmds = ParseFields(drms_env, dbmainhost, process, dataset, requestid, &ppstat);
      if (ppstat == 0)
      {
         fprintf(stderr, "Invalid process field value: %s.\n", process);
         drms_setkey_int(export_log, "Status", 4);
         drms_close_record(export_rec, DRMS_FREE_RECORD);
         fclose(fp);
         fp = NULL;

         /* mem leak - need to free all strings obtained with drms_getkey_string(). */
         continue; /* next export record */
      }

      /* PRE-PROCESSING */

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
      RecordLimit = 0;
      list_llreset(proccmds);
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

         proctype = ndata->type;
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

         /* The JSOC_DBHOST variable will properly identify the db that must contain the input and output series. */
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

         /* The JSOC_DBHOST variable will properly identify the db that must contain the input and output series. */
         if (!SeriesExists(drms_env, seriesout, dbmainhost, &status) || status)
         {
            fprintf(stderr, "Output series %s does not exist.\n", csname);
            quit = 1;
            break;
         }

         args = ((ProcStep_t *)node->data)->args;

         if (proctype == kProc_Reclimit)
         {
            val.string_val = args;
            RecordLimit = drms2int(DRMS_TYPE_STRING, &val, &morestat);

            if (morestat == DRMS_RANGE)
            {
               fprintf(stderr, "Invalid record-limit argument %d.\n", RecordLimit);
               quit = 1;
               break;
            }

            continue; /* Onto the real processing steps. */
         }

         procerr = GenPreProcessCmd(fp,
                                    protocol,
                                    proctype,
                                    args,
                                    dbmainhost, 
                                    cdataset, 
                                    seriesin,
                                    seriesout, 
                                    RecordLimit, 
                                    requestid, 
                                    method, 
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

      list_llfree(&proccmds);

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

