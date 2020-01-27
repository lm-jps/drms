#include <regex.h>
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
                  set_info ds="jsoc.export[<RequestID>] status=4
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

  The export programs, e.g. jsoc_export_as_fits, all connect to hmidb. The cmd-line argument
  JSOC_DBMAINHOST contains this database (and it is always hmidb). jsoc_export_manage
  connects to either hmidb or hmidb2. When it connects to hmidb, it collects export requests
  from jsoc.export_new on hmidb, and when it connects to hmidb2, it collects export requests
  from jsoc.export_new on hmidb2. The cmd-line argument JSOC_DBHOST contains this database.
 *

  jsoc.export status column lifecycle
  a. jsoc_fetch sets status to 2 (jsoc.export_new) and hands off to jsoc_export_manage (via DB)
  b. qsub script runs and does not touch the status
  c. qsub launches drms_run script
  d. drms_run set status to 1 (pending)
  e. if no error drms_run sets status to 0 (complete, success)
  f. if error, drms_run returns error code to qsub script (but does not set status)
  g. drms_run will look for drms_run error - if error, then it sets status to 4

  --> a request is complete at the end of the qsub script

  DEVELOPMENT:
  1. Ensure you have run jsoc_fetch with the -t flag so that the row in jsoc.export_new has status == 12 (to create a dev row);
  2. Run jsoc_export_manage AS LINUX USER production like this:
     $ jsoc_export_manage procser=jsoc.export_procs JSOC_DBUSER=production JSOC_DBHOST=hmidb JSOCROOT_EXPORT=/home/jsoc/cvs/Development/JSOC -t
     The process must connect as DB user production; various tables must be written by production
     -t ensure that only the status == 12 rows in jsoc.export_new (the dev ones) are processed
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
#define kArgTestConvQuotes "tQuotes"
#define kArgLogDir "logdir"
#define kArgValNotUsed "NoT UsEd"

#define kArgQsubInitScript "q-init-script"
#define kArgValQsubInitScript "/SGE2/default/common/settings.sh"

#define kMaxProcNameLen 128
#define kMaxIntVar 64
#define kMaxShVar  32
#define kMaxArgVar 64

#define DIE(msg) { fprintf(stderr,"XXXX jsoc_exports_manager failure: %s\nstatus=%d",msg,status); exit(1); }

#define kHgPatchLog "hg_patch.log"

enum Protocol_enum
{
    kProto_AsIs   = 0,
    kProto_FITS   = 1,
    kProto_MPEG   = 2,
    kProto_JPEG   = 3,
    kProto_PNG    = 4,
    kProto_MP4    = 5,
    kProto_SuAsIs = 6
};

typedef enum Protocol_enum Protocol_t;

const char *protos[] =
{
    "as-is",
    "fits",
    "mpg",
    "jpg",
    "png",
    "mp4",
    "su-as-is"
};

struct ExpProcArg_struct
{
    char *name;
    char *def;
};

typedef struct ExpProcArg_struct ExpProcArg_t;

struct ProcStep_struct
{
    char *name;                      /* name of program */
    char *path;                      /* path to program */
    char *args;                      /* program argument key=value pairs, comma-separated */
    char *input;                     /* data-set input to processing */
    char *output;                    /* data-set output by processing */
    struct ProcStep_struct *parent;  /* the previous step's ProcStep_struct */
    int crout;                       /* the processing step may need its intermediate output series created */
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
    {ARG_STRING, kArgTestConvQuotes, kArgValNotUsed, "Put a record-set query in here to test the code that converts double-quoted strings to single-quoted strings."},
    {ARG_STRING, kArgLogDir, "/home/jsoc/exports/tmp", "The temporary directory for the jsoc_export_manage processing log for the requests."},
    {ARG_STRING, kArgQsubInitScript, kArgValQsubInitScript, "Run this script to initialize the enviornment for qsub-script submission."},
    {ARG_FLAG, kArgTestmode, NULL, "if set, then operates on new requests with status 12 (not 2)"},
    {ARG_FLAG, "h", "0", "help - show usage"},
    {ARG_END}
};

char *module_name = "jsoc_export_manage";

/* The internal (to jsoc_export_manage) variables available for use as export program
 * argument values. */
HContainer_t *gIntVars = NULL;
HContainer_t *gShVars = NULL;

#define WriteLog(fh, ...) __WriteLog(__FILE__, __LINE__, fh, __VA_ARGS__)

static void __WriteLog(const char *file, int lineno, FILE *fh, ...)
{
    va_list args;
    char *finalFmt = NULL;
    char *fmt = NULL;
    size_t sz;
    char timestr[256];

    sz = 512;
    finalFmt = calloc(1, sz);
    if (finalFmt)
    {
        /* print a timestamp and file location prefix */
        time_t ltime;
        ltime = time(NULL); /* get current time */
        snprintf(timestr, sizeof(timestr), "%s", asctime(localtime(&ltime)));
        timestr[strcspn(timestr, "\n")] = '\0';
        snprintf(finalFmt, sz, "[ %s (%s:%d) ] ", timestr, basename(file), lineno);

        va_start(args, fh);
        fmt = va_arg(args, char *);
        finalFmt = base_strcatalloc(finalFmt, fmt, &sz);

        if (finalFmt)
        {
            finalFmt = base_strcatalloc(finalFmt, "\n", &sz);
            if (finalFmt)
            {
                vfprintf(fh, finalFmt, args);
            }
        }

        va_end(args);
    }

    if (finalFmt)
    {
        free(finalFmt);
    }
}

static FILE *OpenWriteLog(const char *name)
{
    FILE *fp = NULL;

    fp = fopen(name, "w");
    WriteLog(fp, "starting export manager");
    return fp;
}

static void CloseWriteLog(FILE *fh)
{
    if (fh)
    {
        fclose(fh);
    }
}

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
static int ParseRecSetSpec(const char *rsquery,
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
    char **filts = NULL;
    int nsets = 0;
    DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
    int iset;

    /* This call does NOT fetch a record template, so it is safe to use, even if the series in
     * rsquery does not exist. */
    if (drms_record_parserecsetspec(rsquery, &allvers, &sets, &settypes, &snames, &filts, &nsets, &rsinfo) == DRMS_SUCCESS)
    {
        *infoout = rsinfo;
        *nsetsout = nsets;

        if (nsets > 0)
        {
            *snamesout = (char **)calloc(nsets, sizeof(char *));
            *filtsout = (char **)calloc(nsets, sizeof(char *));

            if (snamesout && filtsout)
            {
                for (iset = 0; iset < nsets; iset++)
                {
                    if (snames[iset])
                    {
                        (*snamesout)[iset] = strdup(snames[iset]);
                    }
                    else
                    {
                        (*snamesout)[iset] = NULL;
                    }

                    if (filts[iset])
                    {
                        (*filtsout)[iset] = strdup(filts[iset]);
                    }
                    else
                    {
                        (*filtsout)[iset] = NULL;
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

    drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);

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

  // jsoc.export
static void ErrorOutExpRec(DRMS_Record_t **exprec, int expstatus, const char *mbuf)
{
    char cmd[DRMS_MAXQUERYLEN];
    DRMS_Keyword_t *key = NULL;
    char *requestid = NULL;
    int istat = DRMS_SUCCESS;

    // Print mbuf to stderr
    if (mbuf)
    {
        fprintf(stderr, "%s\n", mbuf);
    }

    // Write to export record in jsoc.export
    if (exprec && *exprec)
    {
        // Clear out the md5 hash for this export.

        // There is no good way to get the string value without allocating memory. So we have to alloc and free this.
        requestid = drms_getkey_string(*exprec, "requestid", &istat);

        if (!requestid)
        {
            fprintf(stderr, "Out of memory in ErrorOutExpRec().");
        }
        else
        {
            if (istat == DRMS_SUCCESS && strlen(requestid) > 0)
            {
                snprintf(cmd, sizeof(cmd), "DELETE FROM jsoc.export_md5 WHERE requestid='%s'", requestid);
                if (drms_dms((*exprec)->env->session, NULL, cmd))
                {
                   fprintf(stderr, "Failure deleting expired recent export md5s: %s.\n", cmd);
                }
            }

            free(requestid);
            requestid = NULL;
        }

        drms_setkey_string(*exprec, "errmsg", mbuf);
        drms_setkey_int(*exprec, "Status", expstatus);

        // Close export record
        drms_close_record(*exprec, DRMS_INSERT_RECORD);
        *exprec = NULL;
    }
}

// jsoc.export_new
static void ErrorOutExpNewRec(DRMS_RecordSet_t *exprecs, DRMS_Record_t **exprec, int irec, int expstatus, const char *mbuf)
{
    int closedrec = 0;

    // Print mbuf to stderr
    if (mbuf)
    {
        fprintf(stderr, "%s\n", mbuf);
    }

    // Write to export record in jsoc.export
    if (exprec && *exprec)
    {
        drms_setkey_string(*exprec, "errmsg", mbuf);
        drms_setkey_int(*exprec, "Status", expstatus);

        // Close export record
        drms_close_record(*exprec, DRMS_INSERT_RECORD);
        *exprec = NULL;
        closedrec = 1;
    }

    if (exprecs)
    {
        if (closedrec)
        {
            exprecs->records[irec] = NULL; // Detach record - don't double free.
        }
    }
}


// generate qsub script
/* requestid is the ID and requestorid is the name of the person making the request (and is the only member of the prime-key set.) */
static void make_qsub_call(char *requestid, /* like JSOC_20120906_199_IN*/
                           char *reqdir,
                           const char *notify, /* the email address of the requestor in jsoc.export_user. */
                           const char *dbname,
                           const char *dbuser,
                           const char *dbids,
                           const char *dbexporthost,
                           int submitcode)
{
    FILE *fp = NULL;
    char qsubscript[DRMS_MAXPATHLEN];
    char sql[256];
    char notifyStr[512] = {0};
    int ich;

    // Note that the reqdir where the script is put and is run from will not
    // be the record dir that is visible when the drms_run run is finished.
    snprintf(qsubscript, sizeof(qsubscript), "%s/%s.qsub", reqdir, requestid);
    fp = fopen(qsubscript, "w");

    fprintf(fp, "#! /bin/csh -f\n");
    fprintf(fp, "set echo\n");

    /* Set path based on export root (if set) */
    fprintf(fp, "if (${?JSOCROOT_EXPORT}) then\n");
    fprintf(fp, "  set path = ($JSOCROOT_EXPORT/bin/$JSOC_MACHINE $JSOCROOT_EXPORT/scripts $path)\n");
    fprintf(fp,"endif\n");

    fprintf(fp, "while (`show_info JSOC_DBHOST=%s -q 'jsoc.export_new[%s]' key=Status %s` == %d)\n", dbexporthost, requestid, dbids, submitcode);
    fprintf(fp, "  echo waiting for jsocdb commit >> /home/jsoc/exports/tmp/%s.runlog \n",requestid);
    fprintf(fp, "  sleep 1\nend \n");
    if (dbname)
    {
        fprintf(fp, "setenv JSOC_DBNAME %s\n", dbname);
    }
    if (dbuser)
    {
        fprintf(fp, "setenv JSOC_DBUSER %s\n", dbuser);
    }

    fprintf(fp, "setenv JSOC_DBHOST %s\n", dbexporthost);
    fprintf(fp, "setenv JSOC_DBEXPORTHOST %s\n", dbexporthost);
    fprintf(fp, "drms_run JSOC_DBHOST=%s %s/%s.drmsrun >>& /home/jsoc/exports/tmp/%s.runlog \n", dbexporthost, reqdir, requestid, requestid);
    fprintf(fp, "set DRMS_ERROR=$status\n");
    fprintf(fp, "set NewRecnum=`cat /home/jsoc/exports/tmp/%s.recnum` \n", requestid);
    fprintf(fp, "set WAITCOUNT = 20\n");
    fprintf(fp, "while (`show_info JSOC_DBHOST=%s -q -r 'jsoc.export[%s]' %s` < $NewRecnum)\n", dbexporthost, requestid, dbids);
    fprintf(fp, "  echo waiting for jsocdb drms_run commit >> /home/jsoc/exports/tmp/%s.runlog \n",requestid);
    fprintf(fp, "  @ WAITCOUNT = $WAITCOUNT - 1\n");
    fprintf(fp, "  if ($WAITCOUNT <= 0) then\n    set DRMS_ERROR = -1\n    break\n  endif\n");
    fprintf(fp, "  sleep 1\nend \n");

    /* Reject email addresses with spaces and quotes in them (they are invalid any way). */
    if (notify)
    {
       for (ich = 0; ich < strlen(notify); ich++)
       {
          if (notify[ich] == '\'' || notify[ich] == '"' || notify[ich] == ' ' || notify[ich] == '\t')
          {
             snprintf(notifyStr, sizeof(notifyStr), "0");
             break;
          }
       }

       if (!*notifyStr)
       {
          snprintf(notifyStr, sizeof(notifyStr), notify);
       }
    }
    else
    {
       snprintf(notifyStr, sizeof(notifyStr), "0");
    }

    fprintf(fp, "set Notify=%s\n", notifyStr);
    fprintf(fp, "set REQDIR = `show_info JSOC_DBHOST=%s -q -p 'jsoc.export[%s]' %s`\n", dbexporthost, requestid, dbids);
    fprintf(fp, "if ($DRMS_ERROR) then\n");
    fprintf(fp, "  # export failure\n");
    /* This will clone all the stuff in the export SU, including the proc-steps.txt file. We want to
     * delete proc-steps.txt in the drmsrun script when things are working. */
    fprintf(fp, "  set_info -C JSOC_DBHOST=%s  ds='jsoc.export[%s]' Status=4 %s\n", dbexporthost, requestid, dbids);
    fprintf(fp, "  if (\"$Notify\" != 0) then\n");
    fprintf(fp, "    mail -n -s 'JSOC export FAILED - %s' \"$Notify\" <<!\n", requestid);
    fprintf(fp, "Error status returned from DRMS session.\n");
    fprintf(fp, "See log files at http://jsoc.stanford.edu/$REQDIR\n");
    fprintf(fp, "Also complete log file at /home/jsoc/exports/tmp/%s.runlog\n", requestid);
    fprintf(fp, "!\n");
    fprintf(fp, "  endif\n");
    fprintf(fp, "else\n");
    fprintf(fp, "  # export success\n");

    fprintf(fp, "  if (\"$Notify\" != 0) then\n");
    fprintf(fp, "    mail -n -s 'JSOC export complete - %s' \"$Notify\" <<!\n", requestid);
    fprintf(fp, "JSOC export request %s is complete.\n", requestid);
    fprintf(fp, "Results at http://jsoc.stanford.edu$REQDIR\n");
    fprintf(fp, "!\n");
    fprintf(fp, "  endif\n");

    fprintf(fp, "  rm /home/jsoc/exports/tmp/%s.recnum\n", requestid);
    fprintf(fp, "  mv /home/jsoc/exports/tmp/%s.reqlog /home/jsoc/exports/tmp/done \n", requestid);
    /* The log after the call to drms_run gets lost because of the following line - should preserve this
    * somehow (but it can't go in the new inner REQDIR because that is read-only after drms_run returns. */
    fprintf(fp, "  mv /home/jsoc/exports/tmp/%s.runlog /home/jsoc/exports/tmp/done \n", requestid);
    fprintf(fp, "endif\n");

    /* both databases, internal and external, have the pending-requests table - this is because it is
     * possible for the EXtERNAL export user to submit an export on either database; the passthrough feature
     * allows an external user to submit a request on the internal database; as such, the following statement
     * should be run on both databases; it is possible, but almost never the case, that the export was started
     * from the command line, in which case the DELETE statement is a no-op (except that psql will cause
     * $? to be set to !0) because there will be no row for that user in the pending-requests table; there is no
     * such row so that jsoc_fetch will not block a in-house user such as production - that user is allowed to
     * make multiple simultaneous requests.
     */
    snprintf(sql, sizeof(sql), "DELETE FROM %s WHERE address = '%s'", EXPORT_PENDING_REQUESTS_TABLE, notifyStr);
    // fprintf(fp, "psql -h %s -c \"%s\" %s >& /dev/null\n", dbexporthost, sql, dbname);
    fprintf(fp, "psql -h %s -U %s -c \"%s\" %s\n", dbexporthost, dbuser, sql, dbname);

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
                lst = list_llcreate(sizeof(ExpProcArg_t), (ListFreeFn_t)FreeProcArg);
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
        /* ART - I had to ensure the processing series, jsoc.export_procs, is replicated on
         * hmidb and hmidb2. We cannot call drms_open_records() on series in hmidb if the
         * current export is from jsoc.stanford.edu. The DRMS_Env_t is for hmidb2, so we
         * cannot do all the things we want to on hmidb (DRMS calls that would like to access
         * the hmidb db essentially do not work.)
         *
         * The quick and dirty fix was to copy jsoc.export_procs from hmidb to hmidb2. */
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

                free(schema);
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

/* args - input arguments/values from the processing column of jsoc.export_new.
 * pvarsargs - output arguments/values from jsoc.export_new.
 */
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

                    if (oneval && *oneval != '\0')
                    {
                        valcpy = strdup(oneval);
                        hcon_insert(*pvarsargs, onename, &valcpy);
                    }

                    pc++;
                    onename = pc;
                    oneval = NULL;
                }
                else
                {
                    pc++;
                }
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

static void DestroyVarConts(HContainer_t **pvarsargs)
{
    hcon_destroy(pvarsargs);
}

/* Returns 1 on success, 0 on failure. */
/*   pinfo - information from the processing-step series specific to the current procsessing step.
*    args - argument values from the processing keyword of jsoc.export_new. A=1 ==> arg named 'A' with default value 1.
*           -c ==> arg named '-c' with no default value.
*    argsout - final argument string (comma-separated list of arguments/values).
*    stepdata - has program name, input record-set, output record-set.
*/
static int GenProgArgs(ProcStepInfo_t *pinfo,
                       HContainer_t *args,
                       ProcStep_t *stepdata, /* stepdata->output is the output RECORD-SET SPECIFICATION */
                       const char *reclim,
                       FILE *emLogFH,
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
    ListNode_t *node = NULL;
    ExpProcArg_t *data = NULL;
    const char *intname = NULL;
    const char **pintname = NULL;
    size_t sz = 128;
    const char **pval = NULL;
    const char *val = NULL;
    char *finalargs = NULL;
    int first;

    err = 0;
    finalargs = malloc(sz);
    memset(finalargs, 0, sz);

    if (pinfo)
    {
        if (pinfo->req)
        {
            char **snames = NULL;
            char **filts = NULL;
            int nsets;
            DRMS_RecQueryInfo_t info;

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

                    /* stepdata->output has the full record-set query, but we need only the
                     * series name. Parse stepdata->output. */
                    /* Series may not exist, so don't */
                    if (stepdata->output)
                    {
                        if (ParseRecSetSpec(stepdata->output, &snames, &filts, &nsets, &info))
                        {
                            fprintf(stderr, "Invalid output series record specification %s.\n", stepdata->output);
                            err = 1;
                            break;
                        }

                        /* There can be only one output series. */
                        val = snames[0];
                    }
                    else
                    {
                        val = NULL;
                    }
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

            FreeRecSpecParts(&snames, &filts, nsets);
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
                         * be an entry with n:reclim for hg_patch processing.
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
                istat = DRMS_ERROR_UNKNOWNSERIES;
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
            if (istat != DRMS_SUCCESS && istat != DRMS_ERROR_UNKNOWNSERIES)
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

/* Check for the existence of keyword 'keyname' in the series 'series' in the db on host dbhost.
 * Returns 1 on error, 0 on success. */
static int KeyExists(DRMS_Env_t *env, const char *dbhost, const char *series, const char *keyname, int *status)
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
            char query[512];
            char *schema = NULL;
            char *table = NULL;
            DB_Text_Result_t *res = NULL;

            if (get_namespace(series, &schema, &table))
            {
                fprintf(stderr, "Invalid series name %s.\n", series);
                istat = DRMS_ERROR_UNKNOWNSERIES;
            }
            else
            {
                snprintf (query,
                          sizeof(query),
                          "SELECT * FROM %s.drms_keyword WHERE lower(seriesname) = lower(\'%s\') AND lower(keywordname) = lower(\'%s\')",
                          schema,
                          series,
                          keyname);

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
            /* Use the template record to see if a keyword exists. */
            DRMS_Record_t *template = drms_template_record(env, series, &istat);

            if (istat != DRMS_SUCCESS || !template)
            {
                fprintf(stderr, "Problems obtaining template record for %s on %s.\n", series, dbhost);
            }
            else
            {
                rv = (drms_keyword_lookup(template, keyname, 0) != NULL);
            }
        }
    }

    if (status)
    {
        *status = istat;
    }

    return rv;
}

#define PATTERN_REGEX "s/(\\S+)/(\\S+)/"
enum PatternStyle_enum
{
    PatternStyle_Unknown,
    PatternStyle_None,
    PatternStyle_SuffixOld,
    PatternStyle_SuffixNew,
    PatternStyle_Substitution,
    PatternStyle_Replacement
};
typedef enum PatternStyle_enum PatternStyle_t;

static char **ExtractPattern(const char *str, PatternStyle_t *style)
{
    /* there are 4 forms the 'out' column of jsoc.export can take:
     * 1. '_XYZ' - append '_XYZ' to the end of the name of the series
     * 2. <series> - replace the existing series with <series>
     * 3. s/<str1>/<str2>/ - substitute <str2> for all occurrences of <str1>
     * 4. s/$/XYZ/ - append '_XYZ' to the end of the name of the series (identical to #1, but a new way of
     *    specifying that a series name is to have something appended to it)
     */
    char **pattern = NULL;
    regex_t regexp;
    regmatch_t matches[3]; /* index 0 is the entire string */

    if (style)
    {
        *style = PatternStyle_Unknown;
    }

    if (str)
    {
        if (*str == '_' && strlen(str) > 1)
        {
            /* old-style suffix */
            pattern = calloc(1, sizeof(char **));
            pattern[0] = strdup(str + 1); /* exclude the '_' - the calling function will add it */

            if (style)
            {
                *style = PatternStyle_SuffixOld;
            }
        }
        else
        {
            if (regcomp(&regexp, PATTERN_REGEX, REG_EXTENDED) != 0)
            {
                fprintf(stderr, "bad regular expression '%s'\n", PATTERN_REGEX);
            }
            else
            {
                if (regexec(&regexp, str, sizeof(matches) / sizeof(matches[0]), matches, 0) == 0)
                {
                    if (str[matches[1].rm_so] == '$' && (matches[1].rm_eo - matches[1].rm_so == 1))
                    {
                        /* a new-style suffix */
                        pattern = calloc(1, sizeof(char **));
                        if (pattern)
                        {
                            pattern[0] = calloc(1, sizeof(char) * (matches[2].rm_eo - matches[2].rm_so + 1));
                            if (pattern[0])
                            {
                                strncpy(pattern[0], &(str[matches[2].rm_so]), matches[2].rm_eo - matches[2].rm_so);
                            }
                        }

                        if (style)
                        {
                            *style = PatternStyle_SuffixNew;
                        }
                    }
                    else
                    {
                        /* a substitution */
                        pattern = calloc(2, sizeof(char **));
                        if (pattern)
                        {
                            char *tmp = NULL;

                            pattern[0] = calloc(1, sizeof(char) * (matches[1].rm_eo - matches[1].rm_so + 1));
                            if (pattern[0])
                            {
                                strncpy(pattern[0], &(str[matches[1].rm_so]), matches[1].rm_eo - matches[1].rm_so);
                            }

                            pattern[1] = calloc(1, sizeof(char) * (matches[2].rm_eo - matches[2].rm_so + 1));
                            if (pattern[1])
                            {
                                strncpy(pattern[1], &(str[matches[2].rm_so]), matches[2].rm_eo - matches[2].rm_so);
                            }

                            if (style)
                            {
                                *style = PatternStyle_Substitution;
                            }
                        }
                    }

                    regfree(&regexp);
                }
                else
                {
                    /* replace the current series name with a new series name */
                    pattern = calloc(1, sizeof(char **));
                    if (pattern)
                    {
                        pattern[0] = strdup(str);
                    }

                    if (style)
                    {
                        *style = PatternStyle_Replacement;
                    }
                }
            }
        }
    }
    else
    {
        if (style)
        {
            *style = PatternStyle_None;
        }
    }

    return pattern;
}

static void FreePattern(char ***pattern, PatternStyle_t style)
{
    if (pattern && *pattern)
    {
        if ((*pattern)[0])
        {
            free((*pattern)[0]);
            (*pattern)[0] = NULL;
        }

        if (style == PatternStyle_Substitution)
        {
            /* pattern has two elements */
            if ((*pattern)[1])
            {
                free((*pattern)[1]);
                (*pattern)[1] = NULL;
            }
        }

        free(*pattern);
        *pattern = NULL;
    }
}

static char *FindLastSeriesInPipeline(DRMS_Env_t *env, const char *dbhost, ProcStep_t *stepData, const char *outSeries, int iset, int *isAncestor, int *ierr)
{
    int istat = DRMS_SUCCESS;

    char *series = NULL;
    DB_Handle_t *dbh = NULL;
    int contextOK = 0;
    ProcStep_t *step = NULL;
    const char *setSpec = NULL;
    DRMS_RecordSet_t *recSet = NULL;

    /* returns NULL on error. */
    dbh = GetDBHandle(env, dbhost, &contextOK, 0);

    if (!dbh)
    {
        fprintf(stderr, "jsoc_export_manage: unable to connect to database\n");
        istat = 1;
    }
    else
    {
        int loopNo;
        int seriesExists;
        char **snames = NULL;
        char **filts = NULL;
        int nsets;
        DRMS_RecQueryInfo_t info;

        step = stepData;
        for (loopNo = 0, seriesExists = 0; istat == DRMS_SUCCESS && step && !seriesExists; loopNo++)
        {
            if (loopNo == 0)
            {
                setSpec = outSeries;
            }
            else
            {
                setSpec = step->input; /* for the rest of the steps, */
            }

            if (ParseRecSetSpec(setSpec, &snames, &filts, &nsets, &info) || nsets < 1)
            {
                fprintf(stderr, "invalid record-set specification %s\n", setSpec);
                istat = DRMS_ERROR_INVALIDDATA;
            }
            else
            {
                series = snames[iset];
            }

            if (istat == DRMS_SUCCESS)
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

                    if (get_namespace(snames[iset], &schema, &table))
                    {
                        fprintf(stderr, "invalid series name %s\n", snames[iset]);
                        istat = DRMS_ERROR_INVALIDDATA;
                    }
                    else
                    {
                        snprintf (query, sizeof(query), "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = lower(\'%s\') AND tablename = lower(\'%s\')", schema, table);
                        res = db_query_txt(dbh, query);

                        if (res)
                        {
                            seriesExists = (res->num_rows != 0);
                            db_free_text_result(res);
                            res = NULL;
                        }
                        else
                        {
                            fprintf(stderr, "invalid SQL query: %s\n", query);
                            istat = DRMS_ERROR_QUERYFAILED;
                        }

                        free(schema);
                        free(table);
                    }
                }
                else
                {
                    /* the caller wants to check for the existence of a series in the database to which
                     * this module is currently connected */
                    seriesExists = drms_series_exists(env, snames[iset], &istat);
                    if (istat != DRMS_SUCCESS && istat != DRMS_ERROR_UNKNOWNSERIES)
                    {
                        fprintf(stderr, "problems checking for series '%s' existence on %s\n", snames[iset], dbhost);
                        seriesExists = 0;
                    }
                    else
                    {
                        istat = DRMS_SUCCESS;
                    }
                }
            }

            if (seriesExists && istat == DRMS_SUCCESS)
            {
                series = strdup(snames[iset]);
            }

            FreeRecSpecParts(&snames, &filts, nsets);

            if (loopNo > 0)
            {
                /* change the step only after the first iteration */
                step = step->parent;
            }
        } /* end while */

        if (isAncestor)
        {
            /* loopNo > 1 --> iterated through loop at least twice */
            *isAncestor = series && loopNo > 1;
        }
    }

    if (ierr)
    {
        *ierr = istat;
    }

    return series;
}

/* cpinfo - the current processing step's processing information
 * ppinfo - the previous processing step's processing information. */
static int GenOutRSSpec(DRMS_Env_t *env,
                        const char *dbhost,
                        ProcStepInfo_t *cpinfo,
                        ProcStepInfo_t *ppinfo,
                        ProcStep_t *data,
                        const char *reqid,
                        FILE *emLogFH)
{
    int err = 0;

    if (cpinfo)
    {
        const char *suffix = cpinfo->suffix; /* this is really the 'out' column */
        const char *psuffix = NULL;
        const char *pdataset = NULL;
        char **snamesIn = NULL;
        char **snamesOut = NULL;
        char **filtsIn = NULL;
        char **filtsOut = NULL;
        int nsetsIn;
        int nsetsOut;
        int iset;
        DRMS_RecQueryInfo_t infoIn;
        DRMS_RecQueryInfo_t infoOut;
        char *outDataSets = NULL;
        char *newoutDataSets = NULL;
        char *newfilter = NULL;
        int npkeys;
        size_t sz;
        int len;
        char *repl = NULL;
        int ierr = 0;
        size_t szCanon = 128;
        char *canonicalIn = NULL;
        char **currentPattern = NULL;
        PatternStyle_t currentPatternStyle;
        int isAncestor;
        char *existingSeries = NULL;

        while (1)
        {
            if (ppinfo)
            {
                /* The input to the current processing step, which is identical to ppinfo->output,
                 * the output of the previous step, might have a suffix. If it has a suffix, then
                 * it is ppinfo->suffix. */
                psuffix = ppinfo->suffix;
            }
            else
            {
                /* Since there was no previous processing step, there is no suffix
                 * attached to the input of this processing step. */
            }

            /* Parse input record-set query parts. */
            if (ParseRecSetSpec(data->input, &snamesIn, &filtsIn, &nsetsIn, &infoIn))
            {
                err = 1;
                break;
            }

            if (nsetsIn > 1)
            {
                /* We do not support record-set specs with subsets. But the output of this function will not be used to
                 * create the output record-set. Instead, the data set will be converted to a list of recnums, and that
                 * dataset will be parsed by this function. */
                err = 0;
                break;
            }

            canonicalIn = calloc(szCanon, sizeof(char));
            if (!canonicalIn)
            {
                err = 1;
                fprintf(stderr, "out of memory\n");
                break;
            }

            for (iset = 0; iset < nsetsIn; iset++)
            {
                if (iset > 0)
                {
                    canonicalIn = base_strcatalloc(canonicalIn, ", ", &szCanon);
                }

                canonicalIn = base_strcatalloc(canonicalIn, snamesIn[iset], &szCanon);
                if (*(filtsIn[iset]) != '\0')
                {
                    canonicalIn = base_strcatalloc(canonicalIn, filtsIn[iset], &szCanon);
                }
            }

            currentPattern = ExtractPattern(suffix, &currentPatternStyle);

            if (currentPatternStyle == PatternStyle_Unknown)
            {
                /* unable to extract a valid pattern */
                fprintf(stderr, "unable to extract a valid series pattern from %s\n", suffix);
            }
            else if (currentPatternStyle == PatternStyle_None)
            {
                /* neither a suffix nor an output series specified; the input series might
                 * have a suffix, but that is irrelevant - whatever the input series is, use that for
                 * the output series too */
                outDataSets = strdup(canonicalIn);
            }
            else if (currentPatternStyle == PatternStyle_Substitution)
            {
                /* a current pattern exists, and it is a substitution */
                data->crout= 0;
                if (strcmp(currentPattern[0], currentPattern[1]) != 0)
                {
                    outDataSets = base_strreplace(canonicalIn, currentPattern[0], currentPattern[1]);

                    /* if the input and output series differ, then the output series MAY not exist; run
                     * jsoc_export_clone from the drms_run script; if the output series
                     * already exists, then jsoc_export_clone is a no-op */
                    data->crout = 1;
                }
            }
            else if (currentPatternStyle == PatternStyle_Replacement)
            {
                /* a current pattern exists, and it is a replacement */
                outDataSets = strdup(canonicalIn);
                data->crout = 0;
                for (iset = 0; outDataSets, iset < nsetsIn; iset++)
                {
                    if (strcasecmp(snamesIn[iset], currentPattern[0]) != 0)
                    {
                        newoutDataSets = base_strreplace(outDataSets, snamesIn[iset], currentPattern[0]);
                        free(outDataSets);
                        outDataSets = newoutDataSets;
                        data->crout = 1;
                    }
                }
            }
            else if (currentPatternStyle == PatternStyle_SuffixOld || currentPatternStyle == PatternStyle_SuffixNew)
            {
                /* a current pattern exists, and it is a suffix */
                char replname[DRMS_MAXSERIESNAMELEN];
                char *psuff = NULL;

                /* theoretically, outDataSets could be a comma-separated list of record-set specifications; we are
                 * going to add the suffix to each seriesname in that list */
                outDataSets = strdup(canonicalIn);
                data->crout = 0;
                for (iset = 0; outDataSets, iset < nsetsIn; iset++)
                {
                    if ((psuff = strcasestr(snamesIn[iset], currentPattern[0])) == NULL || *(psuff + strlen(currentPattern[0])) != '\0')
                    {
                        /* append the suffix because the input series does not end with that suffix (it may or may not have a suffix at all) */
                        snprintf(replname, sizeof(replname), "%s_%s", snamesIn[iset], currentPattern[0]);
                        newoutDataSets = base_strreplace(outDataSets, snamesIn[iset], replname);
                        free(outDataSets);
                        outDataSets = newoutDataSets;

                        /* We are writing to at least one output series that differs fromt the input series.
                         * We need to run jsoc_export_clone from the drms_run script. If the output series
                         * already exists, then jsoc_export_clone is a no-op. */
                        data->crout = 1;
                    }
                    else
                    {
                        /* the series already ends with the current suffix - do not modify the series names */
                    }
                }
            }
            else
            {
                fprintf(stderr, "unknown pattern type %d\n", currentPatternStyle);
            }

            if (currentPattern)
            {
                FreePattern(&currentPattern, currentPatternStyle);
            }

            /* Remove input series' filters. */
            for (iset = 0; iset < nsetsIn; iset++)
            {
                if (filtsIn[iset])
                {
                    newoutDataSets = base_strreplace(outDataSets, filtsIn[iset], "");
                    free(outDataSets);
                    outDataSets = newoutDataSets;
                }
            }

            /* Add filters to output series names. */
            if (ParseRecSetSpec(outDataSets, &snamesOut, &filtsOut, &nsetsOut, &infoOut))
            {
                err = 1;
                break;
            }

            sz = 32;
            newfilter = malloc(sz);
            memset(newfilter, 0, sz);
            for (iset = 0; iset < nsetsOut; iset++)
            {
                /* Get number of prime-key keywords for current series. */

                /* Must talk to db that has actual series (i.e., hmidb), despite the fact
                 * that the env might contain a connection to the wrong db. NumPKeyKeys()
                 * will talk to the correct db. */

                /* Tthe output series might not exist though! This is only true if
                 * crout == 1. If the output series doesn't exist, then
                 * obtain the number of prime keys of the input series, snamesIn[iset].
                 * Actually, all the snamesIn should be identical, since we don't
                 * allow exporting from more than one series. */

                 /* Find last series in pipeline that already exists
                  *
                  */
                isAncestor = 0;
                existingSeries = FindLastSeriesInPipeline(env, dbhost, data, snamesOut[iset], iset, &isAncestor, &ierr);
                WriteLog(emLogFH, "[ GenOutRSSpec() ] last existing series in pipeline is %s", existingSeries);

                if (!existingSeries || ierr)
                {
                    fprintf(stderr, "cannot find a valid, existing series in the processing pipeline\n");
                    npkeys = 0;
                }
                else
                {
                    npkeys = NumPKeyKeys(env, dbhost, existingSeries);

                    if (isAncestor)
                    {
                        if (data->crout != 1)
                        {
                            /* the output series does not exist, and we are not allowed to create it */
                            fprintf(stderr, "the output series %s does not exist and cannot be created\n", snamesOut[iset]);
                            npkeys = 0;
                        }
                        else
                        {
                            /* Now, need to check to see if the ancestor series has the RequestID keyword.
                             * The environment might not allow this check, since we might
                             * not be connected to the dbmainhost, so we have to come up with an
                             * independent SQL query that tests for the existence of a keyword.
                             * KeyExists() does connect to the correct db. */
                            if (!KeyExists(env, dbhost, existingSeries, "RequestID", &ierr) || ierr)
                            {
                                /* Need to add one to npkeys, since the output series will have one
                                 * more prime-key key constituent (RequestID) than the input series. */
                                npkeys++;
                            }
                        }
                    }
                }

                if (existingSeries)
                {
                    free(existingSeries);
                    existingSeries = NULL;
                }

                if (npkeys < 1)
                {
                    /* there must be at least one prime key constituent - the reqid keyword */
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

                len = strlen(snamesOut[iset]) + strlen(newfilter) + 16;
                repl = malloc(len);

                if (repl)
                {
                    snprintf(repl, len, "%s%s", snamesOut[iset], newfilter);
                    newoutDataSets = base_strreplace(outDataSets, snamesOut[iset], repl);
                    free(repl);
                    repl = NULL;
                }
                else
                {
                    fprintf(stderr, "Out of memory.\n");
                    err = 1;
                    break;
                }

                free(outDataSets);
                outDataSets = newoutDataSets;

                *newfilter = '\0';
            }

            if (err == 1)
            {
                break;
            }

            free(newfilter);
            newfilter = NULL;

            FreeRecSpecParts(&snamesOut, &filtsOut, nsetsOut);
            FreeRecSpecParts(&snamesIn, &filtsIn, nsetsIn);
            /* Done! outDataSets has record-set specifications that have the proper
             * suffix and that have the proper filters. */
            data->output = outDataSets;

            break; /* one-time through */
        }
        /* The output series name is the input series name, stripped of the suffix,
         * with the output suffix appended. The output filter is a string consisting
         * of [] for every prime-key constituent, except for the requestid keyword,
         * in which case the subfilter is [reqid]. */

         if (canonicalIn)
         {
            free(canonicalIn);
            canonicalIn = NULL;
         }
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
                                 FILE *emLogFH,
                                 int *status)
{
    LinkedList_t *rv = NULL;
    char *activestr = NULL;
    char *pc = NULL;
    PParseState_t state = kPPStBeginProc;
    char *onecmd = NULL;
    char *args = NULL;
    ProcStep_t data; /* current step's data. */
    ProcStep_t *parentData = NULL;
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
    ListNode_t *parentNode = NULL;

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
            data.parent = NULL;
            data.crout = 0;
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
                    WriteLog(emLogFH, "unknown processing step %s\n", onecmd);
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

                data.parent = parentData;

                /* Generate output name. The plan is to take the input name, strip its suffix (if one
                 * exists), then add the new suffix (if one exists). The output series will have
                 * requestid as a keyword. So the output record-set will be <series>[][][reqid]. */
                if (GenOutRSSpec(env, dbhost, cpinfo, ppinfo, &data, reqid, emLogFH))
                {
                    state = kPPStError;
                    data.parent = NULL;
                    continue;
                }

                if (GenProgArgs(cpinfo, varsargs, &data, reclimint, emLogFH, &finalargs))
                {
                    state = kPPStError;
                    data.parent = NULL;
                    continue;
                }

                /* Done with varsargs, free it. */
                DestroyVarConts(&varsargs);

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

                parentNode = list_llinserttail(rv, &data);
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
                    /* We could be here because we attempted to parse a record-set specification that has subsets. If this is true,
                     * we want to re-use the input record-set spec. */
                    adataset = strdup(data.input);
                    state = kPPStBeginProc;
                    pc++; /* Advance to first char after the proc-step delimiter. */
                }

                parentData = (ProcStep_t *)parentNode->data;
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
        if (reclimint)
        {
            *reclim = reclimint;
        }
        else
        {
            *reclim = strdup("0");
        }
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

static char *convertQuotes(const char *arg, DRMS_Record_t **export_rec, DRMS_Record_t **export_log, DRMS_RecordSet_t *exports_new, int irec)
{
    char *ret = NULL;
    size_t sz;
    size_t bufSz;
    char bufCh[2] = {0};
    int pos;
    int error;

    /* states */
    int inSQString; /* we are parsing a single-quoted string. */
    int inEString; /* we are parsing an escaped string. */
    int inDQString; /* we are parsing a double-quoted string (not part of the SQL standard, but something DRMS supports). */
    int inString; /* we are parsing one of the three kinds of strings. */
    int lastE; /* the previous char was an e (so perhaps we are at the beginning of an escaped string).
                * 1 - capital E, 2 - lower-case e.
                */
    int lastSQ; /* we are in a single-quoted string or an escape string and the previous char was a '\''
                 * (so if the next char is a '\'', then we have a '\'' inside a string delimited by '\'' chars.
                 */
    int lastEsc; /* we are in an escaped string and the previous char was an escaping backslash (so
                  * we ignore the "specialness" of a following '\''. */

    if (!arg || strlen(arg) <= 0)
    {
        ErrorOutExpRec(export_rec, 4, "Missing record-set specification.");
        ErrorOutExpNewRec(exports_new, export_log, irec, 4, "Missing record-set specification.");
        return NULL;
    }

    if (strlen(arg) >= 4096)
    {
        ErrorOutExpRec(export_rec, 4, "Record-set specification is too long.");
        ErrorOutExpNewRec(exports_new, export_log, irec, 4, "Record-set specification is too long.");
        return NULL;
    }

    sz = strlen(arg);
    bufSz = sz;
    ret = calloc(1, bufSz);

    if (!ret)
    {
        ErrorOutExpRec(export_rec, 4, "Out of memory.");
        ErrorOutExpNewRec(exports_new, export_log, irec, 4, "Out of memory.");
        return NULL;
    }

    /* We want to change a d-quote to an s-quote in only two cases:
     * 1. We are outside of any string and we see a '\"'.
     * 2. We are inside a string that is enclosed by d-quotes.
     *
     * There are six quoting methods to specify a string in SQL:
     * 1. Enclose the string in single quotes 'this is a string'.
     * 2. Enclose the string in E'this is a string' or e'this is a string'.
     * 3. Enclose the string in $$this is a string$$ or $someTag$this is a string$someTag$.
     * 4. Enclose the string in U&'this is a string' or u&'this is a string'.
     * 5. Enclose the string in B'1010110' or b'1010110.
     * 6. Enclose the string in X'1FF' or X'1ff'.
     *
     * Since d-quotes are not allowed in the last two methods, those methods can be ignored. In addition, if
     * the PGSQL standard_conforming_strings parameter is off (we should require that this is so), then
     * Unicode escape sequences cannot be used to define string constants. But if a d-quote
     * exists in one of the first three methods, it cannot be considered the start of a d-quote-enclosed string.
     * Determining where the end of the string is with the first two methods is tricky because
     * in both cases, single quotes can be inside the strings. This can
     * happen if there are two single quotes in a row (the first single quote escapes the second).
     * With the second method, it can also happen if a single quote is backslash-escaped. Regarding dollar-quoted string...
     * fuck that. They are not part of the SQL standard (they are a PSQL construct), so we will disregard them
     * to simplify things. They can nest, which would make parsing painful. Even without them, things are fairly complicated.
     */

    error = 0;
    for (pos = 0, inSQString = 0, inEString = 0, inDQString = 0, lastSQ = 0, lastE = 0, lastEsc = 0; pos < sz; pos++)
    {
        inString = (inSQString || inEString || inDQString);

        /* Go to the right state first. */
        if (!inString)
        {
            if (lastE != 0)
            {
                /* The previous char was an 'E' or 'e'. */
                if (arg[pos] == '\'')
                {
                    ret = base_strcatalloc(ret, "E'", &bufSz);
                    inEString = 1;
                }
                else
                {
                    /* Shit - we have to distinguish between E and e. */
                    ret = base_strcatalloc(ret, (lastE == 1) ? "E" : "e", &bufSz);
                    bufCh[0] = arg[pos];
                    ret = base_strcatalloc(ret, bufCh, &bufSz);
                }

                lastE = 0;
            }
            else
            {
                if (arg[pos] == '\'')
                {
                    ret = base_strcatalloc(ret, "'", &bufSz);
                    inSQString = 1;
                }
                else if (arg[pos] == 'E')
                {
                    lastE = 1;
                }
                else if (arg[pos] == 'e')
                {
                    lastE = 2;
                }
                else if (arg[pos] == '\"')
                {
                    /* The start of a d-quoted string - replace the d-quote with an s-quote. */
                    ret = base_strcatalloc(ret, "'", &bufSz);
                    inDQString = 1;
                }
                else
                {
                    bufCh[0] = arg[pos];
                    ret = base_strcatalloc(ret, bufCh, &bufSz);
                }
            }
        }
        else
        {
            if (inDQString)
            {
                if (arg[pos] == '\"')
                {
                    /* We are parsing a d-quoted string, and we have a d-quote - end the string. Replace the end d-quote with an s-quote. */
                    ret = base_strcatalloc(ret, "'", &bufSz);
                    inDQString = 0;
                }
                else
                {
                    bufCh[0] = arg[pos];
                    ret = base_strcatalloc(ret, bufCh, &bufSz);
                }
            }
            else if (inSQString || inEString)
            {
                if (lastEsc)
                {
                    /* The previous character was a backslash, so pass this char directly to output - it isn't special in any way. */
                    bufCh[0] = arg[pos];
                    ret = base_strcatalloc(ret, bufCh, &bufSz);
                    lastEsc = 0;
                }
                else if (lastSQ)
                {
                    if (arg[pos] == '\'')
                    {
                        /* We have a single-q, and the last char was a single-q. This is OK - pass both single quotes through. */
                        ret = base_strcatalloc(ret, "''", &bufSz);
                    }
                    else
                    {
                        /* Last time we saw a first consecutive '\'', but this time we did not. End of string. */
                        ret = base_strcatalloc(ret, "'", &bufSz);

                        inSQString = 0;
                        inEString = 0;
                        lastSQ = 0;

                        pos--; /* Otherwise the parser would move on to the char two past the end of the string. */
                    }

                    lastSQ = 0;
                }
                else
                {
                    if (arg[pos] == '\'')
                    {
                        /* This is a first consecutive single-q. Or it may be the end of the string. */
                        lastSQ = 1;
                    }
                    else
                    {
                        if (inEString && arg[pos] == '\\')
                        {
                            lastEsc = 1;
                        }

                        bufCh[0] = arg[pos];
                        ret = base_strcatalloc(ret, bufCh, &bufSz);
                    }
                }
            }
            else
            {
                /* Parser error - if we are in a string, it must be one of the three string types already handled. */
                ErrorOutExpRec(export_rec, 4, "Invalid SQL string in record-set specification.");
                ErrorOutExpNewRec(exports_new, export_log, irec, 4, "Invalid SQL string in record-set specification.");
                error = 1;
                break;
            }
        }
    }

    if (lastE != 0)
    {
        /* We are not insdie a string at this point. */
        ret = base_strcatalloc(ret, (lastE == 1) ? "E" : "e", &bufSz);
    }

    if (lastSQ && (inSQString || inEString))
    {
        /* It could the the case that we were parsing a string delimited by single quotes, and
         * the single quote was the last character. */
        if (pos > 0 && arg[pos - 1] == '\'')
        {
            ret = base_strcatalloc(ret, "'", &bufSz);
            inSQString = 0;
            inEString = 0;
            lastSQ = 0;
        }
    }

    if (inSQString || inEString || inDQString || error)
    {
        /* All strings should have been completely parsed. */
        ErrorOutExpRec(export_rec, 4, "Invalid record-set specification.");
        ErrorOutExpNewRec(exports_new, export_log, irec, 4, "Invalid record-set specification.");
        free(ret);
        ret = NULL;
    }

    return ret;
}

static char *escapeArgument(const char *arg)
{
    char *ret = NULL;
    size_t sz;
    size_t bufSz;
    char bufCh[2] = {0};
    int pos;


    if (strlen(arg) >= 4096)
    {
        fprintf(stderr, "Argument is too long.\n");
        return NULL;
    }

    sz = strlen(arg);
    bufSz = sz;
    ret = calloc(1, bufSz);

    if (!ret)
    {
        fprintf(stderr, "Out of memory.\n");
        return NULL;
    }

    for (pos = 0; pos < sz; pos++)
    {
        /* If the char is an escape sequence, then we have to make a string
         * equivalent of it to pass the sequence via the shell to the
         * export programs. We have to support only newline and tab. */
        if (arg[pos] == '\n')
        {
            ret = base_strcatalloc(ret, "\\\\n", &bufSz);
        }
        else if (arg[pos] == '\t')
        {
            ret = base_strcatalloc(ret, "\\\\t", &bufSz);
        }
        else
        {
            if (!isalnum(arg[pos]))
            {
                ret = base_strcatalloc(ret, "\\", &bufSz);
            }

            bufCh[0] = arg[pos];
            ret = base_strcatalloc(ret, bufCh, &bufSz);
        }
    }

    return ret;
}

/* returns 1 on error, 0 on success */
static int GenExpFitsCmd(FILE *fptr,
                         DRMS_Record_t **export_rec,
                         DRMS_Record_t **export_log,
                         DRMS_RecordSet_t *exports_new,
                         int irec,
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
        {
            cparms = "**NONE**";
        }

        /* ART - multiple processing steps
         * Always use record limit, since we can no longer make the export commands processing-specific. */
        /* ART - Don't use single quotes around the record-set specification. The spec may contain single quotes.
         * I don't think it can contain double quotes, so put the spec inside double quotes.
         * ART - Oh, well, it can contain double quotes, so we are going to escape them. Escape all non-alphanumeric
         * chars in the drms run script's jsoc_export_as_fits command line. The shell will then accept them.
         */

        /* SQL string literals must be enclosed in single quotes. Or they must contain Unicode escape sequences or
         * or they must be C-style strings enclosed in E''. But DRMS allows people to enclose them in double quotes too, so
         * we have to convert double quotes to single quotes, but only if the double quotes are the kind that enclose
         * string literals. So, do two parsing passes. The first converts these double quotes to single quotes.
         */

        char *rssArgConv = convertQuotes(dataset, export_rec, export_log, exports_new, irec);
        char *rssArgEsc = NULL;

        if (rssArgConv)
        {
            rssArgEsc = escapeArgument(rssArgConv);
            if (rssArgEsc)
            {
                fprintf(fptr, "jsoc_export_as_fits JSOC_DBHOST=%s reqid='%s' expversion=%s rsquery=%s n=%s path=$REQDIR ffmt='%s' method='%s' protocol='%s' %s\n", dbmainhost, requestid, PACKLIST_VER, rssArgEsc, RecordLimit, filenamefmt, method, protos[kProto_FITS], dbids);
                free(rssArgEsc);
            }
            else
            {
                rv = 1;
            }

            free(rssArgConv);
        }
        else
        {
            rv = 1;
        }

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
                          DRMS_Record_t **export_rec,
                          DRMS_Record_t **export_log,
                          DRMS_RecordSet_t *exports_new,
                          int irec,
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
        rv = (GenExpFitsCmd(fptr, export_rec, export_log, exports_new, irec, protocol, dbmainhost, requestid, dataset, RecordLimit, filenamefmt, method, dbids) != 0);
    }
    else if (strncasecmp(protocol, protos[kProto_MPEG], strlen(protos[kProto_MPEG])) == 0 ||
             strncasecmp(protocol, protos[kProto_JPEG], strlen(protos[kProto_JPEG])) == 0 ||
             strncasecmp(protocol, protos[kProto_PNG], strlen(protos[kProto_PNG])) == 0 ||
             strncasecmp(protocol, protos[kProto_MP4], strlen(protos[kProto_MP4])) == 0)
    {
        char *dupe = strdup(protocol);
        char *newproto = dupe;
        char *pcomma=index(newproto,',');

        if (pcomma)
            *pcomma = '\0';

        fprintf(fptr, "jsoc_export_as_images in='%s' reqid='%s' expversion='%s' n='%s' method='%s' outpath=$REQDIR ffmt='%s' cparms='%s'",
                dataset, requestid, PACKLIST_VER, RecordLimit, method, filenamefmt, "cparms is not needed");

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

        if (dupe)
        {
            free(dupe);
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
    else if (strncasecmp(protocol, protos[kProto_SuAsIs], strlen(protos[kProto_SuAsIs])) == 0)
    {
        /* dataset has sunums=12345678. Strip off the "sunums=", which is not expected by jsoc_export_SU_as_is. */
        char *dupe = NULL;
        char *sunumList = NULL;

        dupe = strdup(dataset);
        if (!dupe)
        {
            fprintf(stderr, "Out of memory.\n");
            rv = 1;
        }
        else
        {
            sunumList = strchr(dupe, '=');

            if (!sunumList)
            {
                fprintf(stderr, "Bad format for DataSet column; should be 'sunum=...'.\n");
                rv = 1;
            }
            else
            {
                sunumList++;

                fprintf(fptr, "jsoc_export_SU_as_is JSOC_DBHOST=%s ds='%s' requestid='%s'\n",
                        dbmainhost, sunumList, requestid);

                GenErrChkCmd(fptr);

                /* print keyword values for as-is processing */
                fprintf(fptr, "show_info JSOC_DBHOST=%s -ait sunum='%s' n=%s > %s.keywords.txt\n",
                        dbmainhost, sunumList, RecordLimit, requestid);
                GenErrChkCmd(fptr);
            }

            free(dupe);
            dupe = NULL;
            sunumList = NULL;
        }
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



// jsoc.export
static int DBCOMM(DRMS_Record_t **rec, const char *mbuf, int expstatus)
{
    if (mbuf)
    {
        fprintf(stderr, "%s\n", mbuf);
    }

    if (rec && *rec)
    {
        if (drms_setkey_int(*rec, "Status", expstatus))
        {
            return 1; // Abort db changes
        }

        drms_setkey_string(*rec, "errmsg", mbuf);
        drms_close_record(*rec, DRMS_INSERT_RECORD);
        *rec = NULL;
    }

    return 0; // Commit db changes.
}

// jsoc.export_new
static int DBNEWCOMM(DRMS_RecordSet_t **exprecs, DRMS_Record_t **rec, int irec, const char *mbuf, int expstatus)
{
    int closedrec = 0;

    if (mbuf)
    {
        fprintf(stderr, "%s\n", mbuf);
    }

    if (rec && *rec)
    {
        if (drms_setkey_int(*rec, "Status", expstatus))
        {
            return 1; // Abort db changes
        }

        drms_setkey_string(*rec, "errmsg", mbuf);
        drms_close_record(*rec, DRMS_INSERT_RECORD);
        *rec = NULL;
        closedrec = 1;
    }

    if (exprecs && *exprecs)
    {
        if (closedrec)
        {
            (*exprecs)->records[irec] = NULL; // Detach record - don't double free.
        }
        drms_close_records(*exprecs, DRMS_FREE_RECORD);
        *exprecs = NULL;
    }

    return 0; // Commit db changes.
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
    int doSuExport;
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
    int quit = 0;
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
    char msgbuf[1024];
    int submitcode = -1;
    DRMS_Env_t *seriesEnv = NULL;
    char logfile[PATH_MAX];
    char logdir[PATH_MAX];
    const char *logdirArg = NULL;
    const char *qsubInitScript = NULL;
    const char *testQuotes = NULL;
    FILE *emLogFH = NULL;
    char *quoted = NULL;
    char *escaped = NULL;

    if (nice_intro ())
    {
      return (0);
    }

    logdirArg = cmdparams_get_str(&cmdparams, kArgLogDir, NULL);
    snprintf(logdir, sizeof(logdir), "%s", logdirArg);
    if (logdir[strlen(logdir) - 1] == '/')
    {
        logdir[strlen(logdir) - 1] = '\0';
    }

    qsubInitScript = cmdparams_get_str(&cmdparams, kArgQsubInitScript, NULL);

    testQuotes = cmdparams_get_str(&cmdparams, kArgTestConvQuotes, NULL);
    if (testQuotes)
    {
        if (strcmp(testQuotes, kArgValNotUsed) != 0)
        {
            quoted = convertQuotes(testQuotes, NULL, NULL, NULL, -1);
            if (quoted)
            {
                fprintf(stderr, "Quoted string:\n");
                fprintf(stderr, quoted);
                fprintf(stderr, "\n");

                escaped = escapeArgument(quoted);

                if (escaped)
                {
                    fprintf(stderr, "Escaped string:\n");
                    fprintf(stderr, escaped);
                    fprintf(stderr, "\n");

                    free(escaped);
                }

                free(quoted);
            }
            return 0;
        }
    }

  testmode = (TESTMODE || cmdparams_isflagset(&cmdparams, kArgTestmode));
  submitcode = (testmode ? 12 : 2);

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

  /*  op == process, this is export_manage cmd line, NOT for request being managed
   * By default, op is "process". We have never run jsoc_export_manage when op is not "process".
   *
   */
  if (strcmp(op,"process") == 0)
    {
    int irec;
    char ctlrecspec[1024];
    char logfile[PATH_MAX];

    snprintf(ctlrecspec, sizeof(ctlrecspec), "%s[][? Status=%d ?]", EXPORT_SERIES_NEW, submitcode);
    exports_new_orig = drms_open_records(drms_env, ctlrecspec, &status);

    if (!exports_new_orig)
	DIE("Can not open RecordSet");
    if (exports_new_orig->n < 1)  // No new exports to process.
        {
        drms_close_records(exports_new_orig, DRMS_FREE_RECORD);
        return(0);
        }
    exports_new = drms_clone_records(exports_new_orig, DRMS_PERMANENT, DRMS_SHARE_SEGMENTS, &status);
    if (!exports_new)
	DIE("Can not clone RecordSet"); // When jsoc_export_manage runs again, it will try to process this export record again.
    drms_close_records(exports_new_orig, DRMS_FREE_RECORD);

    for (irec=0; irec < exports_new->n; irec++)
    {
      now = timenow();
      export_log = exports_new->records[irec];
      // Get user provided new export request
      status     = drms_getkey_int(export_log, "Status", NULL);
      requestid    = drms_getkey_string(export_log, "RequestID", NULL); /* This is the name of a user, not an ID. */
      dataset    = drms_getkey_string(export_log, "DataSet", NULL);
      process    = drms_getkey_string(export_log, "Processing", NULL);
      protocol   = drms_getkey_string(export_log, "Protocol", NULL);
      filenamefmt= drms_getkey_string(export_log, "FilenameFmt", NULL);
      method     = drms_getkey_string(export_log, "Method", NULL);
      format     = drms_getkey_string(export_log, "Format", NULL);
      reqtime    = drms_getkey_time(export_log, "ReqTime", NULL);
      esttime    = drms_getkey_time(export_log, "EstTime", NULL); // Crude guess for now
      size       = drms_getkey_longlong(export_log, "Size", NULL);
      requestorid = drms_getkey_int(export_log, "Requestor", NULL); /* This is an ID, despite the name "Requestor". It is the
                                                                     * recnum of the requestor's record in jsoc.export_user .
                                                                     * When jsoc_fetch created the export request, it added a record
                                                                     * to jsoc.export_user (even if the user already existed in
                                                                     * the table). The recnum of that record was stored in
                                                                     * jsoc.export_new in the Requestor column. (export_log is
                                                                     * a record in jsoc.export_new). This recnum links
                                                                     * the requestor's record in jsoc.export_user with
                                                                     * the request record in jsoc.export_new.
                                                                     *
                                                                     * Ideally, the word linking the two tables should be
                                                                     * the requestor's name, not the record number. But the Requestor
                                                                     * field is an int, so I think we just punt on this.
                                                                     *
                                                                     * PHS - No, actually by design the name of the requester is
                                                                     * not to be visible on the open web, and jsoc.export_user is
                                                                     * not readable by apache so the method chosen properly hides
                                                                     * the requester name.  so people can export data without others                                                                     * watching what they are doing. The name could have been better.
                                                                     */

        if (strncmp(dataset, "sunums=", 7) == 0)
        {
          doSuExport = 1;
        }
        else
        {
          doSuExport = 0;
        }

        printf("New Request #%d/%d: %s, Status=%d, Processing=%s, DataSet=%s, Protocol=%s, Method=%s\n", irec, exports_new->n, requestid, status, process, dataset, protocol, method);
        fflush(stdout);


        /* open log file for writing */
        snprintf(logfile, sizeof(logfile), "%s/%s.emlog", logdir, requestid);
        emLogFH = OpenWriteLog(logfile);
        WriteLog(emLogFH, "request ID is %s", requestid);

        RegisterIntVar("requestid", 's', requestid);

      // Get user notification email address
      sprintf(requestorquery, "%s[:#%ld]", EXPORT_USER, requestorid); // requestorid is recnum in jsoc.export_user
      requestor_rs = drms_open_records(drms_env, requestorquery, &status);
      if (!requestor_rs)
      {
          return DBNEWCOMM(&exports_new, &export_log, irec, "JSOC error, Can't find requestor info series", 4);
          // When jsoc_export_manage runs again, it will NOT try to process this export record again.
          // Cannot get here.
      }

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
      {
          return DBNEWCOMM(&exports_new, &export_log, irec, "Cant create export control record", 4);
          // When jsoc_export_manage runs again, it will try to process this export record again.
          // Cannot get here.
      }

          // export_log/exports_new is jsoc.export_new.
          // export_rec is jsoc.export.

      drms_setkey_string(export_rec, "RequestID", requestid); /* The is the ID of the request. */
      drms_setkey_string(export_rec, "DataSet", dataset);
      drms_setkey_string(export_rec, "Processing", process);
      drms_setkey_string(export_rec, "Protocol", protocol);
      drms_setkey_string(export_rec, "FilenameFmt", filenamefmt);
      drms_setkey_string(export_rec, "Method", method);
      drms_setkey_string(export_rec, "Format", format);
      drms_setkey_time(export_rec, "ReqTime", reqtime);
      drms_setkey_time(export_rec, "EstTime", esttime); // Crude guess for now
      drms_setkey_longlong(export_rec, "Size", size);
      drms_setkey_int(export_rec, "Requestor", requestorid); /* This is the name of the requestor. */

        WriteLog(emLogFH, "dataset: %s", dataset);
        WriteLog(emLogFH, "processing: %s", process);
        WriteLog(emLogFH, "protocol: %s", protocol);
        WriteLog(emLogFH, "filenamefmt: %s", filenamefmt);
        WriteLog(emLogFH, "method: %s", method);
        WriteLog(emLogFH, "format: %s", format);


      // check  security risk dataset spec or processing request
        if (isbadDataSet() || isbadProcessing())
        {
            snprintf(msgbuf,
                     sizeof(msgbuf),
                     "Illegal format detected - security risk!\nRequestID= %s\n Processing = %s\n, DataSet=%s",
                     requestid,
                     process,
                     dataset);

            ErrorOutExpRec(&export_rec, 4, msgbuf);
            ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
            continue;
        }


        /*
         * MOVED THIS BLOCK OF RECORD-PROCESSING CODE ON 6/26/2015 SO THAT THE CODE THAT CHECKS
         * FOR VALID COMMA-SEPARATED RECORD-SET SUBSETS KNOWS WHETHER OR NOT THE EXPORT REQUEST
         * CONTAINS RECORD-PROCESSING STEPS.
         *
         * --ART
         */

         /* SHELL VAR registration must happen before calling ParseFields(). */
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
        proccmds = ParseFields(drms_env, procser, dbmainhost, process, dataset, requestid, &RecordLimit, emLogFH, &ppstat);
        if (ppstat == 0)
        {
            snprintf(msgbuf, sizeof(msgbuf), "Invalid process field value: %s.", process);
            ErrorOutExpRec(&export_rec, 4, msgbuf);
            ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);

            /* mem leak - need to free all strings obtained with drms_getkey_string(). */
            continue; /* next export record */
        }

        /* Reject request if dataset contains a list of comma-separated record-set specifications.
        * Currently, the code supports only a single record-set specification. */
        /* this should be fixed sometime */
        if (!doSuExport)
        {
            if (ParseRecSetSpec(dataset, &snames, &filts, &nsets, &info))
            {
                /* Can't parse, set this record's status to a failure status. */
                /* export_log is the jsoc.export_new record. */
                snprintf(msgbuf, sizeof(msgbuf), "Unable to parse the export record specification '%s'.", dataset);
                ErrorOutExpRec(&export_rec, 4, msgbuf);
                ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                continue;
            }
            else if (nsets > 1)
            {
                /* The export system supports record-sets with subsets in two circumstances:
                 *   - if there is no processing to be done to the data images to be returned.
                 *   - the data image to be processed are all from the same series.
                 * In the second case, the record-set with subsets must be converted to a
                 * record-set that has one subset (i.e., not multiple subsets). This is
                 * necessary to keep the record-set specification processing code simple. It
                 * does not handle commas in the record-set specification. Instead of modifying
                 * it to handle commas, complicating and already complicated piece of code,
                 * we simply provide a record-set specification that has no commas.
                 */

                if (proccmds && list_llgetnitems(proccmds) != 0)
                {
                    int iseries;
                    const char *setname = NULL;
                    const char *sname = NULL;
                    int notSupported = 0;
                    char dbHostAndPort[128];
                    int makeNewEnv;
                    const char *dbuser = NULL;
                    const char *dbpasswd = NULL;
                    DRMS_RecordSet_t *rs = NULL;
                    char *converted = NULL;
                    size_t szConverted;
                    DRMS_Record_t *record = NULL;
                    char recnumStr[64];
                    int iRecConverted; /* record index of subset in record-sets with multiple sub-sets. */

                    setname = snames[0];

                    /* Iterate through series names. If not all subset series names are identical,
                     * then reject the request. */
                    for (iseries = 1; iseries < nsets; iseries++)
                    {
                        sname = snames[iseries];

                        if (strcasecmp(sname, setname) != 0)
                        {
                            /* An attempt to process records from different series. */
                            notSupported = 1;
                            break;
                        }
                    }

                    if (notSupported)
                    {
                        snprintf(msgbuf, sizeof(msgbuf), "The export system does not currently support comma-separated lists of record-set specifications.");
                        ErrorOutExpRec(&export_rec, 4, msgbuf);
                        ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                        FreeRecSpecParts(&snames, &filts, nsets);
                        continue; /* onto next export request. */
                    }

                    /* Convert the record-set specification from a set of subsets into a single set described
                     * by a list of records in a series. drms_env is not the correct environment if
                     * jsoc_export_manage is running on hmidb2. In that case, we have to open a new
                     * environment so we can call drms_open_recordswithkeys().
                     */
                    if (!seriesEnv)
                    {
#ifdef DRMS_CLIENT
                        /* For a sock module, the db host to which it has access is the db host that the
                         * serving drms_server is connected to. And I don't think there is a way to
                         * determine to which db host drms_server is connected, so we'll HAVE TO
                         * connect to dbhost here, regardless of the existing connection between
                         * drms_server and a db.
                         */
                        struct passwd *pwd = NULL;

                        makeNewEnv = 1;

                        /* Ack - the dbuser is not saved by the sock_module driver. Ack. If this module was called with JSOC_DBUSER, this information
                         * was lost. Hopefully jsoc_export_manage_sock will never be used. */
                        pwd = getpwuid(geteuid());
                        dbuser = pwd->pw_name;

#else
                        /* For a server module, the name of the db host connected to is in drms_env->session->db_handle->dbhost. */
                        makeNewEnv = (strcasecmp(dbmainhost, drms_env->session->db_handle->dbhost) != 0);
                        dbuser = drms_env->session->db_handle->dbuser;
#endif
                        if (makeNewEnv)
                        {
                            snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s:%s", dbmainhost, DRMSPGPORT);
                            if ((seriesEnv = drms_open(dbHostAndPort, dbuser, NULL, DBNAME, NULL)) == NULL)
                            {
                                snprintf(msgbuf, sizeof(msgbuf), "Cannot access database containing series information.");
                                ErrorOutExpRec(&export_rec, 4, msgbuf);
                                ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                                FreeRecSpecParts(&snames, &filts, nsets);
                                continue;
                            }

                            seriesEnv->logfile_prefix = module_name;
                            drms_server_begin_transaction(seriesEnv);
                        }
                        else
                        {
                            seriesEnv = drms_env;
                        }
                    }

                    rs = drms_open_recordswithkeys(seriesEnv, dataset, "", &status);
                    if (!rs || status != DRMS_SUCCESS)
                    {
                        snprintf(msgbuf, sizeof(msgbuf), "Cannot open record-set %s.\n", dataset);
                        ErrorOutExpRec(&export_rec, 4, msgbuf);
                        ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                        FreeRecSpecParts(&snames, &filts, nsets);
                        continue;
                    }

                    if (rs->n == 0)
                    {
                        /* No records to process. Normally, jsoc_export_as_fits or whatever would see that there were no records, and it would return a non-zero
                         * code. Then the drms_run script would return the non-zero code. Then the qsub script would see the non-zero code, and it would
                         * write it to the jsoc.export series. But if we continue here, we never run jsoc_export_as_fits. Instead, write an error status of
                         * 4 to both jsoc.export_new and jsoc.export. */
                        snprintf(msgbuf, sizeof(msgbuf), "Record-set %s contains no records.\n", dataset);
                        ErrorOutExpRec(&export_rec, 4, msgbuf);
                        ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                        FreeRecSpecParts(&snames, &filts, nsets);
                        continue;
                    }

                    /* Create a record-set query in this form:
                     *
                     *    hmi.sharp_cea_720s[:#2405451,#4278371,#4237489]
                     */

                    szConverted = 256;
                    converted = calloc(szConverted, sizeof(char));

                    if (!converted)
                    {
                        snprintf(msgbuf, sizeof(msgbuf), "Out of memory.\n");
                        ErrorOutExpRec(&export_rec, 4, msgbuf);
                        ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                        FreeRecSpecParts(&snames, &filts, nsets);
                        return 1;
                    }

                    converted = base_strcatalloc(converted, setname, &szConverted);
                    converted = base_strcatalloc(converted, "[", &szConverted);

                    for (iRecConverted = 0; iRecConverted < rs->n; iRecConverted++)
                    {
                        record = rs->records[iRecConverted];
                        snprintf(recnumStr, sizeof(recnumStr), "%lld", record->recnum);

                        if (iRecConverted == 0)
                        {
                            converted = base_strcatalloc(converted, ":#" , &szConverted);
                        }
                        else
                        {
                            converted = base_strcatalloc(converted, ",#" , &szConverted);

                        }

                        converted = base_strcatalloc(converted, recnumStr, &szConverted);
                    }

                    converted = base_strcatalloc(converted, "]", &szConverted);

                    /* Because we converted the comma-separated list of record-specification subsets into a list of recnums, we need to
                     * assign the new record-specification into the dataset variable, and then we need to re-parse the processing steps. */
                    if (dataset)
                    {
                        /* The was allocated on the stack. */
                        free(dataset);
                    }

                    dataset = converted;

                    ppstat = 0;
                    list_llfree(&proccmds);
                    proccmds = ParseFields(drms_env, procser, dbmainhost, process, dataset, requestid, &RecordLimit, emLogFH, &ppstat);
                }
            }

            FreeRecSpecParts(&snames, &filts, nsets);
        }

        drms_record_directory(export_rec, reqdir, 1);

      // Insert qsub command to execute processing script into SU
      make_qsub_call(requestid, reqdir, notify, dbname, dbuser, dbids, dbexporthost, submitcode);

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
      fprintf(fp, "set_info_sock -C JSOC_DBHOST=%s ds='jsoc.export[%s]' Status=1\n", dbexporthost, requestid);
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
      fprintf(fp, "echo PATH = $PATH\n");
        fprintf(fp, "echo path = $path\n");

      // Now generate specific processing related commands


      /* PRE-PROCESSING */

      /* For now, this does nothing. Since jsoc_export_manage has no knowledge of specific
       * processing steps, this function cannot assess whether the sequence is appropriate.
       * If we want to enforce a proper sequence, we'll have to put that information in
       * the processing-series table. */
      if (IsBadProcSequence(proccmds))
      {
          list_llfree(&proccmds);
          snprintf(msgbuf, sizeof(msgbuf), "Bad sequence of processing steps, skipping recnum %lld.", export_rec->recnum);
          ErrorOutExpRec(&export_rec, 4, msgbuf);
          ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
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

      /* Create a file to hold the processing steps applied. */
      char fname[PATH_MAX];
      char *anArg = NULL;
      FILE *fpProc = NULL;

      if (proccmds && list_llgetnitems(proccmds) > 0)
      {
          snprintf(fname, sizeof(fname), "%s/proc-steps.txt", reqdir);
          fpProc = fopen(fname, "w");

          if (!fpProc)
          {
                list_llfree(&proccmds);
                snprintf(msgbuf, sizeof(msgbuf), "Unable to open file %s.", fname);
                ErrorOutExpRec(&export_rec, 4, msgbuf);
                ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
                fclose(fp);
                fp = NULL;

                /* mem leak - need to free all strings obtained with drms_getkey_string(). */
                continue; /* next export record */
          }
      }

      LinkedList_t *datasetkwlist = NULL;
      char *rsstr = NULL;
      int firstnode = 1;
      char *lhs = NULL;
      char *rhs = NULL;
      char *argsDup = NULL;

      while (!quit && (node = list_llnext(proccmds)) != NULL)
      {
        ndata = (ProcStep_t *)node->data;

        /* Write processing step info into the proc-steps.txt file. */
        fprintf(fpProc, "\nProcessing-step applied: %s\n", ndata->name);
        fprintf(fpProc, "  argument\t\tvalue\n");
        fprintf(fpProc, "  --------\t\t-----\n");

        /* Parse command line. ACK! strtok modifies the string it parses!! Copy it first. */
        argsDup = strdup(ndata->args);
        if (!argsDup)
        {
            snprintf(msgbuf, sizeof(msgbuf), "Out of memory .");
            quit = 1; /* next export record */
            break;
        }

        for (anArg = strtok(argsDup, " ,"); !quit && anArg; anArg = strtok(NULL, " ,"))
        {
            /* For each arg that has a value, the argument name is separated from the value by an equal sign.
             * I hope there are no commas in the argument values.
             */
            lhs = strdup(anArg);
            if (!lhs)
            {
                snprintf(msgbuf, sizeof(msgbuf), "Out of memory .");
                quit = 1; /* next export record */
                break;
            }

            rhs = strchr(lhs, '=');
            if (rhs)
            {
                *rhs = '\0';
                rhs++;
                fprintf(fpProc, "  %s\t\t%s\n", lhs, rhs);
            }
            else
            {
                fprintf(fpProc, "  %s\n", lhs);
            }

            free(lhs);
            lhs = NULL;
        }

        if (quit)
        {
            break;
        }

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
          /* Need to check input series of the first node only of the processing-step pipeline */

          /* ART - env is not necessarily the correct environment for talking
           * to the database about DRMS objects (like records, keywords, etc.).
           * It is connected to the db on dbexporthost. dbmainhost is the host
           * of the correct jsoc database. This function will ensure that
           * it talks to dbmainhost. */
          if (!doSuExport)
          {
              if (ParseRecSetSpec(cdataset, &snames, &filts, &nsets, &info))
              {
                  snprintf(msgbuf, sizeof(msgbuf), "Invalid input series record-set query %s.", cdataset);
                  quit = 1;
                  break;
              }
          }

          /* If the record-set query is an @file or contains multiple sub-record-set queries. We currently
           * support exports from a single series. */
          if (firstnode)
          {
              for (iset = 0, *csname = '\0'; iset < nsets; iset++)
              {
                  series = snames[iset];
                  if (*csname != '\0')
                  {
                      if (strcmp(series, csname) != 0)
                      {
                          snprintf(msgbuf, sizeof(msgbuf), "jsoc_export_manage FAILURE: attempt to export a recordset containing multiple input series.");
                          quit = 1;
                          break;
                      }
                  }
                  else
                  {
                      snprintf(csname, sizeof(csname), "%s", series);
                  }
              } // end series-name loop
          }
          else if (nsets > 0)
          {
              snprintf(csname, sizeof(csname), "%s", snames[0]);
          }
          else
          {
              snprintf(msgbuf, sizeof(msgbuf), "No input series.\n");
              quit = 1;
          }

          FreeRecSpecParts(&snames, &filts, nsets);

          if (quit)
          {
              break;
          }

          snprintf(seriesin, sizeof(seriesin), "%s", csname);

          if (firstnode)
          {
              firstnode = 0;
              if (!SeriesExists(drms_env, seriesin, dbmainhost, &status) || status)
              {
                  snprintf(msgbuf, sizeof(msgbuf), "Input series %s does not exist.", csname);
                  quit = 1;
                  break;
              }
          }

         /* Ensure that only a single output series is being written to; ensure that the output series exists. */

          /* ART - env is not necessarily the correct environment for talking
           * to the database about DRMS objects (like records, keywords, etc.).
           * It is connected to the db on dbexporthost. dbmainhost is the host
           * of the correct jsoc database. This function will ensure that
           * it talks to dbmainhost. */
          if (!doSuExport)
          {
              if (ParseRecSetSpec(datasetout, &snames, &filts, &nsets, &info))
              {
                  snprintf(msgbuf, sizeof(msgbuf), "Invalid output series record-set query %s.", datasetout);
                  quit = 1;
                  break;
              }
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
                      snprintf(msgbuf, sizeof(msgbuf), "jsoc_export_manage FAILURE: attempt to export a recordset to multiple output series.");
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

          if (quit)
          {
              break;
          }

          snprintf(seriesout, sizeof(seriesout), "%s", csname);

          /* Some processing steps will write to series that contain 'intermediate results', whose names generally
           * end in the suffix '_mod'. The drms-run script will call jsoc_export_clone immediately before the
           * processing module runs. This will result in the creation of the intermediate series if it does not
           * already exist. If the series already does exist, then the call to jsoc_export_clone will be a no-op.
           * Processing steps that write to intermediate series have the crout processing step flag set. If this flag
           * is not set, then we need to check for the previous existence of the output series. */
          if (!ndata->crout)
          {
              if (!SeriesExists(drms_env, seriesout, dbmainhost, &status) || status)
              {
                  snprintf(msgbuf, sizeof(msgbuf), "Output series %s does not exist.", csname);
                  quit = 1;
                  break;
              }
          }
          else
          {
              /* Call jsoc_export_clone. This is really a pre-processing command, so use GenPreProcessCmd(). */
              char cloneargs[512];

              snprintf(cloneargs, sizeof(cloneargs), "dsin=%s dsout=%s", seriesin, seriesout);

              procerr = GenPreProcessCmd(fp,
                                         "jsoc_export_clone",
                                         cloneargs,
                                         dbmainhost,
                                         dbids);
          }

          if (!procerr)
          {
              progpath = ((ProcStep_t *)node->data)->path;
              args = ((ProcStep_t *)node->data)->args;
          }

          procerr = GenPreProcessCmd(fp,
                                     progpath,
                                     args,
                                     dbmainhost,
                                     dbids);

         if (procerr)
         {
             snprintf(msgbuf, sizeof(msgbuf), "Problem running processing command '%s'.", progpath);
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

         if (fpProc)
         {
            fclose(fpProc);
            fpProc = NULL;
         }
      }

      if (proccmds)
      {
          list_llfree(&proccmds);
          proccmds = NULL;
      }

      if (quit)
      {
          ErrorOutExpRec(&export_rec, 4, msgbuf);
          ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
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
                snprintf(msgbuf, sizeof(msgbuf), "Problem obtaining a record-set specification string.");
                quit = 1;
                break;
            }
         }

          if (quit)
          {
              ErrorOutExpRec(&export_rec, 4, msgbuf);
              ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
              fclose(fp);
              fp = NULL;

              /* mem leak - need to free all strings obtained with drms_getkey_string(). */
              continue; /* next export record */
          }

         drms_setkey_string(export_rec, "DataSet", datasetkw);
         free(datasetkw);
         datasetkw = NULL;

         list_llfree(&datasetkwlist);
      }

      /* PROTOCOL-SPECIFIC EXPORT */
      /* Use the dataset output by the last processing step (if processing was performed). */



      procerr = GenProtoExpCmd(fp,
                               &export_rec,
                               &export_log,
                               exports_new,
                               irec,
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
          snprintf(msgbuf, sizeof(msgbuf), "Problem running protocol-export command.");
          ErrorOutExpRec(&export_rec, 4, msgbuf);
          ErrorOutExpNewRec(exports_new, &export_log, irec, 4, msgbuf);
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
            /* $REQDIR is actually the slot dir, not the SU dir. The current directory is the slot dir.
             */

            /* Create the tar file in the parent SU directory (you cannot put it in the current directory if you use '.' as the
             * source directory. tar will attempt to put the tar file in the tar file otherwise, with undefined results).
             */
            fprintf(fp, "tar  chf ../%s.tar ./\n", requestid);
            fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
            /* Delete all files, except for the manifest files and the scripts, in the current directory (the slot dir). */
            fprintf(fp, "find . -not -path . -not -name '%s.*' -not -name 'index.*' -print0 | xargs -0 -L 32 rm -rf\n", requestid);
            fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");
            /* Move the tar file from the parent SU dir to the current directory (the slot dir). */
            fprintf(fp, "mv ../%s.tar .\n", requestid);
            fprintf(fp, "set RUNSTAT = $status\nif ($RUNSTAT) goto EXITPLACE\n");

            /* The current directory (the slot dir) now contains the tar file, the manifest files, and the export scripts.
             * The tar file also contains the manifest files and the export scripts. */
        }

      // DONE, Standard exit here only if no errors above
      // set status=done and mark this version of the export record permanent
      fprintf(fp, "set DoneTime = `date -u '+%%Y.%%m.%%d_%%H:%%M:%%S_UT'`\n");
      fprintf(fp, "set_info_sock JSOC_DBHOST=%s ds='jsoc.export[%s]' Status=0 ExpTime=$DoneTime\n", dbexporthost, requestid);
      // copy the drms_run log file
      fprintf(fp , "# NOTE - this is not the final runlog; the qsub script will continue to write to it in\n");
      fprintf(fp , "# /home/jsoc/exports/tmp; at the end of the qsub script, the runlog gets MOVED to\n");
      fprintf(fp , "# /home/jsoc/exports/tmp/done\n");
      fprintf(fp, "cp /home/jsoc/exports/tmp/%s.runlog ./%s.runlog \n", requestid, requestid);

        /* move the export manager run log to the export su dir */
        fprintf(fp, "if ( -f %s ) then\n", logfile);
        fprintf(fp, "  mv %s ./%s.emlog \n", logfile, requestid);
        fprintf(fp, "endif\n");

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

      // SU now contains both qsub script and drms_run script, ready to execute and lock the record.
      snprintf(command, sizeof(command), "source %s; qsub -q exp.q -v %s -o /home/jsoc/exports/tmp/%s.runlog -e /home/jsoc/exports/tmp/%s.runlog %s/%s.qsub", qsubInitScript, jsocrootstr, requestid, requestid, reqdir, requestid);

      printf(command);
      printf("\n");
  /*
  	"  >>& /home/jsoc/exports/tmp/%s.runlog",
  */
// fprintf(stderr,"export_manage for %s, qsub=%s\n",requestid, command);
      /* close the write log - at the end of the drmsrun script, it will be moved into the export SU dir; cannot do this
       * mv in qsub for some unknown reason
       */
      CloseWriteLog(emLogFH);

      if (system(command))
      {
          return DBCOMM(&export_rec, "Submission of qsub command failed", 4);
          // Cannot get here.
          // When jsoc_export_manage runs again, it will try to process this export record again.
          // The export record in jsoc.export_new will still have a status value of 2 or 12.
      }

      // OK to close the jsoc.export record now that the qsub command succeeded.

      drms_setkey_int(export_rec, "Status", 1);
      drms_close_record(export_rec, DRMS_INSERT_RECORD); // jsoc.export

      // Mark the record in jsoc.export_new as accepted for processing.

      // The qsub script waits for the jsoc.export_new record to be written with status == 1 before it continues.
      // This is important since the qsub script is launched asynchronously and could attempt
      // to read the jsoc.export record before it exists (i.e., before jsoc_export_manage ends its
      // db transaction).
      drms_setkey_int(export_log, "Status", 1); // jsoc.export_new
      drms_close_record(export_log, DRMS_INSERT_RECORD);
      exports_new->records[irec] = NULL; // Detach from record-set; drms_close_records() will free all records
                                         // that never got inserted into jsoc.export_new.
      printf("Request %s submitted\n",requestid);

      /* mem leak - need to free all strings obtained with drms_getkey_string(). */
      } // end looping on new export requests (looping over records in jsoc.export_new)


    if (seriesEnv != NULL && seriesEnv != drms_env)
    {
        /* This rolls-back the transaction needed in the case that the dbmainhost was not the host that jsoc_export_manage connected to at start.
         * We only made this second environment if we needed to call a DRMS API function that required an environment and that environment
         * was different from the one that this module created on start-up. */
        drms_server_end_transaction(seriesEnv, 1, 1);
    }


    // Free all jsoc.export_new records that never got inserted (the remainder had failures).
    drms_close_records(exports_new, DRMS_FREE_RECORD); // jsoc.export_new

    return(0);
    } // End process new requests.
  else if (strcmp(op, "SOMETHINGELSE") == 0)
    {
    }
  else
    DIE("Operation not allowed");
  return(1);
  }
