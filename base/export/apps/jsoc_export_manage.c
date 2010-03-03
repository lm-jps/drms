// #define DEBUG 1
#define DEBUG 0

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

#define DIE(msg) { fprintf(stderr,"XXXX jsoc_exports_manager failure: %s\nstatus=%d",msg,status); exit(1); }

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "process", "<Operation>"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_END}
};

char *module_name = "jsoc_export_manage";

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
               const char *dbuser, const char *dbids)
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

  fprintf(fp, "while (`show_info -q 'jsoc.export_new[%s]' key=Status %s` == 2)\n", requestid, dbids);
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
  fprintf(fp, "drms_run %s/%s.drmsrun >>& /home/jsoc/exports/tmp/%s.runlog \n", reqdir, requestid, requestid);
  fprintf(fp, "set DRMS_ERROR=$status\n");
fprintf(fp, "set NewRecnum=`cat /home/jsoc/exports/tmp/%s.recnum` \n", requestid);
fprintf(fp, "while (`show_info -q -r 'jsoc.export[%s]' %s` < $NewRecnum)\n", requestid, dbids);
fprintf(fp, "  echo waiting for jsocdb drms_run commit >> /home/jsoc/exports/tmp/%s.runlog \n",requestid);
fprintf(fp, "  sleep 1\nend \n");
  if (requestorid)
     fprintf(fp, "set Notify=`show_info -q 'jsoc.export_user[? RequestorID=%d ?]' key=Notify %s` \n", requestorid, dbids);
  fprintf(fp, "set REQDIR = `show_info -q -p 'jsoc.export[%s]' %s`\n", requestid, dbids);
  fprintf(fp, "if ($DRMS_ERROR) then\n");
  fprintf(fp, "  set_keys -C ds='jsoc.export[%s]' Status=4 %s\n", requestid, dbids);
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
  fprintf(fp, "  rm -f /home/jsoc/exports/tmp/%s.recnum\n", requestid);
  /* The log after the call to drms_run gets lost because of the following line - should preserve this
   * somehow (but it can't go in the new inner REQDIR because that is read-only after drms_run returns. */
  fprintf(fp, "  rm -f /home/jsoc/exports/tmp/%s.runlog\n", requestid);
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

  const char *dbname = NULL;
  const char *dbuser = NULL;
  const char *jsocroot = NULL;
  char dbids[128];
  char jsocrootstr[128] = {0};
  int pc = 0;

  if (nice_intro ()) return (0);

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

  /*  op == process  */
  if (strcmp(op,"process") == 0) 
    {
    int irec;
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
      make_qsub_call(requestid, reqdir, (notify ? requestorid : 0), dbname, dbuser, dbids);
  
      // Insert export processing drms_run script into export record SU
      // The script components must clone the export record with COPY_SEGMENTS in the first usage
      // and with SHARE_SEGMENTS in subsequent modules in the script.  All but the last module
      // in the script may clone as DRMS_TRANSIENT.
      // Remember all modules in this script mut be _sock modules.

      // First, prepare initial part of script, same for all processing.
      sprintf(runscript, "%s/%s.drmsrun", reqdir, requestid);
      fp = fopen(runscript, "w");
      fprintf(fp, "#! /bin/csh -f\n");
      fprintf(fp, "set echo\n");
      // force clone with copy segment. 
      fprintf(fp, "set_keys_sock -C ds='jsoc.export[%s]' Status=1\n", requestid);
      // Get new SU for the export record
      fprintf(fp, "set REQDIR = `show_info_sock -q -p 'jsoc.export[%s]'`\n", requestid);
      // cd to SU in export record
      fprintf(fp, "cd $REQDIR\n");
      // save some diagnostic info
      fprintf(fp, "printenv > %s.env\n", requestid);
      fprintf(fp, "echo $HOSTNAME\n");

      // Now generate specific processing related commands
      if (strcmp(process, "no_op") == 0 && strncasecmp(protocol,"fits",4)==0)
        { // No processing but export as full FITS files
        char *cparms, *p = index(protocol, ',');
        if (p)
          {
          *p = '\0';
          cparms = p+1;
          }
        else
          cparms = "**NONE**";
        fprintf(fp, "jsoc_export_as_fits_sock reqid='%s' expversion=%s rsquery='%s' path=$REQDIR ffmt='%s' method='%s' protocol='%s' cparms='%s' %s\n",
          requestid, PACKLIST_VER, dataset, filenamefmt, method, protocol, cparms,  dbids);
        }
      else if (strcmp(process, "no_op") == 0 || strcmp(process,"Not Specified")==0) 
        { // export of as-is records that need staging, get paths to export files with list in index.txt
        fprintf(fp, "jsoc_export_as_is_sock ds='%s' requestid='%s' method='%s' protocol='%s' filenamefmt='%s'\n", dataset, requestid, method, protocol, filenamefmt); 
        }
      else if (strcmp(process, "hg_patch") == 0)
        {
        // Heliographic Patches tracked requested.  Delete the message below when implemented.
        fprintf(stderr,"XX jsoc_export_manage FAIL NOT implemented yet, requestid=%s, process=%s, protocol=%s, method=%s\n",
          requestid, process, protocol, method);
        drms_setkey_int(export_log, "Status", 4);
        }
      else if (strcmp(process, "su_export") == 0 && strcmp(protocol,"as-is")==0)
        { // Use special program for Storage Unit exports.
        fprintf(fp, "jsoc_export_SU_as_is_sock ds='%s' requestid=%s\n", dataset, requestid); 
        }
      else
        { // Unrecognized processing request
        fprintf(stderr,"XX jsoc_export_manage FAIL Do not know what to do, requestid=%s, process=%s, protocol=%s, method=%s\n",
          requestid, process, protocol, method);
        drms_setkey_int(export_log, "Status", 4);
        drms_close_record(export_rec, DRMS_FREE_RECORD);
        continue;
        }

      // Finally, add tail of script used for all processing types.
      fprintf(fp, "set RUNSTAT = $status\n");
      // convert index.txt list into index.json and index.html packing list files. 
      fprintf(fp, "if ($RUNSTAT == 0) then\n");
      fprintf(fp, "  jsoc_export_make_index\n");
      fprintf(fp, "  set RUNSTAT = $status\n");
      fprintf(fp, "endif\n");
      // create tar file if '-tar' suffix on method
      dashp = rindex(method, '-');
      if (dashp && strcmp(dashp, "-tar") == 0)
        {
        fprintf(fp, "if ($RUNSTAT == 0) then\n");
        fprintf(fp, "  tar chf %s.tar *\n", requestid);
        fprintf(fp, "  set RUNSTAT = $status\n");
        fprintf(fp, "endif\n");
        }
      // set status=done and mark this version of the export record permanent
      fprintf(fp, "if ($RUNSTAT == 0) then\n");
      fprintf(fp, "  set_keys_sock ds='jsoc.export[%s]' Status=0\n", requestid);
      // copy the drms_run log file
      fprintf(fp, "  cp /home/jsoc/exports/tmp/%s.runlog ./%s.runlog \n", requestid, requestid);
      // make drms_run completion lock file (always do this)
      fprintf(fp, "  show_info_sock -q -r 'jsoc.export[%s]' > /home/jsoc/exports/tmp/%s.recnum \n", requestid, requestid);
      fprintf(fp, "else\n");
      // make drms_run completion lock file (always do this) - drms_server died, so don't use sock version here
      fprintf(fp, "  show_info -q -r 'jsoc.export[%s]' > /home/jsoc/exports/tmp/%s.recnum \n", requestid, requestid);
      fprintf(fp, "endif\n");
      fprintf(fp, "exit $RUNSTAT\n");
      fclose(fp);
      chmod(runscript, 0555);
  
      // close the current (first) version of the record in jsoc.export
      drms_setkey_int(export_rec, "Status", 1);
      drms_close_record(export_rec, DRMS_INSERT_RECORD);
  
      // SU now contains both qsub script and drms_run script, ready to execute and lock the record.
      sprintf(command,"qsub -q x.q,o.q,j.q -v %s "
  	" -o /home/jsoc/exports/tmp/%s.runlog "
  	" -e /home/jsoc/exports/tmp/%s.runlog "
  	"  %s/%s.qsub ",
        jsocrootstr, requestid, requestid, reqdir, requestid);
  /*
  	"  >>& /home/jsoc/exports/tmp/%s.runlog",
  */
      if (system(command))
        DIE("Submission of qsub command failed");
  
      // Mark the record in jsoc.export_new as accepted for processing.
      drms_setkey_int(export_log, "Status", 1);
      printf("Request %s submitted\n",requestid);
      } // end looping on new export requests

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

