#define DEBUG 0

/*
 *  jsoc_fetch - cgi-bin program to recieve jsoc export and export status requests
 *
 */
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "json.h"
#include "printk.h"
#include "qDecoder.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define MAX_EXPORT_SIZE 100000  // 100GB

#define kExportSeries "jsoc.export"
#define kExportSeriesNew "jsoc.export_new"
#define kExportUser "jsoc.export_user"

#define kArgOp		"op"
#define kArgRequestid	"requestid"
#define kArgDs		"ds"
#define kArgSunum	"sunum"
#define kArgSeg		"seg"
#define kArgProcess	"process"
#define kArgFormat	"format"
#define kArgFormatvar	"formatvar"
#define kArgMethod	"method"
#define kArgProtocol	"protocol"
#define kArgFilenamefmt	"filenamefmt"
#define kArgRequestor	"requestor"
#define kArgNotify	"notify"
#define kArgShipto	"shipto"
#define kArgRequestorid	"requestorid"
#define kArgFile	"file"

#define kOpSeriesList	"series_list"	// show_series
#define kOpSeriesStruct	"series_struct"	// jsoc_info, series structure, ike show_info -l -s
#define kOpRsSummary	"rs_summary"	// jsoc_info, recordset summary, like show_info -c
#define kOpRsList	"rs_list"	// jsoc_info, recordset list, like show_info key=... seg=... etc.
#define kOpRsImage	"rs_image"	// not used yet
#define kOpExpRequest	"exp_request"	// jsoc_fetch, initiate export request
#define kOpExpRepeat	"exp_repeat"	// jsoc_fetch, initiate export repeat
#define kOpExpStatus	"exp_status"	// jsoc_fetch, get status of pending export
#define kOpExpSu	"exp_su"	// jsoc_fetch, export SUs  from list of sunums
#define kOpExpKinds	"exp_kinds"	// not used yet
#define kOpExpHistory	"exp_history"	// not used yet

#define kOptProtocolAsIs "as-is"	// Protocol option value for no change to fits files

#define kNotSpecified	"Not Specified"

#define kNoAsyncReq     "NOASYNCREQUEST"

int dojson, dotxt, dohtml, doxml;

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, kArgOp, kNotSpecified, "<Operation>"},
  {ARG_STRING, kArgDs, kNotSpecified, "<record_set query>"},
  {ARG_STRING, kArgSeg, kNotSpecified, "<record_set segment list>"},
  {ARG_INTS, kArgSunum, "-1", "<sunum list for SU exports>"},
  {ARG_STRING, kArgRequestid, kNotSpecified, "JSOC export request identifier"},
  {ARG_STRING, kArgProcess, kNotSpecified, "string containing program and arguments"},
  {ARG_STRING, kArgRequestor, kNotSpecified, "name of requestor"},
  {ARG_STRING, kArgNotify, kNotSpecified, "email address of requestor"},
  {ARG_STRING, kArgShipto, kNotSpecified, "mail address of requestor"},
  {ARG_STRING, kArgProtocol, kOptProtocolAsIs, "exported file protocol"},
  {ARG_STRING, kArgFilenamefmt, "{seriesname}.{recnum:%d}.{segment}", "exported file filename format"},
  {ARG_STRING, kArgFormat, "json", "return content type"},
  {ARG_STRING, kArgFormatvar, kNotSpecified, "return json in object format"},
  {ARG_STRING, kArgMethod, "url", "return method"},
  {ARG_STRING, kArgFile, kNotSpecified, "uploaded file contents"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_STRING, "QUERY_STRING", kNotSpecified, "AJAX query from the web"},
  {ARG_END}
};

char *module_name = "jsoc_fetch";

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\njsoc_info {-h} "
        "  details are:\n"
	"op=<command> tell which ajax function to execute\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"seg=<list of segment names to append to dataset spec\n"
	"requestid=JSOC export request identifier\n"
	"process=string containing program and arguments\n"
	"requestor=name of requestor\n"
	"notify=email address of requestor\n"
	"shipto=mail address of requestor\n"
	"protocol=exported file protocol\n"
	"filenamefmt=exported file filename format\n"
	"format=return content type\n"
	"method=return method\n"
	"h=help - show usage\n"
	"QUERY_STRING=AJAX query from the web"
	);
    return(1);
    }
  return (0);
  }

char *json_text_to_string(char *in)
  {
  char *o, *new = (char *)malloc(strlen(in)+1);
  char *i;
  for (i=in, o=new; *i; )
    {
    if (*i == '\\')
      {
      i++;
      if (*i == '/' || *i == '"' || *i == '\\' )
        { *o++ = *i++; continue;}
      else if (*i == 'b')
	{ *o++ = '\b'; i++; continue;}
      else if (*i == 'f')
	{ *o++ = '\f'; i++; continue;}
      else if (*i == 'r')
	{ *o++ = '\r'; i++; continue;}
      else if (*i == 't')
	{ *o++ = '\t'; i++; continue;}
      else if (*i == 'n')
	{ *o++ = '\n'; i++; continue;}
// need to do uXXXX too!
      }
    *o++ = *i++;
    }
  *o = '\0';
  return(new);
  }

char *string_to_json(char *in)
  {
  char *new;
  new = json_escape(in);
  return(new);
  }

void drms_sprint_rec_query(char *text, DRMS_Record_t *rec)
  {
  int iprime, nprime=0;
  char **external_pkeys, *pkey;
  DRMS_Keyword_t *rec_key;
  if (!rec)
    {
    sprintf(text, "** No Record **");
    return;
    }
  strcpy(text,rec->seriesinfo->seriesname);
  external_pkeys =
        drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &nprime, NULL);
  if (external_pkeys && nprime > 0)
    {
    for (iprime = 0; iprime < nprime; iprime++)
      {
      char val[1000];
      pkey = external_pkeys[iprime];
      rec_key = drms_keyword_lookup (rec, pkey, 1);
      drms_keyword_snprintfval(rec_key, val, sizeof(val));
      strcat(text, "[");
      strcat(text, val);
      strcat(text, "]");
      }
    }
  else
    sprintf(text, "[:#%lld]",rec->recnum);
  return;
  }

/* quick export of recordset - on entry it is known that all the records are online
 * so no directory file need be built.  The json structure will have 3 elements
 * added:
 *   size : size in megabytes
 *   rcount : number of records
 *   count : number of files
 *   data  : array of count objects containing
 *         record : record query with segment name suffix
 *         filename : path to segment file
 * set online=0 if known to be online, to 1 if should block until online
 * returns number of files found.
 */

int quick_export_rs( json_t *jroot, DRMS_RecordSet_t *rs, int online,  long long size)
  {
  char numval[200];
  char query[DRMS_MAXQUERYLEN];
  char record[DRMS_MAXQUERYLEN];
  char recpath[DRMS_MAXPATHLEN];
  char segpath[DRMS_MAXPATHLEN];
  int i;
  int count = 0;
  int rcount = rs->n;
  json_t *data;
  DRMS_Record_t *rec;
  data = json_new_array();
  count = 0;
  for (i=0; i < rcount; i++)
    {
    DRMS_Segment_t *seg;
    HIterator_t *hit = NULL;
    rec = rs->records[i];
    drms_sprint_rec_query(query, rec);
    while (seg = drms_record_nextseg(rec, &hit, 1))
      {
      DRMS_Record_t *segrec;
      json_t *recobj = json_new_object();
      char *jsonstr;
      segrec = seg->record;
      count += 1;
      strcpy(record, query);
      strcat(record, "{");
      strcat(record, seg->info->name);
      strcat(record, "}");
      drms_record_directory(segrec, segpath, online); // just to insure SUM_get done
      drms_segment_filename(seg, segpath);
      jsonstr = string_to_json(record);
      json_insert_pair_into_object(recobj, "record", json_new_string(jsonstr));
      free(jsonstr);
      jsonstr = string_to_json(segpath);
      json_insert_pair_into_object(recobj, "filename", json_new_string(jsonstr));
      free(jsonstr);
      json_insert_child(data, recobj);
      }
    free(hit);
    }
  if (jroot) // i.e. if dojson, else will be NULL for the dotxt case.
    {
    sprintf(numval, "%d", rcount);
    json_insert_pair_into_object(jroot, "rcount", json_new_number(numval));
    sprintf(numval, "%d", count);
    json_insert_pair_into_object(jroot, "count", json_new_number(numval));
    sprintf(numval, "%d", (int)size);
    json_insert_pair_into_object(jroot, "size", json_new_number(numval));
    json_insert_pair_into_object(jroot, "dir", json_new_string(""));
    json_insert_pair_into_object(jroot, "data", data);
    }
  else
    {
    json_t *recobj = data->child;
    printf("rcount=%d\n", rcount);
    printf("count=%d\n", count);
    printf("size=%lld\n", size);
    printf("dir=/\n");
    printf("# DATA\n");
    while (recobj)
      {
      char *ascii_query, *ascii_path;
      json_t *record = recobj->child;
      json_t *filename = record->next;
      json_t *recquery = record->child;
      json_t *pathname = filename->child;
      ascii_query = json_text_to_string(recquery->text);
      ascii_path = json_text_to_string(pathname->text);
      printf("%s\t%s\n",ascii_query, ascii_path);
      free(ascii_query);
      free(ascii_path);
      recobj = recobj->next;
      }
    }
  return(count);
  }

TIME timenow()
  {
  TIME UNIX_epoch = -220924792.000; /* 1970.01.01_00:00:00_UTC */
  TIME now = (double)time(NULL) + UNIX_epoch;
  return(now);
  }

static void CleanUp(int64_t **psunumarr, SUM_info_t ***infostructs, char **webarglist,
                    char **series, char **paths, char **sustatus, char **susize, int arrsize)
{
   int iarr;

   if (psunumarr && *psunumarr)
   {
      free(*psunumarr);
      *psunumarr = NULL;
   }

   if (infostructs && *infostructs)
   {
      free(*infostructs);
      *infostructs = NULL;
   }

   if (webarglist && *webarglist)
   {
      free(*webarglist);
      *webarglist = NULL;
   }

   if (series)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if (series[iarr])
         {
            free(series[iarr]);
            series[iarr] = 0;
         }
      }
   }

   if (paths)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if (paths[iarr])
         {
            free(paths[iarr]);
            paths[iarr] = 0;
         }
      }
   }

   if (sustatus)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if (sustatus[iarr])
         {
            free(sustatus[iarr]);
            sustatus[iarr] = 0;
         }
      }
   }

   if (susize)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if (susize[iarr])
         {
            free(susize[iarr]);
            susize[iarr] = 0;
         }
      }
   }
}

/* Can't call these from sub-functions - can only be called from DoIt(). And 
 * calling these from within sub-functions is probably not the desired behavior -
 * I'm thinking that the calls from send_file and SetWebArg are mistakes. The
 * return(1) will NOT cause the DoIt() program to return because the return(1)
 * is called from the sub-function.
 */
#define JSONDIE(msg) {die(dojson,msg,"","4",&sunumarr,&infostructs,&webarglist,series,paths,sustatus,susize,arrsize);return(1);}
#define JSONDIE2(msg,info) {die(dojson,msg,info,"4",&sunumarr,&infostructs,&webarglist,series,paths,sustatus,susize,arrsize);return(1);}
#define JSONDIE3(msg,info) {die(dojson,msg,info,"6",&sunumarr,&infostructs,&webarglist,series,paths,sustatus,susize,arrsize);return(1);}

int fileupload = 0;

int die(int dojson, char *msg, char *info, char *stat, int64_t **psunumarr, SUM_info_t ***infostructs, char **webarglist,
        char **series, char **paths, char **sustatus, char **susize, int arrsize)
  {
  char *msgjson;
  char errval[10];
  char *json;
  char message[10000];
  json_t *jroot = json_new_object();
if (DEBUG) fprintf(stderr,"%s%s\n",msg,info);
  strcpy(message,msg); 
  strcat(message,info); 
  if (dojson)
    {
    msgjson = string_to_json(message);
    json_insert_pair_into_object(jroot, "status", json_new_number(stat));
    json_insert_pair_into_object(jroot, "error", json_new_string(msgjson));
    json_tree_to_string(jroot,&json);
    if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
      printf("Content-type: text/html\n\n");
    else
      printf("Content-type: application/json\n\n");
    printf("%s\n",json);
    }
  else
    {
    printf("Content-type: text/plain\n\n");
    printf("status=%s\nerror=%s\n", stat, message);
    }
  fflush(stdout);

  CleanUp(psunumarr, infostructs, webarglist, series, paths, sustatus, susize, arrsize);

  return(1);
  }

static int send_file(DRMS_Record_t *rec, int segno, char *pathret, int size)
  {
  DRMS_Segment_t *seg = drms_segment_lookupnum(rec, 0);
  char path[DRMS_MAXPATHLEN];
  char sudir[DRMS_MAXPATHLEN];
  FILE *fp;
  int b;

  drms_record_directory(rec,sudir,0);
  strcpy(path, "/web/jsoc/htdocs");
  strncat(path, sudir, DRMS_MAXPATHLEN);
  strncat(path, "/", DRMS_MAXPATHLEN);
  strncat(path, seg->filename, DRMS_MAXPATHLEN);

  if (pathret)
  {
     snprintf(pathret, size, "%s", path);
  }

//fprintf(stderr,"path: %s\n",path);
  fp = fopen(path, "r");
  if (!fp)
    return 1;
  switch (seg->info->protocol)
    {
    case DRMS_FITS:
        printf("Content-Type: application/fits\n\n");	
        break;
    default:
        printf("Content-Type: application/binary\n\n");	
//  Content-Type: application/fits
//Length: 152,640 (149K) [application/fits]

        // look at file extension to guess file type here and print content type line
    }
  while ((b = fgetc(fp))!= EOF)
    fputc(b, stdout);
  fclose(fp);
  fflush(stdout);
  return(0);
  }

// function to check web-provided arguments that will end up on command line
char *illegalArg(char *arg)
  {
  int n_singles = 0;
  char *p;
  if (index(arg, ';'))
     return("';' not allowed");
  for (p=arg; *p; p++)
    if (*p == '\'')
      {
      n_singles++;
      *p = '"';
      }
  if (n_singles & 1)
    return("Unbalanced \"'\" not allowed.");
  return(NULL);
  }

static int SetWebArg(Q_ENTRY *req, const char *key, char **arglist, size_t *size)
   {
   char *value = NULL;
   char buf[1024];
   if (req)
      {
      value = (char *)qEntryGetStr(req, key);
      if (value)
         {
         char *arg_bad = illegalArg(value);
         if (arg_bad)
         {
            /* ART - it appears that the original intent was to exit the DoIt()
             * function here - but it is not possible to do that from a function
             * called by DoIt(). But I've retained the original semantics of 
             * returning back to DoIt() from here. */
            die(dojson, "Illegal text in arg: ", arg_bad, "4", NULL, NULL, arglist, NULL, NULL, NULL, NULL, 0);
            return(1);
         }

         if (!cmdparams_set(&cmdparams, key, value))
         {
            /* ART - it appears that the original intent was to exit the DoIt()
             * function here - but it is not possible to do that from a function
             * called by DoIt(). But I've retained the original semantics of 
             * returning back to DoIt() from here. */
            die(dojson, "CommandLine Error", "", "4", NULL, NULL, arglist, NULL, NULL, NULL, NULL, 0);
            return(1);
         }

         /* ART - keep a copy of the web arguments provided via HTTP POST so that we can 
          * debug issues more easily. */
         snprintf(buf, sizeof(buf), "%s='%s' ", key, value);
         *arglist = base_strcatalloc(*arglist, buf, size);
         }
      }
   return(0);
   }

static int SetWebFileArg(Q_ENTRY *req, const char *key, char **arglist, size_t *size)
   {
   char *value = NULL;
   int len;
   char keyname[100], length[20];
   SetWebArg(req, key, arglist, size);     // get contents of upload file
   sprintf(keyname, "%s.length", key);
   len = qEntryGetInt(req, keyname);
   sprintf(length, "%d", len);
   cmdparams_set(&cmdparams, keyname, length);
   sprintf(keyname, "%s.filename", key);
   SetWebArg(req, keyname, arglist, size);     // get name of upload file
   sprintf(keyname, "%s.contenttype", key);
   SetWebArg(req, keyname, arglist, size);     // get name of upload file
   return(0);
   }

/* Module main function. */
int DoIt(void)
  {
						/* Get command line arguments */
  const char *op;
  const char *in;
  const char *seglist;
  const char *requestid = NULL;
  const char *process;
  const char *requestor;
  int requestorid;
  const char *notify;
  const char *format;
  const char *formatvar;
  const char *shipto;
  const char *method;
  const char *protocol;
  const char *filenamefmt;
  char *errorreply;
  int64_t *sunumarr = NULL; /* array of 64-bit sunums provided in the'sunum=...' argument. */
  int nsunums;
  long long size;
  int rcount = 0;
  TIME reqtime;
  TIME esttime;
  TIME exptime;
  TIME now;
  double waittime;
  char *web_query;
  int from_web,status;
  int dodataobj=1, dojson=1, dotxt=0, dohtml=0, doxml=0;
  DRMS_RecordSet_t *exports;
  DRMS_Record_t *export_log;
  char new_requestid[200];
  char status_query[1000];
  char *export_series; 
  int is_POST = 0;
  FILE *requestid_log = NULL;
  char msgbuf[128];
  SUM_info_t **infostructs = NULL;
  char *webarglist = NULL;
  size_t webarglistsz;

  char *paths[DRMS_MAXQUERYLEN/8] = {0};
  char *series[DRMS_MAXQUERYLEN/8] = {0};
  char *sustatus[DRMS_MAXQUERYLEN/8] = {0};
  char *susize[DRMS_MAXQUERYLEN/8] = {0};
  int arrsize = DRMS_MAXQUERYLEN/8;

  if (nice_intro ()) return (0);

  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, kNotSpecified) != 0;

  if (from_web)
     {
     const char * rmeth = NULL;
     Q_ENTRY *req = NULL;

      /* Use qDecoder to parse HTTP POST requests. qDecoder actually handles 
       * HTTP GET requests as well.
       * See http://www.qdecoder.org
       */

     /* Use the REQUEST_METHOD environment variable as a indicator of 
      * a POST request. */
     rmeth = cmdparams_get_str(&cmdparams, "REQUEST_METHOD", NULL);
     is_POST = strcasecmp(rmeth, "post") == 0;

     webarglistsz = 2048;
     webarglist = (char *)malloc(webarglistsz);
     *webarglist = '\0';

     req = qCgiRequestParseQueries(NULL, NULL);
     if (req)
        {
           /* Accept only known key-value pairs - ignore the rest. */
           SetWebArg(req, kArgOp, &webarglist, &webarglistsz);
           SetWebArg(req, kArgRequestid, &webarglist, &webarglistsz);
           SetWebArg(req, kArgDs, &webarglist, &webarglistsz);
           SetWebArg(req, kArgSunum, &webarglist, &webarglistsz);
           SetWebArg(req, kArgSeg, &webarglist, &webarglistsz);
           SetWebArg(req, kArgProcess, &webarglist, &webarglistsz);
           SetWebArg(req, kArgFormat, &webarglist, &webarglistsz);
           SetWebArg(req, kArgFormatvar, &webarglist, &webarglistsz);
           SetWebArg(req, kArgMethod, &webarglist, &webarglistsz);
           SetWebArg(req, kArgProtocol, &webarglist, &webarglistsz);
           SetWebArg(req, kArgFilenamefmt, &webarglist, &webarglistsz);
           SetWebArg(req, kArgRequestor, &webarglist, &webarglistsz);
           SetWebArg(req, kArgNotify, &webarglist, &webarglistsz);
           SetWebArg(req, kArgShipto, &webarglist, &webarglistsz);
           SetWebArg(req, kArgRequestorid, &webarglist, &webarglistsz);
           if (strncmp(cmdparams_get_str (&cmdparams, kArgDs, NULL),"*file*", 6) == 0);
           SetWebFileArg(req, kArgFile, &webarglist, &webarglistsz);

           qEntryFree(req); 
        }
     }
  free(web_query);
  // From here on, called as cgi-bin same as from command line

  op = cmdparams_get_str (&cmdparams, kArgOp, NULL);
  requestid = cmdparams_get_str (&cmdparams, kArgRequestid, NULL);
  in = cmdparams_get_str (&cmdparams, kArgDs, NULL);

  // HACK alert.  due to use of ARG_INTS which is not processed properly by SetWegArgs
  // the following lines are added.  They can be removed after a new function to replace
  // cmdparams_set as used in SetWebArgs
   {
   int cmdline_parse_array (CmdParams_t *params, const char *root, ModuleArgs_Type_t dtype, const char *valist);
   CmdParams_Arg_t *thisarg = (CmdParams_Arg_t *)hcon_lookup(cmdparams.args, kArgSunum);
   if ((status = cmdline_parse_array (&cmdparams, kArgSunum, ARG_INTS, thisarg->strval)))
     {
     snprintf(msgbuf, sizeof(msgbuf), 
              "Invalid array argument , '%s=%s'.\n", kArgSunum, cmdparams_get_str(&cmdparams, kArgSunum, NULL));
     JSONDIE(msgbuf);
     }
   }
  // end hack
  nsunums = cmdparams_get_int64arr(&cmdparams, kArgSunum, &sunumarr, &status);

  if (status != CMDPARAMS_SUCCESS)
  {
     snprintf(msgbuf, sizeof(msgbuf), 
              "Invalid argument on entry, '%s=%s'.\n", kArgSunum, cmdparams_get_str(&cmdparams, kArgSunum, NULL));
     JSONDIE(msgbuf);
  }

  seglist = cmdparams_get_str (&cmdparams, kArgSeg, NULL);
  process = cmdparams_get_str (&cmdparams, kArgProcess, NULL);
  format = cmdparams_get_str (&cmdparams, kArgFormat, NULL);
  formatvar = cmdparams_get_str (&cmdparams, kArgFormatvar, NULL);
  method = cmdparams_get_str (&cmdparams, kArgMethod, NULL);
  protocol = cmdparams_get_str (&cmdparams, kArgProtocol, NULL);
  filenamefmt = cmdparams_get_str (&cmdparams, kArgFilenamefmt, NULL);
  requestor = cmdparams_get_str (&cmdparams, kArgRequestor, NULL);
  notify = cmdparams_get_str (&cmdparams, kArgNotify, NULL);
  shipto = cmdparams_get_str (&cmdparams, kArgShipto, NULL);
  requestorid = cmdparams_get_int (&cmdparams, kArgRequestorid, NULL);

  dodataobj = strcmp(formatvar, "dataobj") == 0;
  dojson = strcmp(format, "json") == 0;
  dotxt = strcmp(format, "txt") == 0;
  dohtml = strcmp(format, "html") == 0;
  doxml = strcmp(format, "xml") == 0;

// SPECIAL DEBUG LOG HERE XXXXXX
{
FILE *runlog = fopen("/home/jsoc/exports/tmp/fetchlog.txt", "a");
 if (runlog)
 {
    char now[100];
    const char *dbhost;
    int fileupload = strncmp(in, "*file*", 6) == 0;
    dbhost = cmdparams_get_str(&cmdparams, "JSOC_DBHOST", NULL);
    sprint_ut(now,time(0) + UNIX_EPOCH);
    fprintf(runlog,"PID=%d\n   %s\n   op=%s\n   in=%s\n   RequestID=%s\n   DBHOST=%s\n   REMOTE_ADDR=%s\n",
            getpid(), now, op, in, requestid, dbhost, getenv("REMOTE_ADDR"));
    if (fileupload)  // recordset passed as uploaded file
    {
       char *file = (char *)cmdparams_get_str (&cmdparams, kArgFile, NULL);
       int filesize = cmdparams_get_int (&cmdparams, kArgFile".length", NULL);
       char *filename = (char *)cmdparams_get_str (&cmdparams, kArgFile".filename", NULL);
       fprintf(runlog,"   UploadFile: size=%d, name=%s, contents=%s\n",filesize,filename,file);
    }

    /* Now print the web arguments (if jsoc_fetch was invoked via an HTTP POST verb). */
    if (from_web && strlen(webarglist) > 0)
    {
       int rlsize = strlen(webarglist) * 2;
       char *rlbuf = malloc(rlsize);
       memset(rlbuf, 0, rlsize);
       char *pwa = webarglist;
       char *prl = rlbuf;

       /* Before printing the webarglist string, escape '%' chars - otherwise fprintf(), will
        * think you want to replace things like "%lld" with a formated value - this happens
        * even though there is not arg provided after webarglist. */
       while (*pwa && (prl - rlbuf < rlsize - 1))
       {
          if (*pwa == '%')
          {
             *prl = '%';
             prl++;
          }

          *prl = *pwa;
          pwa++;
          prl++;
       }

       *prl = '\0';

       fprintf(runlog, "** HTTP POST arguments:\n");
       fprintf(runlog, rlbuf);
       fprintf(runlog, "\n");
       free(rlbuf);
    }

    fprintf(runlog, "**********************\n");
    fclose(runlog);
 }
}

  export_series = kExportSeries;

  long long sunums[DRMS_MAXQUERYLEN/8];  // should be enough!
  int expsucount;

  /*  op == exp_su - export Storage Units */
  if (strcmp(op, kOpExpSu) == 0)
    {
    long long sunum;
    int count;
    int status=0;
    int sums_status = 0; //ISS
    int all_online;

    export_series = kExportSeriesNew;
    // Do survey of sunum list
    size=0;
    all_online = 1;
    count = 0;

    if (!sunumarr || sunumarr[0] < 0)
    {
       nsunums = cmdparams_get_int64arr(&cmdparams, kArgDs, &sunumarr, &status);

       if (status != CMDPARAMS_SUCCESS)
       {
          snprintf(msgbuf, sizeof(msgbuf), 
                   "Invalid argument in exp_su, '%s=%s'.\n", kArgDs, cmdparams_get_str(&cmdparams, kArgDs, NULL));
          JSONDIE(msgbuf);
       }
    }

    if (!sunumarr || sunumarr[0] < 0)
    {
       JSONDIE("There are no SUs in sunum or ds params");
    }

    /* Fetch SUNUM_info_ts for all sunums now. */
    infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nsunums);
    status = drms_getsuinfo(drms_env, (long long *)sunumarr, nsunums, infostructs);

    if (status != DRMS_SUCCESS)
    {
       snprintf(msgbuf, sizeof(msgbuf), 
                "drms_getsuinfo(): failure calling talking with SUMS, error code %d.\n", status);
       printkerr(msgbuf);
       sums_status = 1;
    }

    char onlinestat[128];
    int dirsize;
    char supath[DRMS_MAXPATHLEN];
    char yabuff[64];
    int isunum;

    for (isunum = 0; isunum < nsunums; isunum++)
      {
      SUM_info_t *sinfo;
      TIME expire = 0;

      dirsize = 0;
      memset(onlinestat, 0, sizeof(onlinestat));
      snprintf(supath, sizeof(supath), "NA");

      sunum = sunumarr[isunum];

      sinfo = infostructs[isunum];
      if (*(sinfo->online_loc) == '\0')
         {
         *onlinestat = 'I';
         sunums[count] = sunum;
         paths[count] = strdup("NA");
         series[count] = strdup("NA");
         sustatus[count] = strdup(onlinestat);
         susize[count] = strdup("0");
         count++;
         }
      else
         {
         size += (long long)sinfo->bytes;
         dirsize = (int)sinfo->bytes;

         if (strcmp(sinfo->online_status,"Y")==0)
            {
            int y,m,d,hr,mn;
            char sutime[50];
            sscanf(sinfo->effective_date,"%4d%2d%2d%2d%2d", &y,&m,&d,&hr,&mn);
            sprintf(sutime, "%4d.%02d.%02d_%02d:%02d", y,m,d,hr,mn);
            expire = (sscan_time(sutime) - now)/86400.0;
            snprintf(supath, sizeof(supath), "%s", sinfo->online_loc);
            *onlinestat = 'Y';
            }
         if (strcmp(sinfo->online_status,"N")==0 || expire < 3)
            {  // need to stage or reset retention time
            all_online = 0;

            if (strcmp(sinfo->archive_status, "N") == 0)
               {
               *onlinestat = 'X';
               dirsize = 0;
               }
            else
               {
               *onlinestat = 'N';
               }
            }

         sunums[count] = sunum;
         paths[count] = strdup(supath);
         series[count] = strdup(sinfo->owning_series);
         sustatus[count] = strdup(onlinestat);
         snprintf(yabuff, sizeof(yabuff), "%d", dirsize);
         susize[count] = strdup(yabuff);

         count += 1;
         }

      free(sinfo);
      sinfo = NULL;
      } /* isunum */

    expsucount = count;

    if (count==0)
      JSONDIE("There are no files in this RecordSet");

    // Do quick export if possible
    /* Even if a quick export is NOT possible, VSO wants to see json returned with the status
     * for each sunum originally requested. Even if SUMS is down (in which case, sums_status == 1
     * for each sunum) they want this json. Currently, the only time dodataobj == 1 is when VSO calls 
     * jsoc_fetch. So eventually we might need to modify this a little. 
     *
     * If SUMS is down, and the jsoc_fetch request did NOT originate from VSO, no json is generated,
     * and jsoc_fetch returns with an error, generating appropriate json.
     */
    if (sums_status == 1 && !dodataobj)
    {
       /* SUMS is down! Stop here - don't start a new export request (which will likely fail). */
       JSONDIE("SUMS is down.");
    }

    if (strcmp(method,"url_quick")==0 && strcmp(protocol,kOptProtocolAsIs)==0  && (all_online || dodataobj))
      {
      if (dojson)
        {
        int i;
        char *json;
        char *strval;
        char numval[50];
        json_t *jroot = json_new_object();
        json_t *data;
        if (dodataobj)
          data = json_new_object();
        else
          data = json_new_array();

        for (i=0; i < count; i++)
          {
          json_t *suobj = json_new_object();
          char *jsonstr;
          char numval[40];
          char *sunumstr = NULL;
          sprintf(numval,"%lld",sunums[i]);
          sunumstr = string_to_json(numval); // send as string in case long long fails
          json_insert_pair_into_object(suobj, "sunum", json_new_string(sunumstr));
          jsonstr = string_to_json(series[i]);
          json_insert_pair_into_object(suobj, "series", json_new_string(jsonstr));
          free(jsonstr);
          jsonstr = string_to_json(paths[i]);
          json_insert_pair_into_object(suobj, "path", json_new_string(jsonstr));
          free(jsonstr);
          json_insert_pair_into_object(suobj, "sustatus", json_new_string(sustatus[i]));
          json_insert_pair_into_object(suobj, "susize", json_new_string(susize[i]));
          if (dodataobj)
            {
            json_t *suLabel = json_new_string(sunumstr);
            json_insert_child(suLabel,suobj);
            json_insert_child(data, suLabel);
            }
          else
            json_insert_child(data, suobj);
          if (sunumstr)
            free(sunumstr);
          }
        
        sprintf(numval, "%d", count);
        json_insert_pair_into_object(jroot, "count", json_new_number(numval));
        sprintf(numval, "%d", (int)(size));
        json_insert_pair_into_object(jroot, "size", json_new_number(numval));
        json_insert_pair_into_object(jroot, "dir", json_new_string(""));
        json_insert_pair_into_object(jroot, "data", data);
        json_insert_pair_into_object(jroot, kArgRequestid, json_new_string(""));
        strval = string_to_json((char *)method);
        json_insert_pair_into_object(jroot, kArgMethod, json_new_string(strval));
        free(strval);
        strval = string_to_json((char *)protocol);
        json_insert_pair_into_object(jroot, kArgProtocol, json_new_string(strval));
        free(strval);
        json_insert_pair_into_object(jroot, "wait", json_new_number("0"));
        //ISS -- added sums_status
        char sums_status_str[256]; //ISS
        sprintf(sums_status_str, "%d", sums_status); //ISS
        json_insert_pair_into_object(jroot, "status", json_new_number(sums_status_str)); //ISS
        json_tree_to_string(jroot,&json);
        printf("Content-type: application/json\n\n");
        printf("%s\n",json);
        fflush(stdout);
        free(json);

        /* I think we can free jroot */
        json_free_value(&jroot);
        } /* dojson */  
      else
        {
        int i;
        printf("Content-type: text/plain\n\n");
        printf("# JSOC Quick Data Export of as-is files.\n");
        printf("status=0\n");
        printf("requestid=\"%s\"\n", kNotSpecified);
        printf("method=%s\n", method);
        printf("protocol=%s\n", protocol);
        printf("wait=0\n");
        printf("count=%d\n", count);
        printf("size=%d\n", (int)size);
        printf("dir=/\n");
        printf("# DATA\n");
        for (i=0; i<count; i++)
          printf("%lld\t%s\t%s\t%s\t%s\n",sunums[i],series[i],paths[i], sustatus[i], susize[i]);
        }

      if (!dodataobj || (sums_status == 1 || all_online))
        {
         /* If not a VSO request, we're done. If a VSO request, done if all online, or if SUMS is down. 
          * Otherwise, continue below and start a new request for the items not online. */
           CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
           return(0);
        }
      else if (strcmp(requestid, kNoAsyncReq) == 0) // user never wants full export of leftovers
      {
         CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
         return 0;
      }
      }

    // Must do full export processing
    // But don't do this if the user has signalled that s/he doesn't want to ever
    if (strcmp(requestid, kNoAsyncReq) == 0)
      {
      JSONDIE("User denies required full export");
      }

    // initiate a new asynchronous request, which requires a new requestid
    // Get RequestID
   
    FILE *fp = popen("/home/phil/cvs/JSOC/bin/linux_ia32/GetJsocRequestID", "r");
    if (fscanf(fp, "%s", new_requestid) != 1)
      JSONDIE("Cant get new RequestID");
    pclose(fp);
    strcat(new_requestid, "_SU");
    requestid = new_requestid;

    now = timenow();

    // Log this export request
    if (1)
      {
      char exportlogfile[1000];
      char timebuf[50];
      FILE *exportlog;
      sprintf(exportlogfile, "/home/jsoc/exports/tmp/%s.reqlog", requestid);
      exportlog = fopen(exportlogfile, "a");
      sprint_ut(timebuf, now);
      fprintf(exportlog,"XXXX New SU request started at %s\n", timebuf);
      fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
         getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
      fclose(exportlog);
      }

    // Add Requestor info to jsoc.export_user series 
    // Can not watch for new information since can not read this series.
    //   start by looking up requestor 
    if (strcmp(requestor, kNotSpecified) != 0)
      {
#ifdef SHOULD_BE_HERE
check for requestor to be valid remote DRMS site
#else // for now
      requestorid = 0;
#endif
      }
    else
      requestorid = 0;

    // FORCE process to be su_export
    process = "su_export";

    // Create new record in export control series
    // This will be copied into the cluster-side series on first use.

    if ( !requestid || !*requestid || strcmp(requestid, "none") == 0)
      JSONDIE("Must have valid requestID - internal error.");

    if (strcmp(in, kNotSpecified) == 0 && (!sunumarr || sunumarr[0] < 0))
      JSONDIE("Must have valid Recordset or SU set");

    export_log = drms_create_record(drms_env, export_series, DRMS_PERMANENT, &status);
    if (!export_log)
      JSONDIE("Cant create new export control record");
    drms_setkey_string(export_log, "RequestID", requestid);
    drms_setkey_string(export_log, "DataSet", in);
    drms_setkey_string(export_log, "Processing", process);
    drms_setkey_string(export_log, "Protocol", protocol);
    drms_setkey_string(export_log, "FilenameFmt", filenamefmt);
    drms_setkey_string(export_log, "Method", method);
    drms_setkey_string(export_log, "Format", format);
    drms_setkey_time(export_log, "ReqTime", now);
    drms_setkey_time(export_log, "EstTime", now+10); // Crude guess for now
    drms_setkey_longlong(export_log, "Size", (int)size);
    drms_setkey_int(export_log, "Status", 2);
    drms_setkey_int(export_log, "Requestor", requestorid);
    drms_close_record(export_log, DRMS_INSERT_RECORD); 
    } // end of exp_su

  /*  op == exp_request  */
  else if (strcmp(op,kOpExpRequest) == 0) 
    {
    int sums_status = 0;  //ISS
    int status=0;
    int segcount = 0;
    int irec;
    int all_online = 1;
    char dsquery[DRMS_MAXQUERYLEN];
    char *p;
    char *file, *filename;
    int filesize;
    DRMS_RecordSet_t *rs;
    export_series = kExportSeriesNew;

    size=0;
    strncpy(dsquery,in,DRMS_MAXQUERYLEN);
    fileupload = strncmp(dsquery, "*file*", 6) == 0;
    if (fileupload)  // recordset passed as uploaded file
      {
      file = (char *)cmdparams_get_str (&cmdparams, kArgFile, NULL);
      filesize = cmdparams_get_int (&cmdparams, kArgFile".length", NULL);
      filename = (char *)cmdparams_get_str (&cmdparams, kArgFile".filename", NULL);
      if (filesize >= DRMS_MAXQUERYLEN)
        { //  must use file for all processing
        JSONDIE("Uploaded file limit 4096 chars for a bit longer, will fix later.");
// XXXXXX need to deal with big files
        }
      else if (filesize == 0)
        {
        JSONDIE("Uploaded file is empty");
        }
      else // can treat as command line arg by changing newlines to commas
        {
        int i;
        char c, *p = dsquery;
        int newline = 1;
        int discard = 0;
        strcpy(dsquery, file);
        for (i=0; (c = file[i]) && i<DRMS_MAXQUERYLEN; i++)
          {
          if (newline && c == '#') // discard comment lines
            discard = 1;
          if (c == '\r')
            continue;
          if (c == '\n')
            {
            if (!newline && !discard)
              *p++ = ',';
            newline = 1;
            discard = 0;
            }
          else
            {
            if (!discard)
              *p++ = c;
            newline = 0;
            }
          }
        *p = '\0';
        if (p > dsquery && *(p-1) == ',')
          *(p-1) = '\0';
        }
      }
    else // normal request, check for embedded segment list
      {
      if (index(dsquery,'[') == NULL)
        {
        char *cb = index(dsquery, '{');
        if (cb)
          {
          char *cbin = index(in, '{');
          *cb = '\0';
          strcat(dsquery, "[]");
          strcat(dsquery, cbin);
          }
        else
          strcat(dsquery,"[]");
        }
      if (strcmp(seglist,kNotSpecified) != 0)
        {
        if (index(dsquery,'{') != NULL)
          JSONDIE("Can not give segment list both in key and explicitly in recordset.");
        strncat(dsquery, "{", DRMS_MAXQUERYLEN);
        strncat(dsquery, seglist, DRMS_MAXQUERYLEN);
        strncat(dsquery, "}", DRMS_MAXQUERYLEN);
        }
      if ((p=index(dsquery,'{')) != NULL && strncmp(p+1, "**ALL**", 7) == 0)
        *p = '\0';
      }

    rs = drms_open_records(drms_env, dsquery, &status);
    if (!rs)
        {
        int tmpstatus = status;
        rcount = drms_count_records(drms_env, dsquery, &status);
        if (status == 0)
          {
          char errmsg[100];
          sprintf(errmsg,"%d is too many records in one request.",rcount);
	  JSONDIE2("Can not open RecordSet ",errmsg);
          }
        status = tmpstatus;
	JSONDIE2("Can not open RecordSet, bad query or too many records: ",dsquery);
        }
    rcount = rs->n;
    drms_stage_records(rs, 0, 0);
    drms_record_getinfo(rs);
  
    // Do survey of recordset
// this section should be rewritten to first check in each recordset chunk to see if any
// segments are linked, if not, then just use the sunums from stage reocrds.
// when stage_records follows links, this will be quicker...
// then only check each seg if needed.
    all_online = 1;
    for (irec=0; irec < rcount; irec++) 
      {
      // Must check each segment since some may be linked and/or offline.
      DRMS_Record_t *rec = rs->records[irec];
      DRMS_Segment_t *seg;
      HIterator_t *segp = NULL;
      // Disallow exporting jsoc.export* series
      if (strncmp(rec->seriesinfo->seriesname, "jsoc.export", 11)== 0)
        JSONDIE("Export of jsoc_export series not allowed.");
      while (seg = drms_record_nextseg(rec, &segp, 1))
        {
        DRMS_Record_t *segrec = seg->record;
        SUM_info_t *sinfo = rec->suinfo;
        if (!sinfo)
          {
          fprintf(stderr, "JSOC_FETCH Bad sunum %lld for recnum %lld in RecordSet: %s\n", segrec->sunum, rec->recnum, dsquery);
          // no longer die here, leave it to the export process to deal with missing segments
          all_online = 0;
          }
  	else if (strcmp(sinfo->online_status,"N") == 0)
          all_online = 0;
        else
          { // found good sinfo info
          struct stat buf;
  	  char path[DRMS_MAXPATHLEN];
	  drms_record_directory(segrec, path, 0);
          drms_segment_filename(seg, path);
          if (stat(path, &buf) != 0)
            { // segment specified file is not present.
              // it is an error if the record and QUALITY >=0 but no file matching
              // the segment file name unless the filename is empty.
            if (*(seg->filename))
              {
              DRMS_Keyword_t *quality = drms_keyword_lookup(segrec, "QUALITY",1);
              if (quality && drms_getkey_int(segrec, "QUALITY", 0) >= 0)
                { // there should be a file
fprintf(stderr,"QUALITY >=0, filename=%s, but %s not found\n",seg->filename,path);
  	        // JSONDIE2("Bad path (file missing) in a record in RecordSet: ", dsquery);
                }
              }
            }
          else // Stat succeeded, get size
            {
            if (S_ISDIR(buf.st_mode))
              { // Segment is directory, get size == for now == use system "du"
              char cmd[DRMS_MAXPATHLEN+100];
              FILE *du;
              long long dirsize=0;
              sprintf(cmd,"/usr/bin/du -s -b %s", path);
              du = popen(cmd, "r");
              if (du)
                {
                if (fscanf(du,"%lld",&dirsize) == 1)
                  {
                  size += dirsize;
                  segcount += 1;
                  }
                pclose(du);
                }
//fprintf(stderr,"dir size=%lld\n",dirsize);
              }
            else
              {
              size += buf.st_size;
              segcount += 1;
              }
            }
          }
        }
      if (segp)
        free(segp); 
      }
    if (size > 0 && size < 1024*1024) size = 1024*1024;
    size /= 1024*1024;
  
    // Exit if no records found
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,kOptProtocolAsIs)==0) || strcmp(protocol,"su")==0) && segcount == 0)
      JSONDIE("There are no files in this RecordSet");

    // Return status==3 if request is too large.
    if (size > MAX_EXPORT_SIZE)
      {
      int count = drms_count_records(drms_env, dsquery, &status);
      if (dojson)
        {
        char *json;
        char *strval;
        char numval[100];
        json_t *jroot = json_new_object();
        json_insert_pair_into_object(jroot, kArgRequestid, json_new_string("VOID"));
        // free(strval);
        strval = string_to_json((char *)method);
        json_insert_pair_into_object(jroot, kArgMethod, json_new_string(strval));
        free(strval);
        strval = string_to_json((char *)protocol);
        json_insert_pair_into_object(jroot, kArgProtocol, json_new_string(strval));
        free(strval);
        json_insert_pair_into_object(jroot, "wait", json_new_number("0"));
        json_insert_pair_into_object(jroot, "status", json_new_number("3"));
        sprintf(numval, "%d", count);
        json_insert_pair_into_object(jroot, "count", json_new_number(numval));
        sprintf(numval, "%d", (int)size);
        json_insert_pair_into_object(jroot, "size", json_new_number(numval));
        sprintf(numval,"Request exceeds max byte limit of %dMB", MAX_EXPORT_SIZE);
        strval = string_to_json(numval);
        json_insert_pair_into_object(jroot, "error", json_new_string(strval));
        free(strval);
        json_tree_to_string(jroot,&json);
        if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
           printf("Content-type: text/html\n\n");
        else
          printf("Content-type: application/json\n\n");
        printf("%s\n",json);
        fflush(stdout);
        free(json);
        }  
      else
        {
        printf("Content-type: text/plain\n\n");
        printf("# JSOC Data Export Failure.\n");
  	printf("status=3\n");
        printf("size=%lld\n",size);
        printf("count=%d\n",count);
  	printf("requestid=VOID\n");
  	printf("wait=0\n");
  	}

      CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
      return(0);
      }

    // Do quick export if possible
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,kOptProtocolAsIs)==0) || strcmp(protocol,"su")==0) && all_online)
      {
      if (0 && segcount == 1) // If only one file then do immediate delivery of that file.
        {
           char sfpath[DRMS_MAXPATHLEN];
           int sfret = send_file(rs->records[0], 0, sfpath, sizeof(sfpath));
           if (sfret == 1)
           {
              JSONDIE2("Can not open file for export: ",sfpath);
           }
           else
           {
              CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
              return(sfret);
           }
        }
      else if (dojson)
        {
        char *json;
        char *strval;
        int count;
        json_t *jroot = json_new_object();
        count = quick_export_rs(jroot, rs, 0, size); // add count, size, and array data of names and paths
        json_insert_pair_into_object(jroot, kArgRequestid, json_new_string(""));
        // free(strval);
        strval = string_to_json((char *)method);
        json_insert_pair_into_object(jroot, kArgMethod, json_new_string(strval));
        free(strval);
        strval = string_to_json((char *)protocol);
        json_insert_pair_into_object(jroot, kArgProtocol, json_new_string(strval));
        free(strval);
        json_insert_pair_into_object(jroot, "wait", json_new_number("0"));
        json_insert_pair_into_object(jroot, "status", json_new_number("0"));
        json_tree_to_string(jroot,&json);
        if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
           printf("Content-type: text/html\n\n");
        else
          printf("Content-type: application/json\n\n");
        printf("%s\n",json);
        fflush(stdout);
        free(json);
        }  
      else
        {
        printf("Content-type: text/plain\n\n");
        printf("# JSOC Quick Data Export of as-is files.\n");
  	printf("status=0\n");
  	printf("requestid=\"%s\"\n", kNotSpecified);
  	printf("method=%s\n", method);
  	printf("protocol=%s\n", protocol);
  	printf("wait=0\n");
        quick_export_rs(NULL, rs, 0, size); // add count, size, and array data of names and paths
  	}
      CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
      return(0);
      }

     // Must do full export processing

     // Get RequestID
     {
     FILE *fp = popen("/home/phil/cvs/JSOC/bin/linux_ia32/GetJsocRequestID", "r");
     if (fscanf(fp, "%s", new_requestid) != 1)
       JSONDIE("Cant get new RequestID");
     pclose(fp);
     }
     requestid = new_requestid;

     now = timenow();

    // Log this export request
    if (1)
      {
      char exportlogfile[1000];
      char timebuf[50];
      FILE *exportlog;
      sprintf(exportlogfile, "/home/jsoc/exports/tmp/%s.reqlog", requestid);
      exportlog = fopen(exportlogfile, "w");
      sprint_ut(timebuf, now);
      fprintf(exportlog,"XXXX New export request started at %s\n", timebuf);
      fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
         getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
      fclose(exportlog);
      }


     // Add Requestor info to jsoc.export_user series 
     // Can not watch for new information since can not read this series.
     //   start by looking up requestor 
     if (strcmp(requestor, kNotSpecified) != 0)
      {
      DRMS_Record_t *requestor_rec;
#ifdef IN_MY_DREAMS
      DRMS_RecordSet_t *requestor_rs;
      char requestorquery[2000];
      sprintf(requestorquery, "%s[? Requestor = '%s' ?]", kExportUser, requestor);
      requestor_rs = drms_open_records(drms_env, requestorquery, &status);
      if (!requestor_rs)
        JSONDIE("Cant find requestor info series");
      if (requestor_rs->n == 0)
        { // First request for this user
        drms_close_records(requestor_rs, DRMS_FREE_RECORD);
#endif
        requestor_rec = drms_create_record(drms_env, kExportUser, DRMS_PERMANENT, &status);
        if (!requestor_rec)
          JSONDIE("Cant create new user info record");
        requestorid = requestor_rec->recnum;
        drms_setkey_int(requestor_rec, "RequestorID", requestorid);
        drms_setkey_string(requestor_rec, "Requestor", requestor);
        drms_setkey_string(requestor_rec, "Notify", notify);
        drms_setkey_string(requestor_rec, "ShipTo", shipto);
        drms_setkey_time(requestor_rec, "FirstTime", now);
        drms_setkey_time(requestor_rec, "UpdateTime", now);
        drms_close_record(requestor_rec, DRMS_INSERT_RECORD);
#ifdef IN_MY_DREAMS
        }
      else
        { // returning user, look for updated info
        // WARNING ignore adding new info for now - XXXXXX must fix this later
        requestor_rec = requestor_rs->records[0];
        requestorid = drms_getkey_int(requestor_rec, "RequestorID", NULL);
        drms_close_records(requestor_rs, DRMS_FREE_RECORD);
        }
#endif
       }
     else
       requestorid = 0;

     // Create new record in export control series
     // This will be copied into the cluster-side series on first use.
    if ( !requestid || !*requestid || strcmp(requestid, "none") == 0)
      JSONDIE("Must have valid requestID - internal error.");
    if (strcmp(in, "Not Specified") == 0)
      JSONDIE("Must have Recordset specified");
     export_log = drms_create_record(drms_env, export_series, DRMS_PERMANENT, &status);
     if (!export_log)
      JSONDIE("Cant create new export control record");
     drms_setkey_string(export_log, "RequestID", requestid);
     drms_setkey_string(export_log, "DataSet", dsquery);
     drms_setkey_string(export_log, "Processing", process);
     drms_setkey_string(export_log, "Protocol", protocol);
     drms_setkey_string(export_log, "FilenameFmt", filenamefmt);
     drms_setkey_string(export_log, "Method", method);
     drms_setkey_string(export_log, "Format", format);
     drms_setkey_time(export_log, "ReqTime", now);
     drms_setkey_time(export_log, "EstTime", now+10); // Crude guess for now
     drms_setkey_longlong(export_log, "Size", (int)size);
     drms_setkey_int(export_log, "Status", 2);
     drms_setkey_int(export_log, "Requestor", requestorid);
     drms_close_record(export_log, DRMS_INSERT_RECORD);
     } // End of kOpExpRequest setup
    
  /*  op == exp_repeat  */
  else if (strcmp(op,kOpExpRepeat) == 0) 
    {
    DRMS_RecordSet_t *RsClone;
    char logpath[DRMS_MAXPATHLEN];
    now = timenow();



    if (strcmp(requestid, kNotSpecified) == 0)
      JSONDIE("RequestID must be provided");

    // Log this re-export request
    if (1)
      {
      char exportlogfile[1000];
      char timebuf[50];
      FILE *exportlog;
      sprintf(exportlogfile, "/home/jsoc/exports/tmp/%s.reqlog", requestid);
      exportlog = fopen(exportlogfile, "a");
      sprint_ut(timebuf, now);
      fprintf(exportlog,"XXX New repeat request started at %s\n", timebuf);
      fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
         getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
      fclose(exportlog);
      }

JSONDIE("Re-Export requests temporarily disabled.");

    // First check status in jsoc.export 
    export_series = kExportSeries;
    sprintf(status_query, "%s[%s]", export_series, requestid);
    exports = drms_open_records(drms_env, status_query, &status);
    if (!exports)
      JSONDIE3("Cant locate export series: ", status_query);
    if (exports->n < 1)
      JSONDIE3("Cant locate export request: ", status_query);
    status = drms_getkey_int(exports->records[0], "Status", NULL);
    if (status != 0)
      JSONDIE("Can't re-request a failed or incomplete prior request");
    // if sunum and su exist, then just want the retention updated.  This will
    // be accomplished by checking the record_directory.
    if (drms_record_directory(export_log, logpath, 0) != DRMS_SUCCESS || *logpath == '\0')
      {  // really is no SU so go ahead and resubmit the request
      drms_close_records(exports, DRMS_FREE_RECORD);
  
      // new email provided, update with new requestorid
      if (strcmp(notify, kNotSpecified) != 0)
        {
        DRMS_Record_t *requestor_rec;
        requestor_rec = drms_create_record(drms_env, kExportUser, DRMS_PERMANENT, &status);
        if (!requestor_rec)
          JSONDIE("Cant create new user info record");
        requestorid = requestor_rec->recnum;
        drms_setkey_int(requestor_rec, "RequestorID", requestorid);
        drms_setkey_string(requestor_rec, "Requestor", "NA");
        drms_setkey_string(requestor_rec, "Notify", notify);
        drms_setkey_string(requestor_rec, "ShipTo", "NA");
        drms_setkey_time(requestor_rec, "FirstTime", now);
        drms_setkey_time(requestor_rec, "UpdateTime", now);
        drms_close_record(requestor_rec, DRMS_INSERT_RECORD);
        }
      else
        requestorid = 0;

      // Now switch to jsoc.export_new
      export_series = kExportSeriesNew;
      sprintf(status_query, "%s[%s]", export_series, requestid);
      exports = drms_open_records(drms_env, status_query, &status);
      if (!exports)
        JSONDIE3("Cant locate export series: ", status_query);
      if (exports->n < 1)
        JSONDIE3("Cant locate export request: ", status_query);
      RsClone = drms_clone_records(exports, DRMS_PERMANENT, DRMS_SHARE_SEGMENTS, &status);
      if (!RsClone)
        JSONDIE("Cant create new export control record");
      export_log = RsClone->records[0];
      drms_setkey_int(export_log, "Status", 2);
      if (requestorid)
        drms_setkey_int(export_log, "Requestor", requestorid);
      drms_setkey_time(export_log, "ReqTime", now);
      drms_close_records(RsClone, DRMS_INSERT_RECORD);
      }
    else // old export is still available, do not repeat, but treat as status request.
      {
      drms_close_records(exports, DRMS_FREE_RECORD);
      }
    // if repeating export then export_series is set to jsoc.export_new
    // else if just touching retention then is it jsoc_export
    }

  // Now report back to the requestor by dropping into the code for status request.
  // This is entry point for status request and tail of work for exp_request and exp_su
  // If data was as-is and online and url_quick the exit will have happened above.

  // op = exp_status, kOpExpStatus,  Implied here

  if (strcmp(requestid, kNotSpecified) == 0)
    JSONDIE("RequestID must be provided");

  sprintf(status_query, "%s[%s]", export_series, requestid);
  exports = drms_open_records(drms_env, status_query, &status);
  if (!exports)
    JSONDIE3("Cant locate export series: ", status_query);
  if (exports->n < 1)
    JSONDIE3("Cant locate export request: ", status_query);
  export_log = exports->records[0];

  status     = drms_getkey_int(export_log, "Status", NULL);
  in         = drms_getkey_string(export_log, "DataSet", NULL);
  process = drms_getkey_string(export_log, "Processing", NULL);
  protocol   = drms_getkey_string(export_log, "Protocol", NULL);
  filenamefmt = drms_getkey_string(export_log, "FilenameFmt", NULL);
  method     = drms_getkey_string(export_log, "Method", NULL);
  format     = drms_getkey_string(export_log, "Format", NULL);
  reqtime    = drms_getkey_time(export_log, "ReqTime", NULL);
  esttime    = drms_getkey_time(export_log, "EstTime", NULL); // Crude guess for now
  size       = drms_getkey_longlong(export_log, "Size", NULL);
  requestorid = drms_getkey_int(export_log, "Requestor", NULL);

  // Do special actions on status
  switch (status)
    {
    case 0:
            errorreply = NULL;
            waittime = 0;
            break;
    case 1:
            errorreply = NULL;
            waittime = esttime - timenow();
            break;
    case 2:
            errorreply = NULL;
            waittime = esttime - timenow();
            break;
    case 3:
            errorreply = "Request too large";
            waittime = 999999;
            break;
    case 4:
            waittime = 999999;
            errorreply = "RecordSet specified does not exist";
            break;
    case 5:
            waittime = 999999;
            errorreply = "Request was completed but is now deleted, 7 day limit exceeded";
            break;
    default:
      JSONDIE("Illegal status in export record");
    }

  // Return status information to user
  if (1)
    {
    char *json;
    char *strval;
    char numval[100];
    json_t *jsonval;
    json_t *jroot=NULL;

    if (status == 0)
      {
      // this what the user has been waiting for.  The export record segment dir should
      // contain a file containing the json to be returned to the user.  The dir will be returned as well as the file.
      char logpath[DRMS_MAXPATHLEN];
      FILE *fp;
      int c;
      char *indexfile = (dojson ? "index.json" : "index.txt");
      jroot = json_new_object();
      if (drms_record_directory(export_log, logpath, 0) != DRMS_SUCCESS || *logpath == '\0')
        {
        status = 5;  // Assume storage unit expired.  XXXX better to do SUMinfo here to check
        waittime = 999999;
        errorreply = "Request was completed but is now deleted, 7 day limit exceeded";
        }
      else  
        {
        strncat(logpath, "/", DRMS_MAXPATHLEN);
        strncat(logpath, indexfile, DRMS_MAXPATHLEN);
        fp = fopen(logpath, "r");
        if (!fp)
          JSONDIE2("Export should be complete but return %s file not found", indexfile);
  
        if (dojson)
          printf("Content-type: application/json\n\n");
        else
	  printf("Content-type: text/plain\n\n");
        while ((c = fgetc(fp)) != EOF)
          putchar(c);
        fclose(fp);
        fflush(stdout);
        }
      }

    if (status > 0) // not complete or failure exit path
      {
      if (dojson)
	{
        jroot = json_new_object();
        json_t *data = NULL;

        if (strcmp(op, kOpExpSu) == 0)
        {
           int i;
           data = json_new_array();
           for (i = 0; i < expsucount; i++)
           {
              json_t *suobj = json_new_object();
              char *jsonstr;
              char numval[40];
              sprintf(numval,"%lld",sunums[i]);
              jsonstr = string_to_json(numval); // send as string in case long long fails
              json_insert_pair_into_object(jroot, kArgSunum, json_new_string(jsonstr));
              free(jsonstr);
              jsonstr = string_to_json(series[i]);
              json_insert_pair_into_object(suobj, "series", json_new_string(jsonstr));
              free(jsonstr);
              jsonstr = string_to_json(paths[i]);
              json_insert_pair_into_object(suobj, "path", json_new_string(jsonstr));
              free(jsonstr);
              json_insert_pair_into_object(suobj, "sustatus", json_new_string(sustatus[i]));
              json_insert_pair_into_object(suobj, "susize", json_new_string(susize[i]));

              json_insert_child(data, suobj);
           }
        }
        // in all cases, return status and requestid
        sprintf(numval, "%d", status);
        json_insert_pair_into_object(jroot, "status", json_new_number(numval));
        strval = string_to_json((char *)requestid);
        json_insert_pair_into_object(jroot, kArgRequestid, json_new_string(strval));
        free(strval);
        strval = string_to_json((char *)method);
        json_insert_pair_into_object(jroot, kArgMethod, json_new_string(strval));
        free(strval);
        strval = string_to_json((char *)protocol);
        json_insert_pair_into_object(jroot, kArgProtocol, json_new_string(strval));
        free(strval);
        sprintf(numval, "%1.0lf", waittime);
        json_insert_pair_into_object(jroot, "wait", json_new_number(numval));
        sprintf(numval, "%d", rcount);
        json_insert_pair_into_object(jroot, "rcount", json_new_number(numval));
        sprintf(numval, "%d", (int)size);
        json_insert_pair_into_object(jroot, "size", json_new_number(numval));
        if (strcmp(op, kOpExpSu) == 0)
           json_insert_pair_into_object(jroot, "data", data);
        if (errorreply) 
          {
          strval = string_to_json(errorreply);
          json_insert_pair_into_object(jroot, "error", json_new_string(strval));
          free(strval);
          }
        if (status > 2 )
          {
          strval = string_to_json("jsoc_help@jsoc.stanford.edu");
          json_insert_pair_into_object(jroot, "contact", json_new_string(strval));
          free(strval);
          }
        json_tree_to_string(jroot,&json);
        if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
  	  printf("Content-type: text/html\n\n");
        else
          printf("Content-type: application/json\n\n");
	printf("%s\n",json);
	}
      else
        {
	printf("Content-type: text/plain\n\n");
        printf("# JSOC Data Export Not Ready.\n");
        printf("status=%d\n", status);
        printf("requestid=%s\n", requestid);
        printf("method=%s\n", method);
        printf("protocol=%s\n", protocol);
        printf("wait=%f\n",waittime);
	printf("size=%lld\n",size);
        if (errorreply)
	  printf("error=\"%s\"\n", errorreply);
	if (status > 2)
          {
	  printf("contact=jsoc_help@jsoc.stanford.edu\n");
          }
        else if (strcmp(op, kOpExpSu) == 0)
          {
           int i;
           printf("# DATA\n");
           for (i = 0; i < expsucount; i++)
             {
             printf("%lld\t%s\t%s\t%s\t%s\n", sunums[i], series[i], paths[i], sustatus[i], susize[i]);
             }
          }
        }
      fflush(stdout);
      }
    }

  CleanUp(&sunumarr, &infostructs, &webarglist, series, paths, sustatus, susize, arrsize);
  return(0);
  }
