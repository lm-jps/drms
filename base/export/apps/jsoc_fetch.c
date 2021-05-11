#define DEBUG 0

// TESTMODE = 1 means make jsoc.export_new with status=12 instead of 2
// #define TESTMODE 1
#define TESTMODE 0

/*
 *  jsoc_fetch - cgi-bin program to recieve jsoc export and export status requests
 *
 */


/*
 * There are two ways to emulate a POST request on the cmd-line. Both involve
 * forcing qDecoder, the HTTP-request parsing library, to process a GET request.
 * Since POST passes its argument via stdin to jsoc_fetch, it would be cumbersome
 * to use the POST branch of qDecoder code (which expects args to arrive via stdin,
 * and expects additional env variables). Instead we can pass the arguments via
 * the cmd-line, or via environment variables.
 *   1. Set two shell environment variables, then run jsoc_fetch:
 *      a. setenv REQUEST_METHOD GET
 *      b. setenv QUERY_STRING 'op=exp_su&method=url_quick&format=json&protocol='as-is'&formatvar=dataobj&requestid=NOASYNCREQUEST&sunum=38400738,38400812' (this is an example - substitute your own arguments).
 *      c. jsoc_fetch
 *   2. Provide the QUERY_STRING argument to jsoc_fetch: jsoc_fetch QUERY_STRING='op=exp_su&method=url_quick&format=json&protocol='as-is'&formatvar=dataobj&requestid=NOASYNCREQUEST&sunum=38400738,38400812' (this is an example - substitute your own arguments).
 */
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "db.h"
#include "json.h"
#include "printk.h"
#include "qDecoder.h"
#include "jsmn.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <sys/file.h>
#include <regex.h>
#include <openssl/md5.h>

#define kDefRegexp         "JSOC_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9]+(_IN)?"

#define DUP_EXPORT_WINDOW  24 // hours

// Log files
#define kLockFile          "/home/jsoc/exports/tmp/lock.txt"
#define kLogFileSumm       "/home/jsoc/exports/logs/fetch_log"
#define kLogFileExpSuInt   "/home/jsoc/exports/logs/fetchlogExpSuInt.txt"
#define kLogFileExpSuExt   "/home/jsoc/exports/logs/fetchlogExpSuExt.txt"
#define kLogFileExpReqInt  "/home/jsoc/exports/logs/fetchlogExpReqInt.txt"
#define kLogFileExpReqExt  "/home/jsoc/exports/logs/fetchlogExpReqExt.txt"
#define kLogFileExpStatInt "/home/jsoc/exports/logs/fetchlogExpStatInt.txt"
#define kLogFileExpStatExt "/home/jsoc/exports/logs/fetchlogExpStatExt.txt"

#define MAX_COMPRESSING_EXPORT_SIZE 1000000 // 1000GB
#define MAX_STRAIGHT_EXPORT_SIZE 100000     // 100GB
#define MAX_UNCOMPRESSING_EXPORT_SIZE 10000 // 10GB

#define EXPORT_REQUEST_STATUS_INTERVENTION 25

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
#define kArgFile	"file"
#define kArgTestmode    "t"
#define kArgOnlineOnly  "o"
#define ARG_OMIT_QUALITY_CHECK "q"
#define kArgPassthrough "p"
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
#define kUserHandle	"userhandle"    // user provided unique session handle
#define kArgSizeRatio "sizeratio"
#define kArgDontGenWebPage "W"
#define kArgCgiInstance "instid"
#define kOptProtocolAsIs   "as-is"	   // Protocol option value for no change to fits files
#define kOptProtocolSuAsIs "su-as-is"  // Protocol option value for requesting as-is FITS paths for the exp_su operation

#define kNotSpecified	"Not Specified"
#define kNoAsyncReq     "NOASYNCREQUEST"

int dojson, dotxt, dohtml, doxml;

HContainer_t *gLogs = NULL;

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
  {ARG_STRING, kUserHandle, kNotSpecified, "User specified unique session ID"},
  {ARG_INT, kArgCgiInstance, "-1", "CGI instance ID"},
  {ARG_FLOAT, kArgSizeRatio, "1.0", "For cut-out requests, this is the ratio between the number of cut-out pixels to the number of original-image pixels."},
  {ARG_FLAG, kArgTestmode, NULL, "if set, then creates new requests with status 12 (not 2)"},
  {ARG_FLAG, kArgPassthrough, NULL, "if set, then inserts an X into the request ID to denote that the request originated from an external user, but was executed on the internal database."},
  {ARG_FLAG, kArgDontGenWebPage, NULL, "if set, print to stdout HTML headers."},
  {ARG_FLAG, kArgOnlineOnly, "1", "-o, Only process normal request if all requested SUs are online."},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_FLAG, ARG_OMIT_QUALITY_CHECK, NULL, "if set, then do not automatically add the quality-check filter to the record-set specifcation"},
  {ARG_STRING, "QUERY_STRING", kNotSpecified, "AJAX query from the web"},
  {ARG_STRING, "REMOTE_ADDR", "0.0.0.0", "Remote IP address"},
  {ARG_STRING, "SERVER_NAME", "ServerName", "JSOC Server Name"},
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
        "userhandle=unique session id\n"
	"h=help - show usage\n"
	"o=data must be online\n"
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

manage_userhandle(int register_handle, const char *handle)
  {
  if (register_handle)
    { // add handle and PID to current processing table
    }
  else if (handle && *handle)
    { // remove handle from current processing table
    }
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
  // TIME UNIX_epoch = -220924792.000; /* 1970.01.01_00:00:00_UTC */
  // TIME now = (double)time(NULL) + UNIX_epoch;
  TIME now = CURRENT_SYSTEM_TIME;
  return(now);
  }

static void CleanUp(int64_t **psunumarr, SUM_info_t ***infostructs, char **webarglist, char ***series, char ***paths, char ***susize, int arrsize, const char *userhandle)
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

   if (series && *series)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if ((*series)[iarr])
         {
            free((*series)[iarr]);
            (*series)[iarr] = NULL;
         }
      }

      free(*series);
      *series = NULL;
   }

   if (paths && *paths)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if ((*paths)[iarr])
         {
            free((*paths)[iarr]);
            (*paths)[iarr] = NULL;
         }
      }

      free(*paths);
      *paths = NULL;
   }

   if (susize && *susize)
   {
      for (iarr = 0; iarr < arrsize; iarr++)
      {
         if ((*susize)[iarr])
         {
            free((*susize)[iarr]);
            (*susize)[iarr] = 0;
         }
      }

      free(*susize);
      *susize = NULL;
   }

   if (userhandle && *userhandle)
     manage_userhandle(0, userhandle);

    hcon_destroy(&gLogs);
}

/* Can't call these from sub-functions - can only be called from DoIt(). And
 * calling these from within sub-functions is probably not the desired behavior -
 * I'm thinking that the calls from send_file and SetWebArg are mistakes. The
 * return(1) will NOT cause the DoIt() program to return because the return(1)
 * is called from the sub-function.
 * PHS - but the return value of sub-functions should be checked!
 */
#define JSONDIE(msg) {die(dojson,msg,"","4",&sunumarr,&infostructs,&webarglist,&series,&paths,&susize,arrsize,userhandle);return(1);}
#define JSONDIE2(msg,info) {die(dojson,msg,info,"4",&sunumarr,&infostructs,&webarglist,&series,&paths,&susize,arrsize,userhandle);return(1);}
#define JSONDIE3(msg,info) {die(dojson,msg,info,"6",&sunumarr,&infostructs,&webarglist,&series,&paths,&susize,arrsize,userhandle);return(1);}
#define JSONDIE7(msg) {die(dojson,msg,"","7",&sunumarr,&infostructs,&webarglist,&series,&paths,&susize,arrsize,userhandle);return(1);}
#define JSONDIE25(msg,info) {die(dojson,msg,info,"25",&sunumarr,&infostructs,&webarglist,&series,&paths,&susize,arrsize,userhandle);return(0);}

int fileupload = 0;
static int gGenWebPage = 1; /* For the die() function. */

int die(int dojson, const char *msg, const char *info, const char *cgistat, int64_t **psunumarr, SUM_info_t ***infostructs, char **webarglist, char ***series, char ***paths, char ***susize, int arrsize, const char *userhandle)
{
    char *msgjson = NULL;
    char *json = NULL;
    char message[4096] = {0};
    json_t *jroot = json_new_object();

    if (DEBUG)
    {
        fprintf(stderr,"%s%s\n",msg,info);
    }

    if (msg && *msg != '\0')
    {
        snprintf(message, sizeof(message), "%s", msg);
    }

    if(info && *info != '\0')
    {
        snprintf(message + strlen(message), sizeof(message) - strlen(message), "%s", info);
    }

    if (dojson)
    {
        msgjson = json_escape(message); /* allocs */

        if (msgjson)
        {
            json_insert_pair_into_object(jroot, "status", json_new_number(cgistat));
            json_insert_pair_into_object(jroot, "error", json_new_string(msgjson));
            free(msgjson);
            msgjson = NULL;
        }

        json_tree_to_string(jroot, &json);

        if (gGenWebPage)
        {
            if (fileupload)
            {
                // The returned json should be in the implied <body> tag for iframe requests.
                printf("Content-type: text/html\n\n");
            }
            else
            {
                printf("Content-type: application/json\n\n");
            }
        }

        printf("%s\n", json);
    }
    else
    {
        if (gGenWebPage)
        {
            printf("Content-type: text/plain\n\n");
        }

        printf("status=%s\nerror=%s\n", cgistat, message);
    }

    fflush(stdout);

    CleanUp(psunumarr, infostructs, webarglist, series, paths, susize, arrsize, userhandle);

    return(1);
}

static int JsonCommitFn(DRMS_Record_t **exprec,
                        int ro,
                        int dojson,
                        char *msg,
                        char *info,
                        char *stat,
                        int64_t **psunumarr,
                        SUM_info_t ***infostructs,
                        char **webarglist,
                        char ***series,
                        char ***paths,
                        char ***susize,
                        int arrsize,
                        const char *userhandle)
{
    int rv = 1; // rollback db

    // Return some json or plain text in response to the HTTP request
    die(dojson, msg, "", "4", psunumarr, infostructs, webarglist, series, paths, susize, arrsize, userhandle); // ignore return value

    if (exprec && *exprec)
    {
        if (ro)
        {
            drms_close_record(*exprec, DRMS_FREE_RECORD);
        }
        else
        {
            if (drms_setkey_int(*exprec, "Status", 4))
            {
                drms_close_record(*exprec, DRMS_FREE_RECORD);
                rv = 1;
            }
            else
            {
                // Don't worry about errmsg-write failure.
                drms_setkey_string(*exprec, "errmsg", msg);
                drms_close_record(*exprec, DRMS_INSERT_RECORD);
                rv = 0;
            }
        }

        *exprec = NULL;
    }

    return rv;
}

#define JSONCOMMIT(msg,rec,ro) {return JsonCommitFn(rec, ro, dojson, msg, "", "4", &sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);};


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
                die(dojson, "Illegal text in arg: ", arg_bad, "4", NULL, NULL, arglist, NULL, NULL, NULL, 0, NULL);
                return(1);
            }

            if (!cmdparams_set(&cmdparams, key, value))
            {
                /* ART - it appears that the original intent was to exit the DoIt()
                 * function here - but it is not possible to do that from a function
                 * called by DoIt(). But I've retained the original semantics of
                 * returning back to DoIt() from here. */
                die(dojson, "CommandLine Error", "", "4", NULL, NULL, arglist, NULL, NULL, NULL, 0, NULL);
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

/* Returns 1 on success. */
static int AcquireLock(int fd)
{
   int ret = -1;
   int natt = 0;

   while ((ret = lockf(fd, F_TLOCK, 0)) != 0 && natt < 10)
   {
      // printf("couldn't get lock, trying again.\n");
      sleep(1);
      natt++;
   }

   return (ret == 0);
}

static void ReleaseLock(int fd)
{
   lockf(fd, F_ULOCK, 0);
}

static void FreeLogs(void *val)
{
    FILE **pfptr = (FILE **)val;

    if (pfptr && *pfptr)
    {
        fflush(*pfptr);
        fchmod(fileno(*pfptr), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
        fclose(*pfptr);
        *pfptr = NULL;
    }
}

static void LocalTime(char *buf, int sz)
{
   time_t sounnecessarilycomplicated;
   struct tm *ltime = NULL;

   time(&sounnecessarilycomplicated);
   ltime = localtime(&sounnecessarilycomplicated);

   *buf = '\0';
   if (ltime)
   {
      snprintf(buf, sz, "%s", asctime(ltime));
   }
}

static void WriteLog(const char *logpath, const char *format, ...)
{
    FILE *fptr = NULL;
    FILE **pfptr = NULL;
    int lockfd;
    struct stat stbuf;
    int mustchmodlck = 0;
    int mustchmodlog = 0;

    if (stat(kLockFile, &stbuf) != 0)
    {
        /* If file doesn't exist (it might not, since this code can run at non-JSOC sites, but it contains JSOC-specific things),
         * then simply do not write log. */
        return;
    }
    else
    {
        if ((stbuf.st_mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) != (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))
        mustchmodlck = 1;
    }

    if (stat(logpath, &stbuf) == 0)
    {
        if ((stbuf.st_mode & (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) != (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))
        {
            mustchmodlog = 1;
        }
    }
    else
    {
       /* If the reason we cannot see the log file is that the parent directory is missing, then simply do not write the log file. */
       char *dname = NULL;
       char *path = strdup(logpath);

       if (path)
       {
          dname = dirname(path);
          if (dname)
          {
             if (stat(dname, &stbuf) != 0)
             {
                /* Parent directory does not exist. */
                return;
             }
          }
          else
          {
             return;
          }

          free(path);
       }
       else
       {
          fprintf(stderr, "Out of memory.\n");
          return;
       }
    }

    if (!gLogs)
    {
        gLogs = hcon_create(sizeof(FILE *), 128, (void(*)(const void *value))FreeLogs, NULL, NULL, NULL, 0);

        if (gLogs)
        {
            // Insert NULL fptrs, to initialize.
            hcon_insert(gLogs, kLogFileSumm, &fptr);
            hcon_insert(gLogs, kLogFileExpSuInt, &fptr);
            hcon_insert(gLogs, kLogFileExpSuExt, &fptr);
            hcon_insert(gLogs, kLogFileExpReqInt, &fptr);
            hcon_insert(gLogs, kLogFileExpReqExt, &fptr);
            hcon_insert(gLogs, kLogFileExpStatInt, &fptr);
            hcon_insert(gLogs, kLogFileExpStatExt, &fptr);
        }
        else
        {
            fprintf(stderr, "Out of memory.\n");
            return;
        }
    }

    pfptr = hcon_lookup(gLogs, logpath);

    if (pfptr)
    {
        // Acquire the lock
        lockfd = open(kLockFile, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG);
        if (lockfd >= 0)
        {
            if (AcquireLock(lockfd))
            {
                if (!*pfptr)
                {
                    *pfptr = fopen(logpath, "a");
                }

                if (*pfptr)
                {
                    // Now we can do the actual writing.
                    va_list ap;

                    va_start(ap, format);
                    vfprintf(*pfptr, format, ap);
                    va_end(ap);

                    if (mustchmodlog)
                    {
                       fchmod(fileno(*pfptr), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
                    }
                }
                else
                {
                    fprintf(stderr, "Unable to open log file for writing: %s.\n", logpath);
                }

                ReleaseLock(lockfd);
            }
            else
            {
                fprintf(stderr, "Unable to acquire lock on %s.\n", kLockFile);
            }

            if (mustchmodlck)
            {
               fchmod(lockfd, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
            }
            close(lockfd);
        }
        else
        {
            fprintf(stderr, "Unable to open lock file for writing: %s.\n", kLockFile);
        }
    }
    else
    {
        fprintf(stderr, "Invalid log-file path %s.\n", logpath);
    }
}

void json_insert_runtime(json_t *jroot, double StartTime)
  {
  char runtime[100];
  double EndTime;
  struct timeval thistv;
  gettimeofday(&thistv, NULL);
  EndTime = thistv.tv_sec + thistv.tv_usec/1000000.0;
  sprintf(runtime,"%0.3f",EndTime - StartTime);
  json_insert_pair_into_object(jroot, "runtime", json_new_number(runtime));
  }

// report_summary - record  this call of the program.
static void report_summary(const char *host,
                           double StartTime,
                           const char *remote_IP,
                           const char *op,
                           const char *ds,
                           int n,
                           int internal,
			   int requireOnline,
                           int status)
  {
  double EndTime;
  struct timeval thistv;
  char *logfile = NULL;
  char localnowtxt[128];

  gettimeofday(&thistv, NULL);
  EndTime = thistv.tv_sec + thistv.tv_usec/1000000.0;

  logfile = kLogFileSumm;

  LocalTime(localnowtxt, sizeof(localnowtxt));

  WriteLog(logfile, "*****%s\n", localnowtxt);
  WriteLog(logfile, "host='%s'\t",host);
  WriteLog(logfile, "lag=%0.3f\t",EndTime - StartTime);
  WriteLog(logfile, "IP='%s'\t",remote_IP);
  WriteLog(logfile, "op='%s'\t",op);
  WriteLog(logfile, "ds='%s'\t",ds);
  WriteLog(logfile, "n=%d\t",n);
  WriteLog(logfile, "o=%d\t",requireOnline);
  WriteLog(logfile, "status=%d\n",status);
  }

static void LogReqInfo(const char *fname,
                       long long instanceID,
                       int fileupload,
                       const char *op,
                       const char *dsin,
                       const char *requestid,
                       const char *dbhost,
                       int from_web,
                       const char *webarglist,
                       TIME fetch_time)
  {
  /* Before opening the log file, acquire a lock. Not only will this allow multiple jsoc_fetchs to
   * write to the same log, but it will facilitate synchronization with code that manages compression
   * and clean-up of the log. */

  char nowtxt[100];
      char localnowtxt[128];

  sprint_ut(nowtxt, fetch_time);
  LocalTime(localnowtxt, sizeof(localnowtxt));

  WriteLog(fname, "**********************\n");
  WriteLog(fname, "%s\n   CGI Instance=%lld\n   PID=%d\n   %s\n   op=%s\n   in=%s\n   RequestID=%s\n   DBHOST=%s\n   REMOTE_ADDR=%s\n", localnowtxt, instanceID, getpid(), nowtxt, op, dsin, requestid, dbhost, getenv("REMOTE_ADDR"));
  if (fileupload)  // recordset passed as uploaded file
    {
    char *file = (char *)cmdparams_get_str (&cmdparams, kArgFile, NULL);
    int filesize = cmdparams_get_int (&cmdparams, kArgFile".length", NULL);
    char *filename = (char *)cmdparams_get_str (&cmdparams, kArgFile".filename", NULL);
    WriteLog(fname,"   UploadFile: size=%d, name=%s, contents=%s\n",filesize,filename,file);
    }

    /* Now print the web arguments (if jsoc_fetch was invoked via an HTTP POST verb). */
    if (from_web && strlen(webarglist) > 0)
      {
      int rlsize = strlen(webarglist) * 2;
      char *rlbuf = malloc(rlsize);
      memset(rlbuf, 0, rlsize);
      const char *pwa = webarglist;
      char *prl = rlbuf;

      /* Before printing the webarglist string, escape '%' chars - otherwise fprintf(), will
       * think you want to replace things like "%lld" with a formated value - this happens
       * even though there is no arg provided after webarglist. */
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

      WriteLog(fname, "** HTTP POST arguments:\n");
      WriteLog(fname, rlbuf);
      WriteLog(fname, "\n");
      free(rlbuf);
      }

    WriteLog(fname, "**********************\n");
}

static int sunumComp(const void *val1, const void *val2)
{
    long long sunum1;
    long long sunum2;

    if (val1)
    {
        sunum1 = *((long long *)val1);
    }
    else
    {
        sunum1 = -1;
    }

    if (val2)
    {
        sunum2 = *((long long *)val2);
    }
    else
    {
        sunum2 = -1;
    }

    return (sunum1 < sunum2) ? -1 : (sunum1 > sunum2 ? 1 : 0);
}

int JFMin(int val, int maxVal)
{
    if (val > maxVal)
    {
        return maxVal;
    }
    else
    {
        return val;
    }
}

/* Returns a random integer (almost) uniformly distributed on [1, range]. */
int dieRoll(int range)
{
   int randVal;

   srand(time(NULL));
   randVal = rand(); /* ~ [0, RAND_MAX] uniformly */

   /* Convert to [1,range].
    *
    * randVal / RAND_MAX ~ [0, 1] uniformly
    * floor((randVal / RAND_MAX) * range) ~ [0, range - 1] uniformly, plus a 1 / RAND_MAX chance of range
    * floor((randVal / RAND_MAX) * range) + 1 ~ [1, range] uniformly, plus a 1 / RAND_MAX chance of range + 1
    * MIN[floor((randVal / RAND_MAX) * range) + 1, range] ~ [1, range - 1] uniformly, plus a 1 / RAND_MAX greater chance (than all [1, range - 1]) of range
    */
   return JFMin((int)floor(((double)randVal / RAND_MAX) * range) + 1, range);
}

/* Caller owns the returned string. */
/* If an error occurs, we treat this as if there is no duplicate record. The new request will start a full export. */

/* Do not use dsquery as is. It may have wildcards, like '$', in it which make it difficult to perform future comparisons, i.e.,
 * dsquery is not canonical. To convert dsquery to the canonical form, we have to open the records (drms_open_records()) and
 * make a list of recnums for each series described by dsquery.
 *
 * Saving this canonical form is not practical (it could be a very large string). Instead, we save the md5 hash of dsquery, along
 * with other identifying information (filenamefmt, process, protocol, method), in jsoc.export_md5. That db table maps
 * the md5 hash of all 5 fields of information to an existing request ID, if one exists. Records in jsoc.export_md5 expire after
 * DUP_EXPORT_WINDOW hours. This function first removes those expired records.
 *
 * jsoc.export_md5:
 *   md5   text (prime key)
 *   requestid   text (unique key)
 *   exporttime   timestamp with time zone
 */

static char *GetExistReqID(DRMS_Env_t *env, const char *md5, int window, TIME *timeToCompletion, int *expSize)
{
    char *id = NULL;
    char cmd[DRMS_MAXQUERYLEN];
    DB_Text_Result_t *tres = NULL;
    DB_Text_Result_t *tresInner = NULL;
    int istat;
    char *end = NULL;
    long long status;
    long long sunum;
    SUM_info_t *infostruct = NULL;

    istat = DRMS_SUCCESS;

    *timeToCompletion = DRMS_MISSING_TIME;
    *expSize = 0;

    /* search jsoc.export_md5 for the md5 - map md5->requestID and return the id */
    if (istat == DRMS_SUCCESS)
    {
        snprintf(cmd, sizeof(cmd), "SELECT requestid FROM jsoc.export_md5 WHERE md5 = '%s'", md5);

        if ((tres = drms_query_txt(drms_env->session, cmd)) != NULL)
        {
            if (tres->num_rows == 1)
            {
                if (tres->num_cols == 1)
                {
                    id = strdup(tres->field[0][0]);

                    /* We now need to extract the estimated completion time (we can't actually provide an accurate estimate),
                     * and the SUNUM of the pending/complete original export SU. This will allow us to provide estimates to
                     * the export requestor. Account for obsolete DRMS records. */
                    snprintf(cmd, sizeof(cmd), "SELECT status, esttime, size, sunum FROM jsoc.export T2 WHERE recnum = (SELECT max(recnum) AS recnum FROM jsoc.export T1 WHERE T1.requestid = '%s')", id);

                    if ((tresInner = drms_query_txt(drms_env->session, cmd)) == NULL)
                    {
                       fprintf(stderr, "Failure obtaining estimated completion time and size: %s.\n", cmd);
                       istat = DRMS_ERROR_BADDBQUERY;
                    }
                    else
                    {
                        if (tresInner->num_rows == 1 && tresInner->num_cols == 4)
                        {
                            status = strtoll(tresInner->field[0][0], &end, 10);

                            if (end != tresInner->field[0][0] && (status == 0 || status == 1 || status == 2 || status == 12))
                            {
                                /* Make sure that the SU is online. */
                                sunum = strtoll(tresInner->field[0][3], &end, 10);

                                if (end != tresInner->field[0][3] && sunum >= 0 && sunum != LLONG_MAX)
                                {
                                    /* We parsed at least some part of the sunum value, there is an associated SU, and we did not underflow or overflow. */

                                    /* This function runs in the op == exp_request branch of code in DoIt(). drms_getsuinfo() has never been called in that branch,
                                     * so call it here. */
                                    status = drms_getsuinfo(env, &sunum, 1, &infostruct);

                                    if (status == DRMS_SUCCESS && infostruct)
                                    {
                                        if (*(infostruct->online_loc) != '\0' && *(infostruct->online_status) == 'Y')
                                        {
                                            /* SUMS recognizes the SUNUM as valid, and the SU is online. */
                                            *timeToCompletion = strtod(tresInner->field[0][1], &end);

                                            if (end == tresInner->field[0][1] || *timeToCompletion == HUGE_VAL || *timeToCompletion == -HUGE_VAL)
                                            {
                                                *timeToCompletion = DRMS_MISSING_TIME;
                                            }

                                            *expSize = strtoll(tresInner->field[0][2], &end, 10);

                                            if (end == tresInner->field[0][2] || *expSize < INT_MIN || *expSize > INT_MAX)
                                            {
                                                *expSize = DRMS_MISSING_INT;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        /* We couldn't get estimated completion time and/or size information. There is still an existing,
                                         * valid request, so we still want to return the request ID to the caller. */
                                    }

                                    if (infostruct)
                                    {
                                        free(infostruct);
                                        infostruct = NULL;
                                    }
                                }
                            }
                            else
                            {
                                /* Couldn't get status from jsoc.export - we need to do a fresh export. */
                                istat = DRMS_ERROR_BADDBQUERY;
                            }
                        }
                        else
                        {
                            /* There is a request in jsoc.export_md5 for this hash, but there is no such request in jsoc.export. Treat
                             * this as if there is no such request in jsoc.export_md5 and have the user start a new export request. */
                            istat = DRMS_ERROR_BADDBQUERY;
                        }

                        if (tresInner)
                        {
                            db_free_text_result(tresInner);
                            tresInner = NULL;
                        }
                    }

                    if (istat != DRMS_SUCCESS)
                    {
                        if (id)
                        {
                            free(id);
                            id = NULL;
                        }
                    }
                }
                else
                {
                    istat = DRMS_ERROR_BADDBQUERY;
                }
            }
            else if (tres->num_rows != 0)
            {
                /* We cannot actually get here, since (md5) is the DB primary key. */
                istat = DRMS_ERROR_BADDBQUERY;
            }
            else
            {
                /* There is no existing identical export whose ID we're going to re-use. Since we are going to perform a full, fresh
                 * export, we need to insert a row for the current MD5. However, we do not have the actual ID at this point.
                 * Do this later. */
            }

            db_free_text_result(tres);
            tres = NULL;
        }
        else
        {
            istat = DRMS_ERROR_BADDBQUERY;
        }

        if (istat == DRMS_ERROR_BADDBQUERY)
        {
            fprintf(stderr, "Unexpected result returned from DB query: %s.\n", cmd);
        }
    }

    return id;
}

/* notify - case-sensitive email address provided by user on export web page
 * user_id - returned by checkAddress.py (from jsoc.export_user_info; user_id was created when email was registered)
 * dieStr - error message to return to caller
 * sz - size of the dieStr buffer
 */
static int CheckEmailAddress(const char *logfile, const char *notify, int *user_id, char *dieStr, size_t sz)
{
    /* Check notification email address. */
    int rv = 0;


    /* If there is no email address, then bail. */
    if (*notify == '\0')
    {
        /* Cannot call JSONDIE from functions. */
       snprintf(dieStr, sz, "Email address for notification was not specfied. It is required.");
       rv = 1;
    }

    /* Call script to verify notification email address. If the email address is not registered, then we fail right here. */
    char caCmd[PATH_MAX];
    FILE *caFptr = NULL;
    int caUserID = -1;
    int res;

    /* This is going to be Stanford-specific stuff - I'll localize this when I have time. */
    /* This script needs to connect to the database as database user apache, so the user running jsoc_fetch must have db user apache in their
     * .pgpass file. Normally it is linux user apache who runs jsoc_fetch, and this user has database user apache in their .pgpass. */
    snprintf(caCmd, sizeof(caCmd), "unset REQUEST_METHOD; unset QUERY_STRING; %s %s/checkAddress.py address=%s'&'checkonly=1", BIN_PY3, SCRIPTS_EXPORT, notify);
    WriteLog(logfile, "Calling checkAddress.py: %s\n", caCmd);
    caFptr = popen(caCmd, "r");

    if (caFptr)
    {
        /* C has no good json parsers - use jsmn. */
        jsmn_parser parser;
        jsmntok_t *tokens = NULL;
        size_t szjstokens = 512;
        char rbuf[512];
        size_t nbytes;
        size_t ntot = 0;
        size_t szjson = 1024;
        char *json = NULL; /* output of checkAddress.py. */
        int itok;
        int itchar;
        char *tok = NULL;
        size_t szTok;
        int statusTok = -1;
        int msgTok = -1;
        int useridTok = -1;
        char *endptr = NULL;
        long long caStatus;
        char *caMsg = NULL;
        char tChar[2];

        json = calloc(1, szjson);

        if (json)
        {
            while ((nbytes = fread(rbuf, sizeof(char), sizeof(rbuf) - 1, caFptr)) > 0)
            {
                rbuf[nbytes] = '\0';
                ntot += nbytes;
                json = base_strcatalloc(json, rbuf, &szjson);
            }

            tokens = calloc(1, sizeof(jsmntok_t) * szjstokens);

            if (tokens)
            {
                jsmn_init(&parser);
                res = jsmn_parse(&parser, json, tokens, szjstokens);

                if (res == JSMN_ERROR_NOMEM || res == JSMN_ERROR_INVAL || res == JSMN_ERROR_PART)
                {
                    /* Did not allocate enough tokens to hold parsing results. There shouldn't be 512 tokens in the response, only 2,
                     * so error out. */
                    pclose(caFptr);
                    caFptr = NULL;
                    /* Cannot call JSONDIE from functions. */
                    snprintf(dieStr, sz, "Unexpected response from checkAddress.py.");
                    rv = 1;
                }
                else if (res != JSMN_SUCCESS)
                {
                    /* Something else went wrong. */
                    pclose(caFptr);
                    caFptr = NULL;
                    /* Cannot call JSONDIE from functions. */
                    snprintf(dieStr, sz, "Failure parsing JSON.");
                    rv = 1;
                }

                /* There are 5 "tokens" in the returned json: <JSON object>, "status", <status val>, "msg", <msg val>. We
                 * want to look at status (token[2]). If it is 2, then the address is registered, and we can continue. If it is anything else, we return
                 * error code 6 (export record was not created). checkAddress.py will provide an appropriate error message in tokens[4]. tokens[0].size
                 * is the total number of tokens (aside from the root object).
                 */

                /* This JSON parser is pretty poor, so we need all this code to fetch the property values we want to look at. */
                for (itok = 1; itok <= tokens[0].size; itok++)
                {
                    szTok = 128;

                    tok = calloc(1, szTok);

                    if (!tok)
                    {
                        pclose(caFptr);
                        caFptr = NULL;
                        /* Cannot call JSONDIE from functions. */
                        snprintf(dieStr, sz, "Out of memory.");
                        rv = 1;
                    }

                    tChar[1] = '\0';
                    for (itchar = tokens[itok].start; itchar < tokens[itok].end; itchar++)
                    {
                        tChar[0] = json[itchar];
                        tok = base_strcatalloc(tok, (char *)&tChar, &szTok);
                    }

                    if (*tok)
                    {
                        if (statusTok != -1)
                        {
                            /* tok is the status value. */
                            caStatus = strtoll(tok, &endptr, 10);
                            if (tok == endptr)
                            {
                                /* Bad status returned from checkAddress.py. */
                                pclose(caFptr);
                                caFptr = NULL;
                                /* Cannot call JSONDIE from functions. */
                                snprintf(dieStr, sz, "Bad status code returned from checkAddress.py.");
                                rv = 1;
                            }

                            statusTok = -1;
                        }

                        if (msgTok != -1)
                        {
                            /* tok is the msg value. */
                            caMsg = strdup(tok);
                            msgTok = -1;
                        }

                        if (useridTok != -1)
                        {
                            caUserID = strtoll(tok, &endptr, 10);
                            useridTok = -1;
                        }

                        if (strcasecmp(tok, "status") == 0)
                        {
                            statusTok = itok + 1;
                        }
                        else if (strcasecmp(tok, "msg") == 0)
                        {
                            msgTok = itok + 1;
                        }
                        else if (strcasecmp(tok, "user_id") == 0)
                        {
                            useridTok = itok + 1;
                        }
                    }

                    free(tok);
                }

                free(tokens);
                tokens = NULL;
            }

            free(json);
            json = NULL;
        }

        WriteLog(logfile, "checkAddress.py returned status: %lld\n", caStatus);
        if (caStatus != 2)
        {
            /* Cannot continue because the notification email address has not been registered. */
            if (caMsg)
            {
                /* Cannot call JSONDIE from functions. */
                snprintf(dieStr, sz, "Cannot process your request. %s", caMsg);
                rv = 3;
            }
            else
            {
                /* Cannot call JSONDIE from functions. */
                snprintf(dieStr, sz, "Cannot process your request. Your notification email address has not been registered.");
                rv = 3;
            }
        }

        if (caMsg)
        {
            free(caMsg);
            caMsg = NULL;
        }

        pclose(caFptr);
        caFptr = NULL;
    }
    else
    {
        /* Cannot call JSONDIE from functions. */
        snprintf(dieStr, sz, "Unable to call checkAddress.py.");
        rv = 1;
    }

    if (rv == 0 && user_id)
    {
        *user_id = caUserID;
    }

    return rv;
}

/*
  parameters:
    env (DRMS_Env_t *) - DRMS environment
    address (char *)- email address registered for export
    ipAddress (char *) - IP address of the user making the export request
    timeOutInterval (int) - time out, in minutes; when time-out is exceeded, then it is OK to perform a new export
    pending_request_ids (LinkedList_t) - a list of all pending requests for `address`
   return values:
     0 - no pending request
     1 - error, cannot search for pending requests (missing address/ipAddress, db connection error, EXPORT_PENDING_REQUESTS_TABLE does not exist, etc.)
     2 - pending request, blocked email address
     3 - pending request, blocked IP address

   EXPORT_PENDING_REQUESTS_TABLE schema:
     address text not null (primekey)
     ip_address text not null
     request_id text
     start_time timestamp

  the SQL will be executed on the JSOC_DBHOST database; so each database server has the EXPORT_PENDING_REQUESTS_TABLE table
 */
static int CheckUserLoad(DRMS_Env_t *env, const char *address, const char *ipAddress, int timeOutInterval, LinkedList_t *pending_request_ids)
{
    char command[256] = {0};
    DB_Binary_Result_t *bres = NULL;
    int max_requests = 0;
    int exempt = 0;
    int row = 0;
    char pending_request[32] = {0};
    int rv = -1;

    /* look for pending request in EXPORT_PENDING_REQUESTS_TABLE - address is primary key, index on ipAddress exists too */
    if (address)
    {
        /* determine if user is exempt from single-request limit */
        snprintf(command, sizeof(command), "SELECT max_requests FROM %s WHERE address ILIKE '%s'", EXPORT_PENDING_REQUESTS_MAX_TABLE, address);

        if ((bres = drms_query_bin(env->session, command)) == NULL)
        {
            fprintf(stderr, "DB query failure [%s]\n", command);
            rv = 1;
        }
        else
        {
            if (bres->num_rows > 0)
            {
                max_requests = db_binary_field_getint(bres, row, 0);
            }
            else
            {
                exempt = 0;
            }
        }

        snprintf(command, sizeof(command), "SELECT request_id FROM %s WHERE address ILIKE '%s' AND CURRENT_TIMESTAMP - start_time < interval '%d minutes'", EXPORT_PENDING_REQUESTS_TABLE, address, timeOutInterval);

        if ((bres = drms_query_bin(env->session, command)) == NULL)
        {
            fprintf(stderr, "DB query failure [%s]\n", command);
            rv = 1;
        }
        else
        {
            if (bres->num_rows > 0)
            {
                /* (address, request_id) is the prime key - in general, there is 0 or 1 rows returned, but we now have a list of
                 * people who can make multiple, simultaneous requests */

                for (row = 0; row < bres->num_rows; row++)
                {
                    /* extract response (request_id returns a string) */
                    db_binary_field_getstr(bres, row, 0, sizeof(pending_request), pending_request);

                    list_llinserttail(pending_request_ids, pending_request);
                }

                exempt = (bres->num_rows <= max_requests);

                if (exempt)
                {
                    rv = 0;
                }
                else
                {
                    /* an existing request is pending for this user and it has not timed out */
                    rv = 2;
                }
            }
            else
            {
                /* either no pending request, or a pending request has timed out */
                rv = 0;
            }
        }
    }

    if (rv == -1 || rv == 0)
    {
        /* we need to limit by IP address too; careful though - it is legitimate for a site to have one or more
         * users who share the same IP address; but we do want to prevent the case where a site uses hundreds of
         * users with the same IP address to get around the limits imposed on email addresses */
        if (ipAddress)
        {
            snprintf(command, sizeof(command), "SELECT count(*) FROM %s WHERE ip_address = '%s' AND CURRENT_TIMESTAMP - start_time < interval '%d minutes'", EXPORT_PENDING_REQUESTS_TABLE, ipAddress, timeOutInterval);

            if ((bres = drms_query_bin(env->session, command)) == NULL)
            {
                fprintf(stderr, "DB query failure [%s]\n", command);
                rv = 1;
            }
            else
            {
                if (bres->num_rows > 0)
                {
                    /* extract response (count() returns a bigint) */
                    if (db_binary_field_getlonglong(bres, 0, 0) > 10)
                    {
                        /* 10 existing requests are pending for this IP address that have not timed out */
                        rv = 3;
                    }
                    else
                    {
                        /* either no pending request, or a pending request has timed out */
                        rv = 0;
                    }
                }
                else
                {
                    /* this should never happen - count() should return a single row */
                    fprintf(stderr, "DB query failure [%s] - did not return any rows\n", command);
                    rv = 1;
                }
            }
        }
    }

    if (bres)
    {
        db_free_binary_result(bres);
        bres = NULL;
    }

    if (rv == -1)
    {
        /* could not look up anything in dbTable, reject request */
        rv = 1;
    }

    return rv;
}

static int IsNearlineSeries(const char *series)
{
    return (strncmp(series, "iris.lev1", 9) == 0);
}


/* Module main function. */
int DoIt(void)
{
						/* Get command line arguments */
  const char *op;
  const char *dsin;
  const char *seglist;
  const char *requestid = NULL;
  const char *process;
  const char *requestor;
  int requestorid = -1; /* id in jsoc.export_user_info */
  const char *notify;
  const char *format;
  const char *formatvar;
  const char *shipto;
  const char *method;
  const char *protocol;
  const char *filenamefmt;
  const char *userhandle;
  float sizeRatio;
  const char *Server;
  const char *Remote_Address;
  char dbhost[DRMS_MAXHOSTNAME];
  int testmode = 0;
  int passthrough = 0;
  int genWebPage = 1;
  int requireOnline = 1;
  int omit_quality_check = 0;
  char *errorreply;
  int64_t *sunumarr = NULL; /* array of 64-bit sunums provided in the'sunum=...' argument. */
  int nsunums;
  long long size;
  int rcount = 0;
  int rcountlimit = 0;
  TIME reqtime;
  TIME esttime;
  TIME fetch_time = timenow();
  double waittime;
  char *web_query;
  int from_web = 0;
  int status;
  int dodataobj=1, dojson=1, dotxt=0, dohtml=0, doxml=0;
  DRMS_RecordSet_t *exports;
  DRMS_Record_t *exprec = NULL;  // Why was the name changed from export_log ??
  char new_requestid[200];
  char status_query[1024] = {0};
  char msgbuf[128] = {0};
  SUM_info_t **infostructs = NULL;
  char *webarglist = NULL;
  size_t webarglistsz;
  struct timeval thistv;
  double StartTime;
  long long instanceID = -1;

    /* Allocate the size of these arrays dynamically. Originally there were 1024, but
     * we could need bigger arrays. */
  char **paths = NULL;
  char **series = NULL;
  char *sustatus = NULL;
  char **susize = NULL;
  int arrsize = -1; /* Fill this in later, when we know how many elements we have. */

    int postorget = 0;
    int insertexprec = 1;

    char *pRemoteAddress = NULL;
    char ipAddress[16] = {0};

    if (getenv("REQUEST_METHOD"))
    {
        postorget = (strcasecmp(getenv("REQUEST_METHOD"), "POST") == 0 ||
                     strcasecmp(getenv("REQUEST_METHOD"), "GET") == 0);
    }

  if (nice_intro ()) return (0);

  gettimeofday(&thistv, NULL);
  StartTime = thistv.tv_sec + thistv.tv_usec/1000000.0;
  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = (strcmp (web_query, kNotSpecified) != 0) || postorget;

    /* jsocextfetch removes QUERY_STRING and REQUEST_METHOD from env, and passes through the ds program argument; we still need the ip address, which
     * will be in the REMOTE_ADDR env */
    if ((pRemoteAddress = getenv("REMOTE_ADDR")) != NULL)
    {
        snprintf(ipAddress, sizeof(ipAddress), "%s", pRemoteAddress);
    }
    else
    {
        snprintf(ipAddress, sizeof(ipAddress), "%s", "<UNKNOWN>");
    }

    if (from_web)
    {
        /* Log the QUERY_STRING */
        char *qs = getenv("QUERY_STRING");
        if (qs)
        {
            WriteLog(kLogFileExpReqExt, "QUERY_STRING is %s.\n", qs);
        }

        Q_ENTRY *req = NULL;

        /* If we are here then one of three things is true (implied by the existence of QUERY_STRING):
         *   1. We are processing an HTTP GET. The webserver will put the arguments in the
         *      QUERY_STRING environment variable.
         *   2. We are processing an HTTP POST. The webserver will NOT put the arguments in the
         *      QUERY_STRING environment variable. Instead the arguments will be passed to jsoc_fetch
         *      via stdin. QUERY_STRING should not be set, but it looks like it might be. In any
         *      case qDecoder will ignore it.
         *   3. jsoc_fetch was invoked via the cmd-line, and the caller provided the QUERY_STRING
         *      argument. The caller is trying to emulate an HTTP request - they want to invoke
         *      the web-processing code, most likely to develop or debug a problem.
         *
         *   If we are in case 3, then we need to make sure that the QUERY_STRING environment variable
         *   is set since qDecoder will be called, and to process a GET, QUERY_STRING must be set.
         */

        if (!getenv("QUERY_STRING"))
        {
            /* Either case 2 or 3. Definitely not case 1. */
            if (!postorget)
            {
                /* Case 3 - set QUERY_STRING from cmd-line arg. */
                setenv("QUERY_STRING", web_query, 1);

                /* REQUEST_METHOD is not set - set it to GET. */
                setenv("REQUEST_METHOD", "GET", 1);
            }
        }

        /* Use qDecoder to parse HTTP POST requests. qDecoder actually handles
         * HTTP GET requests as well.
         * See http://www.qdecoder.org
         */

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
            SetWebArg(req, kUserHandle, &webarglist, &webarglistsz);
            SetWebArg(req, kArgCgiInstance, &webarglist, &webarglistsz);
            SetWebArg(req, kArgSizeRatio, &webarglist, &webarglistsz);
            if (strncmp(cmdparams_get_str (&cmdparams, kArgDs, NULL),"*file*", 6) == 0);
            SetWebFileArg(req, kArgFile, &webarglist, &webarglistsz);
            SetWebArg(req, kArgTestmode, &webarglist, &webarglistsz);
            SetWebArg(req, kArgPassthrough, &webarglist, &webarglistsz);
            SetWebArg(req, kArgOnlineOnly, &webarglist, &webarglistsz);
            SetWebArg(req, ARG_OMIT_QUALITY_CHECK, &webarglist, &webarglistsz);
            SetWebArg(req, kArgDontGenWebPage, &webarglist, &webarglistsz);

            qEntryFree(req);
        }
    }
  free(web_query);
  // From here on, called as cgi-bin same as from command line

  op = cmdparams_get_str (&cmdparams, kArgOp, NULL);
  requestid = cmdparams_get_str (&cmdparams, kArgRequestid, NULL);
  dsin = cmdparams_get_str (&cmdparams, kArgDs, NULL);
  userhandle = cmdparams_get_str (&cmdparams, kUserHandle, NULL);
  sizeRatio = cmdparams_get_float(&cmdparams, kArgSizeRatio, NULL);
  Remote_Address = cmdparams_get_str(&cmdparams, "REMOTE_ADDR", NULL);
  Server = cmdparams_get_str(&cmdparams, "SERVER_NAME", NULL);
  instanceID = cmdparams_get_int(&cmdparams, kArgCgiInstance, NULL);

  // the following lines are added.  They can be removed after a new function to replace
  // cmdparams_set as used in SetWebArgs

  /* No need for the hack any more - SetWebArg(), when setting an ARG_INTS argument,
   * will trigger the code that parses the array elements and creates the
   * associated arguments. */

  // end hack
  nsunums = cmdparams_get_int64arr(&cmdparams, kArgSunum, &sunumarr, &status);

  if (status != CMDPARAMS_SUCCESS)
  {
     snprintf(msgbuf, sizeof(msgbuf), "Invalid argument on entry, '%s=%s'.\n", kArgSunum, cmdparams_get_str(&cmdparams, kArgSunum, NULL));
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

  if (strlen(drms_env->session->db_handle->dbhost) > 0)
  {
     snprintf(dbhost, sizeof(dbhost), drms_env->session->db_handle->dbhost);
  }
  else
  {
     *dbhost = '\0';
  }

  if (strcmp(userhandle, kNotSpecified) != 0)
    manage_userhandle(1, userhandle);
  else
    userhandle = "";

  testmode = (TESTMODE || cmdparams_isflagset(&cmdparams, kArgTestmode));
  passthrough = cmdparams_isflagset(&cmdparams, kArgPassthrough);
  genWebPage = (cmdparams_isflagset(&cmdparams, kArgDontGenWebPage) == 0);
  requireOnline = cmdparams_isflagset(&cmdparams, kArgOnlineOnly);
  omit_quality_check = cmdparams_isflagset(&cmdparams, ARG_OMIT_QUALITY_CHECK);

  gGenWebPage = genWebPage;

  dodataobj = strcmp(formatvar, "dataobj") == 0;
  dojson = strcmp(format, "json") == 0;
  dotxt = strcmp(format, "txt") == 0;
  dohtml = strcmp(format, "html") == 0;
  doxml = strcmp(format, "xml") == 0;

  // Extract the record limit from the Process field, if present.
  // Leave as part of process to forward into jsoc.export
  if (strncmp(process, "n=", 2) == 0)
    {
    rcountlimit = atoi(process+2);
    }

// SPECIAL DEBUG LOG HERE XXXXXX

  // long long sunums[DRMS_MAXQUERYLEN/8];  // should be enough!
  // 1024 is not enough.
  // sunums is used only in the kOpExpSu case, but it is used throughout DoIt(), so declare here.
  long long *sunums = NULL;
  int expsucount;
  const char *lfname = NULL;
  int fileupload = strncmp(dsin, "*file*", 6) == 0;
  int internal = (strcmp(dbhost, "hmidb") == 0);

  /*  op == exp_su - export Storage Units */
  if (strcmp(op, kOpExpSu) == 0)
    {
    long long sunum;
        int isunum;
        const char *seriesKey = NULL;
    int count;
    int status=0;
    int sums_status = 0; //ISS
    int all_online;

    if (internal)
    {
        lfname = kLogFileExpSuInt;
    }
    else
    {
        lfname = kLogFileExpSuExt;
    }

    // Do survey of sunum list
    size=0;
    all_online = 1;
    count = 0;

    if (!sunumarr || sunumarr[0] < 0)
    {
       /* Use the ds field - should be an array of numbers. */
       if (!cmdparams_set(&cmdparams, kArgSunum, dsin))
       {
          snprintf(msgbuf, sizeof(msgbuf), "Invalid argument in exp_su, '%s=%s'.\n", kArgDs, dsin);
          JSONDIE(msgbuf);
       }

       nsunums = cmdparams_get_int64arr(&cmdparams, kArgSunum, &sunumarr, &status);

       if (status != CMDPARAMS_SUCCESS)
       {
          snprintf(msgbuf, sizeof(msgbuf), "Invalid argument in exp_su, '%s=%s'.\n", kArgDs, dsin);
          JSONDIE(msgbuf);
       }
    }

    if (!sunumarr || sunumarr[0] < 0)
    {
       JSONDIE("There are no SUs in sunum or ds params");
    }

    /* print to the log the sunum array */
    size_t bufSz = 256;
    char *sunumListStr = calloc(bufSz, sizeof(char));
    char sunumStrBuf[32];

    if (sunumListStr)
    {
        for (isunum = 0; isunum < nsunums; isunum++)
        {
            sunum = sunumarr[isunum];

            if (isunum < nsunums - 1)
            {
                snprintf(sunumStrBuf, sizeof(sunumStrBuf), "%lld,", sunum);
            }
            else
            {
                snprintf(sunumStrBuf, sizeof(sunumStrBuf), "%lld", sunum);
            }

            sunumListStr = base_strcatalloc(sunumListStr, sunumStrBuf, &bufSz);
        }

        LogReqInfo(lfname, instanceID, fileupload, op, sunumListStr, requestid, dbhost, from_web, webarglist, fetch_time);

        free(sunumListStr);
        sunumListStr = NULL;
    }
    else
    {
        /* requestid is not provided via the command-line for kOpExpSu. */
        LogReqInfo(lfname, instanceID, fileupload, op, dsin, requestid, dbhost, from_web, webarglist, fetch_time);
    }

    /* Fetch SUNUM_info_ts for all sunums now. THIS CODE DOES NOT FOLLOW LINKS TO TARGET SEGMENTS. */
    infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nsunums);
    status = drms_getsuinfo(drms_env, (long long *)sunumarr, nsunums, infostructs);

    if (status != DRMS_SUCCESS)
    {
       snprintf(msgbuf, sizeof(msgbuf),
                "drms_getsuinfo(): failure calling talking with SUMS, error code %d.\n", status);
       printkerr(msgbuf);
       sums_status = 1;
    }

        /* ART FETCH BLOCK - To block certain SUs, we have to get the earliest T_REC from the SU. We want this to be
         * quick, so batch this into a single db request. THERE MUST BE A DB INDEX on the sunum column of every
         * series that we wish to partially block, otherwise the following query will take a long time to
         * run.
         *
         * Sample SQL - SELECT sunum, min(t_obs) FROM hmi.lev1 WHERE sunum IN (227768258, 227769522) GROUP BY sunum;
         *
         * Complication - the sunums might be for different series - crap! Need to loop over series first. Make a container,
         * keyed by series that contains a list of sorted sunums.
         *
         * 2014.03.10 - Added a new column 'acceptrate' to su_production.fetchblock. The value is the per-series percentage of
         * requests to accept. For example, an acceptrate of 75 means that 75 percent of all requests for old data in the
         * seriesname series will be accepted. This feature was added so that we do not completly block access to all old
         * data.
         */
        const int NUM_ARGS = 16;
        char stmnt[256];
        char *argin[NUM_ARGS];
        DB_Type_t intype[NUM_ARGS];
        int nExe;
        int iArg; /* iter over placeholders in prepared statement. */
        int iExe; /* number of times to call prepared statement. */
        char nbuf[32];
        HContainer_t *seriesSunums = NULL;
        HContainer_t *seriesMinBadTimes = NULL;
        HContainer_t *seriesMaxBadTimes = NULL;
        HContainer_t *seriesAcceptRates = NULL;
        HContainer_t *filterOut = NULL;
        LinkedList_t *sunumList = NULL;
        HIterator_t *hit = NULL;
        void *iterGet = NULL;
        void *hlookupGet = NULL;
        DB_Binary_Result_t *bres = NULL;
        DB_Binary_Result_t **pBres = NULL; /* pointer to bres */
        int iBres;
        DB_Text_Result_t *tres = NULL;
        int irow;
        ListNode_t *node = NULL;
        char *tmpDup = NULL;
        TIME minBadTime;
        TIME maxBadTime;
        int acceptRate;
        long long *sunumArrSorted = NULL; /* sorted list of sunums for one series */
        char *timeCol = NULL;
        int dontFilter;
        char timeValStr[256];
        char *endptr = NULL;
        int doFetchBlock = 0;
        int istat = DRMS_SUCCESS;

        seriesSunums = hcon_create(sizeof(LinkedList_t *), DRMS_MAXSERIESNAMELEN, (void (*)(const void *value))list_llfree, NULL, NULL, NULL, 0);
        seriesMinBadTimes = hcon_create(sizeof(TIME), DRMS_MAXSERIESNAMELEN, NULL, NULL, NULL, NULL, 0);
        seriesMaxBadTimes = hcon_create(sizeof(TIME), DRMS_MAXSERIESNAMELEN, NULL, NULL, NULL, NULL, 0);
        seriesAcceptRates = hcon_create(sizeof(int), DRMS_MAXSERIESNAMELEN, NULL, NULL, NULL, NULL, 0);
        filterOut = hcon_create(sizeof(int), sizeof(nbuf), NULL, NULL, NULL, NULL, 0);

        if (!seriesSunums || !seriesMinBadTimes || !seriesMaxBadTimes || !filterOut)
        {
            istat = DRMS_ERROR_OUTOFMEMORY;
        }

        if (istat == DRMS_SUCCESS)
        {
            /* Disable fetch block if there is no su_production.fetchblock table. */
            if (drms_query_tabexists(drms_env->session, "su_production", "fetchblock", &istat))
            {
                doFetchBlock = 1;
            }
        }

        if (istat == DRMS_SUCCESS && doFetchBlock)
        {
            for (isunum = 0; isunum < nsunums; isunum++)
            {
                sunum = sunumarr[isunum];
                seriesKey = infostructs[isunum]->owning_series;

                dontFilter = 0;

                /* We should skip series that are not going to be blocked at all because the series does not appear in su_production.fetchblock.
                 * If this isn't fast, we can read the entire table into memory and then do the search via hash lookup. What I'm doing is really
                 * inefficient - I can repeatedly look for a series that insn't in this table. But there are typically not that many sunums to lookup.
                 */

                /* Don't query for this series if we have already. We put an empty list in seriesSunums if the series is not filtered. */
                if (!hcon_member_lower(seriesSunums, seriesKey))
                {
                    tmpDup = strdup(seriesKey);
                    if (!tmpDup)
                    {
                        istat = DRMS_ERROR_OUTOFMEMORY;
                        break;
                    }

                    strtolower(tmpDup);
                    snprintf(stmnt, sizeof(stmnt), "SELECT mindate, maxdate, acceptrate FROM su_production.fetchblock WHERE lower(seriesname) = '%s'", tmpDup);
                    free(tmpDup);

                    /* Don't retrieve data in binary format!! Who knows what PQexecParams() will do with a timestamp? */
                    if ((tres = drms_query_txt(drms_env->session, stmnt)) == NULL)
                    {
                        fprintf(stderr, "Failed to query su_production.fetchblock.\n");
                        istat = DRMS_ERROR_QUERYFAILED;
                        break;
                    }
                    else
                    {
                        if (tres->num_cols != 3 || tres->num_rows > 1)
                        {
                            fprintf(stderr, "Unexpected query result.\n");
                            istat = DRMS_ERROR_BADQUERYRESULT;
                            break;
                        }
                        else
                        {
                            if (tres->num_rows == 0)
                            {
                                /* This series is not being filtered in any way. */
                                dontFilter = 1;
                            }
                            else
                            {
                                snprintf(timeValStr, sizeof(timeValStr), "%s", tres->field[0][0]);
                                minBadTime = sscan_time(timeValStr);

                                snprintf(timeValStr, sizeof(timeValStr), "%s", tres->field[0][1]);
                                maxBadTime = sscan_time(timeValStr);

                                acceptRate = strtoll(tres->field[0][2], &endptr, 0);
                                if (acceptRate == 0 && endptr == tres->field[0][2])
                                {
                                   acceptRate = 0;
                                }
                            }
                        }
                    }

                    db_free_text_result(tres);

                    /* create an empty list. */
                    sunumList = list_llcreate(sizeof(long long), NULL);
                    if (!sunumList)
                    {
                        istat = DRMS_ERROR_OUTOFMEMORY;
                        break;
                    }

                    hcon_insert_lower(seriesSunums, seriesKey, &sunumList);

                    if (dontFilter)
                    {
                        continue;
                    }

                    hcon_insert_lower(seriesMinBadTimes, seriesKey, &minBadTime);
                    hcon_insert_lower(seriesMaxBadTimes, seriesKey, &maxBadTime);
                    hcon_insert_lower(seriesAcceptRates, seriesKey, &acceptRate);
                    list_llinserttail(sunumList, &sunum);
                }

                hlookupGet = hcon_lookup_lower(seriesSunums, seriesKey);
                if (!hlookupGet)
                {
                    istat = DRMS_ERROR_DATASTRUCT;
                    break;
                }

                sunumList = *(LinkedList_t **)hlookupGet;

                if (list_llgetnitems(sunumList) > 0)
                {
                    /* If the series is to be filtered, then there is at least one sunum in its list. */
                    list_llinserttail(sunumList, &sunum);
                }
                else
                {
                    /* If the list contained in the container is empty, then we are not filtering for this series. */
                }
            } /* sunum loop*/
        }

        /* Each sunumList now contains a list of UNSORTED sunums for a single series series. Loop over series (iterate over seriesSunums). */
        if (istat == DRMS_SUCCESS && doFetchBlock)
        {
            int nSeriesSunums = 0;
            size_t stsz;
            char *sql = NULL;
            TIME timeVal = 0;
            int zeroVal = 0;
            DRMS_Record_t *template = NULL;
            DRMS_Keyword_t *pkey = NULL;
            int nPKeys;
            int iKey;

            hit = hiter_create(seriesSunums);
            if (!hit)
            {
                istat = DRMS_ERROR_OUTOFMEMORY;
            }
            else
            {
                /* seriesKey is lowercase from here on. */
                while ((iterGet = hiter_extgetnext(hit, &seriesKey)) != NULL && istat == DRMS_SUCCESS)
                {
                    sunumList = *(LinkedList_t **)iterGet;

                    /* If sunumList is empty, there are no sunums for series seriesKey. */
                    if ((nSeriesSunums = list_llgetnitems(sunumList)) == 0)
                    {
                        continue;
                    }

                    sunumArrSorted = calloc(nSeriesSunums, sizeof(long long));

                    if (!sunumArrSorted)
                    {
                        istat = DRMS_ERROR_OUTOFMEMORY;
                        break;
                    }

                    /* sunumList - a list of sunums for the series seriesKey. I guess we should sort the list of sunums. */
                    isunum = 0;
                    list_llreset(sunumList);
                    while ((node = list_llnext(sunumList)) != NULL)
                    {
                        if (!node->data)
                        {
                            istat = DRMS_ERROR_DATASTRUCT;
                            if (sunumArrSorted)
                            {
                                free(sunumArrSorted);
                                sunumArrSorted = NULL;
                            }
                            break;
                        }

                        sunumArrSorted[isunum++] = *(long long *)node->data;
                    }

                    if (istat == DRMS_SUCCESS)
                    {
                        qsort(sunumArrSorted, nSeriesSunums, sizeof(long long), sunumComp);

                        /* We need the column in seriesKey that serves as the time keyword. Iterate through the
                         * prime keys and grab the first one that is a time keyword. */
                        template = drms_template_record(drms_env, seriesKey, &istat);
                        if (istat != DRMS_SUCCESS || !template)
                        {
                            istat = DRMS_ERROR_UNKNOWNSERIES;
                            if (sunumArrSorted)
                            {
                                free(sunumArrSorted);
                                sunumArrSorted = NULL;
                            }
                            break;
                        }

                        /* Identify the time prime-key keyword. */
                        timeCol = NULL;
                        iKey = 0;
                        nPKeys = template->seriesinfo->pidx_num;

                        while (iKey < nPKeys)
                        {
                            pkey = template->seriesinfo->pidx_keywords[iKey];

                            if (drms_keyword_isindex(pkey))
                            {
                                /* Use slotted keyword */
                                pkey = drms_keyword_slotfromindex(pkey);
                            }

                            if (drms_keyword_gettype(pkey) == DRMS_TYPE_TIME)
                            {
                                /* Got it! */
                                timeCol = pkey->info->name;
                                break;
                            }

                            iKey++;
                        }
                    }

                    if (istat == DRMS_SUCCESS)
                    {
                        /* If there is no timeCol for this series, then continue onto the next series. We cannot filter this SUNUM based on
                         * its series' time keyword. */
                        if (!timeCol)
                        {
                            if (sunumArrSorted)
                            {
                                free(sunumArrSorted);
                                sunumArrSorted = NULL;
                            }
                            continue;
                        }
                    }

                    if (istat == DRMS_SUCCESS)
                    {
                        /* We have a sorted list of nSeriesSunums sunums for series seriesKey. */
                        nExe = nSeriesSunums / NUM_ARGS; /* integer division - this is the number of times we will execute the prepared insert statement. */

                        if (nExe > 0)
                        {
                            for (iArg = 0; iArg < NUM_ARGS; iArg++)
                            {
                                argin[iArg] = calloc(nExe, sizeof(db_int8_t));
                                intype[iArg] = DB_INT8;
                            }

                            iArg = 0;
                            iExe = 0;
                            isunum = 0;

                            while (isunum < nExe * NUM_ARGS)
                            {
                                sunum = sunumArrSorted[isunum];

                                if (iArg == NUM_ARGS)
                                {
                                    iArg = 0;
                                    iExe++;
                                }

                                memcpy(argin[iArg] + iExe * db_sizeof(intype[iArg]), &sunum, db_sizeof(intype[iArg]));
                                iArg++;
                                isunum++;
                            }

                            stsz = 512;
                            sql = calloc(stsz, sizeof(char));

                            if (sql)
                            {
                                sql = base_strcatalloc(sql, "SELECT sunum, min(", &stsz);
                                sql = base_strcatalloc(sql, timeCol, &stsz);
                                sql = base_strcatalloc(sql, ") AS mindate FROM ", &stsz);
                                sql = base_strcatalloc(sql, seriesKey, &stsz);
                                sql = base_strcatalloc(sql, " WHERE sunum IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) GROUP BY sunum", &stsz);

                                pBres = drms_query_bin_ntuple(drms_env->session, sql, nExe, NUM_ARGS, intype, (void **)argin);

                                for (iArg = 0; iArg < NUM_ARGS; iArg++)
                                {
                                    if (argin[iArg])
                                    {
                                        free(argin[iArg]);
                                        argin[iArg] = NULL;
                                    }
                                }

                                if (pBres)
                                {
                                    for (iBres = 0; iBres < nExe; iBres++)
                                    {
                                        bres = pBres[iBres];

                                        if (bres)
                                        {
                                            /* Now need to iterate through returned rows to see which sunums need to be filtered out. If an sunum
                                             * should be filtered out, then add it to the filterOut container. This container wil be consulted
                                             * in the sunum loop below.
                                             */
                                            for (irow = 0; irow < bres->num_rows; irow++)
                                            {
                                                sunum = db_binary_field_getlonglong(bres, irow, 0);
                                                timeVal = db_binary_field_getdouble(bres, irow, 1);

                                                /* Check timeval. */
                                                if ((hlookupGet = hcon_lookup_lower(seriesMinBadTimes, seriesKey)) != NULL)
                                                {
                                                    minBadTime = *((TIME *)hlookupGet);
                                                }
                                                else
                                                {
                                                    minBadTime = -1;
                                                }

                                                if ((hlookupGet = hcon_lookup_lower(seriesMaxBadTimes, seriesKey)) != NULL)
                                                {
                                                    maxBadTime = *((TIME *)hlookupGet);
                                                }
                                                else
                                                {
                                                    maxBadTime = -1;
                                                }

                                                if (minBadTime >= 0 && maxBadTime < 0)
                                                {
                                                    maxBadTime = INFINITY;
                                                }

                                                if (timeVal > minBadTime && timeVal < maxBadTime)
                                                {
                                                    snprintf(nbuf, sizeof(nbuf), "%lld", sunum);
                                                    if ((hlookupGet = hcon_lookup_lower(seriesAcceptRates, seriesKey)) != NULL)
                                                    {
                                                       acceptRate = *((int *)hlookupGet);
                                                       hcon_insert(filterOut, nbuf, &acceptRate);
                                                    }
                                                    else
                                                    {
                                                       hcon_insert(filterOut, nbuf, &zeroVal);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    db_free_binary_result_tuple(&pBres, nExe);
                                    pBres = NULL;
                                }

                                free(sql);
                                sql = NULL;
                            }
                        }
                    }

                    if (istat == DRMS_SUCCESS)
                    {
                        if (nSeriesSunums % NUM_ARGS != 0)
                        {
                            /* There are some left-overs not accounted for by the previous loop on records. This loop doesn't require
                             * using a prepared statement since it will be executed one time. */
                            stsz = 512;
                            sql = calloc(stsz, sizeof(char));

                            if (sql)
                            {
                                sql = base_strcatalloc(sql, "SELECT sunum, min(", &stsz);
                                sql = base_strcatalloc(sql, timeCol, &stsz);
                                sql = base_strcatalloc(sql, ") AS mindate FROM ", &stsz);
                                sql = base_strcatalloc(sql, seriesKey, &stsz);
                                sql = base_strcatalloc(sql, " WHERE sunum IN (", &stsz);

                                isunum = NUM_ARGS * nExe;
                                while (isunum < nSeriesSunums)
                                {
                                    sunum = sunumArrSorted[isunum];

                                    snprintf(nbuf, sizeof(nbuf), "%lld", sunum);
                                    sql = base_strcatalloc(sql, nbuf, &stsz);
                                    if (isunum < nSeriesSunums - 1)
                                    {
                                        sql = base_strcatalloc(sql, ", ", &stsz);
                                    }

                                    isunum++;
                                }

                                sql = base_strcatalloc(sql, ") GROUP BY sunum", &stsz);

                                /* Blast a dookie...uh, I mean, run the query.*/
                                if ((bres = drms_query_bin(drms_env->session, sql)) == NULL)
                                {
                                    fprintf(stderr, "Failed to obtain the earliest date of the data the SUS for series %s.\n", seriesKey);
                                    istat = DRMS_ERROR_QUERYFAILED;
                                }
                                else
                                {
                                    if (bres->num_cols != 2)
                                    {
                                        fprintf(stderr, "Unexpected query result.\n");
                                        istat = DRMS_ERROR_BADQUERYRESULT;
                                    }
                                    else
                                    {
                                        /* Now need to iterate through returned rows to see which sunums need to be filtered out. If an sunum
                                         * should be filtered out, then add it to the filterOut container. This container wil be consulted
                                         * in the sunum loop below. */
                                        for (irow = 0; irow < bres->num_rows; irow++)
                                        {
                                            sunum = db_binary_field_getlonglong(bres, irow, 0);
                                            timeVal = db_binary_field_getdouble(bres, irow, 1);

                                            /* Check timeval. */
                                            if ((hlookupGet = hcon_lookup_lower(seriesMinBadTimes, seriesKey)) != NULL)
                                            {
                                                minBadTime = *((TIME *)hlookupGet);
                                            }
                                            else
                                            {
                                                minBadTime = -1;
                                            }

                                            if ((hlookupGet = hcon_lookup_lower(seriesMaxBadTimes, seriesKey)) != NULL)
                                            {
                                                maxBadTime = *((TIME *)hlookupGet);
                                            }
                                            else
                                            {
                                                maxBadTime = -1;
                                            }

                                            if (minBadTime >= 0 && maxBadTime < 0)
                                            {
                                                maxBadTime = INFINITY;
                                            }

                                            if (timeVal > minBadTime && timeVal < maxBadTime)
                                            {
                                                snprintf(nbuf, sizeof(nbuf), "%lld", sunum);
                                                if ((hlookupGet = hcon_lookup_lower(seriesAcceptRates, seriesKey)) != NULL)
                                                {
                                                   acceptRate = *((int *)hlookupGet);
                                                   hcon_insert(filterOut, nbuf, &acceptRate);
                                                }
                                                else
                                                {
                                                   hcon_insert(filterOut, nbuf, &zeroVal);
                                                }
                                            }
                                        }
                                    }
                                }

                                db_free_binary_result(bres);
                                bres = NULL;
                                free(sql);
                                sql = NULL;
                            }
                            else
                            {
                                istat = DRMS_ERROR_OUTOFMEMORY;
                            }
                        }

                        if (sunumArrSorted)
                        {
                            free(sunumArrSorted);
                            sunumArrSorted = NULL;
                        }
                    }
                } /* filtered series loop */

                hiter_destroy(&hit);
            }
        }

        status = istat;

        /* What am I supposed to do if status != DRMS_SUCCESS? For now, it looks like we ignore filtering. */

        if (seriesSunums)
        {
            hcon_destroy(&seriesSunums);
        }

        if (seriesMinBadTimes)
        {
            hcon_destroy(&seriesMinBadTimes);
        }

        if (seriesMaxBadTimes)
        {
            hcon_destroy(&seriesMaxBadTimes);
        }

        if (seriesAcceptRates)
        {
           hcon_destroy(&seriesAcceptRates);
        }


    char onlinestat[128];
    long long dirsize;
    char supath[DRMS_MAXPATHLEN];
    char yabuff[64];
    int roll;
        int reject;

    /* Since we know how many sunums we are processing, allocate enough space for the sunums variable here. */
    sunums = calloc(nsunums, sizeof(long long));
    if (!sunums)
    {
        JSONDIE("Not enough memory to allocate SUNUM array for exp_su request.");
    }

    paths = calloc(nsunums, sizeof(char *));
    if (!paths)
    {
        JSONDIE("Not enough memory to allocate paths array for exp_su request.");
    }

    series = calloc(nsunums, sizeof(char *));
    if (!series)
    {
        JSONDIE("Not enough memory to allocate series array for exp_su request.");
    }

    /* This one is a little different, since each status code is a char, not a char *. */
    sustatus = calloc(nsunums, sizeof(char));
    if (!sustatus)
    {
        JSONDIE("Not enough memory to allocate sustatus array for exp_su request.");
    }

    susize = calloc(nsunums, sizeof(char *));
    if (!susize)
    {
        JSONDIE("Not enough memory to allocate susize array for exp_su request.");
    }

    arrsize = nsunums;

    char *partition_omit_pattern = "^/((SUM[0-9])|(SUM1[0-9])|(SUM2[0-2]))/";
    regex_t pp_regex;
    int filter_partition = 0;
    int omit = 0;

#if defined(FILTER_PARTITIONS) && FILTER_PARTITIONS
    if (!regcomp(&pp_regex, partition_omit_pattern, REG_EXTENDED | REG_NOSUB))
    {
        filter_partition = 1;
    }
#endif

    for (isunum = 0; isunum < nsunums; isunum++)
      {
      SUM_info_t *sinfo;
      TIME expire = 0;

      dirsize = 0;
      memset(onlinestat, 0, sizeof(onlinestat));
      snprintf(supath, sizeof(supath), "NA");

      sunum = sunumarr[isunum];

      sinfo = infostructs[isunum];

          /* ART FETCH BLOCK - Block requests for data of a certain T_REC. A table, su_producton.fetchblock (text seriesname,
           * timestamp mindate, timestamp maxdate), specifies a date range for each series whose records we wish to block
           * from download. Data for that series with a T_REC that falls within this range will be marked with a sustatus of 'N',
           * and all_online will be set to 0.
           *
           * Code above has figured out if the sunum needs to be filtered. filterOut is a container keyed by sunum. The container
           * values are the acceptance rate percentages. A value of 75 means that we want to accept 75% of the requests for
           * this SU. To reject a request, we return sustatus 'I' to the caller so that they do not attempt to download
           * the SU.
           */
          reject = 0;
          snprintf(nbuf, sizeof(nbuf), "%lld", sunum);
          if ((hlookupGet = hcon_lookup(filterOut, nbuf)) != NULL)
          {
              acceptRate = *((int *)hlookupGet);
              roll = dieRoll(100); /* random variable ~ [1, 100] uniformly */

              /* Reject this SUNUM if the number rolled on the die is above the acceptRate threshold. */
              if (roll > acceptRate)
              {
                  reject = 1;
                  *(sinfo->online_loc) = '\0';
              }
          }

      if (*(sinfo->online_loc) == '\0')
      {
          if (!reject)
          {
              *onlinestat = 'I';
          }
          else
          {
              *onlinestat = 'N';
          }

          sunums[count] = sunum; // XXXXXXXXXX
          paths[count] = strdup("NA");
          series[count] = strdup("NA");
          sustatus[count] = onlinestat[0];
          susize[count] = strdup("0");
          count++;
      }
      else
      {
          /* ART - temporarily block /SUM0.../SUM22 since these disks are old and could die if they are used heavily; */
          size += (long long)sinfo->bytes;
          dirsize = (long long)sinfo->bytes;

          omit = 0;

          if (filter_partition)
          {
              if (regexec(&pp_regex, sinfo->online_loc, 0, 0, 0) == 0)
              {
                  /* matches one of the disabled SUMS partitions */
                  *onlinestat = 'N';
                  all_online = 0;
                  omit = 1;
              }
          }

          if (!omit)
          {
              if (strcmp(sinfo->online_status,"Y")==0)
              {
                  int y,m,d,hr,mn;
                  char sutime[50];
                  sscanf(sinfo->effective_date,"%4d%2d%2d%2d%2d", &y,&m,&d,&hr,&mn);
                  sprintf(sutime, "%4d.%02d.%02d_%02d:%02d", y,m,d,hr,mn);
                  expire = (sscan_time(sutime) - fetch_time)/86400.0;
                  snprintf(supath, sizeof(supath), "%s", sinfo->online_loc);
                  *onlinestat = 'Y';
              }
              if (strcmp(sinfo->online_status,"N")==0 || expire < 3)
              {  // need to stage or reset retention time
                  if (strcmp(sinfo->archive_status, "N") == 0)
                  {
                      *onlinestat = 'X';
                  }
                  else
                  {
                      *onlinestat = 'N';
                      /* Don't set status of offline, unless there is at least one SU that is offline that can be brought back online. */
                      all_online = 0;
                  }
              }
          }

         sunums[count] = sunum; // XXXXXXXXXX
         paths[count] = strdup(supath);
         series[count] = strdup(sinfo->owning_series);
         sustatus[count] = onlinestat[0];
         snprintf(yabuff, sizeof(yabuff), "%lld", dirsize);
         susize[count] = strdup(yabuff);

         count += 1;
         }

      free(sinfo);
      sinfo = NULL;
      } /* isunum */

      if (filter_partition)
      {
          regfree(&pp_regex);
      }

        if (filterOut)
        {
            hcon_destroy(&filterOut);
        }

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

    if (strcmp(method,"url_quick")==0 && (strcmp(protocol,kOptProtocolAsIs)==0 || strcmp(protocol,kOptProtocolSuAsIs)==0)  && (all_online || dodataobj))
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
          numval[0] = sustatus[i];
          numval[1] = '\0';
          /* No need to escape sustatus - the char codes are all ASCII. */
          json_insert_pair_into_object(suobj, "sustatus", json_new_string(numval));
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
        sprintf(numval, "%lld", size);
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
        if (genWebPage)
        {
            printf("Content-type: application/json\n\n");
        }
        printf("%s\n",json);
        fflush(stdout);
        free(json);

        /* I think we can free jroot */
        json_free_value(&jroot);
        } /* dojson */
      else
        {
        int i;
        if (genWebPage)
        {
            printf("Content-type: text/plain\n\n");
        }
        printf("# JSOC Quick Data Export of as-is files.\n");
        printf("status=0\n");
        printf("requestid=\"%s\"\n", kNotSpecified);
        printf("method=%s\n", method);
        printf("protocol=%s\n", protocol);
        printf("wait=0\n");
        printf("count=%d\n", count);
        printf("size=%lld\n", size);
        printf("dir=/\n");
        printf("# DATA\n");
        for (i=0; i<count; i++)
          printf("%lld\t%s\t%s\t%c\t%s\n",sunums[i],series[i],paths[i], sustatus[i], susize[i]);
        }

      report_summary(Server, StartTime, Remote_Address, op, dsin, 0, internal, requireOnline, 0);
      if (!dodataobj || (sums_status == 1 || all_online))
        {
         /* If not a VSO request, we're done. If a VSO request, done if all online, or if SUMS is down.
          * Otherwise, continue below and start a new request for the items not online. */
            if (sustatus)
            {
                free(sustatus);
                sustatus = NULL;
            }

            if (sunums)
            {
                free(sunums);
                sunums = NULL;
            }

           CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
           return(0);
        }
      else if (strcmp(requestid, kNoAsyncReq) == 0) // user never wants full export of leftovers
         {
         if (sustatus)
         {
            free(sustatus);
            sustatus = NULL;
         }

         if (sunums)
         {
            free(sunums);
            sunums = NULL;
         }

         CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
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
    char jsocFetchPath[PATH_MAX];
    char reqidGenPath[PATH_MAX];
    char *binPath = NULL;

    memset(jsocFetchPath, '\0', sizeof(jsocFetchPath));

    /* This call to GetJsocRequestID is in kOpExpSu. */
    if (readlink("/proc/self/exe", jsocFetchPath, sizeof(jsocFetchPath)) == -1)
    {
        JSONDIE("Cannot obtain jsoc_fetch path.\n");
    }
    else
    {
        binPath = dirname(jsocFetchPath); // can modify jsocFetchPath
        snprintf(reqidGenPath, sizeof(reqidGenPath), "%s/GetJsocRequestID", binPath);
    }

    FILE *fp = popen(reqidGenPath, "r");

    if (fp)
    {
        if (fscanf(fp, "%s", new_requestid) != 1)
        {
            pclose(fp);
            JSONDIE("Cant get new RequestID");
        }

        pclose(fp);
    }
    else
    {
        JSONDIE("Cant get new RequestID");
    }

    strcat(new_requestid, "_SU");
    requestid = new_requestid;

    // Log this export request
    if (1)
      {
      char exportlogfile[1000];
      char timebuf[50];
      FILE *exportlog;
      sprintf(exportlogfile, "/home/jsoc/exports/tmp/%s.reqlog", requestid);
      exportlog = fopen(exportlogfile, "a");
      sprint_ut(timebuf, fetch_time);
      fprintf(exportlog,"XXXX New SU request started at %s\n", timebuf);
      fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
         getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
      fclose(exportlog);
      }

    if ( !requestid || !*requestid || strcmp(requestid, "none") == 0)
        JSONDIE("Must have valid requestID - internal error.");

    if (strcmp(dsin, kNotSpecified) == 0 && (!sunumarr || sunumarr[0] < 0))
        JSONDIE("Must have valid Recordset or SU set");

    // FORCE process to be su_export
    char *dataSetCol = NULL;
    size_t szDataSetCol = 512;
    char numBuf[64];
    char *processingCol = "n=0|no_op";

    dataSetCol = calloc(1, szDataSetCol);
    if (!dataSetCol)
    {
        JSONDIE("Out of memory creating Processing string.");
    }

    dataSetCol = base_strcatalloc(dataSetCol, "sunums=", &szDataSetCol);

    // Some SUs maybe online, but at least one is offline. Request that the export system process ALL SUs. This is slightly inefficient
    // since SUMS will have to query its db again to get paths that we already have right now. But this is only a minor inefficiency,
    // and it is much easier to send all SUs to the export system, so I think sending all SUs is justified. There should not be too many
    // SUs in this request to fit on a jsoc_export_SU_as_is command line (rs.py will break-up a large request into smaller ones).

    for (isunum = 0; isunum < nsunums; isunum++)
    {
        if (isunum > 0)
        {
            dataSetCol = base_strcatalloc(dataSetCol, ",", &szDataSetCol);
        }

        snprintf(numBuf, sizeof(numBuf), "%lld", ((long long signed int *)sunumarr)[isunum]);
        dataSetCol = base_strcatalloc(dataSetCol, numBuf, &szDataSetCol);
    }

    // Create new record in export control series
    // This will be copied into the cluster-side series on first use.
    /* jsoc.export_new */
    /* We are in kOpExpSu. */
    exprec = drms_create_record(drms_env, kExportSeriesNew, DRMS_PERMANENT, &status);
    if (!exprec)
      JSONDIE("Cant create new export control record");
    drms_setkey_string(exprec, "RequestID", requestid);
    drms_setkey_string(exprec, "DataSet", dataSetCol);
    drms_setkey_string(exprec, "Processing", processingCol);
    drms_setkey_string(exprec, "Protocol", protocol);
    drms_setkey_string(exprec, "FilenameFmt", filenamefmt);
    drms_setkey_string(exprec, "Method", method);
    drms_setkey_string(exprec, "Format", format);
    drms_setkey_time(exprec, "ReqTime", fetch_time);
    drms_setkey_time(exprec, "EstTime", fetch_time+10); // Crude guess for now
    drms_setkey_longlong(exprec, "Size", (int)size);
    drms_setkey_int(exprec, "Status", (testmode ? 12 : 2));

    if (dataSetCol)
    {
        free(dataSetCol);
        dataSetCol = NULL;
    }
    // drms_close_record(exprec, DRMS_INSERT_RECORD);
    } // end of exp_su

    /*  op == exp_request  */
    else if (strcmp(op,kOpExpRequest) == 0)
    {
        int status=0;
        int segcount = 0;
        int irec;
        int all_online = 1;
        int record_is_online = -1;
        int non_exportable_SU = -1;
        int intervention_needed = -1;
        int all_nearline = 0;
        char dsquery[DRMS_MAXQUERYLEN] = {0};
        char *tmp_recordset = NULL;
        size_t sz_tmp_recordset = 0;
        char *p;
        char *file, *filename;
        int filesize;
        DRMS_RecordSet_t *rs;
        int compressedStorage;
        int compressedDownload;
        char dieStr[1024] = {0};
        int caStatus = 0;
        int clStatus = 0; /* assume passed test */
        HContainer_t *argsCont = NULL;
        HIterator_t *hIter = NULL;
        void *pCmdParamArg = NULL;
        CmdParams_Arg_t *cmdParamArg = NULL;
        char cmdParamArgBuf[512] = {0};
        char series_lower[DRMS_MAXSERIESNAMELEN];
        LinkedList_t *pending_request_ids = NULL;
        ListNode_t *node = NULL;
        char *pending_request_id = NULL;
        char *pending_requests = NULL;
        size_t pending_requests_sz = 64;

        if (internal)
        {
           lfname = kLogFileExpReqInt;
        }
        else
        {
           lfname = kLogFileExpReqExt;
        }

        /* requestid was not provided on the command-line. It is created near the end of this code block. */
        LogReqInfo(lfname, instanceID, fileupload, op, dsin, requestid, dbhost, from_web, webarglist, fetch_time);

        /* Write all command-line argument values so we can debug. */
        argsCont = cmdparams_get_argscont(&cmdparams);
        if (argsCont)
        {
            hIter = hiter_create(argsCont);
            if (hIter)
            {
                while ((pCmdParamArg = hiter_getnext(hIter)) != NULL)
                {
                    cmdParamArg = (CmdParams_Arg_t *)pCmdParamArg;

                    if (cmdParamArg->strval)
                    {
                        snprintf(cmdParamArgBuf, sizeof(cmdParamArgBuf), "%s=%s\n", cmdParamArg->name ? cmdParamArg->name : "Unnamed", cmdParamArg->strval);
                    }

                    WriteLog(lfname, cmdParamArgBuf);
                }

                hiter_destroy(&hIter);
            }
        }

        caStatus = CheckEmailAddress(lfname, notify, &requestorid, dieStr, sizeof(dieStr));

        if (caStatus == 1)
        {
            if (gLogs)
            {
                hcon_destroy(&gLogs);
            }
            JSONDIE(dieStr);
        }
        else if (caStatus == 2 || caStatus == 3)
        {
            if (gLogs)
            {
               hcon_destroy(&gLogs);
            }
            JSONDIE2(dieStr, "");
        }

        pending_request_ids = list_llcreate(sizeof(pending_request_ids), NULL);
        pending_requests = calloc(1, pending_requests_sz);

        /* we know that the user is a registered export user - now reject the user if they have a pending request */
        if (instanceID >= 0)
        {
            /* instanceID is the only way to be 100% sure that jsoc_fetch was invoked in a web/CGI environment;
             * jsocextfetch.py sets instanceID directly (and then disables 'from_web' in jsoc_fetch by unsetting
             * QUERY_STRING); the jsoc_fetch CGI sets instanceID in QUERY_STRING
             *
             * it is possible that we are using the internal DB to satisfy an export request from the external DB
             * (the passthrough feature).
             */
            clStatus = CheckUserLoad(drms_env, notify, ipAddress, EXPORT_PENDING_REQUESTS_TIME_OUT, pending_request_ids);
        }

        if (clStatus == 1)
        {
            JSONDIE("error checking list of pending export requests");
        }
        else if (clStatus == 2)
        {
            /* make a string out of all request ids */
            list_llreset(pending_request_ids);
            while ((node = list_llnext(pending_request_ids)) != NULL)
            {
                pending_request_id = (char *)(node->data);

                if (*pending_request_id == '\0')
                {
                    pending_requests = base_strcatalloc(pending_requests, "[ UNKNOWN ]", &pending_requests_sz);
                }
                else
                {
                    pending_requests = base_strcatalloc(pending_requests, pending_request_id, &pending_requests_sz);
                }

                if (node->next)
                {
                    pending_requests = base_strcatalloc(pending_requests, ",", &pending_requests_sz);
                }
            }

            snprintf(dieStr, sizeof(dieStr), "User %s has %d pending export requests (%s); please wait until at least one request has completed before submitting a new one.", notify, list_llgetnitems(pending_request_ids), pending_requests);
            JSONDIE7(dieStr);
        }
        else if (clStatus == 3)
        {
            snprintf(dieStr, sizeof(dieStr), "There are too many requests from %s; please wait until one or more requests have completed before submitting a new one.", notify);
            JSONDIE7(dieStr);
        }

        list_llfree(&pending_request_ids);

        size=0;
        strncpy(dsquery,dsin,DRMS_MAXQUERYLEN);
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
          /* This block ensures that the record-set query has a DRMS filter. It adds an empty filter if need be. */
      if (index(dsquery,'[') == NULL)
        {
        char *cb = index(dsquery, '{');
        if (cb)
          {
          char *cbin = index(dsin, '{');
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
      } /* normal request */

        // Check for series existence. The jsoc_fetch cgi can be used outside of the
        // exportdata.html context, in which case there is no check for series existence
        // before we reach this point in code. Let's catch bad-series errors here
        // so we can tell the user that they're trying to export a non-existent
        // series.
        char mbuf[1024] = {0};
        char *allvers = NULL;
        char **sets = NULL;
        DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
        char **snames = NULL;
        char **filts = NULL;
        char **segs = NULL;
        int nsets = 0;
        DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
        int iset;
        int firstlastExists = 0;
        const char *setName = NULL;
        char *seriesname = NULL;
        DRMS_Record_t *series_template = NULL;

        if (drms_record_parserecsetspec_plussegs(dsquery, &allvers, &sets, &settypes, &snames, &filts, &segs, &nsets, &rsinfo) == DRMS_SUCCESS)
        {
            /* Below, we need to know whether or not there was a FIRSTLAST symbol in the record-set specification.
             * But because record-set subsets may exist, we need to iterate through them. If any subset contains
             * a FIRSTLAST symbol, set a flag indicating that and check the flag below.
             *
             * Although drms_record_parserecsetspec() is a rec-set parser, it does not recognize the FIRSTLAST symbols, so
             * we need to use drms_recordset_query(), the definitive parser. We actually already parsed the info we need,
             * but that info gets discarded and not saved into the record-set struct.
             */
            char *query = NULL;
            char *pkwhere = NULL;
            char *npkwhere = NULL;
            int filter;
            int mixed;
            HContainer_t *firstlast = NULL;
            HContainer_t *pkwhereNFL = NULL;
            int recnumq;
            static regex_t *quality_reg_expression = NULL;
            const char *quality_pattern = NULL;

            if (nsets > 0)
            {
                setName = snames[0];
                sz_tmp_recordset = strlen(dsquery) + 1;
                tmp_recordset = calloc(sz_tmp_recordset, sizeof(char));
            }

            for (iset = 0; iset < nsets; iset++)
            {
                if (strcmp(setName, snames[iset]) != 0)
                {
                    snprintf(mbuf, sizeof(mbuf), "Cannot specify subsets of records from different series.\n");
                    JSONDIE(mbuf);
                }

                if (!drms_series_exists(drms_env, snames[iset], &status))
                {
                    snprintf(mbuf, sizeof(mbuf), "Cannot export series '%s' - it does not exist.\n", snames[iset]);
                    JSONDIE(mbuf);
                }

                status = drms_recordset_query(drms_env, sets[iset], &query, &pkwhere, &npkwhere, &seriesname, &filter, &mixed, NULL, &firstlast, &pkwhereNFL, &recnumq);
                if (status != DRMS_SUCCESS)
                {
                    snprintf(mbuf, sizeof(mbuf), "Bad record-set subset specification '%s'.\n", sets[iset]);
                    JSONDIE(mbuf);
                }

                /* Iterate through firstlast, looking for anything that isn't 'N'. */
                if (firstlast)
                {
                    HIterator_t *hiter = NULL;
                    char *code = NULL;

                    hiter = hiter_create(firstlast);
                    if (hiter)
                    {
                        while ((code = hiter_getnext(hiter)) != NULL)
                        {
                            if (*code != 'N')
                            {
                                firstlastExists = 1;
                                break;
                            }
                        }

                        hiter_destroy(&hiter);
                    }
                    else
                    {
                        JSONDIE("Out of memory.\n");
                    }
                }

                if (!omit_quality_check)
                {
                    series_template = drms_template_record(drms_env, seriesname, &status);

                    if (status != DRMS_SUCCESS)
                    {
                        snprintf(mbuf, sizeof(mbuf), "cannot export series '%s' - it does not exist\n", seriesname);
                        JSONDIE(mbuf);
                    }

                    if (hcon_member_lower(&series_template->keywords, "quality"))
                    {
                        /* scan the record-set specification (sets[iset]) looking for 'quality'; if there, do nothing, otherwise
                         * force a check for image existence into the record-set specification */
                        if (!quality_reg_expression)
                        {
                            quality_pattern = "\\[(\\?|\\?[[:print:]]*\\W)quality(\\?|\\W[[:print:]]*\\?)\\]";

                            /* ART - this does not get freed! */
                            quality_reg_expression = calloc(1, sizeof(regex_t));
                            if (quality_reg_expression)
                            {
                                if (regcomp(quality_reg_expression, quality_pattern, REG_EXTENDED | REG_ICASE) != 0)
                                {
                                    JSONDIE2("cannot compile regular expression", quality_pattern);
                                }
                            }
                            else
                            {
                                JSONDIE("out of memory");
                            }
                        }

                        if(regexec(quality_reg_expression, sets[iset], 0, NULL, 0) != 0)
                        {
                            /* no match - no 'quality' substring in specification */
                            if (tmp_recordset)
                            {
                                tmp_recordset = base_strcatalloc(tmp_recordset, seriesname, &sz_tmp_recordset);
                                if (filts && filts[iset] && *filts[iset] != '\0')
                                {
                                    tmp_recordset = base_strcatalloc(tmp_recordset, filts[iset], &sz_tmp_recordset);
                                }

                                /* force quality check; just add it! this works! */
                                tmp_recordset = base_strcatalloc(tmp_recordset, "[? drms.image_exists(quality) ?]", &sz_tmp_recordset);

                                if (segs && segs[iset] && *segs[iset] != '\0')
                                {
                                    tmp_recordset = base_strcatalloc(tmp_recordset, segs[iset], &sz_tmp_recordset);
                                }

                                if (iset + 1 < nsets)
                                {
                                    tmp_recordset = base_strcatalloc(tmp_recordset, ",", &sz_tmp_recordset);
                                }
                            }
                        }
                    }
                }

                if (query)
                {
                    free(query);
                }

                if (pkwhere)
                {
                    free(pkwhere);
                }

                if (npkwhere)
                {
                    free(npkwhere);
                }

                if (seriesname)
                {
                    free(seriesname);
                }

                if (firstlast)
                {
                    hcon_destroy(&firstlast);
                }

                if (pkwhereNFL)
                {
                    hcon_destroy(&pkwhereNFL);
                }
            }
        }
        else
        {
            snprintf(mbuf, sizeof(mbuf), "Bad record-set query '%s'.\n", dsquery);
            JSONDIE(mbuf);
        }

        if (tmp_recordset)
        {
            if (*tmp_recordset != '\0')
            {
                snprintf(dsquery, sizeof(dsquery), "%s", tmp_recordset);
            }

            free(tmp_recordset);
            tmp_recordset = NULL;
        }

        if (rcountlimit == 0)
        {
            rs = drms_open_records(drms_env, dsquery, &status);
        }
        else
        {
            // rcountlimit specified via "n=" parameter in process field.
            rs = drms_open_nrecords (drms_env, dsquery, rcountlimit, &status);
        }

        if (!rs)
        {
            int tmpstatus = status;
            rcount = drms_count_records(drms_env, dsquery, &status);
            if (status == 0)
            {
                // open_records failed but query is OK so probably too many records for memory limit.
                char errmsg[128] = {0};

                sprintf(errmsg, "%d is too many records in one request.", rcount);
                drms_record_freerecsetspecarr_plussegs(&allvers, &sets, &settypes, &snames, &filts, &segs, nsets);
                JSONDIE2("Can not open RecordSet ", errmsg);
            }
            status = tmpstatus;
            drms_record_freerecsetspecarr_plussegs(&allvers, &sets, &settypes, &snames, &filts, &segs, nsets);
            JSONDIE2("Can not open RecordSet, bad query or too many records: ", dsquery);
        }

        /* We've got the records to be exported open already - if the user has used a FIRSTLAST symbol, like '$' or '^',
         * then convert the record-set specification to one that contains a list of recnums. There are some bugs in
         * jsoc_export_manage having to do with characters like '$' gumming up DB queries and shell command-lines. Let's
         * get rid of these characters as much as possible as upstream as possible.
         *
         * Even if there is no first-last symbol, we still want to convert to a record list so that we can create
         * a hash of the record-set query so that we can effectively compare export requests for identity.
         * Do not use dsquery as is for this purpose. It may have wildcards, like '$', in it which make it difficult to perform
         * future comparisons, i.e., dsquery is not canonical. To convert dsquery to the canonical form, we have to open the
         * records (drms_open_records()) and make a list of recnums for each series described by dsquery.
         */
        char *newSpec = NULL;
        size_t szNewSpec = DRMS_MAXQUERYLEN;
        char recnumStr[64];

        newSpec = calloc(1, szNewSpec);

        if (newSpec) newSpec = base_strcatalloc(newSpec, setName, &szNewSpec);
        if (newSpec) newSpec = base_strcatalloc(newSpec, "[", &szNewSpec);

        for (irec = 0; irec < rs->n && newSpec; irec++)
        {
            snprintf(recnumStr, sizeof(recnumStr), "%lld", rs->records[irec]->recnum);

            if (irec == 0)
            {
                newSpec = base_strcatalloc(newSpec, ":#" , &szNewSpec);
            }
            else
            {
                newSpec = base_strcatalloc(newSpec, ",#" , &szNewSpec);
            }

            if (newSpec) newSpec = base_strcatalloc(newSpec, recnumStr, &szNewSpec);
        }

        if (newSpec) newSpec = base_strcatalloc(newSpec, "]", &szNewSpec);

        if (segs && segs[0])
        {
            if (newSpec) newSpec = base_strcatalloc(newSpec, segs[0], &szNewSpec);
        }

        if (!newSpec)
        {
            JSONDIE("Out of memory.\n");
        }

        if (firstlastExists)
        {
            snprintf(dsquery, sizeof(dsquery), "%s", newSpec);
        }

        rcount = rs->n;

        /* Check for duplicate exports within the last DUP_EXPORT_WINDOW hours. If a duplicate is found, then simply return the
         * request ID to the caller. Use the most recent duplicate. */

        /* As far as I can tell, the format parameter is not used by jsoc_fetch all at. And formatvar is used to to set dodataobj
         * only. And dodataobj isn't something used to identify the set of data being exported. To be a duplicate
         * export, this new export request's record-set, export-file-name format, protocol, and method have to match
         * the same parameters of an existing export. The SU for the existing export also must still be on disk. */

        /* existRec is in jsoc.export, not jsoc.export_new! */
        char *existReqID = NULL;
        TIME timeToCompletion;
        int expSize;
        size_t szInput = 64;
        char *md5Input = NULL;
        unsigned char md5[MD5_DIGEST_LENGTH]; // A byte string.
        char md5Str[2 * MD5_DIGEST_LENGTH + 1]; // A null-terminated hex string representation of the md5.

        /* create md5 from canonicalQuery, filenamefmt, process, protocol, and method */
        memset(md5, '\0', sizeof(md5));
        md5Input = calloc(szInput, sizeof(char));

        if (md5Input) md5Input = base_strcatalloc(md5Input, newSpec, &szInput);

        if (filenamefmt && *filenamefmt)
        {
            if (md5Input) md5Input = base_strcatalloc(md5Input, filenamefmt, &szInput);
        }

        if (process && *process)
        {
            if (md5Input) md5Input = base_strcatalloc(md5Input, process, &szInput);
        }

        if (protocol && *protocol)
        {
            if (md5Input) md5Input = base_strcatalloc(md5Input, protocol, &szInput);
        }

        if (method && *method)
        {
            if (md5Input) md5Input = base_strcatalloc(md5Input, method, &szInput);
        }

        if (md5Input)
        {
            MD5((const unsigned char *)md5Input, strlen(md5Input), md5);

            // Ack C!!
            int ibyte;

            for (ibyte = 0; ibyte < MD5_DIGEST_LENGTH; ibyte++)
            {
                snprintf(md5Str + ibyte * 2, sizeof(md5Str) - ibyte * 2, "%02x", md5[ibyte]);
            }
        }
        else
        {
            JSONDIE("Out of memory.\n");
        }

        existReqID = GetExistReqID(drms_env, md5Str, DUP_EXPORT_WINDOW, &timeToCompletion, &expSize);

        free(newSpec);


	/* We need rcount before we can return duplicate-export information back to the caller. */
        if (existReqID)
        {
            char numBuf[64];
            char *jsonStr = NULL;
            char *jsonText = NULL;
            json_t *jroot = NULL;
            TIME waitTime = 0;

            if (dojson)
            {
                jroot = json_new_object();

                if (!jroot)
                {
                    JSONDIE("Out of memory.");
                }

                sprintf(numBuf, "%d", testmode ? 12 : 2);
                json_insert_pair_into_object(jroot, "status", json_new_number(numBuf));

                jsonStr = string_to_json((char *)existReqID);
                json_insert_pair_into_object(jroot, kArgRequestid, json_new_string(jsonStr));
                free(jsonStr);

                jsonStr = string_to_json((char *)method);
                json_insert_pair_into_object(jroot, kArgMethod, json_new_string(jsonStr));
                free(jsonStr);

                jsonStr = string_to_json((char *)protocol);
                json_insert_pair_into_object(jroot, kArgProtocol, json_new_string(jsonStr));
                free(jsonStr);

                waitTime = timeToCompletion - timenow();
                if (waitTime < 0)
                {
                    waitTime = 0;
                }

                sprintf(numBuf, "%1.0lf",  waitTime);
                json_insert_pair_into_object(jroot, "wait", json_new_number(numBuf));

                sprintf(numBuf, "%d", rcount);
                json_insert_pair_into_object(jroot, "rcount", json_new_number(numBuf));

                sprintf(numBuf, "%d", expSize);
                json_insert_pair_into_object(jroot, "size", json_new_number(numBuf));

                json_tree_to_string(jroot, &jsonText);

                json_free_value(&jroot);

                if (genWebPage)
                {
                    if (fileupload)
                    {
                        // The returned json should be in the implied <body> tag for iframe requests.
                        printf("Content-type: text/html\n\n");
                    }
                    else
                    {
                        printf("Content-type: application/json\n\n");
                    }
                }

                if (jsonText)
                {
                    printf("%s\n", jsonText);
                    free(jsonText);
                }
            }
            else
            {
                if (genWebPage)
                {
                    printf("Content-type: text/plain\n\n");
                }
                printf("# JSOC Data Export Not Ready.\n");
                printf("status=%d\n", testmode ? 12 : 2);
                printf("requestid=%s\n", existReqID);
                printf("method=%s\n", method);
                printf("protocol=%s\n", protocol);
                printf("wait=%f\n", timeToCompletion);
                printf("size=%d\n", expSize);
            }

            fflush(stdout);

            CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
            free(existReqID);
            existReqID = NULL;
            drms_record_freerecsetspecarr_plussegs(&allvers, &sets, &settypes, &snames, &filts, &segs, nsets);
            drms_close_records(rs, DRMS_FREE_RECORD);
            return(0);
        }

        drms_record_freerecsetspecarr_plussegs(&allvers, &sets, &settypes, &snames, &filts, &segs, nsets);

    drms_stage_records(rs, 0, 0);

        /* drms_record_getinfo() will now fill-in the rec->suinfo for all recs in rs, PLUS all
         * linked records (so if rs is an aia.lev1_euv_12s rs, then records in aia.lev1 will
         * get their rec->suinfo structs initialized). */
    drms_record_getinfo(rs);

    // Do survey of recordset
// this section should be rewritten to first check in each recordset chunk to see if any
// segments are linked, if not, then just use the sunums from stage reocrds.
// when stage_records follows links, this will be quicker...
// then only check each seg if needed.
    all_online = 1;
    all_nearline = 1;
    non_exportable_SU = 0;
    intervention_needed = 0;
    compressedStorage = -1;

    /* iterate through all records and all segments; if `reject_if_offline` is false, then
     * if `record_is_online` is false, remove the record from the requested record set */
    for (irec = 0; irec < rcount; irec++)
    {
        // Must check each segment since some may be linked and/or offline.
        DRMS_Segment_t *seg = NULL;
        HIterator_t *segp = NULL;
        DRMS_Record_t *tRec = NULL;
        DRMS_Record_t *segrec = NULL;
        SUM_info_t *sinfo = NULL;
        DRMS_Segment_t *tSeg = NULL;
        DRMS_Record_t *rec = drms_recordset_fetchnext(drms_env, rs, &status, NULL, NULL);

        if (!rec)
        {
            /* Exit rec loop - last record was fetched last time. */
            break;
        }

        if (*rec->seriesinfo->seriesname != '\0')
        {
            snprintf(series_lower, sizeof(series_lower), "%s", rec->seriesinfo->seriesname);
            strtolower(series_lower);
        }

        // Disallow exporting jsoc.export* series
        if (strncmp(rec->seriesinfo->seriesname, "jsoc.export", 11)== 0)
        {
            JSONDIE("Export}} of jsoc_export series not allowed.");
        }

        // ART - hack: force all_nearline true if the series is iris.lev1 for all records; eventually
        // we will need to talk to SUMS to figure out if
        else if (!series_lower || !IsNearlineSeries(series_lower))
        {
            all_nearline = 0;
        }

          /* Don't follow links when obtaining the segments.
           * If you do that, and there is a problem following the
           * link (e.g., the link is not set), then the
           * function returns NULL, which would terminate the
           * while loop. But in this case, we want to continue
           * with the next segment. It is as if the segment
           * file does not exist, which shouldn't cause this
           * loop to terminate.
           */

        record_is_online = 1;

        while (seg = drms_record_nextseg(rec, &segp, 0))
        {
            /* If this is a linked segment, then follow the link. */
            if (seg->info->islink)
            {
                /* Do not use drms_link_follow(). That API function does not recursively follow links. And it
                 * does not fetch the target segment in the linked record. drms_segment_lookup() does
                 * both of these tasks. */
                tSeg = drms_segment_lookup(rec, seg->info->name);

                if (!tSeg)
                {
                    /* No link set for this record, or segment struct is missing from target record. */

                    /* The whole point of this section of code is to:
                    * 1. Determine if all segment files are online,
                    * 2. See if there is at least one segment file to export, and
                    * 3. Count the number of bytes in all the segment files being exported.
                    *
                    * So, if there is no link, this is the same as saying there is no segment file. */
                    status = DRMS_SUCCESS;
                    continue;
                }

                tRec = tSeg->record;

                /* We have a single segment linked to a segment in another record. */
                segrec = tRec; /* Use the target rec (aia.lev1), not the source rec (aia.lev1_euv_12s). */
            }
            else
            {
                tSeg = seg;
                segrec = seg->record;
            }

          /* If all segments being exported are stored uncompressed, note that. This will determine which payload limit is used. Err on
           * the conservative side. The smaller data limits are used if data are stored compressed (and subsequently exported), so
           * pretend the whole series comprises compressed data if a single segment is compressed. */
          if (compressedStorage == -1)
          {
             /* Determine this only for the first record */
             if (strstr(tSeg->cparms, "compress"))
             {
                compressedStorage = 1;
             }
          }

          sinfo = segrec->suinfo;

          if (!sinfo)
          {
              /* There was a request for an sunum of -1. */
              all_online = 0;
              all_nearline = 0;
              record_is_online = 0;
              non_exportable_SU = 1;

              /* There is no SU. Break out of segment loop. */
              break;
          }
          else if (*(sinfo->online_loc) == '\0')
          {
              /* If we get here, then we have a bad SUNUM. */
              fprintf(stderr, "JSOC_FETCH Bad sunum %lld for recnum %s:%lld in RecordSet: %s\n", segrec->sunum, segrec->seriesinfo->seriesname, segrec->recnum, dsquery);
              // no longer die here, leave it to the export process to deal with missing segments
              all_online = 0;
              all_nearline = 0;
              record_is_online = 0;
              non_exportable_SU = 1;

              /* Bad SUNUM. Break out of segment loop. */
              break;
          }
          else if (strcmp(sinfo->online_status,"N") == 0)
          {
              /* Good SUNUM, but data are offline. We should estimate the segment size by using the entire SU size. There is nothing else we can
               * do since we cannot bring the SU online in jsoc_fetch (it is a CGI program that needs to return quickly) and SUMS does
               * not store file-size info within the SU. We should break out of the segment loop here anyway - there is no SU. */
              all_online = 0;
              record_is_online = 0;
              // all_nearline = ?; we need a new SUMS table to provide this informaton
              /* Use sinfo to get SU size info. Must cast double to 64-bit integer - SUMS design problem. */
              size += (long long)sinfo->bytes;
              if ((long long)sinfo->bytes > 0)
              {
                  /* Assume the user wants a least one segment file in the SU. */
                  segcount += 1;
              }

              /* Good SUNUM, but offline. Break out of segment loop. */
              break;
          }
          else
          {
              /* The SU is online. */
              struct stat buf;
              char path[DRMS_MAXPATHLEN];
              //drms_record_directory(segrec, path, 0); I think this was a typo.
              drms_segment_filename(tSeg, path);
              if (stat(path, &buf) != 0)
              { // segment specified file is not present.
                  // it is a WARNING if the record and QUALITY >=0 but no file matching
                  // the segment file name unless the filename is empty.
                  if (*(tSeg->filename))
                  {
                      DRMS_Keyword_t *quality = drms_keyword_lookup(segrec, "QUALITY",1);
                      if (quality && drms_getkey_int(segrec, "QUALITY", 0) >= 0)
                      { // there should be a file
                          fprintf(stderr,"QUALITY >=0, filename=%s, but %s not found\n",tSeg->filename,path);
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
        } // segment loop

        if (requireOnline && !all_online && !all_nearline)
        {
            if (non_exportable_SU)
            {
                /* invalid SU - this isn't really expected if the QUALITY flag check is part of the export request */
                if (sinfo->sunum)
                {
                    char sunum_buf[64] = {0};

                    snprintf(sunum_buf, sizeof(sunum_buf), "%lld", sinfo->sunum);
                    JSONDIE2("One or more Storage Units are invalid", sunum_buf);
                }
                else
                {
                    JSONDIE("One or more records contain no Storage Units");
                }
            }
            else
            {
                /* we are not going to be able to complete the export, so leave the record loop now */
                intervention_needed = 1;
                break;
            }
        }

        if (compressedStorage == -1)
        {
            /* No segment was stored compressed. */
            compressedStorage = 0;
        }

        if (segp)
        {
            hiter_free(segp);
            free(segp);
            segp = NULL;
        }
    } // record loop

        if (size > 0 && size < 1024*1024) size = 1024*1024;
        size /= 1024*1024;

    // Exit if no records found
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,kOptProtocolAsIs)==0 || strcmp(protocol,kOptProtocolSuAsIs)==0) || strcmp(protocol,"su")==0) && segcount == 0)
      JSONDIE("There are no files in this RecordSet");

    // Return status==3 if request is too large.

    /* We want to cap the download payload size to 100G. If there is no change in compression status, then the maximum size of
     * the stored file is 100 GB. But if a compressed file is going to be uncompressed, then downloaded, the maximum size of the
     * stored file is 10 GB (we assume a 10:1 compression ratio). If an uncompressed file is going to be compressed, then downloaded,
     * the maximum size of the stored file is 1000 GB (we assume a 10:1 compression ratio).
     *
     *             downloaded
     *           C            U
     *  s
     *  t
     *  o   C    M            S
     *  r
     *  e   U    L            M
     *  d
     *
     *
     * C ==> Compressed
     * U ==> Uncompressed
     *
     * L = large limit (1000 GB)
     * M = medium limit (100 GB)
     * S = small limit (10 GB)
     *
     *  -ART
     */
    long long sizeLimit; // Maximum size of STORED file.

    compressedDownload = (strstr(protocol, "**NONE**") == NULL);


    if (compressedStorage && !compressedDownload)
    {
        sizeLimit = MAX_UNCOMPRESSING_EXPORT_SIZE / sizeRatio;
    }
    else if (!compressedStorage && compressedDownload)
    {
        sizeLimit = MAX_COMPRESSING_EXPORT_SIZE / sizeRatio;
    }
    else
    {
        sizeLimit = MAX_STRAIGHT_EXPORT_SIZE / sizeRatio;
    }

    /* Can't figure out how sizeLimit can be -2^63, but it is here. Print out a lot of stuff. */
    WriteLog(lfname, "size is %lld.\n", size);
    WriteLog(lfname, "sizeRatio is %lf.\n", sizeRatio);
    WriteLog(lfname, "sizeLimit is %lld.\n", sizeLimit);

    if (size > sizeLimit)
      {
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
        sprintf(numval, "%d", rcount);
        json_insert_pair_into_object(jroot, "count", json_new_number(numval));
        sprintf(numval, "%d", (int)size);
        json_insert_pair_into_object(jroot, "size", json_new_number(numval));
        sprintf(numval,"Request exceeds max byte limit of %lldMB", sizeLimit);
        strval = string_to_json(numval);
        json_insert_pair_into_object(jroot, "error", json_new_string(strval));
        free(strval);
        json_tree_to_string(jroot,&json);
        if (genWebPage)
        {
            if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
                printf("Content-type: text/html\n\n");
            else
                printf("Content-type: application/json\n\n");
        }
        printf("%s\n",json);
        fflush(stdout);
        free(json);
        }
      else
        {
        if (genWebPage)
        {
            printf("Content-type: text/plain\n\n");
        }
        printf("# JSOC Data Export Failure.\n");
  	printf("status=3\n");
        printf("size=%lld\n",size);
        printf("count=%d\n",rcount);
  	printf("requestid=VOID\n");
  	printf("wait=0\n");
  	}

      CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
      return(0);
      }

    // Do quick export if possible
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,kOptProtocolAsIs)==0 || strcmp(protocol,kOptProtocolSuAsIs)==0) || strcmp(protocol,"su")==0) && all_online)
      {
      if (0 && segcount == 1) // If only one file then do immediate delivery of that file.
        {
           char sfpath[DRMS_MAXPATHLEN] = {0};
           int sfret = send_file(rs->records[0], 0, sfpath, sizeof(sfpath));
           if (sfret == 1)
           {
              JSONDIE2("Can not open file for export: ", sfpath);
           }
           else
           {
              CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
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
        if (genWebPage)
        {
            if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
                printf("Content-type: text/html\n\n");
            else
                printf("Content-type: application/json\n\n");
        }
        printf("%s\n",json);
        fflush(stdout);
        free(json);
        }
      else
        {
        if (genWebPage)
        {
            printf("Content-type: text/plain\n\n");
        }
        printf("# JSOC Quick Data Export of as-is files.\n");
  	printf("status=0\n");
  	printf("requestid=\"%s\"\n", kNotSpecified);
  	printf("method=%s\n", method);
  	printf("protocol=%s\n", protocol);
  	printf("wait=0\n");
        quick_export_rs(NULL, rs, 0, size); // add count, size, and array data of names and paths
  	}
      CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);
      return(0);
    } /* end url quick */

        // Must do full export processing, unless intervention is needed; if intervention is needed, then still
        // create the record in jsoc.export_new so the person checking for intervention can see the
        // record set that had the issue

        // Get RequestID
        char jsocFetchPath[PATH_MAX];
        char reqidGenPath[PATH_MAX];
        char *binPath = NULL;

        memset(jsocFetchPath, '\0', sizeof(jsocFetchPath));

        /* This call to GetJsocRequestID is in kOpExpRequest. */
        if (readlink("/proc/self/exe", jsocFetchPath, sizeof(jsocFetchPath)) == -1)
        {
            JSONDIE("Cannot obtain jsoc_fetch path.\n");
        }
        else
        {
            binPath = dirname(jsocFetchPath); // can modify jsocFetchPath
            snprintf(reqidGenPath, sizeof(reqidGenPath), "%s/GetJsocRequestID", binPath);
        }

        FILE *fp = popen(reqidGenPath, "r");

        if (fp)
        {
            if (fscanf(fp, "%s", new_requestid) != 1)
            {
                pclose(fp);
                JSONDIE("Cant get new RequestID");
            }

            pclose(fp);
        }
        else
        {
            JSONDIE("Cant get new RequestID");
        }

        if (strcmp(dbhost, SERVER) == 0)
        {
            if (passthrough)
            {
                /* Denote a pass-through export by inserting an X in the request ID. We need to distinguish this type of export
                * from an internal export (even though use the internal database) because the export web page needs to distinguish
                * them. */
                strcat(new_requestid, "_X");
            }

            strcat(new_requestid, "_IN");
        }

        requestid = new_requestid;

        /* Insert a row into jsoc.export_md5 for this new request. If there is a failure in jsoc_export_manage, we'll have to
         * delete this row. */
        char cmd[DRMS_MAXQUERYLEN];

        if (!intervention_needed)
        {
            /* if intervention is needed, then all we want to do is to add a record to jsoc.export_new (with status = X) */
            char timeStrBuf[64];
            time_t secs;
            struct tm *currentUT;

            secs = time(NULL);
            currentUT = gmtime(&secs);

            strftime(timeStrBuf, sizeof(timeStrBuf), "%Y-%m-%d %H:%M:%S UTC", currentUT);
            snprintf(cmd, sizeof(cmd), "INSERT INTO jsoc.export_md5 (md5, requestid, exporttime) VALUES('%s', '%s', '%s')", md5Str, requestid, timeStrBuf);
            if (drms_dms(drms_env->session, NULL, cmd))
            {
               fprintf(stderr, "Failure obtaining estimated completion time and size: %s.\n", cmd);
               JSONDIE("Cant save new export-request hash.");
            }

            // Log this export request
            if (1)
            {
                char exportlogfile[1000];
                char timebuf[50];
                FILE *exportlog;
                sprintf(exportlogfile, "/home/jsoc/exports/tmp/%s.reqlog", requestid);
                exportlog = fopen(exportlogfile, "w");
                sprint_ut(timebuf, fetch_time);
                fprintf(exportlog,"XXXX New export request started at %s\n", timebuf);
                fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
                getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
                fclose(exportlog);
            }
        }

        /* jsoc_fetch no longer manages jsoc.export_user; instead, when the user registers an email address,
         * they also register a user name, and a snail-mail address too; these values are stored in jsoc.export_user_info
         * (internal DB only) by the email-registration code; when a user enters their registered email address into
         * the exportdata.html text box, the user-name and snail text boxes get filled and made read-only; they can
         * register their name and snail only if they are also registering a new email address */

        // Create new record in export control series
        // This will be copied into the cluster-side series on first use.
        if ( !requestid || !*requestid || strcmp(requestid, "none") == 0)
        {
            JSONDIE("Must have valid requestID - internal error.");
        }

        if (strcmp(dsin, "Not Specified") == 0)
        {
            JSONDIE("Must have Recordset specified");
        }

        /* jsoc.export_new */
        /* We are in kOpExpRequest. */
        exprec = drms_create_record(drms_env, kExportSeriesNew, DRMS_PERMANENT, &status);
        if (!exprec)
        {
            JSONDIE("Cant create new export control record");
        }

        drms_setkey_string(exprec, "RequestID", requestid);
        drms_setkey_string(exprec, "DataSet", dsquery);
        drms_setkey_string(exprec, "Processing", process);
        drms_setkey_string(exprec, "Protocol", protocol);
        drms_setkey_string(exprec, "FilenameFmt", filenamefmt);
        drms_setkey_string(exprec, "Method", method);
        drms_setkey_string(exprec, "Format", format);
        drms_setkey_time(exprec, "ReqTime", fetch_time);
        drms_setkey_time(exprec, "EstTime", fetch_time+10); // Crude guess for now
        drms_setkey_longlong(exprec, "Size", (int)size);

        if (intervention_needed)
        {
            drms_setkey_int(exprec, "Status", (int)EXPORT_REQUEST_STATUS_INTERVENTION);
        }
        else
        {
            drms_setkey_int(exprec, "Status", (testmode ? 12 : 2));
        }

        /* we are in kOpExpRequest */
        /* set `requestor` column in jsoc.export_new; requestorid is also id in jsoc.export_user_info - the two tables can be joined;
         * `requestorid` was obtained from jsoc.export_user_info by the call to check the user's email address;
         * if the user's address has not been inserted into jsoc.export_user_info, then requestorid can be -1 - this
         * should only happen while transitioning to the new user-info code
         */
        drms_setkey_int(exprec, "requestor", requestorid);

        if (intervention_needed)
        {
            if (exprec)
            {
                drms_close_record(exprec, DRMS_INSERT_RECORD);
            }

            JSONDIE25("One or more data files are offline; please send a request for help to jsoc@sun.stanford.edu and we will place the requested data online, and then you may try again; ", requestid);
        }
        else
        {
            /* insert new row into the pending-requests table (if the user is exempt, then there could be other rows in this table) */
            snprintf(cmd, sizeof(cmd), "INSERT INTO %s(address, ip_address, request_id, start_time) VALUES('%s', '%s', '%s', CURRENT_TIMESTAMP)", EXPORT_PENDING_REQUESTS_TABLE, notify, ipAddress, requestid);
            if (drms_dms(drms_env->session, NULL, cmd))
            {
                fprintf(stderr, "WARNING: Failure inserting row into %s [%s]\n", EXPORT_PENDING_REQUESTS_TABLE, cmd);
                /* don't kill the whole export if we can't insert into the pending list; the worst that happens is that
                * we won't protect against users submitting too many export requests */
            }
        }
    } // End of kOpExpRequest setup

    /*  op == exp_repeat  */
    else if (strcmp(op,kOpExpRepeat) == 0)
    {
        char logpath[DRMS_MAXPATHLEN];

        /* requestid must be provided on the command-line. It is not created by jsoc_fetch. */
        if (strcmp(requestid, kNotSpecified) == 0)
        {
            JSONDIE("RequestID must be provided");
        }

        // Log this re-export request
        if (1)
        {
            char exportlogfile[1000];
            char timebuf[50];
            FILE *exportlog;

            snprintf(exportlogfile, sizeof(exportlogfile), "/home/jsoc/exports/tmp/%s.reqlog", requestid);
            exportlog = fopen(exportlogfile, "a");

            if (exportlog)
            {
                sprint_ut(timebuf, fetch_time);
                fprintf(exportlog,"XXX New repeat request started at %s\n", timebuf);
                fprintf(exportlog,"REMOTE_ADDR=%s\nHTTP_REFERER=%s\nREQUEST_METHOD=%s\nQUERY_STRING=%s\n",
                getenv("REMOTE_ADDR"), getenv("HTTP_REFERER"), getenv("REQUEST_METHOD"), getenv("QUERY_STRING"));
                fclose(exportlog);
                exportlog = NULL;
            }
        }

        JSONDIE("Re-Export requests temporarily disabled.");

        // First check status in jsoc.export
        snprintf(status_query, sizeof(status_query), "%s[%s]", kExportSeries, requestid);
        exports = drms_open_records(drms_env, status_query, &status);
        if (!exports)
        {
            JSONDIE3("Cant locate export series: ", status_query);
        }

        if (exports->n < 1)
        {
            JSONDIE3("Cant locate export request: ", status_query);
        }

        /* jsoc.export */
        status = drms_getkey_int(exports->records[0], "Status", NULL);
        if (status != 0)
        {
            JSONDIE("Can't re-request a failed or incomplete prior request");
        }

        // if sunum and su exist, then just want the retention updated.  This will
        // be accomplished by checking the record_directory.

        // exprec is NULL - maybe it should be exports->records[0] - ART
        if (drms_record_directory(exprec, logpath, 0) != DRMS_SUCCESS || *logpath == '\0')
        {  // really is no SU so go ahead and resubmit the request
            drms_close_records(exports, DRMS_FREE_RECORD);
            exports = NULL;

            /* check email registration, as in kOpExpRequest */

            // Now switch to jsoc.export_new
            // I think that this code requires that we do not delete old jsoc.export_new records, but we do delete them to
            // make jsoc_export_manage run quickly, and because the same information exists in jsoc.export - ART
            // I think this code clones the existing jsoc.export_new record, and then sets its status to 2 so that
            // jsoc_export_manage will re-do the export - ART
            sprintf(status_query, "%s[%s]", kExportSeriesNew, requestid);

            // For exp_repeat, the exprec will not have been opened yet.
            exports = drms_open_records(drms_env, status_query, &status);

            if (!exports)
            {
                JSONDIE3("Cant locate export series: ", status_query);
            }
            if (exports->n < 1)
            {
                JSONDIE3("Cant locate export request: ", status_query);
            }

            /* exprec is a new record in jsoc.export_new; this is kOpExpRepeat */
            exprec = drms_clone_record(exports->records[0], DRMS_PERMANENT, DRMS_SHARE_SEGMENTS, &status);

            if (!exprec)
            {
                JSONDIE("Cant create new export control record");
            }

            drms_setkey_int(exprec, "Status", 2);

          /* in exp repeat (not used yet) */
          if (requestorid)
          {
              /* requestorid is determined by email-address check */
              drms_setkey_int(exprec, "requestor", requestorid);
          }

          drms_setkey_time(exprec, "ReqTime", fetch_time);

          // drms_close_records(RsClone, DRMS_INSERT_RECORD);
          drms_close_records(exports, DRMS_FREE_RECORD);
      }
      else // old export is still available, do not repeat, but treat as status request.
      {
          drms_close_records(exports, DRMS_FREE_RECORD);
      }

      // if repeating export then export_series is set to jsoc.export_new
      // else if just touching retention then is it jsoc_export
    } // End kOpExpRepeat

  // Now report back to the requestor by dropping into the code for status request.
  // This is entry point for status request and tail of work for exp_request and exp_su
  // If data was as-is and online and url_quick the exit will have happened above.

  // op = exp_status, kOpExpStatus,  Implied here
    if (strcmp(op,kOpExpStatus) == 0)
    {
        char mybuf[128] = {0};

        // There is no case statement for kOpExpStatus above. We need to read in exprec
        // here.

        /* requestid must be provided on the command-line. It is not created by jsoc_fetch. */
        if (!requestid || !*requestid || strcmp(requestid, kNotSpecified) == 0)
        {
            JSONDIE("RequestID must be provided");
        }
        else
        {
            regex_t regexp;

            if (regcomp(&regexp, kDefRegexp, REG_EXTENDED) != 0)
            {
                snprintf(mybuf, sizeof(mybuf), "Bad regular expression '%s'.", kDefRegexp);
                JSONDIE(mybuf);
            }
            else
            {
                if (regexec(&regexp, requestid, (size_t)0, (regmatch_t *)NULL, 0) != 0)
                {
                    /* No match (failure). */
                    regfree(&regexp);
                    snprintf(mybuf, sizeof(mybuf), "Bad RequestID '%s' provided.", requestid);
                    JSONDIE(mybuf);
                }

                regfree(&regexp);
            }
        }

        if (internal)
        {
            lfname = kLogFileExpStatInt;
        }
        else
        {
            lfname = kLogFileExpStatExt;
        }

        LogReqInfo(lfname, instanceID, fileupload, op, dsin, requestid, dbhost, from_web, webarglist, fetch_time);

        // Must check jsoc.export, NOT jsoc.export_new.
        sprintf(status_query, "%s[%s]", kExportSeries, requestid);
        exports = drms_open_records(drms_env, status_query, &status);
        if (!exports)
            JSONDIE3("Cant locate export series: ", status_query);
        if (exports->n < 1)
            JSONDIE3("Cant locate export request: ", status_query);
        exprec = exports->records[0];
        exports->records[0] = NULL; // Detach this record from the record-set so we can free record-set, but not exprec.
        drms_close_records(exports, DRMS_FREE_RECORD);
        insertexprec = 0;
    }

    // ******************************************** //
    // A jsoc.export_new record was created in      //
    // memory, but not saved to the db. If we're    //
    // going to exit                                //
    // from this module after this point, we must   //
    // call drms_close_record() on the newly        //
    // created record in jsoc.export_new. The       //
    // handle to this record is exprec.             //
    // ******************************************** //

    /* generate HTML to send to web user */

  if (strcmp(requestid, kNotSpecified) == 0)
  {
      // ART - must save exprec first (it was created in one of the case blocks above).
      JSONCOMMIT("RequestID must be provided", &exprec, !insertexprec);
  }

    // export_series, exp_rec is jsoc.export_new; no need to call drms_open_records(), exprec is already available
    // actually, there is no need to use exprec - these values are in memory already - ART

  status     = drms_getkey_int(exprec, "Status", NULL);
  process = drms_getkey_string(exprec, "Processing", NULL);
  protocol   = drms_getkey_string(exprec, "Protocol", NULL);
  filenamefmt = drms_getkey_string(exprec, "FilenameFmt", NULL);
  method     = drms_getkey_string(exprec, "Method", NULL);
  format     = drms_getkey_string(exprec, "Format", NULL);
  reqtime    = drms_getkey_time(exprec, "ReqTime", NULL);
  esttime    = drms_getkey_time(exprec, "EstTime", NULL); // Crude guess for now
  size       = drms_getkey_longlong(exprec, "Size", NULL);
  char *export_errmsg = drms_getkey_string(exprec, "errmsg", NULL);

  // Do special actions on status
  switch (status)
    {
    case 0:
            errorreply = NULL;
            waittime = 0;
            break;
    case 1:
            errorreply = NULL;
            waittime = esttime - fetch_time;
            break;
    case 12:
    case 2:
            errorreply = NULL;
            waittime = esttime - fetch_time;
            break;
    case 3:
            errorreply = "Request too large";
            waittime = 999999;
            break;
    case 4:
            waittime = 999999;
            if (strcmp("NA", export_errmsg))
              errorreply = export_errmsg;
            else
              errorreply = "RecordSet specified does not exist";
            break;
    case 5:
            waittime = 999999;
            errorreply = "Request was completed but is now deleted, 7 day limit exceeded";
            break;
    default:
        {
            // ART - must save exprec first
            JSONCOMMIT("Illegal status in export record", &exprec, !insertexprec);
        }
    }

  // Return status information to user
  if (1)
    {
    char *json;
    char *strval;
    char numval[100];
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
      if (drms_record_directory(exprec, logpath, 0) != DRMS_SUCCESS || *logpath == '\0')
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
        {
            // ART - must save exprec first
            char dbuf[1024];

            snprintf(dbuf, sizeof(dbuf), "Export should be complete but return %s file not found", indexfile);
            JSONCOMMIT(dbuf, &exprec, !insertexprec);
        }

        if (genWebPage)
        {
            if (dojson)
                printf("Content-type: application/json\n\n");
            else
                printf("Content-type: text/plain\n\n");
        }

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
                numval[0] = sustatus[i];
                numval[1] = '\0';
                /* No need to escape sustatus - the char codes are all ASCII. */
              json_insert_pair_into_object(suobj, "sustatus", json_new_string(numval));
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
        json_free_value(&jroot);

        if (genWebPage)
        {
            if (fileupload)  // The returned json should be in the implied <body> tag for iframe requests.
                printf("Content-type: text/html\n\n");
            else
                printf("Content-type: application/json\n\n");
        }

    	printf("%s\n",json);
        free(json);
        json = NULL;
	}
      else
        {
        if (genWebPage)
        {
        	printf("Content-type: text/plain\n\n");
        }
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
             printf("%lld\t%s\t%s\t%c\t%s\n", sunums[i], series[i], paths[i], sustatus[i], susize[i]);
             }
          }
        }
      fflush(stdout);
      }
    }

    if (sustatus)
    {
        free(sustatus);
        sustatus = NULL;
    }

    if (sunums)
    {
        free(sunums);
        sunums = NULL;
    }

  report_summary(Server, StartTime, Remote_Address, op, dsin, rcountlimit, internal, requireOnline, status);
  CleanUp(&sunumarr, &infostructs, &webarglist, &series, &paths, &susize, arrsize, userhandle);

    if (exprec)
    {
        insertexprec ? drms_close_record(exprec, DRMS_INSERT_RECORD) : drms_close_record(exprec, DRMS_FREE_RECORD);
    }

  return(0);
  }
