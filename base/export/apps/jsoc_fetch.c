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

#include <time.h>


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

#define kNotSpecified	"Not Specified"

int dojson, dotxt, dohtml, doxml;

static char x2c (char *what) {
  register char digit;

  digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0'));
  return (digit);
}

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, kArgOp, kNotSpecified, "<Operation>"},
  {ARG_STRING, kArgDs, kNotSpecified, "<record_set query>"},
  {ARG_STRING, kArgSeg, kNotSpecified, "<record_set segment list>"},
  {ARG_STRING, kArgSunum, kNotSpecified, "<sunum list for SU exports>"},
  {ARG_STRING, kArgRequestid, kNotSpecified, "JSOC export request identifier"},
  {ARG_STRING, kArgProcess, kNotSpecified, "string containing program and arguments"},
  {ARG_STRING, kArgRequestor, kNotSpecified, "name of requestor"},
  {ARG_STRING, kArgNotify, kNotSpecified, "email address of requestor"},
  {ARG_STRING, kArgShipto, kNotSpecified, "mail address of requestor"},
  {ARG_STRING, kArgProtocol, "as-is", "exported file protocol"},
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
 *   size : size in bytes
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
    sprintf(numval, "%lld", size);
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

SUM_t *my_sum=NULL;

SUM_info_t *drms_get_suinfo(long long sunum)
  {
  int status;
  if (my_sum && my_sum->sinfo->sunum == sunum)
    return(my_sum->sinfo);
  if (!my_sum)
    {
    if ((my_sum = SUM_open(NULL, NULL, printkerr)) == NULL)
      {
      printkerr("drms_open: Failed to connect to SUMS.\n");
      return(NULL);
      }
    }
  if (status = SUM_info(my_sum, sunum, printkerr))
    {
    printkerr("Fail on SUM_info, status=%d\n", status);
    return(NULL);
    }

  return(my_sum->sinfo);
  }

#ifdef NEVER
// use at end of any section that exits Doit after using SUM_info
  if (my_sum)
    SUM_close(my_sum,printkerr);
#endif

#define JSONDIE(msg) {die(dojson,msg,"","4");return(1);}
#define JSONDIE2(msg,info) {die(dojson,msg,info,"4");return(1);}
#define JSONDIE3(msg,info) {die(dojson,msg,info,"6");return(1);}

int fileupload = 0;

int die(int dojson, char *msg, char *info, char *stat)
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
  if (my_sum)
    SUM_close(my_sum,printkerr);
  return(1);
  }

int send_file(DRMS_Record_t *rec, int segno)
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
fprintf(stderr,"path: %s\n",path);
  fp = fopen(path, "r");
  if (!fp)
    JSONDIE2("Can not open file for export: ",path);
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
int illegalArg(char *arg)
  {
  if (index(arg, ';'))
     return(1);
  return(0);
  }

static int SetWebArg(Q_ENTRY *req, const char *key)
   {
   char *value = NULL;
   if (req)
      {
      value = (char *)qEntryGetStr(req, key);
      if (value)
         {
         if (illegalArg(value))
            JSONDIE("Illegal text in arg");
         if (!cmdparams_set(&cmdparams, key, value))
	    JSONDIE("CommandLine Error");
         }
      }
   return(0);
   }

static int SetWebFileArg(Q_ENTRY *req, const char *key)
   {
   char *value = NULL;
   int len;
   char keyname[100], length[20];
   SetWebArg(req, key);     // get contents of upload file
   sprintf(keyname, "%s.length", key);
   len = qEntryGetInt(req, keyname);
   sprintf(length, "%d", len);
   cmdparams_set(&cmdparams, keyname, length);
   sprintf(keyname, "%s.filename", key);
   SetWebArg(req, keyname);     // get name of upload file
   sprintf(keyname, "%s.contenttype", key);
   SetWebArg(req, keyname);     // get name of upload file
   return(0);
   }

/* Module main function. */
int DoIt(void)
  {
						/* Get command line arguments */
  const char *op;
  const char *in;
  const char *seglist;
  const char *requestid;
  const char *process;
  const char *requestor;
  long requestorid;
  const char *notify;
  const char *format;
  const char *formatvar;
  const char *shipto;
  const char *method;
  const char *protocol;
  const char *filenamefmt;
  char *errorreply;
  const char *sunumlist;
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

     req = qCgiRequestParseQueries(NULL, NULL);
     if (req)
        {
        /* Accept only known key-value pairs - ignore the rest. */
	SetWebArg(req, kArgOp);
	SetWebArg(req, kArgRequestid);
	SetWebArg(req, kArgDs);
	SetWebArg(req, kArgSunum);
	SetWebArg(req, kArgSeg);
	SetWebArg(req, kArgProcess);
	SetWebArg(req, kArgFormat);
	SetWebArg(req, kArgFormatvar);
	SetWebArg(req, kArgMethod);
	SetWebArg(req, kArgProtocol);
	SetWebArg(req, kArgFilenamefmt);
	SetWebArg(req, kArgRequestor);
	SetWebArg(req, kArgNotify);
	SetWebArg(req, kArgShipto);
	SetWebArg(req, kArgRequestorid);
        if (strncmp(cmdparams_get_str (&cmdparams, kArgDs, NULL),"*file*", 6) == 0);
	  SetWebFileArg(req, kArgFile);

        qEntryFree(req); 
        }
     }
  free(web_query);
  // From here on, called as cgi-bin same as from command line

  op = cmdparams_get_str (&cmdparams, kArgOp, NULL);
  requestid = cmdparams_get_str (&cmdparams, kArgRequestid, NULL);
  in = cmdparams_get_str (&cmdparams, kArgDs, NULL);
  sunumlist = cmdparams_get_str (&cmdparams, kArgSunum, NULL);
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

  export_series = kExportSeries;

  long long sunums[DRMS_MAXQUERYLEN/8];  // should be enough!
  char *paths[DRMS_MAXQUERYLEN/8];
  char *series[DRMS_MAXQUERYLEN/8];
  char *sustatus[DRMS_MAXQUERYLEN/8];
  char *susize[DRMS_MAXQUERYLEN/8];
  int expsucount;

  /*  op == exp_su - export Storage Units */
  if (strcmp(op, kOpExpSu) == 0)
    {
    char *this_sunum, *sunumlist, *sunumlistptr;
    long long sunum;
    int count;
    int status=0;
    int all_online;

    export_series = kExportSeriesNew;
    // Do survey of sunum list
    size=0;
    all_online = 1;
    count = 0;
    sunumlist = strdup(in);

    char onlinestat[128];
    int dirsize;
    char supath[DRMS_MAXPATHLEN];
    char yabuff[64];

    while (this_sunum = strtok_r(sunumlist, ",", &sunumlistptr))
      {
      SUM_info_t *sinfo;
      TIME expire;

      dirsize = 0;
      memset(onlinestat, 0, sizeof(onlinestat));
      snprintf(supath, sizeof(supath), "NA");

      sunum = atoll(this_sunum);
      sunumlist = NULL;
      sinfo = drms_get_suinfo(sunum);
      if (!sinfo)
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
         size += sinfo->bytes;
         dirsize = sinfo->bytes;

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
      }

    expsucount = count;

    if (count==0)
      JSONDIE("There are no files in this RecordSet");

    // Do quick export if possible
    if (strcmp(method,"url_quick")==0 && strcmp(protocol,"as-is")==0  && all_online)
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
        json_insert_pair_into_object(jroot, "status", json_new_number("0"));
        json_tree_to_string(jroot,&json);
        printf("Content-type: application/json\n\n");
        printf("%s\n",json);
        fflush(stdout);
        free(json);
        }  
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
        printf("size=%lld\n", size);
        printf("dir=/\n");
        printf("# DATA\n");
        for (i=0; i<count; i++)
          printf("%lld\t%s\t%s\t%s\t%s\n",sunums[i],series[i],paths[i], sustatus[i], susize[i]);
        }
      if (my_sum)
        SUM_close(my_sum,printkerr);
      return(0);
      }

    // Must do full export processing

    // Get RequestID
    {
    FILE *fp = popen("/home/phil/cvs/JSOC/bin/linux_ia32/GetJsocRequestID", "r");
    if (fscanf(fp, "%s", new_requestid) != 1)
      JSONDIE("Cant get new RequestID");
    pclose(fp);
    requestid = new_requestid;
    }

    now = timenow();

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
    drms_setkey_longlong(export_log, "Size", size);
    drms_setkey_int(export_log, "Status", 2);
    drms_setkey_int(export_log, "Requestor", requestorid);
    drms_close_record(export_log, DRMS_INSERT_RECORD); 
    } // end of exp_su

  /*  op == exp_request  */
  else if (strcmp(op,kOpExpRequest) == 0) 
    {
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
// XXXXXX need to deal with big files
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
          strcpy(cb, "[]");
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
	JSONDIE2("Can not open RecordSet: ",dsquery);
    rcount = rs->n;
  
    // Do survey of recordset
    all_online = 1;
    for (irec=0; irec < rcount; irec++) 
      {
      // Must check each segment since some may be linked and offline.
      DRMS_Record_t *rec = rs->records[irec];
      DRMS_Segment_t *seg;
      HIterator_t *segp = NULL;
      while (seg = drms_record_nextseg(rec, &segp, 1))
        {
        DRMS_Record_t *segrec = seg->record;
        SUM_info_t *sinfo = drms_get_suinfo(segrec->sunum);
        if (!sinfo)
          JSONDIE2("Bad sunum in a record in RecordSet: ", dsquery);
  	if (strcmp(sinfo->online_status,"N") == 0)
          all_online = 0;
        else
          {
          struct stat buf;
  	  char path[DRMS_MAXPATHLEN];
  	  drms_record_directory(segrec, path, 0);
          drms_segment_filename(seg, path);
          if (stat(path, &buf) != 0)
  	    JSONDIE2("Bad path (stat failed) in a record in RecordSet: ", dsquery);
          size += buf.st_size;
          segcount += 1;
          }
        }
      if (segp)
        free(segp); 
      }
    if (my_sum)
      SUM_close(my_sum,printkerr);
  
    // Exit if no records found
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,"as-is")==0) || strcmp(protocol,"su")==0) && segcount == 0)
      JSONDIE("There are no files in this RecordSet");

    // Do quick export if possible
    if ((strcmp(method,"url_quick")==0 && (strcmp(protocol,"as-is")==0) || strcmp(protocol,"su")==0) && all_online)
      {
      if (0 && segcount == 1) // If only one file then do immediate delivery of that file.
        {
        return(send_file(rs->records[0], 0));
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
      return(0);
      }

    // Must do full export processing

    // Get RequestID
    {
    FILE *fp = popen("/home/phil/cvs/JSOC/bin/linux_ia32/GetJsocRequestID", "r");
    if (fscanf(fp, "%s", new_requestid) != 1)
      JSONDIE("Cant get new RequestID");
    pclose(fp);
    requestid = new_requestid;
    }

    now = timenow();

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
    drms_setkey_longlong(export_log, "Size", size);
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
  size       = drms_getkey_int(export_log, "Size", NULL);
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
        sprintf(numval, "%lld", size);
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

  if (strcmp(op, kOpExpSu) == 0)
    {
     /* free everything */
     int i;
     for (i = 0; i < expsucount; i++)
       {
        free(series[i]);
        free(paths[i]);
        free(sustatus[i]);
        free(susize[i]);
       }
    }

  return(0);
  }
