#define DEBUG 0

/*
 *  jsoc_info - prints information about given recordset
 *
 *  This is a cgi-bin compatible version of show_info
 *
 */

/**
\defgroup jsoc_info jsoc_info - same as show info, except that can output json and run under cgi-bin
@ingroup su_export

Prints keyword, segment, and other information and/or file path for given recordset, designed for use as a cgi-bin program.

\ref jsoc_info can list various kinds of information about a JSOC data series
or recordset.  Exactly what information gets printed is
controlled by command-line arguments (see below). 
The output format is in json formatted text (see e.g. www.json.org).
The output structure is described at: http://jsoc.stanford.edu/jsocwiki/AjaxJsocConnect.

\par Synopsis:

\code
jsoc_info [-hRz] op=<command> ds=<record_set> [[key=<keylist>] [seg=<seglist>] [link=<linklist>]
or
jsoc_info QUERY_STRING=<url equivalent of command-line args above>
\endcode

\param command
Specifies the operation to be performed by jsoc_info.  Commands are "series_struct" which
is like "show_info -l"; "rs_summary" which is like "show_info -c"; and "rs_list" which
mimics show_info normal mode returning keyword and segment information as specified in the
keylist and seglist parameters.

\param record_set
A series name followed by an optional record-set specification (i.e.,
\a seriesname[RecordSet_filter]). Causes selection of a subset of
records in the series. This argument is required.

\param keylist
Comma-separated list of keyword names. For each keyword listed,
information will be displayed. Several special psuedo keyword names
are accepted.  These are: **ALL** means show all keywords (see show_info -a);
**NONE** means show no keywords; *recnum* means show the hidden keyword "recnum";
*sunum* means show the hidden keyword "sunum";
*size* means show the size of storage unit (bytes);
*online* means show the hidden keyword "online";
*retain* means show retention date, i.e. date at which SUMS may remove the segment storage;
*archive* means show archive status, i.e. has or will SUMS write the storage unit to tape;
*logdir* means show the path to the processing log directory; and *dir_mtime* instructs
jsoc_info to show the last modify time of the record directory in SUMS.
The results are presented in arrays named "name" and "value".

\param seglist
Comma-separated list of segment names. For each segment listed, the
segment's filename is displayed.  If the record directory is online the full
path will be provided.  If the R=1 parameter is set a "keyword" with name "online" will
be set to 1 if the record files are online, or 0 if off-line.
The psuedo segment names **ALL** and **NONE** result in all or no segment information
being returned.  Additional segment information, e.g. axis dimensions, is also provided
via the key_array "dims".

\param linklist
Comma-separated list of link names.  For each link listed the links target
record query is displayed.  The psuedo link name **ALL** results in all links to be
displayed.  The default is the same as **NONE**.

\sa
show_info

@{
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "json.h"
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include "printk.h"

static char x2c (char *what)
  {
  char digit;
  digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0'));
  return (digit);
  }

static void CGI_unescape_url (char *url)
  {
  int x, y;
  for (x = 0, y = 0; url[y]; ++x, ++y)
    {
    if ((url[x] = url[y]) == '%')
      {
      url[x] = x2c (&url[y+1]);
      y += 2;
      }
    }
  url[x] = '\0';
  }

/* drms_record_getlogdir */

/* Returns path of directory that contains any saved log information for the given record */
/* If log is offline, returns message, if log was not saved or otherwise not found returns NULL */
/* The returned char* should be freed after use. */

char *drms_record_getlogdir(DRMS_Record_t *rec)
  { 
  char *logpath;
  char query[DRMS_MAXQUERYLEN];
  DB_Text_Result_t *qres;

  sprintf(query, "select sunum from %s.drms_session where sessionid=%lld", rec->sessionns, rec->sessionid);
  if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0)
    {
    if (qres->field[0][0][0] == '\0')
      logpath = strdup("No log avaliable");
    else
      {
      DRMS_StorageUnit_t *su;
      int status, save_retention = drms_env->retention;
      int retrieve = 0;
      su = malloc(sizeof(DRMS_StorageUnit_t));
      su->sunum = atoll(qres->field[0][0]);
      drms_env->retention = DRMS_LOG_RETENTION;
      status = drms_su_getsudir(drms_env, su, retrieve);
      if (!status)
        logpath = strdup(su->sudir);
      else
        logpath = strdup("Log offline");
      free(su);
      drms_env->retention = save_retention;
      }
    }
  else
    logpath = strdup("Log query failed");
  db_free_text_result(qres);
  return logpath;
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

int drms_ismissing_keyval(DRMS_Keyword_t *key)
  {
  XASSERT(key);
  switch(key->info->type)
    {
    case DRMS_TYPE_CHAR:
      return(drms_ismissing_char(key->value.char_val));
    case DRMS_TYPE_SHORT:
      return(drms_ismissing_short(key->value.short_val));
    case DRMS_TYPE_INT:
      return(drms_ismissing_int(key->value.int_val));
    case DRMS_TYPE_LONGLONG:
      return(drms_ismissing_longlong(key->value.longlong_val));
    case DRMS_TYPE_FLOAT:
      return(drms_ismissing_float(key->value.float_val));
    case DRMS_TYPE_DOUBLE:
      return(drms_ismissing_double(key->value.double_val));
    case DRMS_TYPE_TIME:
      return(drms_ismissing_time(key->value.time_val));
    case DRMS_TYPE_STRING:
      return(drms_ismissing_string(key->value.string_val));
    default:
      fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)key->info->type);
      XASSERT(0);
    }
  return 0;
  }


ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "Not Specified", "<Operation>, values are: series_struct, rs_summary, or rs_list "},
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "key", "Not Specified", "<comma delimited keyword list>, keywords or special values: **ALL**, **NONE**, *recnum*, *sunum*, *size*, *online*, *retain*, *archive*, *logdir*, *dir_mtime*  "},
  {ARG_STRING, "link", "Not Specified", "<comma delimited linkname list>, links or special values: **ALL**, **NONE**"},
  {ARG_STRING, "seg", "Not Specified", "<comma delimited segment list>, segnames or special values: **ALL**, **NONE** "},
  {ARG_INT, "n", "0", "RecordSet Limit"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_FLAG, "R", "0", "Show record query"},
  {ARG_FLAG, "z", "0", "emit JSON output"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_END}
};

char *module_name = "jsoc_info";
/** @}*/
int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\njsoc_info {-h} {-z} {-R} "
	"op=<command> ds=<recordset query> {n=<rslimit>} {key=<keylist>} {seg=<segment_list>}\n"
        "  details are:\n"
	"-h -> print this message\n"
	"-z -> emit json output, default at present.\n"
	"-R -> include record query.\n"
	"op=<command> tell which ajax function to execute, values are: series_struct, rs_summary, or rs_list \n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
        "n=<rslimit> set optional record count limit, <0 from end, >0 from head\n"
	"key=<comma delimited keyword list>, keywords or special values: **ALL**, **NONE**, *recnum*, *sunum*, *size*, *online*, *retain*, *archive*, *logdir*, *dir_mtime* \n"
	"seg=<comma delimited segment list>, segnames or special values: **ALL**, **NONE** \n"
	"QUERY_STRING=<cgi-bin params>, parameter string as delivered from cgi-bin call.\n"
	);
    return(1);
    }
  return (0);
  }

/* find first record in series that owns the given record */
DRMS_RecordSet_t *drms_find_rec_first(DRMS_Record_t *rec, int wantprime)
  {
  int nprime;
  int status;
  DRMS_RecordSet_t *rs;
  char query[DRMS_MAXQUERYLEN];
  strcpy(query, rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  if (wantprime && nprime > 0) 
    // only first prime key is used for now
     // for (iprime = 0; iprime < nprime; iprime++)
      strcat(query, "[#^]");
  else
    strcat(query, "[:#^]");
// fprintf(stderr,"test 1 query is %s\n",query);
  rs = drms_open_records(rec->env, query, &status);
// fprintf(stderr,"test 1 status is %d\n",status);
  return(rs);
  }

/* find last record in series that owns the given record */
DRMS_RecordSet_t *drms_find_rec_last(DRMS_Record_t *rec, int wantprime)
  {
  int nprime;
  int status;
  DRMS_RecordSet_t *rs;
  char query[DRMS_MAXQUERYLEN];
  strcpy(query, rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  if (wantprime && nprime > 0) 
    // only first prime key is used for now
     // for (iprime = 0; iprime < nprime; iprime++)
      strcat(query, "[#$]");
  else
    strcat(query, "[:#$]");
  rs = drms_open_records(rec->env, query, &status);
  return(rs);
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

char * string_to_json(char *in)
  { // for json vers 0.9 no longer uses wide chars
  char *new;
  new = json_escape(in);
  return(new);
  }


static void list_series_info(DRMS_Record_t *rec, json_t *jroot)
  {
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  DRMS_Link_t *link;
  HIterator_t *last = NULL;
  char intstring[100];
  char *notework;
  json_t *indexarray, *primearray, *keyarray, *segarray, *linkarray;
  json_t *primeinfoarray;
  int npkeys;

  /* add description from seriesinfo */
  notework = string_to_json(rec->seriesinfo->description);
  json_insert_pair_into_object(jroot, "note", json_new_string(notework));
  free(notework);
  /* add retention, unitsize, archive, and tapegroup integers */
  sprintf(intstring, "%d", rec->seriesinfo->retention);
  json_insert_pair_into_object(jroot, "retention", json_new_number(intstring));
  sprintf(intstring, "%d", rec->seriesinfo->unitsize);
  json_insert_pair_into_object(jroot, "unitsize", json_new_number(intstring));
  sprintf(intstring, "%d", rec->seriesinfo->archive);
  json_insert_pair_into_object(jroot, "archive", json_new_number(intstring));
  sprintf(intstring, "%d", rec->seriesinfo->tapegroup);
  json_insert_pair_into_object(jroot, "tapegroup", json_new_number(intstring));
  
  /* show the prime index keywords */
  // both the original simple list and new array of objects are generated -- XXXXX REMOVE SOMEDAY                             
  // for compatibility.  The older "primekeys" array may be removed in the future.  23Nov09 -- XXXXX REMOVE SOMEDAY           
  // old lines marked with trailing XXXXX REMOVE SOMEDAY
  primearray = json_new_array(); // XXXXX REMOVE SOMEDAY
  primeinfoarray = json_new_array();
  npkeys = rec->seriesinfo->pidx_num;
  if (npkeys > 0)
    {
    int i;
    json_t *primeinfo, *val;
    char *jsonstr;
    for (i=0; i<npkeys; i++)
        {
        json_t *primeinfo = json_new_object();
        DRMS_Keyword_t *pkey, *skey;
        pkey = rec->seriesinfo->pidx_keywords[i];
	if (pkey->info->recscope > 1)
        {
           char rawval[100];
           skey = drms_keyword_slotfromindex(pkey);
           json_insert_pair_into_object(primeinfo, "name", json_new_string(skey->info->name));
           json_insert_pair_into_object(primeinfo, "slotted", json_new_number("1"));
           drms_keyword_snprintfval(drms_keyword_stepfromvalkey(skey), rawval, sizeof(rawval));
           jsonstr = string_to_json(rawval);
           val = json_new_string(jsonstr);
           free(jsonstr);
           json_insert_pair_into_object(primeinfo, "step", val);
           json_insert_child(primearray, json_new_string(skey->info->name)); // XXXXX REMOVE SOMEDAY                         

        }
        else
        {
           json_insert_pair_into_object(primeinfo, "name", json_new_string(pkey->info->name));
           json_insert_pair_into_object(primeinfo, "slotted", json_new_number("0"));
           json_insert_child(primearray, json_new_string(pkey->info->name)); // XXXXX REMOVE SOMEDAY                         
        }
        json_insert_child(primeinfoarray, primeinfo);            
        }
    }
  else
  {
     json_insert_child(primearray, json_new_null()); // XXXXX REMOVE SOMEDAY                                                   
     json_insert_child(primeinfoarray, json_new_null());
  }
  json_insert_pair_into_object(jroot, "primekeys", primearray); // XXXXX REMOVE SOMEDAY                                       
  json_insert_pair_into_object(jroot, "primekeysinfo", primeinfoarray);
 
  /* show DB index keywords */
  indexarray = json_new_array();
  if (rec->seriesinfo->dbidx_num > 0)
    {
    int i;
    for (i=0; i<rec->seriesinfo->dbidx_num; i++)
        json_insert_child(indexarray, json_new_string((rec->seriesinfo->dbidx_keywords[i])->info->name));
    }
  else
    json_insert_child(indexarray, json_new_null());
  json_insert_pair_into_object(jroot, "dbindex", indexarray);

if (DEBUG) fprintf(stderr,"   starting all keywords\n");
  /* show all keywords */
  keyarray = json_new_array();

  while ((key = drms_record_nextkey(rec, &last, 0)))
    {
    json_t *keyinfo = json_new_object();
    json_t *keytype;
    json_t *defval, *recscope;
    char rawval[100], *jsonstr;
if (DEBUG) fprintf(stderr,"   starting keyword %s\n",key->info->name);
    json_insert_pair_into_object(keyinfo, "name", json_new_string(key->info->name));
    if (key->info->islink)
	keytype = json_new_string("link");
    else
	keytype = json_new_string(drms_type_names[key->info->type]);
    json_insert_pair_into_object(keyinfo, "type", keytype);
    // scope                                                                                                                      
    jsonstr = string_to_json((char *)drms_keyword_getrecscopestr(key, NULL));
    recscope = json_new_string(jsonstr);
    free(jsonstr);
    json_insert_pair_into_object(keyinfo, "recscope", recscope);
    // default                                                                                                                    
    drms_keyword_snprintfval(key, rawval, sizeof(rawval));
    jsonstr = string_to_json(rawval);
    defval = json_new_string(jsonstr);
    free(jsonstr);
    json_insert_pair_into_object(keyinfo, "defval", defval);

    notework = string_to_json(key->info->unit);
    json_insert_pair_into_object(keyinfo, "units", json_new_string(notework));
    free(notework);
    notework = string_to_json(key->info->description);
    json_insert_pair_into_object(keyinfo, "note", json_new_string(notework));
    free(notework);
    json_insert_child(keyarray, keyinfo);
    }
  json_insert_pair_into_object(jroot, "keywords", keyarray);
if (DEBUG) fprintf(stderr," done with keywords, start segments\n");
  
  /* show the segments */
  segarray = json_new_array();
  if (rec->segments.num_total)
    {
       if (last)
       {
          hiter_destroy(&last);
       }

      while ((seg = drms_record_nextseg(rec, &last, 0)))
      { /* segment name, units, protocol, dims, description */
      json_t *seginfo = json_new_object();
      int naxis = seg->info->naxis;
      json_insert_pair_into_object(seginfo, "name", json_new_string(seg->info->name));
      if (seg->info->islink)
	    {
            char linkinfo[DRMS_MAXNAMELEN+10];
	    sprintf(linkinfo, "link via %s", seg->info->linkname);
            json_insert_pair_into_object(seginfo, "units", json_new_null());
            json_insert_pair_into_object(seginfo, "protocol", json_new_string(linkinfo));
            json_insert_pair_into_object(seginfo, "dims", json_new_null());
	    }
	else
	    {
            char prot[DRMS_MAXNAMELEN];
            char diminfo[160];
            int iaxis;
            strcpy(prot, drms_prot2str(seg->info->protocol));
            json_insert_pair_into_object(seginfo, "units", json_new_string(seg->info->unit));
            json_insert_pair_into_object(seginfo, "protocol", json_new_string(prot));
	    diminfo[0] = '\0';
	    for (iaxis=0; iaxis<naxis; iaxis++)
	        {
	        if (iaxis != 0)
                    strcat(diminfo,"x");
		if (seg->info->scope == DRMS_VARDIM)
                    strcat(diminfo,"VAR");
		else
		    {
	            char size[10];
		    sprintf(size,"%d",seg->axis[iaxis]);
                    strcat(diminfo,size);
		    }
		}
            json_insert_pair_into_object(seginfo, "dims", json_new_string(diminfo));
            }
      notework = string_to_json(seg->info->description);
      json_insert_pair_into_object(seginfo, "note", json_new_string(notework));
      free(notework);
      json_insert_child(segarray, seginfo);
      }
    }
//  else
//    json_insert_child(segarray, json_new_null());
  json_insert_pair_into_object(jroot, "segments", segarray);

if (DEBUG) fprintf(stderr," done with segments, start links\n");
  /* show the links */
  linkarray = json_new_array();
  if (rec->links.num_total)
    {
       if (last)
       {
          hiter_destroy(&last);
       }

    while ((link = drms_record_nextlink(rec, &last)))
      {
if (DEBUG) fprintf(stderr," link: %s\n",link->info->name);
      json_t *linkinfo = json_new_object();
      json_insert_pair_into_object(linkinfo, "name", json_new_string(link->info->name));
      json_insert_pair_into_object(linkinfo, "target", json_new_string(link->info->target_series));
      json_insert_pair_into_object(linkinfo, "kind", json_new_string(link->info->type == STATIC_LINK ? "STATIC" : "DYNAMIC"));
      notework = string_to_json(link->info->description);
      json_insert_pair_into_object(linkinfo, "note", json_new_string(notework));
      free(notework);
      json_insert_child(linkarray,linkinfo);
      }
    }
//  else
//    json_insert_child(linkarray,json_new_null());
  json_insert_pair_into_object(jroot, "links", linkarray);

  if (last)
  {
     hiter_destroy(&last);
  }

  return;
  }

void get_series_stats(DRMS_Record_t *rec, json_t *jroot)
  {
  DRMS_RecordSet_t *rs;
  int nprime;
  int status;
  char query[DRMS_MAXQUERYLEN];
  json_t *interval = json_new_object();

  nprime = rec->seriesinfo->pidx_num;
  if (nprime > 0)
    sprintf(query,"%s[#^]", rec->seriesinfo->seriesname);
  else
    sprintf(query,"%s[:#^]", rec->seriesinfo->seriesname);
  rs = drms_open_records(rec->env, query, &status);

  if (!rs || rs->n < 1)
    {
    json_insert_pair_into_object(interval, "FirstRecord", json_new_string("NA"));
    json_insert_pair_into_object(interval, "FirstRecnum", json_new_string("NA"));
    json_insert_pair_into_object(interval, "LastRecord", json_new_string("NA"));
    json_insert_pair_into_object(interval, "LastRecnum", json_new_string("NA"));
    json_insert_pair_into_object(interval, "MaxRecnum", json_new_number("0"));
    if (rs) drms_free_records(rs);
    json_insert_pair_into_object(jroot, "Interval", interval);
    return;
    }
  else
    {
    char recquery[DRMS_MAXQUERYLEN];
    char *jsonquery;
    char val[100];
    int status;
    drms_sprint_rec_query(recquery,rs->records[0]);
    jsonquery = string_to_json(recquery);
    status = json_insert_pair_into_object(interval, "FirstRecord", json_new_string(jsonquery));
if (status != JSON_OK) fprintf(stderr, "json_insert_pair_into_object, status=%d, text=%s\n",status,jsonquery);
    free(jsonquery);
    sprintf(val,"%lld", rs->records[0]->recnum);
    json_insert_pair_into_object(interval, "FirstRecnum", json_new_number(val));
    drms_free_records(rs);
  
    if (nprime > 0)
      sprintf(query,"%s[#$]", rec->seriesinfo->seriesname);
    else
      sprintf(query,"%s[:#$]", rec->seriesinfo->seriesname);
    rs = drms_open_records(rec->env, query, &status);
    drms_sprint_rec_query(recquery,rs->records[0]);
    jsonquery = string_to_json(recquery);
    json_insert_pair_into_object(interval, "LastRecord", json_new_string(jsonquery));
    free(jsonquery);
    sprintf(val,"%lld", rs->records[0]->recnum);
    json_insert_pair_into_object(interval, "LastRecnum", json_new_number(val));
    drms_free_records(rs);
 
    sprintf(query,"%s[:#$]", rec->seriesinfo->seriesname);
    rs = drms_open_records(rec->env, query, &status);
    sprintf(val,"%lld", rs->records[0]->recnum);
    json_insert_pair_into_object(interval, "MaxRecnum", json_new_number(val));
    drms_free_records(rs);
    }
  json_insert_pair_into_object(jroot, "Interval", interval);
  return;
  }

#define JSONDIE(msg) \
  {	\
  char *msgjson;	\
  char *json;	\
  json_t *jroot = json_new_object();	\
  msgjson = string_to_json(msg);	\
  json_insert_pair_into_object(jroot, "status", json_new_number("1"));	\
  json_insert_pair_into_object(jroot, "error", json_new_string(msgjson));	\
  json_tree_to_string(jroot,&json);	\
  printf("Content-type: application/json\n\n");	\
  printf("%s\n",json);	\
  free(json); \
  fflush(stdout);	\
  if (my_sum) \
    SUM_close(my_sum,printkerr); \
  return(1);	\
  }

/* Module main function. */
int DoIt(void)
  {
  char *op;
  char *in;
  char *keylist;
  char *seglist;
  char *linklist;
  char *web_query;
  int from_web, keys_listed, segs_listed, links_listed;
  int max_recs = 0;

  if (nice_intro ()) return (0);

  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, "Not Specified") != 0;

  if (from_web)
    {
    char *getstring, *p;
    CGI_unescape_url(web_query);
    getstring = strdup (web_query);
    for (p=strtok(getstring,"&"); p; p=strtok(NULL, "&"))
      {
      char *key=p, *val=index(p,'=');
      if (!val)
	 JSONDIE("Bad QUERY_STRING");
      *val++ = '\0';
      cmdparams_set(&cmdparams, key, val);
      }
    // Force JSON for now
    cmdparams_set (&cmdparams,"z", "1");
    free(getstring);
    }

  op = cmdparams_get_str (&cmdparams, "op", NULL);
  in = cmdparams_get_str (&cmdparams, "ds", NULL);
  keylist = strdup (cmdparams_get_str (&cmdparams, "key", NULL));
  seglist = strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
  linklist = strdup (cmdparams_get_str (&cmdparams, "link", NULL));
  max_recs = cmdparams_get_int (&cmdparams, "n", NULL);
  keys_listed = strcmp (keylist, "Not Specified");
  segs_listed = strcmp (seglist, "Not Specified");
  links_listed = strcmp (linklist, "Not Specified");

  /*  op == series_struct  */
  if (strcmp(op,"series_struct") == 0) 
    {
    char *p, *seriesname;
    json_t *jroot;
    char *json;
    char *final_json;
    DRMS_Record_t *rec;
    int status=0;
    /* Only want keyword info so get only the template record for drms series or first record for other data */
    seriesname = strdup (in);
    if ((p = index(seriesname,'['))) *p = '\0';
    rec = drms_template_record (drms_env, seriesname, &status);
    if (status)
      JSONDIE("series not found");
    jroot = json_new_object();
    list_series_info(rec, jroot);
    get_series_stats(rec, jroot);
    json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    json_tree_to_string(jroot,&json);
    final_json = json_format_string(json);
    free(json);
    /* send the output json back to client */
    printf("Content-type: application/json\n\n");
    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);
    free(seriesname);
    return(0);
    }

  /*  op == rs_summary  */
  if (strcmp(op,"rs_summary") == 0) 
    {
    json_t *jroot = json_new_object();
    char *json, *final_json;
    int count=0, status=0;
    int countlimit = abs(max_recs);
    char val[100];
    /* get series count */
    char *bracket = index(in, '{');
    if (bracket)
	*bracket = '\0';
    if (countlimit)
      {
      DRMS_RecordSet_t *recordset = drms_open_nrecords (drms_env, in, max_recs, &status);
      count = recordset->n;
      drms_close_records(recordset, DRMS_FREE_RECORD);
      }
    else
      count = drms_count_records(drms_env, in, &status);
    if (bracket)
	*bracket = '{';
    if (status)
      JSONDIE("series not found");
    /* send the output json back to client */
    sprintf(val, "%d", count);
    json_insert_pair_into_object(jroot, "count", json_new_number(val));
    json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    json_tree_to_string(jroot,&json);
    final_json = json_format_string(json);
    free(json);
    /* send the output json back to client */
    printf("Content-type: application/json\n\n");
    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);
    return(0);
    }

  /*  op == rs_list  */
  if (strcmp(op,"rs_list") == 0) 
    {
    int wantRecInfo = cmdparams_get_int(&cmdparams, "R", NULL);
    DRMS_RecordSet_t *recordset;
    DRMS_Record_t *rec, *template;
    DRMS_RecChunking_t cstat = kRecChunking_None;
    char seriesname[DRMS_MAXQUERYLEN];
    char *keys[1000];
    char *segs[1000];
    char *links[1000];
    int ikey, nkeys = 0;
    int iseg, nsegs = 0;
    int ilink, nlinks = 0;
    char count[100];
    json_t *jroot, **keyvals = NULL, **segvals = NULL, **segdims = NULL, **linkvals = NULL, *recinfo;
    json_t **segcparms = NULL;
    json_t **segbzeros = NULL;
    json_t **segbscales = NULL;
    json_t *json_keywords = json_new_array();
    json_t *json_segments = json_new_array();
    json_t *json_links = json_new_array();
    char *json;
    char *final_json;
    int status=0;
    int irec, nrecs;
    int record_set_staged = 0;
    char *lbracket;
    jroot = json_new_object();
    recinfo = json_new_array();


    /* Get template record */
    strcpy(seriesname, in);
    lbracket = index(seriesname, '[');
    if (lbracket) *lbracket = '\0';
    template = drms_template_record(drms_env, seriesname, &status);
    if (status)
      JSONDIE(" jsoc_info: series not found.");

    /* Open record_set(s) */
    if (max_recs == 0)
      recordset = drms_open_recordset (drms_env, in, &status);
    else // max_recs specified via "n=" parameter.                                                                            
      recordset = drms_open_nrecords (drms_env, in, max_recs, &status);

    if (!recordset) 
      JSONDIE(" jsoc_info: series not found.");
    nrecs = recordset->n;
    if (nrecs == 0)
      {
      json_insert_pair_into_object(jroot, "count", json_new_number("0"));
      json_insert_pair_into_object(jroot, "status", json_new_number("0"));
      json_tree_to_string(jroot,&json);
      printf("Content-type: application/json\n\n");
      printf("%s\n",json);
      free(json);
      fflush(stdout);
      return(0);
      }
  
    /* get list of keywords to print for each record */
    nkeys = 0;
    if (keys_listed) 
      { /* get specified list */
      char *thiskey;
      CGI_unescape_url(keylist);
      for (thiskey=strtok(keylist, ","); thiskey; thiskey=strtok(NULL,","))
	{
	if (strcmp(thiskey,"**NONE**")==0)
	  {
	  nkeys = 0;
	  break;
	  }
	if (strcmp(thiskey, "**ALL**")==0)
          {
          DRMS_Keyword_t *key;
          HIterator_t *last = NULL;

          while ((key = drms_record_nextkey(template, &last, 0)))
          {
             if (!drms_keyword_getimplicit(key))
             {
                keys[nkeys++] = strdup (key->info->name);
             }
          }
          
          if (last)
          {
             hiter_destroy(&last);
          }

	  }
  	else
	  keys[nkeys++] = strdup(thiskey);
	}
      }
    free (keylist);
    /* place to put an array of keyvals per keyword */
    if (nkeys)
      keyvals = (json_t **)malloc(nkeys * sizeof(json_t *));
    for (ikey=0; ikey<nkeys; ikey++)
      {
      json_t *val = json_new_array();
      keyvals[ikey] = val;
      }
  
    /* get list of segments to show for each record */
    nsegs = 0;
    if (segs_listed) 
      { /* get specified segment list */
      char *thisseg;
      CGI_unescape_url(seglist);
      for (thisseg=strtok(seglist, ","); thisseg; thisseg=strtok(NULL,","))
	{
	if (strcmp(thisseg,"**NONE**")==0)
	  {
	  nsegs = 0;
	  break;
	  }
	if (strcmp(thisseg, "**ALL**")==0)
	  {
          DRMS_Segment_t *seg;
          HIterator_t *last = NULL;

          while ((seg = drms_record_nextseg(template, &last, 0)))
            segs[nsegs++] = strdup (seg->info->name);

          if (last)
          {
             hiter_destroy(&last);
          }
	  }
  	else
	  segs[nsegs++] = strdup(thisseg);
	}
      }
    free (seglist);
    /* place to put an arrays of segvals and segdims per segment */
    if (nsegs)
      {
      segvals = (json_t **)malloc(nsegs * sizeof(json_t *));
      segdims = (json_t **)malloc(nsegs * sizeof(json_t *));
      segcparms = (json_t **)malloc(nsegs * sizeof(json_t *));
      segbzeros = (json_t **)malloc(nsegs * sizeof(json_t *));
      segbscales = (json_t **)malloc(nsegs * sizeof(json_t *));
      }
    for (iseg=0; iseg<nsegs; iseg++)
    {
       json_t *val = json_new_array();
       segvals[iseg] = val;
       val = json_new_array();
       segdims[iseg] = val;
       val = json_new_array();
       segcparms[iseg] = val;
       val = json_new_array();
       segbzeros[iseg] = val;
       val = json_new_array();
       segbscales[iseg] = val;
    }

    /* get list of links to print for each record */
    nlinks = 0;
    if (links_listed) 
      { /* get specified list */
      char *thislink;
      CGI_unescape_url(linklist);
      for (thislink=strtok(linklist, ","); thislink; thislink=strtok(NULL,","))
	{
	if (strcmp(thislink,"**NONE**")==0)
	  {
	  nlinks = 0;
	  break;
	  }
	if (strcmp(thislink, "**ALL**")==0)
          {
          DRMS_Link_t *link;
          HIterator_t *last = NULL;

          while ((link = drms_record_nextlink(template, &last)))
            links[nlinks++] = strdup (link->info->name);

          if (last)
          {
             hiter_destroy(&last);
          }
	  }
  	else
	  links[nlinks++] = strdup(thislink);
	}
      }
    free (linklist);
    /* place to put an array of linkvals per keyword */
    if (nlinks)
      linkvals = (json_t **)malloc(nlinks * sizeof(json_t *));
    for (ilink=0; ilink<nlinks; ilink++)
      {
      json_t *val = json_new_array();
      linkvals[ilink] = val;
      }

    /* loop over set of selected records */
    for (irec = 0; irec < nrecs; irec++) 
      {
      char recquery[DRMS_MAXQUERYLEN];
      char *jsonquery;
      json_t *recobj = json_new_object();

      if (max_recs == 0)
        rec = drms_recordset_fetchnext(drms_env, recordset, &status, &cstat, NULL);
      else
        {
        rec = recordset->records[irec];  /* pointer to current record */
        status = DRMS_SUCCESS;
        }

      if (wantRecInfo)
	{
        drms_sprint_rec_query(recquery,rec);
        jsonquery = string_to_json(recquery);
        json_insert_pair_into_object(recobj, "name", json_new_string(jsonquery));
        free(jsonquery);
	}
      /* now get keyword information */
      for (ikey=0; ikey<nkeys; ikey++) 
        {
        DRMS_Keyword_t *rec_key_ikey; 
        json_t *thiskeyval = keyvals[ikey]; 
        json_t *val;
        char rawval[20000];
        char *jsonval;

        if (strcmp(keys[ikey],"*recnum*") == 0)
	  {
	  sprintf(rawval,"%lld",rec->recnum);
	  val = json_new_number(rawval);
	  }
        else if (strcmp(keys[ikey],"*sunum*") == 0)
	  {
	  sprintf(rawval,"%lld",rec->sunum);
	  val = json_new_number(rawval);
	  }
        else if (strcmp(keys[ikey],"*size*") == 0)
	  {
          char size[40];
	  SUM_info_t *sinfo = drms_get_suinfo(rec->sunum);
          if (!sinfo)
	    val = json_new_string("NA");
	  else
            {
            sprintf(size,"%.0f", sinfo->bytes);
	    val = json_new_string(size);
            }
	  }
        else if (strcmp(keys[ikey],"*online*") == 0)
	  {
	  SUM_info_t *sinfo = drms_get_suinfo(rec->sunum);
          if (!sinfo)
	    val = json_new_string("NA");
	  else
	    val = json_new_string(sinfo->online_status);
	  }
        else if (strcmp(keys[ikey],"*retain*") == 0)
	  {
	  SUM_info_t *sinfo = drms_get_suinfo(rec->sunum);
          if (!sinfo)
	    val = json_new_string("NA");
	  else
	    {
            int y,m,d;
	    char retain[20];
            if (strcmp("N", sinfo->online_status) == 0)
              val = json_new_string("N/A");
            else
              {
              sscanf(sinfo->effective_date, "%4d%2d%2d", &y,&m,&d);
              sprintf(retain, "%4d.%02d.%02d",y,m,d);
	      val = json_new_string(retain);
              }
	    }
	  }
        else if (strcmp(keys[ikey],"*archive*") == 0)
	  {
	  SUM_info_t *sinfo = drms_get_suinfo(rec->sunum);
          if (!sinfo)
	    val = json_new_string("NA");
	  else
            {
	    if(sinfo->pa_status == DAAP && sinfo->pa_substatus == DAADP)
              val = json_new_string("Pending");
            else
	      val = json_new_string(sinfo->archive_status);
            }
	  }
        else if (strcmp(keys[ikey], "*dir_mtime*") == 0)
          { // get record dir last change date
	  struct stat buf;
          char path[DRMS_MAXPATHLEN];
          char timebuf[100];
          if (!record_set_staged)
	    {
            drms_stage_records(recordset, 0, 0);
            record_set_staged = 1;
	    }
          drms_record_directory (rec, path, 0);
          stat(path, &buf);
          sprint_ut(timebuf, buf.st_mtime + UNIX_EPOCH);
          jsonval = string_to_json(timebuf);
          val = json_new_string(jsonval);
          free(jsonval);
          }
        else if (strcmp(keys[ikey], "*logdir*") == 0)
          {
	  char *logdir = drms_record_getlogdir(rec);
	  if (logdir)
	    {
	    jsonval = string_to_json(logdir);
	    free(logdir);
	    }
	  else
	    jsonval = string_to_json("NO LOG");
	  val = json_new_string(jsonval);
  	  free(jsonval);
          }
        else
	  {
          rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1); 
          if (!rec_key_ikey)
	    {
            fprintf(stderr,"jsoc_info error, keyword not in series: %s\n",keys[ikey]);
	    // JSONDIE("Keyword not in series");
            jsonval = string_to_json("Invalid KeyLink");
            }
          else if (drms_ismissing_keyval(rec_key_ikey))
            jsonval = string_to_json("MISSING");
          else
            {
	    drms_keyword_snprintfval(rec_key_ikey, rawval, sizeof(rawval));
	    /* always report keyword values as strings */
	    jsonval = string_to_json(rawval);
            }
	  val = json_new_string(jsonval);
	  free(jsonval);
	  }
        json_insert_child(thiskeyval, val);
        }
  
      /* now show desired segments */
      int online = 0;
      for (iseg=0; iseg<nsegs; iseg++) 
        {
        DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
        char *jsonpath;
        char *jsondims;
        char path[DRMS_MAXPATHLEN];
        json_t *thissegval = segvals[iseg]; 
        json_t *thissegdim = segdims[iseg]; 
        json_t *thissegcparms = segcparms[iseg];
        json_t *thissegbzero = segbzeros[iseg];
        json_t *thissegbscale = segbscales[iseg];
        int iaxis, naxis = rec_seg_iseg->info->naxis;
        char dims[100], dimval[20];

        // Get paths into segvals
        if (!record_set_staged)
	  {
          drms_stage_records(recordset, 0, 0);
          record_set_staged = 1;
	  }
        drms_record_directory (rec_seg_iseg->record, path, 0);
//        if (!*path)
//          JSONDIE("Can not retrieve record path, SUMS may be offline");
        if (!*path)
        {
           strcpy(path, "NoDataDirectory");
        }
        else
        {
           strncat(path, "/", DRMS_MAXPATHLEN);
           strncat(path, rec_seg_iseg->filename, DRMS_MAXPATHLEN);
        }

        jsonpath = string_to_json(path);
        json_insert_child(thissegval, json_new_string(jsonpath));
        free(jsonpath);
        online = strncmp(path, "/SUM",4) == 0;

        // Get seg dimension info into segdims
        dims[0] = '\0';
        for (iaxis=0; iaxis<naxis; iaxis++)
          {
          if (iaxis)
            strcat(dims, "x");
          sprintf(dimval,"%d",rec_seg_iseg->axis[iaxis]);
          strcat(dims, dimval);
          }
        jsondims = string_to_json(dims);
        json_insert_child(thissegdim, json_new_string(jsondims));
        free(jsondims);

        /* Print bzero and bscale values (use format of those implicit keywords, which is %g) IFF 
         * the segment protocol implies these values (protocols fits, fitsz, tas, etc.) */
        char keybuf[DRMS_MAXKEYNAMELEN];
        DRMS_Keyword_t *anckey = NULL;
        char *jsonkeyval = NULL;

        /* cparms */
        if (strlen(rec_seg_iseg->cparms))
        {
           jsonkeyval = string_to_json(rec_seg_iseg->cparms);
           json_insert_child(thissegcparms, json_new_string(jsonkeyval));
           free(jsonkeyval);
        }

        /* bzero */
        snprintf(keybuf, sizeof(keybuf), "%s_bzero", segs[iseg]);
        anckey = drms_keyword_lookup(rec, keybuf, 1);

        if (anckey)
        {
           drms_keyword_snprintfval(anckey, keybuf, sizeof(keybuf));

           /* always report keyword values as strings */
           jsonkeyval = string_to_json(keybuf);
           json_insert_child(thissegbzero, json_new_string(jsonkeyval));
           free(jsonkeyval);
        }

        /* bscale */
        anckey = NULL;
        snprintf(keybuf, sizeof(keybuf), "%s_bscale", segs[iseg]);
        anckey = drms_keyword_lookup(rec, keybuf, 1);

        if (anckey)
        {
           drms_keyword_snprintfval(anckey, keybuf, sizeof(keybuf));

           /* always report keyword values as strings */
           jsonkeyval = string_to_json(keybuf);
           json_insert_child(thissegbscale, json_new_string(jsonkeyval));
           free(jsonkeyval);
        }
        }

      /* now show desired links */
      for (ilink=0; ilink<nlinks; ilink++) 
        {
        DRMS_Link_t *rec_link = hcon_lookup_lower (&rec->links, links[ilink]); 
        DRMS_Record_t *linked_rec = drms_link_follow(rec, links[ilink], &status);
        char linkquery[DRMS_MAXQUERYLEN];
        if (rec_link->info->type == DYNAMIC_LINK)
          drms_sprint_rec_query(linkquery, linked_rec);
        else
          sprintf(linkquery, "%s[:#%lld]", linked_rec->seriesinfo->seriesname, linked_rec->recnum);
        drms_close_record(linked_rec, DRMS_FREE_RECORD);

        json_t *thislinkval = linkvals[ilink]; 
        json_insert_child(thislinkval, json_new_string(linkquery));
        }

      /* finish record info for this record */
      if (wantRecInfo)
	{
        json_insert_pair_into_object(recobj, "online", json_new_number(online ? "1" : "0"));
        json_insert_child(recinfo, recobj);
	}
      }
  
  /* Finished.  Clean up and exit. */
      if (wantRecInfo)
        json_insert_pair_into_object(jroot, "recinfo", recinfo);

      for (ikey=0; ikey<nkeys; ikey++) 
        {
        json_t *keyname = json_new_string(keys[ikey]); 
        json_t *keyobj = json_new_object();
        json_insert_pair_into_object(keyobj, "name", keyname);
        json_insert_pair_into_object(keyobj, "values", keyvals[ikey]);
        json_insert_child(json_keywords, keyobj);
        }
      json_insert_pair_into_object(jroot, "keywords", json_keywords);

      for (iseg=0; iseg<nsegs; iseg++) 
        {
        json_t *segname = json_new_string(segs[iseg]); 
        json_t *segobj = json_new_object();
        json_insert_pair_into_object(segobj, "name", segname);
        json_insert_pair_into_object(segobj, "values", segvals[iseg]);
        json_insert_pair_into_object(segobj, "dims", segdims[iseg]);
        json_insert_pair_into_object(segobj, "cparms", segcparms[iseg]);
        json_insert_pair_into_object(segobj, "bzeros", segbzeros[iseg]);
        json_insert_pair_into_object(segobj, "bscales", segbscales[iseg]);
        json_insert_child(json_segments, segobj);
        }
      json_insert_pair_into_object(jroot, "segments", json_segments);

      for (ilink=0; ilink<nlinks; ilink++) 
        {
        json_t *linkname = json_new_string(links[ilink]); 
        json_t *linkobj = json_new_object();
        json_insert_pair_into_object(linkobj, "name", linkname);
        json_insert_pair_into_object(linkobj, "values", linkvals[ilink]);
        json_insert_child(json_links, linkobj);
        }
      json_insert_pair_into_object(jroot, "links", json_links);

      sprintf(count, "%d", nrecs);
      json_insert_pair_into_object(jroot, "count", json_new_number(count));
      json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    
    drms_close_records(recordset, DRMS_FREE_RECORD);
    json_tree_to_string(jroot,&json);
    // final_json = json_format_string(json);
    final_json = json;
    printf("Content-type: application/json\n\n");
    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);
    if (my_sum)
      SUM_close(my_sum,printkerr);
    return(0);
    }
  if (my_sum)
    SUM_close(my_sum,printkerr);
  return(0);
  }

