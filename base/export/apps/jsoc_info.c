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
jsoc_info [-hRoz] op=<command> ds=<record_set> [[key=<keylist>] [seg=<seglist>] [link=<linklist>]
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
**NONE** means show no keywords; 
*recnum* means show the hidden keyword "recnum";
*sunum* means show the hidden keyword "sunum";
*size* means show the size of storage unit (bytes);
*online* means show the hidden keyword "online";
*retain* means show retention date, i.e. date at which SUMS may remove the segment storage;
*archive* means show archive status, i.e. has or will SUMS write the storage unit to tape;
*recdir* means show the SU dir;
*logdir* means show the path to the processing log directory; 
*dirmtime* means show the last modification time of the record directory in SUMS; and 
*spec* means show the record's specification
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

/*
 * To emulate a POST request on the cmd-line, force qDecoder, the HTTP-request parsing library, to process a GET request.
 * Since POST passes its argument via stdin to jsoc_fetch, it would be cumbersome
 * to use the POST branch of qDecoder code (which expects args to arrive via stdin, 
 * and expects additional env variables). Instead we can pass the arguments via
 * the cmd-line, or via environment variables.
 *   1. Set two shell environment variables, then run jsoc_fetch:
 *      a. setenv REQUEST_METHOD GET
 *      b. setenv QUERY_STRING 'op=exp_su&method=url_quick&format=json&protocol='as-is'&formatvar=dataobj&requestid=NOASYNCREQUEST&sunum=38400738,38400812' (this is an example - substitute your own arguments).
 *      c. jsoc_info
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "json.h"
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include "printk.h"
#include "exputil.h"
#include "qDecoder.h"
#include "fitsexport.h"

char *MISSING_KEY_NAME = "???";
char *MISSING_KEY_VALUE = "???";
char *MISSING_SEG_NAME = "???";
char *MISSING_LINK_NAME = "???";
char *INVALID = "invalid";
char *INVALID_SEG = "InvalidSegName";

#define USE_FITS_NAMES_FOR_COLUMNS (0)

/* invalidObj is used simply for a fixed address to denote that a keyword, segment, or link struct is bad */
char invalidObj;
const char *userhandle = NULL;

#define JSONDIE(msg) \
do  {	\
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
  manage_userhandle(0, userhandle); \
  return(1);	\
  } while(0)

#ifndef NDEBUG
#define JSOC_INFO_ASSERT(expression, msg) XASSERT(expression)
#else 
#define JSOC_INFO_ASSERT(expression, msg) \
do { \
    if (!(expression)) \
    { \
        JSONDIE(msg); \
    } \
} while(0)
#endif

struct requisitionStructT
{ 
    unsigned int requireRecnum : 1;
    unsigned int requireSunum : 1;
    unsigned int requireSUMinfoSize : 1;
    unsigned int requireSUMinfoOnline : 1;
    unsigned int requireSUMinfoRetain : 1;
    unsigned int requireSUMinfoArchive : 1;
    unsigned int requireRecdir : 1;
    unsigned int requireLogdir : 1;
    unsigned int requireDirmtime : 1;
    unsigned int requireSpec : 1;
    unsigned int requireSUMinfo : 1;
};

static int isInvalidKey(DRMS_Keyword_t *key)
{
    if (key)
    {
        return (void *)(key->record) == (void *)(&invalidObj);
    }
    
    return 1;
}

static int isInvalidSeg(DRMS_Segment_t *seg)
{
    if (seg)
    {
        return (void *)(seg->record) == (void *)(&invalidObj);
    }
    
    return 1;
}

static int isInvalidLink(DRMS_Link_t *link)
{
    if (link)
    {
        return (void *)(link->record) == (void *)(&invalidObj);
    }
    
    return 1;
}

static char *string_to_json(const char *in)
{ 
    // for json vers 0.9 no longer uses wide chars
    char *new = NULL;


    new = strdup(in);
    new = json_escape(new);
    return new;
}

void manage_userhandle(int register_handle, const char *handle)
{
    char cmd[1024];
    
    if (register_handle) // add handle and PID to current processing table
    {
        long PID = getpid();
        exputl_manage_cgibin_handles("a", handle, PID, NULL);
    }
    else if (handle && *handle) // remove handle from current processing table
    {
        exputl_manage_cgibin_handles("d", handle, -1, NULL);
    }
}

static int populateKeyList(const char *listOfKeys, LinkedList_t *reqSegs, DRMS_Record_t *template, DRMS_Record_t **jsdTemplate, DRMS_RecordSet_t *recordSet, int *recsStaged, struct requisitionStructT *requisition, LinkedList_t *reqKeys)
{
    char *listOfKeysWorking = NULL;
    char *currentKey = NULL;
    char *saver = NULL;
    HIterator_t *last = NULL;
    DRMS_Keyword_t *keyTemplate = NULL;
    int status = DRMS_SUCCESS;
    int sumInfoSizeKey = 0;
    int sumInfoOnlineKey = 0;
    int sumInfoRetainKey = 0;
    int sumInfoArchiveKey = 0;
    int recnumKey = 0;
    int sunumKey = 0;
    int recdirKey = 0;
    int logdirKey = 0;
    int dirmtimeKey = 0;
    int specKey = 0;
    int sumInfoFakeKey = 0;
    int fakeKey = 0;
    
    JSOC_INFO_ASSERT(listOfKeys && *listOfKeys != '\0' && reqSegs && template && jsdTemplate && recordSet && recsStaged && requisition && reqKeys, "populateKeyList(): invalid arguments");
    listOfKeysWorking = strdup(listOfKeys);
    JSOC_INFO_ASSERT(listOfKeysWorking, "populateKeyList(): out of memory");

    /* never use strtok() under any circumstances - you never know what code inside your loop is going to 
     * call strtok() */
    for (currentKey = strtok_r(listOfKeysWorking, ",", &saver); currentKey; currentKey=strtok_r(NULL, ",", &saver))
    {
        if ((strcmp(currentKey,"**NONE**") == 0))
        {
            JSOC_INFO_ASSERT(list_llgetnitems(reqKeys) == 0, "Invalid key list (cannot specify **NONE** with other keys).");
        }
        else if ((strcmp(currentKey, "**ALL**") == 0))
        {
            /* the fake keywords can be combined with **ALL** */
                             
            /* look-up all keywords; use proximal keyword names; use unexpanded per-segment keyword names, 
             * i.e., use the jsdTemplate, not the regular template */
            if (!*jsdTemplate)
            {
                *jsdTemplate = drms_create_jsdtemplate_record(drms_env, template->seriesinfo->seriesname, &status);
            }
            
            JSOC_INFO_ASSERT(*jsdTemplate, "Could not obtain the JSD template record.");
            JSOC_INFO_ASSERT(last == NULL, "");

            while ((keyTemplate = drms_record_nextkey(*jsdTemplate, &last, 0)) != NULL)
            {
                JSOC_INFO_ASSERT(keyTemplate->info && *(keyTemplate->info->name) != '\0', "Invalid keyword information.");

                if (!drms_keyword_getimplicit(keyTemplate))
                {
                    if (drms_keyword_getperseg(keyTemplate))
                    {
                        /* include the per-segment keyword only if the user has requested information for at least one
                         * segment
                         */
                        if (list_llgetnitems(reqSegs) > 0)
                        {
                            if (!list_llfind(reqKeys, (void *)&keyTemplate))
                            {
                                /* **ALL** could have been provided, in which case we want to avoid duplicates */
                                list_llinserttail(reqKeys, (void *)&keyTemplate);
                            }
                        }
                    }
                    else
                    {
                        if (!list_llfind(reqKeys, (void *)&keyTemplate))
                        {
                            /* this keyword could have been provided (below), in which case we want to avoid duplicates */
                            list_llinserttail(reqKeys, (void *)&keyTemplate);
                        }
                    }
                }
            }

            if (last)
            {
                hiter_destroy(&last);
            }                     
        }
        else 
        {
            /* the fake keywords can be combined with **ALL** */
            if ((recnumKey = (strcasecmp(currentKey, "*recnum*") == 0)) && !requisition->requireRecnum)
            {
                requisition->requireRecnum = 1;
            }
            
            if ((sunumKey = (strcasecmp(currentKey, "*sunum*") == 0)) && !requisition->requireSunum)
            {
                requisition->requireSunum = 1;
            }

            if ((sumInfoSizeKey = (strcasecmp(currentKey, "*size*") == 0)) && !requisition->requireSUMinfoSize)
            {
                requisition->requireSUMinfoSize = 1;
            }
            
            if ((sumInfoOnlineKey = (strcasecmp(currentKey, "*online*") == 0)) && !requisition->requireSUMinfoOnline)
            {
                requisition->requireSUMinfoOnline = 1;
            }
            
            if ((sumInfoRetainKey = (strcasecmp(currentKey, "*retain*") == 0)) && !requisition->requireSUMinfoRetain)
            {
                requisition->requireSUMinfoRetain = 1;
            }
            
            if ((sumInfoArchiveKey = (strcasecmp(currentKey, "*archive*") == 0)) && !requisition->requireSUMinfoArchive)
            {
                requisition->requireSUMinfoArchive = 1;
            }
            
            if ((recdirKey = (strcasecmp(currentKey, "*recdir*") == 0)) && !requisition->requireRecdir)
            {
                requisition->requireRecdir = 1;                
            }
            
            if ((logdirKey = (strcasecmp(currentKey, "*logdir*") == 0)) && !requisition->requireLogdir)
            {
                requisition->requireLogdir = 1;
            }

            if ((dirmtimeKey = (strcasecmp(currentKey, "*dirmtime*") == 0)) && !requisition->requireDirmtime)
            {
                requisition->requireDirmtime = 1;                
            }
            
            if ((specKey = (strcasecmp(currentKey, "*spec*") == 0)) && !requisition->requireSpec)
            {
                requisition->requireSpec = 1;
            }

            sumInfoFakeKey = sumInfoSizeKey | sumInfoOnlineKey | sumInfoRetainKey | sumInfoArchiveKey;
            fakeKey = sumInfoFakeKey | recnumKey | sunumKey | recdirKey | logdirKey | dirmtimeKey | specKey;

            if (!requisition->requireSUMinfo)
            {                    
                requisition->requireSUMinfo = sumInfoFakeKey;
            }

            /* ugh - if **ALL** was in the key list, then we do not want to add these - they are
             * duplicates */
            if (!fakeKey)
            {
                if (!*jsdTemplate)
                {
                    *jsdTemplate = drms_create_jsdtemplate_record(drms_env, template->seriesinfo->seriesname, &status);
                }
                
                JSOC_INFO_ASSERT(*jsdTemplate, "Could not obtain the JSD template record.");
            
                /* look-up keyword; if not found, we need to print some kind of invalid-key string */
                keyTemplate = drms_keyword_lookup(*jsdTemplate, currentKey, 0);

                if (!keyTemplate || !(keyTemplate->info) || *(keyTemplate->info->name) == '\0')
                {
                    keyTemplate = calloc(1, sizeof(DRMS_Keyword_t));
                    keyTemplate->record = (DRMS_Record_t *)(&invalidObj);
                    keyTemplate->info = (DRMS_KeywordInfo_t *)calloc(1, sizeof(DRMS_KeywordInfo_t));
                    snprintf(keyTemplate->info->name, sizeof(keyTemplate->info->name), "%s", currentKey);                    
                }
                else if (drms_keyword_getimplicit(keyTemplate))
                {
                    keyTemplate = calloc(1, sizeof(DRMS_Keyword_t));
                    keyTemplate->record = (DRMS_Record_t *)(&invalidObj);
                    keyTemplate->info = (DRMS_KeywordInfo_t *)calloc(1, sizeof(DRMS_KeywordInfo_t));
                    snprintf(keyTemplate->info->name, sizeof(keyTemplate->info->name), "%s", currentKey);
                }

                if (!list_llfind(reqKeys, (void *)&keyTemplate))
                {
                    /* **ALL** could have been provided, in which case we want to avoid duplicates */
                    list_llinserttail(reqKeys, (void *)&keyTemplate);
                }
            }
        }
    }
    
    free(listOfKeysWorking);
    
    return list_llgetnitems(reqKeys);
}

static int populateSegList(const char *listOfSegs, int followLinks, DRMS_Record_t *template, DRMS_RecordSet_t *recordSet, int *recsStaged, struct requisitionStructT *requisition, LinkedList_t *reqSegs, int *allSegs)
{
    char *listOfSegsWorking = NULL;
    char *currentSeg = NULL;
    char *saver = NULL;
    HIterator_t *last = NULL;
    DRMS_Segment_t *segTemplate = NULL;

    JSOC_INFO_ASSERT(listOfSegs && *listOfSegs != '\0' && template && recordSet && recsStaged && requisition && reqSegs, "populateSegList(): invalid arguments");
    
    listOfSegsWorking = strdup(listOfSegs);
    JSOC_INFO_ASSERT(listOfSegsWorking, "populateSegList(): out of memory");
    
    if (strstr(listOfSegsWorking, "**NONE**") != NULL)
    {
        JSOC_INFO_ASSERT(list_llgetnitems(reqSegs) == 0, "Invalid segment list (cannot specify **NONE** with other segments).");
    }
    else if (strstr(listOfSegsWorking, "**ALL**") != NULL)
    {
        if (allSegs)
        {
            *allSegs = 1;
        }
        
        JSOC_INFO_ASSERT(last == NULL, "about to leak");

        while ((segTemplate = drms_record_nextseg(template, &last, 0)) != NULL)
        {
            JSOC_INFO_ASSERT(segTemplate && segTemplate->info && *(segTemplate->info->name) != '\0', "Invalid segment information.");
        
            if (followLinks && segTemplate->info->islink)
            {
                /* Since rec is a template record, cannot follow links in the ordinary manner. Use this function - it finds the template
                 * segment of the series linked to. */
                int lnkstat = DRMS_SUCCESS;

                drms_template_segment_followlink(segTemplate, &lnkstat);
                JSOC_INFO_ASSERT(lnkstat != DRMS_SUCCESS, "Unable to follow link.");
            }
            
            if (!list_llfind(reqSegs, (void *)&segTemplate))
            {
                list_llinserttail(reqSegs, (void *)&segTemplate);
            }
        }
        
        if (last)
        {
            hiter_destroy(&last);
        }
    }
    else
    {
        for (currentSeg = strtok_r(listOfSegsWorking, ",", &saver); currentSeg; currentSeg = strtok_r(NULL, ",", &saver))
        {        
            /* don't follow links */
            segTemplate = hcon_lookup_lower(&(template->segments), currentSeg);
    
            if (!segTemplate || !(segTemplate->info) || *(segTemplate->info->name) == '\0')
            {
                segTemplate = calloc(1, sizeof(DRMS_Segment_t));
                segTemplate->record = (DRMS_Record_t *)(&invalidObj);
                segTemplate->info = calloc(1, sizeof(DRMS_SegmentInfo_t));
                snprintf(segTemplate->info->name, sizeof(segTemplate->info->name), "%s", currentSeg);
            }
            else
            {
                if (followLinks && segTemplate->info->islink)
                {
                    /* Since rec is a template record, cannot follow links in the ordinary manner. Use this function - it finds the template
                     * segment of the series linked to. 
                     */
                    int lnkstat = DRMS_SUCCESS;

                    drms_template_segment_followlink(segTemplate, &lnkstat);
                    if (lnkstat != DRMS_SUCCESS)
                    {
                        segTemplate = calloc(1, sizeof(DRMS_Segment_t));
                        segTemplate->record = (DRMS_Record_t *)(&invalidObj);
                        segTemplate->info = calloc(1, sizeof(DRMS_SegmentInfo_t));
                        snprintf(segTemplate->info->name, sizeof(segTemplate->info->name), "%s", currentSeg);
                    }
                }
            }
        
            if (!list_llfind(reqSegs, (void *)&segTemplate))
            {
                /* avoid duplicates */
                list_llinserttail(reqSegs, (void *)&segTemplate);
            }
        }
    }

    if (list_llgetnitems(reqSegs) > 0)
    {        
        if (!requisition->requireSUMinfo)
        {
            /* we need this for the online property of each segment */
            requisition->requireSUMinfo = 1;
        }
    }
    
    return list_llgetnitems(reqSegs);
}

static int populateLinkList(const char *listOfLinks, DRMS_Record_t *template, LinkedList_t *reqLinks)
{
    char *listOfLinksWorking = NULL;
    char *currentLink = NULL;
    char *saver = NULL;
    HIterator_t *last = NULL;
    DRMS_Link_t *linkTemplate = NULL;

    JSOC_INFO_ASSERT(listOfLinks && *listOfLinks != '\0' && template && reqLinks, "populateLinkList(): invalid arguments");
    
    listOfLinksWorking = strdup(listOfLinks);
    JSOC_INFO_ASSERT(listOfLinksWorking, "populateLinkList(): out of memory");

    for (currentLink = strtok_r(listOfLinksWorking, ",", &saver); currentLink != NULL; currentLink = strtok_r(NULL, ",", &saver))
    {
        if (strcmp(currentLink, "**NONE**") == 0)
        {
            JSOC_INFO_ASSERT(list_llgetnitems(reqLinks) == 0, "Invalid link list (cannot specify **NONE** with other links).");
        }
        else if (strcmp(currentLink, "**ALL**")==0)
        {
            DRMS_Link_t *link = NULL;
            
            JSOC_INFO_ASSERT(last == NULL, "about to leak");
            while ((linkTemplate = drms_record_nextlink(template, &last)) != NULL)
            {
                JSOC_INFO_ASSERT(linkTemplate->info && *(linkTemplate->info->name) != '\0', "Invalid link information.");
                if (!list_llfind(reqLinks, (void *)&linkTemplate))
                {
                    list_llinserttail(reqLinks, (void *)&linkTemplate);
                }
            }

            if (last)
            {
                hiter_destroy(&last);
            }
        }
        else
        {
            linkTemplate = hcon_lookup_lower(&template->links, currentLink);
                    
            if (!linkTemplate || !(linkTemplate->info) || *(linkTemplate->info->name) == '\0')
            {
                linkTemplate = calloc(1, sizeof(DRMS_Link_t));
                linkTemplate->record = (DRMS_Record_t *)(&invalidObj);
                linkTemplate->info = (DRMS_LinkInfo_t *)calloc(1, sizeof(DRMS_LinkInfo_t));
                snprintf(linkTemplate->info->name, sizeof(linkTemplate->info->name), "%s", currentLink);
            }
        
            if (!list_llfind(reqLinks, (void *)&linkTemplate))
            {
                list_llinserttail(reqLinks, (void *)&linkTemplate);
            }
        }
    }
    
    return list_llgetnitems(reqLinks);
}

static char x2c (char *what)
  {
  char digit;
  digit = (char)(what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit = (char)(digit + (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0')));
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

/* returns a container (`log_dirs`) of DRMS log directories; the key is <namespace>:<session id>, and the value is 
 * a directory
 * parameters:
 *   env
 *   recs
 *   nrecs
 *   log_dirs
 */
static int GetLogDirs(DRMS_Env_t *env, DRMS_Record_t **recs, int nrecs, HContainer_t *log_dirs)
{
    /* assumes that all records in recs are from the same series (so the namespace is consistent); recs is a subset 
     * of records from a record-set with potentially many subsets */
    int err = 0;
    int drms_status = DRMS_SUCCESS;
    int irec;
    HContainer_t *session_ids = NULL;
    LinkedList_t *ns_session_ids = NULL;
    LinkedList_t **p_ns_session_ids = NULL;
    HContainer_t *ns_session_ids_set = NULL;
    char dupe_flag = 'T';
    HIterator_t *ns_iter = NULL;
    const char *session_ns = NULL;
    long long session_id = 0;
    ListNode_t *node = NULL;
    char session_id_str[64];
    char *session_id_list_str = NULL;
    size_t sz_session_id_list_str = 256;
    char query[DRMS_MAXQUERYLEN];
    DB_Binary_Result_t *qres = NULL;
    long long *sunums = NULL;
    int irow;
    SUM_info_t **info_structs = NULL;
    char log_id[128] = {0};
    char *log_dir = NULL;
    
    
    if (!recs || nrecs <= 0)
    {
        return err;
    }
    
    /* collect all session IDs - ugh, the namespace does not necessarily need to be consistent, even when the record 
     * belong to the same series */
     
    /* dupe map <session namespace>:<session ID> -> char (bool implementation) */
    ns_session_ids_set = hcon_create(sizeof(char), 64, NULL, NULL, NULL, NULL, 0);
    for (irec = 0; irec < nrecs && !err; irec++)
    {
        /* group all session ids into session namespaces; one list per namespace stored in an hcon keyed by
         * namespace (which can be 63 bytes max according to PG)
         */
        if (session_ids == NULL)
        {
            session_ids = hcon_create(sizeof(LinkedList_t *), 64, (void (*)(const void *))list_llfree, NULL, NULL, NULL, 0);
            err = (session_ids == NULL);
        }
        
        if (!err)
        {
            /* skip <session namespace>:<session ID> dupes */
            snprintf(log_id, sizeof(log_id), "%s:%llu", recs[irec]->sessionns, recs[irec]->sessionid);
            if (!hcon_member_lower(ns_session_ids_set, log_id))
            {
                /* get list for this session namespace */
                p_ns_session_ids = (LinkedList_t **)hcon_lookup_lower(session_ids, recs[irec]->sessionns);
                if (!p_ns_session_ids)
                {
                    /* new namespace, create new list */
                    ns_session_ids = list_llcreate(sizeof(long long), NULL);
                    hcon_insert(session_ids, recs[irec]->sessionns, &ns_session_ids);
                    err = (ns_session_ids == NULL);
                }
                else
                {
                    ns_session_ids = *(LinkedList_t **)p_ns_session_ids;
                    err = (ns_session_ids == NULL);
                }
            
                if (!err)
                {
                    list_llinserttail(ns_session_ids, &recs[irec]->sessionid);
                    
                    /* update dupes map */
                    hcon_insert(ns_session_ids_set, log_id, &dupe_flag);
                }
            }
        }
    }
    
    /* no longer need dupes map */
    if (ns_session_ids_set)
    {
        hcon_destroy(&ns_session_ids_set);
    }
    
    if (!err)
    {
        /* iterate through each namespace's list */
        ns_iter = hiter_create(session_ids);

        /* namespace loop */
        while ((p_ns_session_ids = (LinkedList_t **)hiter_extgetnext(ns_iter, &session_ns)) != NULL && !err)
        {
            ns_session_ids = *p_ns_session_ids;
            
            /* iter through each sessionid */
            list_llreset(ns_session_ids);     
            while ((node = list_llnext(ns_session_ids)) != NULL)
            {
                session_id = *(long long *)node->data;
                snprintf(session_id_str, sizeof(session_id_str), "%lld", session_id);
                if (session_id_list_str == NULL)
                {
                    session_id_list_str = calloc(1, sizeof(sz_session_id_list_str));
                }
                
                session_id_list_str = base_strcatalloc(session_id_list_str, session_id_str, &sz_session_id_list_str);
            
                if (node->next != NULL)
                {
                    session_id_list_str = base_strcatalloc(session_id_list_str, ",", &sz_session_id_list_str);
                }
            } /* session id loop */
            
            /* do the actual DB query */
            snprintf(query, sizeof(query), "SELECT sessionid, sunum FROM %s.drms_session WHERE sessionid IN (%s)", session_ns, session_id_list_str);
            qres = drms_query_bin(env->session, query);
            err = (qres == NULL);
            
            if (!err)
            {
                if (qres->num_rows > 0)
                {    
                    /* create a list of SUs so we can call drms_getsuinfo() */
                    sunums = calloc(qres->num_rows, sizeof(long long));
                    err = (sunums == NULL); /* out of memory */
                }
            }
            
            if (!err)
            {
                if (qres->num_rows > 0)
                {
                    for (irow = 0; irow < qres->num_rows; irow++)
                    {
                        sunums[irow] = db_binary_field_getlonglong(qres, irow, 1);
                    }

                    info_structs = (SUM_info_t **)calloc(qres->num_rows, sizeof(SUM_info_t *));
                    err = (info_structs == NULL); /* out of memory */
                }
            }
            
            if (!err)
            {
                if (qres->num_rows > 0)
                {
                    err = (drms_getsuinfo(env, sunums, qres->num_rows, info_structs) != DRMS_SUCCESS);
                }
            }
            
            if (sunums)
            {
                free(sunums);
                sunums = NULL;
            }
            
            if (!err)
            {
                /* extract logdir path */
                for (irow = 0; irow < qres->num_rows && !err; irow++)
                {
                    /* <session namespace>:<session id> */
                    snprintf(log_id, sizeof(log_id), "%s:%llu", session_ns, db_binary_field_getlonglong(qres, irow, 0));

                    if (*info_structs[irow]->online_loc == '\0' || *info_structs[irow]->online_status != 'Y')
                    {
                        /* invalid SU */
                        log_dir = strdup("NoDataDirectory");
                    }
                    else
                    {
                        /* valid, online SU */
                        log_dir = strdup(info_structs[irow]->online_loc);
                    }
                    
                    hcon_insert(log_dirs, log_id, &log_dir);
                }
            }
            
            if (info_structs)
            {
                free(info_structs);
                info_structs = NULL;
            }

            if (qres)
            {
                db_free_binary_result(qres);
                qres = NULL;
            }
        } /* namespace loop */

        if (ns_iter)
        {
            hiter_destroy(&ns_iter);
        }
    }
    
    /* all session-id info should now be in the out HContainer_t; no longer need this mem */
    if (session_ids)
    {
        hcon_destroy(&session_ids);
    }

    return err;
}

static const char *GetLogDir(DRMS_Record_t *rec, HContainer_t *log_dirs)
{
    char log_id[128];
    char **p_log_dir = NULL;
    LinkedList_t **p_ns_session_ids = NULL;
    const char *ret = NULL;

    
    snprintf(log_id, sizeof(log_id), "%s:%llu", rec->sessionns, rec->sessionid);
    
    /* retrieve appropriate session_id-list */
    p_log_dir = (char **)hcon_lookup_lower(log_dirs, log_id);
    
    if (p_log_dir)
    {
        ret = *p_log_dir;
    }
    
    return ret;
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


/*******
IF YOU ADD A NEW ARGUMENT: 1. Add a corresponding line of code in the from_web block of code in DoIt(), 2.
 ensure that the cmdparams_get_* call appears AFTER the from_web block of code in DoIt()
 *******/
ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "Not Specified", "<Operation>, values are: series_struct, rs_summary, or rs_list "},
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "key", "Not Specified", "<comma delimited keyword list>, keywords or special values: **ALL**, **NONE**, *recnum*, *sunum*, *size*, *online*, *retain*, *archive*, *recdir*, *logdir*, *dirmtime*, *spec*"},
  {ARG_STRING, "link", "Not Specified", "<comma delimited linkname list>, links or special values: **ALL**, **NONE**"},
  {ARG_STRING, "seg", "Not Specified", "<comma delimited segment list>, segnames or special values: **ALL**, **NONE** "},
  {ARG_STRING, "userhandle", "Not Specified", "Unique request identifier to allow possible user kill of this program."},
    {ARG_FLAG, "l", NULL, "Follow links to linked segments."},
  {ARG_INT, "n", "0", "RecordSet Limit"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_FLAG, "R", "0", "Show record query"},
  {ARG_FLAG, "z", "0", "emit JSON output"},
  {ARG_FLAG, "o", "0", "print additional DRMS information when op==series_struct"},
    {ARG_FLAG, "f", NULL, "print FITS keyword names, instead of DRMS keyword names"},
    {ARG_FLAG, "r", NULL, "do NOT display run time"},
    {ARG_FLAG, "s", NULL, "suppress HTTP headers"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_STRING, "REMOTE_ADDR", "0.0.0.0", "Remote IP address"},
  {ARG_STRING, "SERVER_NAME", "ServerName", "JSOC Server Name"},
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
	"key=<comma delimited keyword list>, keywords or special values: **ALL**, **NONE**, *recnum*, *sunum*, *size*, *online*, *retain*, *archive*, *recdir*, *logdir*, *dirmtime*, *spec* \n"
	"seg=<comma delimited segment list>, segnames or special values: **ALL**, **NONE** \n"
        "userhandle=<userhandle> unique id to allow user to kill this program by passing userhandle to jsoc_userkill.\n"
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
      strcat(query, "[^]");
  else
    strcat(query, "[:#^]");
// fprintf(stderr,"test 1 query is %s\n",query);
  rs = drms_open_nrecords(rec->env, query, 1, &status);
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
      strcat(query, "[$]");
  else
    strcat(query, "[:#$]");
  rs = drms_open_nrecords(rec->env, query, -1, &status);
  return(rs);
  }

/* temp hack to let -o add owner to series_struct, 28 Dec 11 */
int wantowner = 0;

/* returns series owner as static string */
char *drms_getseriesowner(DRMS_Env_t *drms_env, char *series, int *status)
   {
   char *nspace = NULL;
   char *relname = NULL;
   int istat = DRMS_SUCCESS;
   DB_Text_Result_t *qres = NULL;
   static char owner[256];
   owner[0] = '\0';

   if (!get_namespace(series, &nspace, &relname))
      {
      char query[1024];
      strtolower(nspace);
      strtolower(relname);

      snprintf(query, sizeof(query), "SELECT pg_catalog.pg_get_userbyid(T1.relowner) AS owner FROM pg_catalog.pg_class AS T1, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '%s') AS T2 WHERE T1.relnamespace = T2.oid AND T1.relname = '%s'", nspace, relname);

      if ((qres = drms_query_txt(drms_env->session, query)) != NULL)
         {
         if (qres->num_cols == 1 && qres->num_rows == 1)
            strcpy(owner, qres->field[0][0]);
         db_free_text_result(qres);
         }
      }
   *status = owner[0] != 0;
   return(owner);
   }

static json_t *createJsonStringVal(const char *text)
{
    json_t *rv = NULL;
    char *escText = NULL;
    char *jsonLibShouldReallyDeclareThisConst = NULL;
    
    if (text && *text != '\0')
    {
        jsonLibShouldReallyDeclareThisConst = strdup(text);
        JSOC_INFO_ASSERT(jsonLibShouldReallyDeclareThisConst, "out of memory");
        escText = string_to_json(jsonLibShouldReallyDeclareThisConst);
        JSOC_INFO_ASSERT(escText, "out of memory");
        rv = json_new_string(escText);
        JSOC_INFO_ASSERT(rv, "out of memory");
        free(escText);
        escText = NULL;
        free(jsonLibShouldReallyDeclareThisConst);
        jsonLibShouldReallyDeclareThisConst = NULL;
    }
    
    return rv;
}

static int list_series_info(DRMS_Env_t *drms_env, DRMS_Record_t *rec, json_t *jroot, int followLinks, int useFitsKeyNames)
{
    int error = 0;
    
    DRMS_Keyword_t *key;
    DRMS_Segment_t *seg;
    DRMS_Link_t *link;
    HIterator_t *last = NULL;
    char intstring[100];
    char *notework;
    char *owner;
    json_t *indexarray = NULL;
    json_t *primearray = NULL;
    json_t *keyarray = NULL;
    json_t *segarray = NULL;
    json_t *linkarray = NULL;
    json_t *primeinfoarray = NULL;
    int npkeys;
    int status;
    char prevKeyName[DRMS_MAXNAMELEN] = "";
    char baseKeyName[DRMS_MAXNAMELEN];
    char fitsName[16];
    char *fitsValue = NULL;
    json_t *jsonVal = NULL;
    const char *keyNameOut = NULL;
    DRMS_Keyword_t *skey = NULL; /* slot key */

    /* add description from seriesinfo */
    notework = string_to_json(rec->seriesinfo->description);
    json_insert_pair_into_object(jroot, "note", json_new_string(notework));
    free(notework);
    /* add retention, unitsize, archive, and tapegroup integers */
    int16_t newSuRet = drms_series_getnewsuretention(rec->seriesinfo);
    int16_t stagingRet = drms_series_getstagingretention(rec->seriesinfo);
    
    if (stagingRet == 0)
    {
        stagingRet = (int16_t)abs(STDRETENTION);
    }

    snprintf(intstring, sizeof(intstring), "%hd", newSuRet);
    json_insert_pair_into_object(jroot, "retention", json_new_number(intstring));
    snprintf(intstring, sizeof(intstring), "%hd", stagingRet);
    json_insert_pair_into_object(jroot, "stagingretention", json_new_number(intstring));
    sprintf(intstring, "%d", rec->seriesinfo->unitsize);
    json_insert_pair_into_object(jroot, "unitsize", json_new_number(intstring));
    sprintf(intstring, "%d", rec->seriesinfo->archive);
    json_insert_pair_into_object(jroot, "archive", json_new_number(intstring));
    sprintf(intstring, "%d", rec->seriesinfo->tapegroup);
    json_insert_pair_into_object(jroot, "tapegroup", json_new_number(intstring));
    /* add ownder for series */

    if (wantowner)
    {
        owner = string_to_json(drms_getseriesowner(drms_env, rec->seriesinfo->seriesname, &status));
        json_insert_pair_into_object(jroot, "owner", json_new_string(owner));
        free(owner);
    }
  
    /* show the prime index keywords */
    // both the original simple list and new array of objects are generated -- XXXXX REMOVE SOMEDAY                             
    // for compatibility.  The older "primekeys" array may be removed in the future.  23Nov09 -- XXXXX REMOVE SOMEDAY           
    // old lines marked with trailing XXXXX REMOVE SOMEDAY
    primearray = json_new_array(); // XXXXX REMOVE SOMEDAY
    primeinfoarray = json_new_array();
    JSOC_INFO_ASSERT(primeinfoarray, "out of memory");
    npkeys = rec->seriesinfo->pidx_num;

    if (npkeys > 0)
    {
        int i;
        json_t *primeinfo = NULL;
        char *jsonstr;

        for (i=0; i<npkeys; i++)
        {
            json_t *primeinfo = json_new_object();
            XASSERT(primeinfo);
            DRMS_Keyword_t *pkey = NULL;
            DRMS_Keyword_t *stepKey = NULL;
            char rawval[128];
            int isSlotted = 0;
            pkey = rec->seriesinfo->pidx_keywords[i];
            error = 0;
            
            if (!pkey || !pkey->info || *(pkey->info->name) == '\0')
            {
                fprintf(stderr, "Corrupt DRMS pkey keyword %s.\n", MISSING_KEY_NAME);
                error = 1;
            }

            if (!error)
            {            
                if (drms_keyword_isindex(pkey))
                {
                    isSlotted = 1;
                    skey = drms_keyword_slotfromindex(pkey);

                    if (!skey || !skey->info || *(skey->info->name) == '\0')
                    {
                        fprintf(stderr, "Corrupt DRMS index keyword %s.\n", pkey->info->name);
                        error = 1;
                    }

                    if (!error)
                    {
                        if (USE_FITS_NAMES_FOR_COLUMNS && useFitsKeyNames)
                        {
                            if (fitsexport_getmappedextkeyname(skey, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                            {
                                keyNameOut = fitsName;
                            }
                            else
                            {
                                fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", skey->info->name);
                                keyNameOut = MISSING_KEY_NAME;
                                error = 1;
                            }
                        }
                        else
                        {
                            keyNameOut = skey->info->name;
                        }
                    }
                    else
                    {
                        /* problem with skey */
                        keyNameOut = MISSING_KEY_NAME;
                    }
                
                    stepKey = drms_keyword_stepfromvalkey(skey);
                
                    if (!stepKey || !stepKey->info || *(stepKey->info->name) == '\0')
                    {
                        fprintf(stderr, "Corrupt DRMS step keyword for keyword %s.\n", skey->info->name);
                        error = 1;
                    }
                
                    if (!error)
                    {
                        if (USE_FITS_NAMES_FOR_COLUMNS && useFitsKeyNames)
                        {
                            JSOC_INFO_ASSERT(fitsValue == NULL, "about to leak");
                            fitsexport_getmappedextkeyvalue(stepKey, &fitsValue);

                            if (fitsValue)
                            {                    
                                snprintf(rawval, sizeof(rawval), "%s", fitsValue);
                                free(fitsValue);
                                fitsValue = NULL;
                            }
                            else
                            {
                                fprintf(stderr, "Unable to map DRMS keyword value %s to FITS keyword value.\n", stepKey->info->name);
                                snprintf(rawval, sizeof(rawval), "%s", MISSING_KEY_VALUE);
                                error = 1;
                            }
                        }
                        else
                        {                
                            drms_keyword_snprintfval(stepKey, rawval, sizeof(rawval));
                        }
                    }
                    else
                    {
                        snprintf(rawval, sizeof(rawval), "%s", MISSING_KEY_VALUE);
                    }                
                }
                else
                {
                    /* pkey is not an index keyword */
                    if (USE_FITS_NAMES_FOR_COLUMNS && useFitsKeyNames)
                    {
                        if (fitsexport_getmappedextkeyname(pkey, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                        {
                            keyNameOut = fitsName;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", pkey->info->name);
                            keyNameOut = MISSING_KEY_NAME;
                            error = 1;
                        }
                    }
                    else
                    {
                        keyNameOut = pkey->info->name;   
                    }                    
                }
            }
            else
            {
                /* invalid pkey */
                keyNameOut = MISSING_KEY_NAME;
            }

            /* primeinfo - keyword name */
            jsonVal = createJsonStringVal(keyNameOut);
            json_insert_pair_into_object(primeinfo, "name", jsonVal);
            jsonVal = NULL;
            
            /* primeinfo - slotted */
            jsonVal = isSlotted ? json_new_number("1") : json_new_number("0");
            JSOC_INFO_ASSERT(jsonVal, "out of memory");
            json_insert_pair_into_object(primeinfo, "slotted", jsonVal);
            jsonVal = NULL;
            
            /* primeinfo - step key value (optional) */
            if (stepKey)
            {
                jsonVal = createJsonStringVal(rawval);
                json_insert_pair_into_object(primeinfo, "step", jsonVal);
                jsonVal = NULL;
            }
            
            json_insert_child(primeinfoarray, primeinfo);            
            
            /* primearray */
            jsonVal = createJsonStringVal(keyNameOut);
            json_insert_child(primearray, jsonVal); // XXXXX REMOVE SOMEDAY
            jsonVal = NULL;
        }
        
        error = 0;
    }
    else
    {
        jsonVal = json_new_null();
        XASSERT(jsonVal);
        json_insert_child(primearray, jsonVal); // XXXXX REMOVE SOMEDAY
        jsonVal = json_new_null();
        XASSERT(jsonVal);
        json_insert_child(primeinfoarray, jsonVal);
        jsonVal = NULL;
    }
    
    json_insert_pair_into_object(jroot, "primekeys", primearray); // XXXXX REMOVE SOMEDAY                                       
    json_insert_pair_into_object(jroot, "primekeysinfo", primeinfoarray);
 
    
    /* show DB index keywords */
    indexarray = json_new_array();
    XASSERT(indexarray);

    if (rec->seriesinfo->dbidx_num > 0)
    {
        int i;

        for (i=0; i<rec->seriesinfo->dbidx_num; i++)
        {
            error = 0;
            key = rec->seriesinfo->dbidx_keywords[i];

            if (drms_keyword_isindex(key))
            {
                skey = drms_keyword_slotfromindex(key);

                if (!skey || !skey->info || *(skey->info->name) == '\0')
                {
                    fprintf(stderr, "Corrupt DRMS index keyword %s.\n", key->info->name);
                    error = 1;
                }

                if (!error)
                {
                    if (USE_FITS_NAMES_FOR_COLUMNS && useFitsKeyNames)
                    {
                        if (fitsexport_getmappedextkeyname(skey, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                        {
                            keyNameOut = fitsName;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", skey->info->name);
                            keyNameOut = MISSING_KEY_NAME;
                            error = 1;
                        }
                    }
                    else
                    {
                        keyNameOut = skey->info->name;
                    }
                }
                else
                {
                    /* problem with skey */
                    keyNameOut = MISSING_KEY_NAME;
                }
            }
            else
            {
                if (!error)
                {
                    if (USE_FITS_NAMES_FOR_COLUMNS && useFitsKeyNames)
                    {
                        if (fitsexport_getmappedextkeyname(key, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                        {
                            keyNameOut = fitsName;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", key->info->name);
                            keyNameOut = MISSING_KEY_NAME;
                            error = 1;
                        }
                    }
                    else
                    {
                        keyNameOut = key->info->name;
                    }
                }
                else
                {
                    /* problem with skey */
                    keyNameOut = MISSING_KEY_NAME;
                }
            }

            jsonVal = createJsonStringVal(keyNameOut);
            json_insert_child(indexarray, jsonVal);
            jsonVal = NULL;
        }
    }
    else
    {
        jsonVal = json_new_null();
        XASSERT(jsonVal);
        json_insert_child(indexarray, jsonVal);
        jsonVal = NULL;
    }

    json_insert_pair_into_object(jroot, "dbindex", indexarray);

if (DEBUG) fprintf(stderr,"   starting all keywords\n");

    json_t *keyinfo= NULL;
    json_t *keytype = NULL;
    json_t *defval = NULL;
    json_t *recscope = NULL;
    char rawval[128];
    char *jsonstr = NULL;
    char *typework = NULL;
    char *scopework = NULL;
    char *unitswork = NULL;
    char *defvalwork = NULL;

    /* show all keywords */
    keyarray = json_new_array();
    XASSERT(keyarray);
    
    /* If useFitsKeyNames is set, then do not use the key container from rec (which is the template
     * record). In the template record, per-segment keywords have been "expanded". Use the 
     * jsd template records (created with drms_create_jsdtemplate_record()). It must be destroyed 
     * with drms_destroy_jsdtemplate_record(). */
     
    if (useFitsKeyNames)
    {
        DRMS_Record_t *jsdTemplate = NULL;
        DRMS_Keyword_t *jsdKeys = NULL;
        char *typeStr = NULL;
        char *scopeStr = NULL;
        char *defStr = NULL;
        char *unitsStr = NULL;
        char *noteStr = NULL;

        jsdTemplate = drms_create_jsdtemplate_record(drms_env, rec->seriesinfo->seriesname, &status);
        if (!jsdTemplate || status != DRMS_SUCCESS)
        {
            error = 1;
        }
        else
        {
            while ((key = drms_record_nextkey(jsdTemplate, &last, 0)))
            {
                error = 0;
                
                if (!key->info || *(key->info->name) == '\0')
                {
                    fprintf(stderr, "Corrupt DRMS keyword.\n");
                    error = 1;
                    continue; /* we can't really do anything with this keyword */
                }

                if (drms_keyword_getimplicit(key))
                {
                    /* do not print faux keys - they have no corresponding FITS keyword */
                    continue;
                }
                
                keyinfo = json_new_object();
                XASSERT(keyinfo);
                                    
                if (fitsexport_getmappedextkeyname(key, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                {
                    keyNameOut = fitsName;
                }
                else
                {
                    fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", key->info->name);
                    keyNameOut = MISSING_KEY_NAME;
                    error = 1;
                }

                jsonVal = createJsonStringVal(keyNameOut);
                json_insert_pair_into_object(keyinfo, "name", jsonVal);
                jsonVal = NULL;
                
                if (key->info->islink)
                {
                    /* provide link name and target keyword name */
                    char keylinkMap[DRMS_MAXLINKNAMELEN + DRMS_MAXKEYNAMELEN + 16];
                    char *tmpstr = NULL;                        
                    json_t *linkinfo = NULL;
                    int lnkstat = DRMS_SUCCESS;
                    DRMS_Keyword_t *linkedkw = NULL;
                    
                    /* display the target keyword data type; must follow link now */
                    /* per-segment target keywords complicate things */
                    linkedkw = drms_jsd_template_keyword_followlink(key, &lnkstat);
                    
                    if (lnkstat != DRMS_SUCCESS || !linkedkw || !linkedkw->info || *(linkedkw->info->name) == '\0')
                    {
                        fprintf(stderr, "Cannot obtain target keyword for %s.\n", key->info->name);
                        error = 1;
                    }
                    
                    if (!error)
                    {
                        if (fitsexport_getmappedextkeyname(linkedkw, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                        {
                            keyNameOut = fitsName;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", key->info->target_key);
                            keyNameOut = MISSING_KEY_NAME;
                            error = 1;
                        }
                    }
                    else
                    {
                        keyNameOut = MISSING_KEY_NAME;
                    }
                    
                    snprintf(keylinkMap, sizeof(keylinkMap), "%s->%s", key->info->linkname, keyNameOut);
                    jsonVal = createJsonStringVal(keylinkMap);
                    json_insert_pair_into_object(keyinfo, "linkinfo", linkinfo);
                    jsonVal = NULL;
                                            
                    /* accumulate strings for keytype, default, units, and description from link target */
                    if (lnkstat == DRMS_SUCCESS && linkedkw)
                    {
                        typeStr = strdup(drms_type2str(linkedkw->info->type));
                        scopeStr = strdup(drms_keyword_getrecscopestr(linkedkw, NULL));
                        
                        JSOC_INFO_ASSERT(typeStr, "out of memory");
                        JSOC_INFO_ASSERT(scopeStr, "out of memory");

                        fitsexport_getmappedextkeyvalue(linkedkw, &fitsValue);

                        if (fitsValue)
                        {                    
                            snprintf(rawval, sizeof(rawval), "%s", fitsValue);
                            free(fitsValue);
                            fitsValue = NULL;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", linkedkw->info->name);
                            snprintf(rawval, sizeof(rawval), "%s", MISSING_KEY_VALUE);
                            error = 1;
                        }

                        defStr = strdup(rawval);
                        JSOC_INFO_ASSERT(defStr, "out of memory");
                        unitsStr = strdup(linkedkw->info->unit);
                        JSOC_INFO_ASSERT(unitsStr, "out of memory");
                        /* if present keyword has description, use it.  else use target keyword description. */
                        if (*(key->info->description) == '\0' || *(key->info->description) == ' ')
                        {
                            noteStr = strdup(linkedkw->info->description);
                        }
                        else
                        {
                            noteStr = strdup(key->info->description);
                        }
                        
                        JSOC_INFO_ASSERT(noteStr, "out of memory");
                    }
                    else
                    {
                        typeStr = strdup("link");
                        JSOC_INFO_ASSERT(typeStr, "out of memory");

                        // but scopework and notework are now not initialized, but they are used below --> memory corruption;
                        // write something that will help the user debug
                        scopeStr = strdup("error following link");
                        JSOC_INFO_ASSERT(scopeStr, "out of memory");
                        defStr = string_to_json("error following link");
                        JSOC_INFO_ASSERT(defStr, "out of memory");
                        unitsStr = string_to_json("error following link");
                        JSOC_INFO_ASSERT(unitsStr, "out of memory");
                        noteStr = string_to_json("error following link");
                        JSOC_INFO_ASSERT(noteStr, "out of memory");
                    }
                }
                else
                {
                    /* accumulate strings for keytype, default val, units, and description from template record */
                    typeStr = strdup(drms_type2str(key->info->type));
                    JSOC_INFO_ASSERT(typeStr, "out of memory");
                    scopeStr = strdup(drms_keyword_getrecscopestr(key, NULL));
                    JSOC_INFO_ASSERT(scopeStr, "out of memory");
                    
                    fitsexport_getmappedextkeyvalue(key, &fitsValue);

                    if (fitsValue)
                    {                    
                        snprintf(rawval, sizeof(rawval), "%s", fitsValue);
                        free(fitsValue);
                        fitsValue = NULL;
                    }
                    else
                    {
                        fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", key->info->name);
                        snprintf(rawval, sizeof(rawval), "%s", MISSING_KEY_VALUE);
                        error = 1;
                    }

                    defStr = strdup(rawval);
                    JSOC_INFO_ASSERT(defStr, "out of memory");
                    unitsStr = strdup(key->info->unit);
                    JSOC_INFO_ASSERT(unitsStr, "out of memory");
                    noteStr = strdup(key->info->description);
                    JSOC_INFO_ASSERT(noteStr, "out of memory");
                }
        
                jsonVal = createJsonStringVal(typeStr);
                json_insert_pair_into_object(keyinfo, "type", jsonVal);
                jsonVal = NULL;
                // scope                                                                                                                      
                // redundant - persegment = key->info->kwflags & kKeywordFlag_PerSegment;
                jsonVal = createJsonStringVal(scopeStr);
                json_insert_pair_into_object(keyinfo, "recscope", jsonVal);
                jsonVal = NULL;

                jsonVal = createJsonStringVal(defStr);
                json_insert_pair_into_object(keyinfo, "defval", jsonVal);
                jsonVal = NULL;

                jsonVal = createJsonStringVal(unitsStr);
                json_insert_pair_into_object(keyinfo, "units", jsonVal);
                jsonVal = NULL;

                jsonVal = createJsonStringVal(noteStr);
                XASSERT(jsonVal);
                json_insert_pair_into_object(keyinfo, "note", jsonVal);
                jsonVal = NULL;

                json_insert_child(keyarray, keyinfo);
            }
         
            if (last)
            {
                hiter_destroy(&last);
            }
            
            drms_destroy_jsdtemplate_record(&jsdTemplate);
        }
    }
    else
    {
        /* 
         * not printing FITS keyword names/values
         */
        /* We don't want to follow the link just yet - we need a combination of source and target 
        * keyword information below. For now, just get the source keyword info. */
        while ((key = drms_record_nextkey(rec, &last, 0)))
        {
            int persegment = key->info->kwflags & kKeywordFlag_PerSegment;

if (DEBUG) fprintf(stderr,"   starting keyword %s\n",key->info->name);
            /* If a keyword is a linked keyword, then it cannot be a per-segment keyword also. */
            if (persegment)
            {
                char *underscore;

                strcpy(baseKeyName, key->info->name);
                underscore = rindex(baseKeyName, '_');
                if (underscore) *underscore = '\0';
                if (strcmp(prevKeyName, baseKeyName) == 0)
                continue;  // only report the first instance of persegment keywords.
                strcpy(prevKeyName, baseKeyName);
            }
        
            keyinfo = json_new_object();
            json_insert_pair_into_object(keyinfo, "name", json_new_string(persegment ? baseKeyName : key->info->name));
        
            if (key->info->islink)
            {
                /* provide link name and target keyword name */
                char linknames[100], *tmpstr;
                json_t *linkinfo;
                sprintf(linknames,"%s->%s", key->info->linkname, key->info->target_key);
                tmpstr = string_to_json(linknames);
                linkinfo = json_new_string(tmpstr);
                json_insert_pair_into_object(keyinfo, "linkinfo", linkinfo);
                free(tmpstr);

                /* Display the target keyword data type. Must follow link now. */
                int lnkstat = DRMS_SUCCESS;
                DRMS_Keyword_t *linkedkw = drms_template_keyword_followlink(key, &lnkstat);

               /* accumulate strings for keytype, default, units, and description from link target */
                if (lnkstat == DRMS_SUCCESS && linkedkw)
                {
                    keytype = json_new_string(drms_type_names[linkedkw->info->type]);
                    scopework = string_to_json((char *)drms_keyword_getrecscopestr(linkedkw, NULL));
                    drms_keyword_snprintfval(linkedkw, rawval, sizeof(rawval));
                    defvalwork = string_to_json(rawval);
                    unitswork = string_to_json(linkedkw->info->unit);
                    /* if present keyword has description, use it.  else use target keyword description. */
                    if (*(key->info->description) == '\0' || *(key->info->description) == ' ')
                        notework = string_to_json(linkedkw->info->description);
                    else
                        notework = string_to_json(key->info->description);
                }
                else
                {
                    keytype = json_new_string("link");

                    // but scopework and notework are now not initialized, but they are used below --> memory corruption;
                    // write something that will help the user debug
                    scopework = string_to_json("error following link");
                    defvalwork = string_to_json("error following link");
                    unitswork = string_to_json("error following link");
                    notework = string_to_json("error following link");
                }
            }
            else
            {
                /* accumulate strings for keytype, default val, units, and description from template record */
                keytype = json_new_string(drms_type_names[key->info->type]);
                if (persegment)
                    scopework = string_to_json("segment");
                else
                    scopework = string_to_json((char *)drms_keyword_getrecscopestr(key, NULL));
                drms_keyword_snprintfval(key, rawval, sizeof(rawval));
                defvalwork = string_to_json(rawval);
                unitswork = string_to_json(key->info->unit);
                notework = string_to_json(key->info->description);
            }
        
            json_insert_pair_into_object(keyinfo, "type", keytype);
            // scope                                                                                                                      
            // redundant - persegment = key->info->kwflags & kKeywordFlag_PerSegment;
            XASSERT(scopework);
            json_insert_pair_into_object(keyinfo, "recscope", json_new_string(scopework));
            free(scopework);
            scopework = NULL;

            XASSERT(defvalwork);
            json_insert_pair_into_object(keyinfo, "defval", json_new_string(defvalwork));
            free(defvalwork);
            defvalwork = NULL;

            XASSERT(unitswork);
            json_insert_pair_into_object(keyinfo, "units", json_new_string(unitswork));
            free(unitswork);
            unitswork = NULL;

            XASSERT(notework);
            json_insert_pair_into_object(keyinfo, "note", json_new_string(notework));
            free(notework);
            notework = NULL;

            json_insert_child(keyarray, keyinfo);
        }
        
        if (last)
        {
            hiter_destroy(&last);
        }
    }
    

  json_insert_pair_into_object(jroot, "keywords", keyarray);
if (DEBUG) fprintf(stderr," done with keywords, start segments\n");
  
  /* show the segments */
  segarray = json_new_array();
  if (rec->segments.num_total)
    {
      DRMS_Segment_t *oseg = NULL; /* Original seg. */
        
      while ((seg = drms_record_nextseg(rec, &last, 0)))
      {
          oseg = seg;
          
          if (followLinks && seg->info->islink)
          {
              /* Since rec is a template record, cannot follow links in the ordinary manner. Use this function - it finds the template
               * segment of the series linked to. */
              int lnkstat = DRMS_SUCCESS;
              
              seg = drms_template_segment_followlink(seg, &lnkstat);
              if (lnkstat)
              {
                  continue;
              }
          }
          
          /* segment name, units, protocol, dims, description */
      json_t *seginfo = json_new_object();
      int naxis = seg->info->naxis;
          
      /* ART - Use original segment name (I'm not certain if this is the right name to use). */
      json_insert_pair_into_object(seginfo, "name", json_new_string(oseg->info->name));
      if (!followLinks && seg->info->islink)
	    {
            char linkinfo[DRMS_MAXNAMELEN+10];
	    sprintf(linkinfo, "link via %s", seg->info->linkname);
            json_insert_pair_into_object(seginfo, "type", json_new_string(drms_type_names[seg->info->type]));
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
            json_insert_pair_into_object(seginfo, "type", json_new_string(drms_type_names[seg->info->type]));
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

  return error;
}

static int get_series_stats(DRMS_Record_t *rec, json_t *jroot)
  {
  DRMS_RecordSet_t *rs;
  int nprime;
  int status;
  char query[DRMS_MAXQUERYLEN];
  json_t *interval = json_new_object();

  nprime = rec->seriesinfo->pidx_num;
  if (nprime > 0)
    sprintf(query,"%s[^]", rec->seriesinfo->seriesname);
  else
    sprintf(query,"%s[:#^]", rec->seriesinfo->seriesname);
  rs = drms_open_nrecords(rec->env, query, 1, &status);

  if (status == DRMS_ERROR_QUERYFAILED)
  {
     if (rs) 
     {
        drms_free_records(rs);
     }

     return status;
  }

      /* Print shadow-table status. */
      int shadowStat;
      char shadowStr[16];
      int hasShadow;
      
      if (wantowner)
      {
          /* Use the -o flag to also control the display of the HasShadow property. IDL users have not updated
           * the interface to jsoc_info to expect additional properties (and it must be that
           * they are not using a JSON parser - if they were, then additional properties would
           * not be a problem). -o means "print ownership information", but we are also going to
           * use it to control the printing of shadow-table disposition. */
          hasShadow= drms_series_shadowexists(rec->env, rec->seriesinfo->seriesname, &shadowStat);
          
          if (shadowStat)
          {
              snprintf(shadowStr, sizeof(shadowStr), "?");
          }
          else
          {
              snprintf(shadowStr, sizeof(shadowStr), "%s", hasShadow ? "yes" : "no");
          }
      }
      
  if (!rs || rs->n < 1)
    {
    json_insert_pair_into_object(interval, "FirstRecord", json_new_string("NA"));
    json_insert_pair_into_object(interval, "FirstRecnum", json_new_string("NA"));
    json_insert_pair_into_object(interval, "LastRecord", json_new_string("NA"));
    json_insert_pair_into_object(interval, "LastRecnum", json_new_string("NA"));
    json_insert_pair_into_object(interval, "MaxRecnum", json_new_number("0"));
    if (wantowner)
    {
        json_insert_pair_into_object(interval, "HasShadow", json_new_string(shadowStr));
    }
        
    if (rs) drms_free_records(rs);
    json_insert_pair_into_object(jroot, "Interval", interval);
    return DRMS_SUCCESS;
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
      sprintf(query,"%s[$]", rec->seriesinfo->seriesname);
    else
      sprintf(query,"%s[:#$]", rec->seriesinfo->seriesname);
    rs = drms_open_nrecords(rec->env, query, -1, &status);

    if (status == DRMS_ERROR_QUERYFAILED)
    {
       if (rs)
       {
          drms_free_records(rs);
       }

       return status;
    }

    drms_sprint_rec_query(recquery,rs->records[0]);
    jsonquery = string_to_json(recquery);
    json_insert_pair_into_object(interval, "LastRecord", json_new_string(jsonquery));
    free(jsonquery);
    sprintf(val,"%lld", rs->records[0]->recnum);
    json_insert_pair_into_object(interval, "LastRecnum", json_new_number(val));
    drms_free_records(rs);
 
    sprintf(query,"%s[:#$]", rec->seriesinfo->seriesname);
    rs = drms_open_records(rec->env, query, &status);

    if (status == DRMS_ERROR_QUERYFAILED)
    {
       if (rs)
       {
          drms_free_records(rs);
       }

       return status;
    }

    sprintf(val,"%lld", rs->records[0]->recnum);
    json_insert_pair_into_object(interval, "MaxRecnum", json_new_number(val));
    if (wantowner)
    {
        json_insert_pair_into_object(interval, "HasShadow", json_new_string(shadowStr));
    }
    drms_free_records(rs);
    }
  json_insert_pair_into_object(jroot, "Interval", interval);
  return 0;
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


#define LOGFILEBASE    "fetch_log"
#define LOCKFILEBASE   "lock.txt"

// report_summary - record  this call of the program.
void report_summary(const char *host, double StartTime, const char *remote_IP, const char *op, const char *ds, int n, int status)
{
  FILE *log;
  int sleeps;
  double EndTime;
  struct timeval thistv;
  struct stat stbuf;
  char *logfile = calloc(1, PATH_MAX);
  char *lockfile = calloc(1, PATH_MAX);

  base_strlcat(logfile, EXPORT_LOG_DIR, PATH_MAX);

  if (logfile[strlen(logfile) - 1] != '/')
  {
     base_strlcat(logfile, "/", PATH_MAX);
  }

  base_strlcat(logfile, LOGFILEBASE, PATH_MAX);

  base_strlcat(lockfile, EXPORT_LOCK_DIR, PATH_MAX);

  if (lockfile[strlen(lockfile) - 1] != '/')
  {
     base_strlcat(lockfile, "/", PATH_MAX);
  }

   base_strlcat(lockfile, LOCKFILEBASE, PATH_MAX);

  int mustchmodlck = (stat(lockfile, &stbuf) != 0);
  int mustchmodlog = (stat(logfile, &stbuf) != 0);
  int lockfd = open(lockfile, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG);
      
  if (lockfd >= 0)
    {
    gettimeofday(&thistv, NULL);
    EndTime = thistv.tv_sec + thistv.tv_usec/1000000.0;
      
    for(sleeps=0; lockf(lockfd,F_TLOCK,0); sleeps++)
      {
      if (sleeps >= 5)
        {
        fprintf(stderr,"Lock stuck on %s, no report made.\n", logfile);
        lockf(lockfd,F_ULOCK,0);
        return;
        }
        sleep(1);
      }
      
      log = fopen(logfile,"a");
      
      if (log)
        {
        fprintf(log, "host='%s'\t",host);
        fprintf(log, "lag=%0.3f\t",EndTime - StartTime);
        fprintf(log, "IP='%s'\t",remote_IP);
        fprintf(log, "op='%s'\t",op);
        fprintf(log, "ds='%s'\t",ds);
        fprintf(log, "n=%d\t",n);
        fprintf(log, "status=%d\n",status);
        fflush(log);
        if (mustchmodlog)
          {
          fchmod(fileno(log), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
          }
        fclose(log);
        }
      
        if (mustchmodlck)
          {
          fchmod(lockfd, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
          }
        lockf(lockfd,F_ULOCK,0);
        close(lockfd);
    }
  else
    {
       fprintf(stderr, "Unable to open lock file for writing: %s.\n", lockfile);
    }
}

/* callback that fires when the signal thread catches the SIGINT signal. */
int OnSIGINT(void *data)
{
   printf("Content-Type: application/json\n\n{\"status\":-1}\n");
   return 0;
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
            if (!cmdparams_set(&cmdparams, key, value))
            {
                /* ART - the original intent was to return from the DoIt()
                 * function here - but it is not possible to do that from a function
                 * called by DoIt(). But I've retained the original semantics of 
                 * returning back to DoIt() from here. */
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

static long long GetSegFileSize(DRMS_Record_t *rec, const char *segment)
{
    char size[64];
    struct stat stat_buf;
    long long num_bytes = -1;
    DRMS_Segment_t *seg = NULL;
    char seg_file_name[PATH_MAX] = {0};


    /* follows links */
    seg = drms_segment_lookup(rec, segment);
    if (seg && seg->record)
    {
        /* make seg file path - use the parent record of seg (links may have been followed); rec->suinfo should not be NULL since 
         * we called drms_record_getinfo() on the entire record-set, and this call follows links; *rec->suinfo->online_loc == '\0' 
         * implies an invalid SUNUM
         */
        if (rec->suinfo && *rec->suinfo->online_loc != '\0' && *rec->suinfo->online_status == 'Y')
        {
            /* OK to use drms_segment_filename() - we already 'staged' records, so we have created rec->su */
            drms_segment_filename(seg, seg_file_name);
            
            if (*seg_file_name != '\0')
            {                
                if (stat(seg_file_name, &stat_buf) == 0)
                {
                    if (num_bytes == -1)
                    {
                        num_bytes++;
                    }

                    /* online */
                    num_bytes += stat_buf.st_size;
                }
            }
        }
    }
    
    return num_bytes;
}

static void free_log_dir(void *data)
{
    char **p_log_dir = NULL;


    p_log_dir = (char **)data;

    if (p_log_dir && *p_log_dir)
    {
        free(*p_log_dir);
    }
}


/* Module main function. */
int DoIt(void)
  {
  const char *op;
  const char *in;
  char *keylist;
  char *seglist;
  char *linklist;
  char *web_query;
  const char *Remote_Address;
  const char *Server;
  int followLinks = 0;
  int from_web, keys_listed, segs_listed, links_listed;
  int max_recs = 0;
  struct timeval thistv;
  double StartTime;
  CleanerData_t cleaner;
  int useFitsKeyNames = 0;
  int skipRunTime = 0;
  int printHTTPHeaders = 1;
  LinkedList_t *reqSegs = NULL;
  LinkedList_t *reqKeys = NULL;
  LinkedList_t *reqLinks = NULL;
  json_t *recArray = NULL;
    char *jsonOut = NULL;
    char *final_json = NULL;
    
    int missingKeyNumber = 0;
    int missingSegNumber = 0;
    int missingLinkNumber = 0;
    char missingKeyNameBuf[DRMS_MAXKEYNAMELEN] = { 0 };
    char missingSegNameBuf[DRMS_MAXSEGNAMELEN] = { 0 };
    char missingLinkNameBuf[DRMS_MAXLINKNAMELEN] = { 0 };

  if (nice_intro ()) return (0);

  /* Register function that will be called when this DRMS module catches the SIGINT signal. */
  /* This will print an error message to stderr if it fails. */
  cleaner.cb = (pFn_Cleaner_t)&OnSIGINT;
  cleaner.data = NULL;
  drms_server_registercleaner(drms_env, &cleaner);

  gettimeofday(&thistv, NULL);
  StartTime = thistv.tv_sec + thistv.tv_usec/1000000.0;

  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, "Not Specified") != 0;

    int postorget = 0;
    char *webarglist = NULL;
    size_t webarglistsz;

    if (getenv("REQUEST_METHOD"))
    {
        postorget = (strcasecmp(getenv("REQUEST_METHOD"), "POST") == 0 || strcasecmp(getenv("REQUEST_METHOD"), "GET") == 0);
    }

    if (from_web)
    {
        Q_ENTRY *req = NULL;
        /* If we are here then one of three things is true (implied by the existence of QUERY_STRING):
         *   1. We are processing an HTTP GET. The webserver will put the arguments in the
         *      QUERY_STRING environment variable.
         *   2. We are processing an HTTP POST. The webserver will NOT put the arguments in the
         *      QUERY_STRING environment variable. Instead the arguments will be passed to jsoc_info
         *      via stdin. QUERY_STRING should not be set, but it looks like it might be. In any
         *      case qDecoder will ignore it.
         *   3. jsoc_info was invoked via the cmd-line, and the caller provided the QUERY_STRING
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
            if (SetWebArg(req, "op", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "ds", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "key", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "seg", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "link", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "n", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "userhandle", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "l", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "REMOTE_ADDR", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "SERVER_NAME", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "o", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "R", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "f", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");
            if (SetWebArg(req, "r", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");            
            if (SetWebArg(req, "s", &webarglist, &webarglistsz)) JSONDIE("Bad QUERY_STRING");

            
            /* force json output */
            cmdparams_set (&cmdparams,"z", "1");

            qEntryFree(req); 
        }
        
        free(webarglist);
        webarglist = NULL;
    }
    
    if (web_query)
    {
        free(web_query);
        web_query = NULL;
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
  userhandle = cmdparams_get_str (&cmdparams, "userhandle", NULL);
      followLinks = cmdparams_isflagset(&cmdparams, "l");
  Remote_Address = cmdparams_get_str(&cmdparams, "REMOTE_ADDR", NULL);
  Server = cmdparams_get_str(&cmdparams, "SERVER_NAME", NULL);

	/* -o hack to allow owner added to series_struct 28 Dec 11 */
	wantowner = cmdparams_get_int (&cmdparams, "o", NULL);
	
    useFitsKeyNames = cmdparams_isflagset(&cmdparams, "f");
    skipRunTime = cmdparams_isflagset(&cmdparams, "r");
    printHTTPHeaders = !cmdparams_isflagset(&cmdparams, "s");

  // allow possible user kill
  if (strcmp(userhandle, "Not Specified") != 0)
    manage_userhandle(1, userhandle);
  else
    userhandle = "NoHandle";

  /*  op == series_struct  */
  if (strcmp(op,"series_struct") == 0) 
    {
    char *p, *seriesname;
    json_t *jroot;
    DRMS_Record_t *rec;
    int status=0;
    /* Only want keyword info so get only the template record for drms series or first record for other data */
    seriesname = strdup (in);
    if ((p = index(seriesname,'['))) *p = '\0';
    if ((p = index(seriesname,'{'))) *p = '\0';
    rec = drms_template_record (drms_env, seriesname, &status);
        
        if (status == DRMS_ERROR_QUERYFAILED)
        {
            const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);
            
            if (emsg)
            {
                JSONDIE((char *)emsg);
            }
            else
            {
                JSONDIE("problem with database query");
            }
        }
        else if (status)
            JSONDIE("series not found");
        
    jroot = json_new_object();
    list_series_info(drms_env, rec, jroot, followLinks, useFitsKeyNames);
    if (get_series_stats(rec, jroot) == DRMS_ERROR_QUERYFAILED)
    {
       const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);

       if (emsg)
       {
          JSONDIE((char *)emsg);
       }
       else
       {
          JSONDIE("problem with database query");
       }
    }

    if (!skipRunTime)
    {
        json_insert_runtime(jroot, StartTime);
    }
    json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    json_tree_to_string(jroot, &jsonOut);
    final_json = json_format_string(jsonOut);
    free(jsonOut);
    
    if (printHTTPHeaders)
    {
    /* send the output json back to client */
        printf("Content-type: application/json\n\n");
    }
    
    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);
    free(seriesname);
    manage_userhandle(0, userhandle);
    // report_summary(Server, StartTime, Remote_Address, op, in, max_recs, 0);
    return(0);
    }

  /*  op == rs_summary  */
  if (strcmp(op,"rs_summary") == 0) 
    {
    json_t *jroot = json_new_object();
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
          
          if (status == DRMS_ERROR_QUERYFAILED)
          {
              const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);
              
              if (emsg)
              {
                  JSONDIE((char *)emsg);
              }
              else
              {
                  JSONDIE("problem with database query");
              }
          }
          
          if (recordset)
          {
              count = recordset->n;
          }
          else
          {
              drms_close_records(recordset, DRMS_FREE_RECORD);
              JSONDIE("unable to open records");
          }
          
      drms_close_records(recordset, DRMS_FREE_RECORD);
      }
    else
      count = drms_count_records(drms_env, (char *)in, &status);
    if (bracket)
	*bracket = '{';
    if (status == DRMS_ERROR_QUERYFAILED)
    {
        const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);
        
        if (emsg)
        {
            JSONDIE((char *)emsg);
        }
        else
        {
            JSONDIE("problem with database query");
        }
    }
    else if (status)
        JSONDIE("series not found");
    
    /* send the output json back to client */
    sprintf(val, "%d", count);
    json_insert_pair_into_object(jroot, "count", json_new_number(val));
    if (!skipRunTime)
    {
        json_insert_runtime(jroot, StartTime);
    }
    json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    json_tree_to_string(jroot, &jsonOut);
    final_json = json_format_string(jsonOut);
    free(jsonOut);

    /* send the output json back to client */
    if (printHTTPHeaders)
    {
        printf("Content-type: application/json\n\n");
    }

    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);
    report_summary(Server, StartTime, Remote_Address, op, in, max_recs, 0);
    manage_userhandle(0, userhandle);
    return(0);
    }

  /*  op == rs_list  */
  if (strcmp(op,"rs_list") == 0) 
    {
    int wantRecInfo = cmdparams_get_int(&cmdparams, "R", NULL);
    DRMS_RecordSet_t *recordset = NULL;
    DRMS_Record_t *rec, *template;
    DRMS_RecChunking_t cstat = kRecChunking_None;
    char seriesname[DRMS_MAXQUERYLEN];
    char *keys[1000];
    char *segs[1000];
    char *links[1000];
    int ikey, nkeys = 0;
    int iseg, nsegs = 0;
    int allSegs = 0; /* did the user provide **ALL** for the seglist? */
    int ilink, nlinks = 0;
    char count[100];
    json_t *jroot, **keyvals = NULL, **segvals = NULL, **segdims = NULL, **linkvals = NULL, *recinfo = NULL;
    json_t **segcparms = NULL;
    json_t **segbzeros = NULL;
    json_t **segbscales = NULL;
    int status = DRMS_SUCCESS;
    int irec, nrecs;
    int record_set_staged = 0;
    char *lbracket;
    int sumInfoStr = 0;
    jroot = json_new_object();
    DRMS_Record_t *jsdTemplate = NULL;
    DRMS_Segment_t *segTemplate = NULL;
    DRMS_Keyword_t *keyTemplate = NULL;
    DRMS_Link_t *linkTemplate = NULL;
    HIterator_t *last = NULL;
    const char *keyNameOut = NULL;
    const char *valOut = NULL;
    char fitsName[16];
    char *fitsValue = NULL;
    json_t *keyObj = NULL;
    int badKey = 0;
    int badSeg = 0;
    int badLink = 0;
    ListNode_t *lnKey = NULL;
    ListNode_t *lnSeg = NULL;
    ListNode_t *lnLink = NULL;
    HContainer_t *log_dirs = NULL;

    /* Get template record */
    strcpy(seriesname, in);
    lbracket = index(seriesname, '[');
    if (lbracket) *lbracket = '\0';
    template = drms_template_record(drms_env, seriesname, &status);
    if (status == DRMS_ERROR_QUERYFAILED)
    {
       const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);

       if (emsg)
       {
          JSONDIE((char *)emsg);
       }
       else
       {
          JSONDIE("problem with database query");
       }
    }
    else if (status)
      JSONDIE(" jsoc_info: series not found.");

    /* Open record_set(s) */
    if (max_recs == 0)
      //      recordset = drms_open_recordset (drms_env, in, &status);
      // temporarily reverting to drms_open_records until I can fix the problem with
      // not passing a segment-list ot drms_open_recordset().
      recordset = drms_open_records (drms_env, in, &status);
    else // max_recs specified via "n=" parameter.                                                                            
      recordset = drms_open_nrecords (drms_env, in, max_recs, &status);

    if (status == DRMS_ERROR_QUERYFAILED)
    {
       const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);

       if (emsg)
       {
          JSONDIE((char *)emsg);
       }
       else
       {
          JSONDIE("problem with database query");
       }
    }

    if (!recordset) 
      JSONDIE(" jsoc_info: series not found.");
    nrecs = recordset->n;
    if (nrecs == 0)
      {
      json_insert_pair_into_object(jroot, "count", json_new_number("0"));
        if (!skipRunTime)
        {
            json_insert_runtime(jroot, StartTime);
        }
      json_insert_pair_into_object(jroot, "status", json_new_number("0"));
      json_tree_to_string(jroot, &jsonOut);
      
        if (printHTTPHeaders)
        {
            printf("Content-type: application/json\n\n");
        }

      printf("%s\n",jsonOut);
      free(jsonOut);
      fflush(stdout);
      manage_userhandle(0, userhandle);
      return(0);
      }
    
    struct requisitionStructT requisition = { 0 };
    
    if (useFitsKeyNames && wantRecInfo)
    {
        /* if we are printing FITS key names, and not DRMS key names, then we will determine whether or not the SU is online if the user 
         * requesting the printing of record information */
        requisition.requireSUMinfo = 1;
    }
          
    /* get list of segments to show for each record - do segments before keys so we know which segment-specific 
     * keys to operate on */
    nsegs = 0;
    reqSegs = list_llcreate(sizeof(DRMS_Segment_t *), NULL);
    if (!reqSegs)
    {
        JSONDIE("Out of memory.");
    }

    if (segs_listed) 
    { 
        /* this means that the user has provided the seg=blah1,blah2,... argument */
        /* get specified segment list */
        char *thisseg;
        CGI_unescape_url(seglist);
        
        if (useFitsKeyNames)
        {            
            nsegs = populateSegList(seglist, followLinks, template, recordset, &record_set_staged, &requisition, reqSegs, &allSegs);
        }
        else
        {
            /* not using FITS keyword names */
            if (strstr(seglist, "**NONE**") != NULL)
            {
                nsegs = 0;
            }
            else if (strstr(seglist, "**ALL**") != NULL)
            {
                /* will never result in an InvalidSegName printed to the output */
                DRMS_Segment_t *seg;
                DRMS_Segment_t *oseg = NULL;
                
                allSegs = 1;

                while ((seg = drms_record_nextseg(template, &last, 0)))
                {
                    oseg = seg; /* Original seg. */

                    if (followLinks && seg->info->islink)
                    {
                        /* Since rec is a template record, cannot follow links in the ordinary manner. Use this function - it finds the template
                        * segment of the series linked to. */
                        int lnkstat = DRMS_SUCCESS;

                        seg = drms_template_segment_followlink(seg, &lnkstat);
                        if (lnkstat)
                        {
                            continue;
                        }
                    }

                    /* ART - Use original segment name (I'm not certain if this is the right name to use). */
                    segs[nsegs++] = strdup (oseg->info->name);
                }

                if (last)
                {
                    hiter_destroy(&last);
                }
            }
            else
            {
                /* can result in an InvalidSegName printed to the output */
                for (thisseg=strtok(seglist, ","); thisseg; thisseg=strtok(NULL,","))
                {                    
                    segs[nsegs++] = strdup(thisseg);
                }
            }
        }        
    }

    free (seglist);

    /* get list of keywords to print for each record */
    /* Depending on the set of keywords to print, we will know whether or not 
     * we need to call SUM_infoEx(). Here's the list of keys that necessitate 
     * a SUM_infoEx() call:
     *  
     *   *size*
     *   *online*
     *   *retain*
     *   *archive* 
     */
    nkeys = 0;
    reqKeys = list_llcreate(sizeof(DRMS_Keyword_t *), NULL);
    if (!reqKeys)
    {
        JSONDIE("Out of memory.");
    }

    if (keys_listed) 
    {
        /* this means that the user has provided the key=blah1,blah2,... argument */
        /* get specified list */
        char *thiskey = NULL;
        CGI_unescape_url(keylist);
        ListNode_t *node = NULL;
      
        if (useFitsKeyNames)
        {
            nkeys = populateKeyList(keylist, reqSegs, template, &jsdTemplate, recordset, &record_set_staged, &requisition, reqKeys);
        }
        else
        {
            /* not using FITS keyword names */
            for (thiskey = strtok(keylist, ","); thiskey; thiskey = strtok(NULL,","))
            {        
                if (strcmp(thiskey,"**NONE**") == 0)
                {
                    nkeys = 0;
                    break;
                }

                if (strcmp(thiskey, "**ALL**") == 0)
                {
                    DRMS_Keyword_t *key;

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
                {
                    keys[nkeys++] = strdup(thiskey);
                }
                
                if (strcasecmp(thiskey, "*size*") == 0)
                {
                    requisition.requireSUMinfoSize = 1;
                }
            
                if (strcasecmp(thiskey, "*online*") == 0)
                {
                    requisition.requireSUMinfoOnline = 1;
                }
            
                if (strcasecmp(thiskey, "*retain*") == 0)
                {
                    requisition.requireSUMinfoRetain = 1;
                }
            
                if (strcasecmp(thiskey, "*archive*") == 0)
                {
                    requisition.requireSUMinfoArchive = 1;
                }
            
                if (strcasecmp(thiskey, "*recdir*") == 0)
                {
                    requisition.requireRecdir = 1;                
                }
            
                if (strcasecmp(thiskey, "*logdir*") == 0)
                {
                    requisition.requireLogdir = 1;
                }

                if (strcasecmp(thiskey, "*dirmtime*") == 0)
                {
                    requisition.requireDirmtime = 1;                
                }

                if (requisition.requireSUMinfoSize || requisition.requireSUMinfoOnline || requisition.requireSUMinfoRetain || requisition.requireSUMinfoArchive )
                {
                    requisition.requireSUMinfo = 1;
                }
            }
        }
    }
    free (keylist);
    
    /* get list of links to print for each record */
    nlinks = 0;
    reqLinks = list_llcreate(sizeof(DRMS_Link_t *), NULL);
    if (!reqLinks)
    {
        JSONDIE("Out of memory.");
    }
    
    if (links_listed) 
    { 
        /* get specified list */
        char *thislink;
        CGI_unescape_url(linklist);
        
        if (useFitsKeyNames)
        {
            nlinks = populateLinkList(linklist, template, reqLinks);
        }
        else
        {
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

                    while ((link = drms_record_nextlink(template, &last)))
                        links[nlinks++] = strdup (link->info->name);

                    if (last)
                    {
                        hiter_destroy(&last);
                    }
                }
                else
                {
                    links[nlinks++] = strdup(thislink);
                }
            }
        }        
      }
    free (linklist);  

    if (!useFitsKeyNames)
    {
        /* place to put an array of keyvals per keyword */
        if (nkeys)
          keyvals = (json_t **)malloc(nkeys * sizeof(json_t *));
        for (ikey=0; ikey<nkeys; ikey++)
          {
          json_t *val = json_new_array();
          keyvals[ikey] = val;
          }

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

        /* place to put an array of linkvals per keyword */
        if (nlinks)
          linkvals = (json_t **)malloc(nlinks * sizeof(json_t *));
        for (ilink=0; ilink<nlinks; ilink++)
        {
            json_t *val = json_new_array();
            linkvals[ilink] = val;
        }
    }

    /* check all flags that require records to be 'staged' (this is not a true staging - this only creates the rec->su structs
     * since we have the `retrieve` flag set to false) */
    if ((requisition.requireSUMinfoSize || requisition.requireRecdir || requisition.requireDirmtime || nsegs > 0) && !record_set_staged)
    {
        /* stage records */
        drms_stage_records(recordset, 0, 0);
        record_set_staged = 1;
    }
    
    /* need to find out if we will be needing SUM info if so, call drms_record_getinfo() */
    if (requisition.requireSUMinfo)
    {
        /* gets info for all linked-records' SUs too */
        drms_record_getinfo(recordset);
    }
    
    if (requisition.requireLogdir)
    {
        int iset;
        int nrecs;
        
        /* call SUM_get() with batches of SUs */
        for (iset = 0; iset < recordset->ss_n && status == DRMS_SUCCESS; iset++)
        {
            /* iterate over record subsets */

            nrecs = drms_recordset_getssnrecs(recordset, iset, &status);
            
            if (status == DRMS_SUCCESS)
            {
                /* will call drms_getsuinfo() on log-dir SUs */
                if (!log_dirs)
                {
                    log_dirs = hcon_create(sizeof(char *), 64, (void (*)(const void *))free_log_dir, NULL, NULL, NULL, 0);
                }
                
                JSOC_INFO_ASSERT(log_dirs != NULL, "out of memory creating log_dir container");
                GetLogDirs(drms_env, &recordset->records[(recordset->ss_starts)[iset]], nrecs, log_dirs);
            }
        }
    }

    recArray = json_new_array(); 
    JSOC_INFO_ASSERT(recArray, "out of memory");

    int missingKeyName = 0;

    /* loop over set of selected records */
    for (irec = 0; irec < nrecs; irec++) 
      {
      char recquery[DRMS_MAXQUERYLEN];
      char numStr[64];
      char *jsonquery;
      json_t *recobj = json_new_object();
      JSOC_INFO_ASSERT(recobj, "out of memory");

      if (max_recs == 0)
        rec = drms_recordset_fetchnext(drms_env, recordset, &status, &cstat, NULL);
      else
        {
        rec = recordset->records[irec];  /* pointer to current record */
        status = DRMS_SUCCESS;
        }

        json_t *jsonVal = NULL;

      /* now get keyword information */
        if (useFitsKeyNames)
        {
            json_t *keywordArray = NULL;
            
            /* always include specification */
            drms_sprint_rec_query(recquery, rec);
            jsonVal = createJsonStringVal(recquery);
            json_insert_pair_into_object(recobj, "specification", jsonVal);
            jsonVal = NULL;
            
            if (requisition.requireRecnum)
            {
                snprintf(numStr, sizeof(numStr), "%lld", rec->recnum);
                jsonVal = json_new_number(numStr);
                JSOC_INFO_ASSERT(jsonVal, "out of memory");
                json_insert_pair_into_object(recobj, "recnum", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireSunum)
            {
                snprintf(numStr, sizeof(numStr), "%lld", rec->sunum);
                jsonVal = json_new_number(numStr);
                JSOC_INFO_ASSERT(jsonVal, "out of memory");
                json_insert_pair_into_object(recobj, "sunum", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireSUMinfoSize)
            {
                char size[64];
                struct stat statBuf;
                char *segFilePath = NULL;
                size_t szPath = DRMS_MAXPATHLEN;
                SUM_info_t *sinfo = NULL;
                long long numBytes = -1;
                DRMS_Segment_t *segTemplate = NULL;
                DRMS_Segment_t *seg = NULL;
                char seg_file_name[PATH_MAX] = {0};

                if (segs_listed)
                {
                    if (reqSegs && list_llgetnitems(reqSegs) > 0)
                    {
                        /* first, let's see if the segment-file is online; if so, use the size as returned by stat() */
                        segFilePath = calloc(1, szPath);

                        if (segFilePath)
                        {
                            list_llreset(reqSegs);

                            while ((lnSeg = list_llnext(reqSegs)) != NULL)
                            {
                                segTemplate = *((DRMS_Segment_t **)(lnSeg->data));
                                seg = drms_segment_lookup(rec, segTemplate->info->name);
                            
                                if (seg && seg->record)
                                {                                
                                    /* make seg file path - use the parent record of seg (links may have been followed) */
                                    if (seg->record->suinfo && *seg->record->suinfo->online_loc != '\0' && *seg->record->suinfo->online_status == 'Y')
                                    {
                                        /* OK to use drms_segment_filename() - we already 'staged' records, so we have created rec->su */
                                        drms_segment_filename(seg, seg_file_name);
                
                                        if (*seg_file_name != '\0')
                                        {            
                                            segFilePath = base_strcatalloc(segFilePath, seg->record->suinfo->online_loc, &szPath);
                                            segFilePath = base_strcatalloc(segFilePath, "/", &szPath);
                                            segFilePath = base_strcatalloc(segFilePath, seg_file_name, &szPath);
                
                                            if (stat(segFilePath, &statBuf) == 0)
                                            {
                                                if (numBytes == -1)
                                                {
                                                    numBytes++;
                                                }
    
                                                /* online */
                                                numBytes += statBuf.st_size;
                                            }
                                        }
                                    }
                                }
                            }
                
                            if (numBytes != -1)
                            {
                                long long numBytesLL = numBytes;
                                snprintf(size, sizeof(size), "%lld", numBytesLL);
                                jsonVal = json_new_number(size);
                                JSOC_INFO_ASSERT(jsonVal, "out of memory");
                            }
                    
                            free(segFilePath);
                            segFilePath = NULL;
                        }
                    }
                }
                else
                {
                    /* no segs listed - use all segs in record */
                    long long numSubBytes = 0;

                    last = NULL;
                    while ((seg = drms_record_nextseg(rec, &last, 1)) != NULL) /* follows links */
                    {
                        numSubBytes = GetSegFileSize(rec, seg->info->name);
                        if (numSubBytes > 0)
                        {
                            if (numBytes == -1)
                            {
                                numBytes++;
                            }

                            numBytes += numSubBytes;
                        }
                    }
                
                    if (last)
                    {
                        hiter_destroy(&last);
                    }
                }

                if (numBytes == -1)
                {
                    jsonVal = json_new_string("NA");
                }
                else
                {
                    long long numBytesLL = numBytes;
                    snprintf(size, sizeof(size), "%lld", numBytesLL);
                    jsonVal = json_new_string(size);
                }

                if (numBytes == -1)
                {
                    jsonVal = createJsonStringVal("NA");
                }

                json_insert_pair_into_object(recobj, "size", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireSUMinfoOnline)
            {
                if (!rec->suinfo)
                {
                    jsonVal = createJsonStringVal("NA");
                }
                else
                {
                    jsonVal = createJsonStringVal(rec->suinfo->online_status);
                }

                json_insert_pair_into_object(recobj, "online", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireSUMinfoRetain)
            {
                if (!rec->suinfo)
                {
                    jsonVal = createJsonStringVal("NA");
                }
                else
                {
                    int year;
                    int month;
                    int day;
                    char dateStr[64];
                    
                    if (strcmp("N", rec->suinfo->online_status) == 0)
                    {
                        jsonVal = createJsonStringVal("NA");
                    }
                    else
                    {
                        sscanf(rec->suinfo->effective_date, "%4d%2d%2d", &year, &month, &day);
                        snprintf(dateStr, sizeof(dateStr), "%4d.%02d.%02d", year, month, day);                        
                        jsonVal = createJsonStringVal(dateStr);
                    }
                }

                json_insert_pair_into_object(recobj, "expdate", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireSUMinfoArchive)
            {
                if (!rec->suinfo)
                {
                    jsonVal = createJsonStringVal("NA");
                }
                else
                {
                    if (rec->suinfo->pa_status == DAAP && rec->suinfo->pa_substatus == DAADP)
                    {
                        jsonVal = createJsonStringVal("Pending");
                    }
                    else
                    {
                        jsonVal = createJsonStringVal(rec->suinfo->archive_status);
                    }
                }

                json_insert_pair_into_object(recobj, "archive", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireRecdir)
            {
                char recPath[DRMS_MAXPATHLEN];

                if (rec->suinfo && *rec->suinfo->online_loc != '\0' && *rec->suinfo->online_status == 'Y')
                {
                    snprintf(recPath, sizeof(recPath), "%s/" DRMS_SLOTDIR_FORMAT, rec->suinfo->online_loc, rec->slotnum);
                }
                else
                {
                    snprintf(recPath, sizeof(recPath), "NoDataDirectory");
                }

                jsonVal = createJsonStringVal(recPath);
                json_insert_pair_into_object(recobj, "recdir", jsonVal);
                jsonVal = NULL;
            }
            
            if (requisition.requireLogdir)
            {
                const char *logdir = GetLogDir(rec, log_dirs);
                
                if (logdir)
                {
                    jsonVal = createJsonStringVal(logdir);
                }
                else
                {
                    jsonVal = createJsonStringVal("NO LOG");
                }
                
                json_insert_pair_into_object(recobj, "logdir", jsonVal);
                jsonVal = NULL;
            }

            if (requisition.requireDirmtime)
            {
                /* get record dir last change date */
                struct stat buf;
                char recPath[DRMS_MAXPATHLEN];
                char timebuf[128];

                if (rec->suinfo && *rec->suinfo->online_loc != '\0' && *rec->suinfo->online_status == 'Y')
                {
                    snprintf(recPath, sizeof(recPath), "%s/" DRMS_SLOTDIR_FORMAT, rec->suinfo->online_loc, rec->slotnum);
                    stat(recPath, &buf);
                    sprint_ut(timebuf, buf.st_mtime + UNIX_EPOCH);
                }
                else
                {
                    snprintf(timebuf, sizeof(timebuf), "NA");
                }
                
                jsonVal = createJsonStringVal(timebuf);
                json_insert_pair_into_object(recobj, "dirmtime", jsonVal);
                jsonVal = NULL;
            }
                        
            if (requisition.requireSpec)
            {
                char specBuf[DRMS_MAXSERIESNAMELEN * 2];

                drms_snprint_rec_spec(specBuf, sizeof(specBuf), rec);
                jsonVal = createJsonStringVal(specBuf);                
                JSOC_INFO_ASSERT(jsonVal, "out of memory");
                json_insert_pair_into_object(recobj, "spec", jsonVal);
                jsonVal = NULL;
            }

            keywordArray = json_new_array(); /* each record has a keyword array - an array of { "name" : "key1", "value" : "rumble"}
                                              * objects
                                              */
            JSOC_INFO_ASSERT(keywordArray, "out of memory");
            json_insert_pair_into_object(recobj, "keywords", keywordArray);

            list_llreset(reqKeys);
            while ((lnKey = list_llnext(reqKeys)) != NULL)
            {
                keyTemplate = *((DRMS_Keyword_t **)(lnKey->data));
                keyNameOut = NULL;
                valOut = NULL;
                badKey = 0;
                missingKeyName = 0;
                
                if (isInvalidKey(keyTemplate))
                {
                    badKey = 1;
                    
                    if (keyTemplate && keyTemplate->info && *(keyTemplate->info->name) != '\0')
                    {
                        keyNameOut = keyTemplate->info->name;
                    }
                    else
                    {
                        missingKeyName = 1;
                    }
                }
                else
                {
                    /* do not print segment-specific keys here - they go in the segment objects */
                    if (!drms_keyword_getperseg(keyTemplate))
                    {
                        DRMS_Keyword_t *keyWithVal = NULL;

                        /* get keyword name from template keyword */                            
                        if (fitsexport_getmappedextkeyname(keyTemplate, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                        {
                            keyNameOut = fitsName;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", keyTemplate->info->name);
                            missingKeyName = 1;
                        }

                        /* key is the template keyword, but we need the actual keyword from the current record; 
                         * follow the link if the keyword is a linked keyword */
                        keyWithVal = drms_keyword_lookup(rec, keyTemplate->info->name, 1);
                
                        if (keyWithVal)
                        {
                            /* for the f != 1, case missing values are replaced with the string "MISSING"; do
                             * not do that here, in the f == 1 case; we want to show the exact string that would
                             * appear in the FITS file were these data to be exported */
                            JSOC_INFO_ASSERT(fitsValue == NULL, "about to leak");
                            fitsexport_getmappedextkeyvalue(keyWithVal, &fitsValue);

                            if (fitsValue)
                            {
                                valOut = fitsValue;
                            }
                            else
                            {
                                valOut = MISSING_KEY_VALUE;
                            }
                        }
                        else
                        {
                            /* if the keyword is linked, we don't know if the key is missing from the source record
                             * or the target record */
                            /* should not get here - we tested all key names for validity */
                            valOut = MISSING_KEY_VALUE;
                        }                    
                    }
                    else
                    {
                        /* per-segment keywords are handled in the segments objects */
                    }
                }
                
                if (keyNameOut || missingKeyName)
                {
                    /* skip per-segment keywords (which are !keyNameOut && !missingKeyName) */
                
                    /* make json */
                    keyObj = json_new_object();
                    JSOC_INFO_ASSERT(keyObj, "out of memory");

                    if (missingKeyName)
                    {
                        snprintf(missingKeyNameBuf, sizeof(missingKeyNameBuf), "%s%02d", MISSING_KEY_NAME, missingKeyNumber++);
                        jsonVal = createJsonStringVal(missingKeyNameBuf);
                    }
                    else
                    {
                        jsonVal = createJsonStringVal(keyNameOut);
                    }
                    json_insert_pair_into_object(keyObj, "name", jsonVal);
                    jsonVal = NULL;

                    if (valOut)
                    {
                        jsonVal = createJsonStringVal(valOut);
                        json_insert_pair_into_object(keyObj, "value", jsonVal);
                        jsonVal = NULL;
                    }
                    
                    if (badKey)
                    {
                        jsonVal = json_new_true();
                        JSOC_INFO_ASSERT(jsonVal, "out of memory");
                        json_insert_pair_into_object(keyObj, INVALID, jsonVal);
                        jsonVal = NULL;
                    }

                    json_insert_child(keywordArray, keyObj);
                }

                if (fitsValue)
                {                
                    free(fitsValue);
                    fitsValue = NULL;
                }
            }
        }
        else
        {
            /* using drms key names and values */
            if (wantRecInfo)
            {
                drms_sprint_rec_query(recquery,rec);
                jsonquery = string_to_json(recquery);
                json_insert_pair_into_object(recobj, "name", json_new_string(jsonquery));
                free(jsonquery);
            }
        
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
                else if (strcmp(keys[ikey], "*size*") == 0)
                {
                    char size[64];
                    struct stat statBuf;
                    SUM_info_t *sinfo = NULL;
                    long long numBytes = -1;
                    long long numSubBytes = 0;
                    DRMS_Segment_t *seg = NULL;

                    /* first, let's see if the segment-file is online; if so, use the size as returned by stat() */
                    if (segs_listed)
                    {
                        for (iseg = 0; iseg < nsegs; iseg++)
                        {
                            numSubBytes = GetSegFileSize(rec, segs[iseg]);
                            if (numSubBytes > 0)
                            {
                                numBytes += numSubBytes;
                            }
                        }
                    }
                    else
                    {
                        last = NULL;
                        while ((seg = drms_record_nextseg(rec, &last, 1)) != NULL) /* follows links */
                        {
                            numSubBytes = GetSegFileSize(rec, seg->info->name);
                            if (numSubBytes > 0)
                            {
                                numBytes += numSubBytes;
                            }
                        }
                
                        if (last)
                        {
                            hiter_destroy(&last);
                        }
                    }

                    if (numBytes == -1)
                    {
                        val = json_new_string("NA");
                    }
                    else
                    {
                        long long numBytesLL = numBytes;
                        snprintf(size, sizeof(size), "%lld", numBytesLL);
                        val = json_new_string(size);
                    }
                }
                else if (strcmp(keys[ikey],"*online*") == 0)
                {
                    SUM_info_t *sinfo = rec->suinfo;

                    if (!sinfo)
                    {
                        val = json_new_string("NA");
                    }
                    else
                    {
                        val = json_new_string(sinfo->online_status);
                    }
                }
                else if (strcmp(keys[ikey],"*retain*") == 0)
                {
                    SUM_info_t *sinfo = rec->suinfo;
                
                    if (!sinfo)
                    {
                        val = json_new_string("NA");
                    }
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
                    SUM_info_t *sinfo = rec->suinfo;
                    
                    if (!sinfo)
                    {
                        val = json_new_string("NA");
                    }
                    else    
                    {
                        if (sinfo->pa_status == DAAP && sinfo->pa_substatus == DAADP)
                        {
                            val = json_new_string("Pending");
                        }
                        else
                        {
                            val = json_new_string(sinfo->archive_status);
                        }
                    }
                }
                else if (strcmp(keys[ikey], "*recdir*") == 0)
                {
                    // get record directory
                    char path[DRMS_MAXPATHLEN];
                
                    if (rec->suinfo && *rec->suinfo->online_loc != '\0' && *rec->suinfo->online_status == 'Y')
                    {
                        snprintf(path, sizeof(path), "%s/" DRMS_SLOTDIR_FORMAT, rec->suinfo->online_loc, rec->slotnum);
                    }
                    else
                    {
                        snprintf(path, sizeof(path), "NoDataDirectory");
                    }
              
                    jsonval = string_to_json(path);
                    val = json_new_string(jsonval);
                    free(jsonval);
                }
                else if (strcmp(keys[ikey], "*logdir*") == 0)
                {
                    const char *logdir = GetLogDir(rec, log_dirs);
                
                    if (logdir)
                    {
                        jsonval = string_to_json(logdir);
                    }
                    else
                    {
                        jsonval = string_to_json("NO LOG");
                    }
                
                    val = json_new_string(jsonval);
                    free(jsonval);
                }
                else if (strcmp(keys[ikey], "*dirmtime*") == 0)
                {
                    // get record dir last change date
                    struct stat buf;
                    char path[DRMS_MAXPATHLEN];
                    char timebuf[128];
                                

                    if (rec->suinfo && *rec->suinfo->online_loc != '\0' && *rec->suinfo->online_status == 'Y')
                    {
                        snprintf(path, sizeof(path), "%s/" DRMS_SLOTDIR_FORMAT, rec->suinfo->online_loc, rec->slotnum);
                        stat(path, &buf);
                        sprint_ut(timebuf, buf.st_mtime + UNIX_EPOCH);
                    }
                    else
                    {
                        snprintf(timebuf, sizeof(timebuf), "NA");
                    }
                
                    jsonval = string_to_json(timebuf);
                    val = json_new_string(jsonval);
                    free(jsonval);
                }
                else if (strcmp(keys[ikey], "*spec*") == 0)
                {
                    char specBuf[DRMS_MAXSERIESNAMELEN * 2];

                    drms_snprint_rec_spec(specBuf, sizeof(specBuf), rec);
                    val = createJsonStringVal(specBuf);                
                    JSOC_INFO_ASSERT(val, "out of memory");
                }
                else
                {
                    rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1); 
                    if (!rec_key_ikey)
                    {
                        jsonval = string_to_json("Invalid KeyLink");
                    }
                    else if (drms_ismissing_keyval(rec_key_ikey) && strcmp(keys[ikey],"QUALITY") != 0)
                    {
                        jsonval = string_to_json("MISSING");
                    }
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
        }
        
        int online = 0; /* I think this is part of a bug; it gets set for each segment, but it is a 
                         * per-record variable */
  
        /* now show desired segments */
        if (useFitsKeyNames)
        {
            json_t *segmentArray = NULL;
            json_t *keywordArray = NULL;
            char *pch = NULL;
            json_t *segObj = NULL;
            DRMS_Segment_t *segTemplate = NULL;
            DRMS_Segment_t *seg = NULL;
            DRMS_Keyword_t *key = NULL;
            int segNum = 0;
            char recordDir[PATH_MAX];
            char path[PATH_MAX];
            char dims[128];
            int iaxis = 0;
            char keyName[DRMS_MAXKEYNAMELEN];
            DRMS_Keyword_t *anckey = NULL;
            
            segmentArray = json_new_array(); /* each record has a segment array - an array of { "name" : "seg1", "value" : "rumble"}
                                              * objects */
            JSOC_INFO_ASSERT(segmentArray, "out of memory");
            json_insert_pair_into_object(recobj, "segments", segmentArray);

            list_llreset(reqSegs);
            while ((lnSeg = list_llnext(reqSegs)) != NULL)
            {
                badSeg = 0;
                segTemplate = *((DRMS_Segment_t **)(lnSeg->data));
                
                segObj = json_new_object();
                JSOC_INFO_ASSERT(segObj, "out of memory");

                if (isInvalidSeg(segTemplate))
                {
                    /* the segment is not defined in this series */
                    badSeg = 1;
                
                    /* name */                    
                    if (segTemplate && segTemplate->info && *(segTemplate->info->name) != '\0')
                    {
                        jsonVal = createJsonStringVal(segTemplate->info->name);
                    }
                    else
                    {
                        snprintf(missingSegNameBuf, sizeof(missingSegNameBuf), "%s%02d", MISSING_SEG_NAME, missingSegNumber++);
                        jsonVal = createJsonStringVal(missingSegNameBuf);
                    }
                
                    json_insert_pair_into_object(segObj, "name", jsonVal);
                    jsonVal = NULL;

                    jsonVal = json_new_true();
                    JSOC_INFO_ASSERT(jsonVal, "out of memory");
                    json_insert_pair_into_object(segObj, INVALID, jsonVal);
                    jsonVal = NULL;
                }
                else
                {
                    /* the segment IS defined in this series, but the current record may not have a segment struct (because
                     * the user provided the segment filter in curly braces section of the record-set specification) */
                    segNum = segTemplate->info->segnum;
                
                    /* segment info from actual record */
                    seg = drms_segment_lookup(rec, segTemplate->info->name); 

                    if (seg)
                    {
                        char *dimStr = NULL;
                        size_t szDimStr = 32;
                        
                        /* the segment is defined in this series, AND a segment struct DOES exist for this record (or linked record) */
                        /* a record does not always have a segment */

                        /* name */
                        jsonVal = createJsonStringVal(segTemplate->info->name);
                        json_insert_pair_into_object(segObj, "name", jsonVal);
                        jsonVal = NULL;                        
                        
                        /* path */
                        if (seg->record->suinfo && *seg->record->suinfo->online_loc != '\0' && *seg->record->suinfo->online_status == 'Y')
                        {
                            /* OK to use drms_segment_filename() - we already 'staged' records, so we have created rec->su */
                            drms_segment_filename(seg, path);
                        }
                        else
                        {
                            snprintf(path, sizeof(path), "NoDataDirectory");
                        }                    
                    
                        /* online */
                        if ((strcmp("Y", rec->suinfo->online_status) == 0) && (*path != '\0'))
                        {
                            jsonVal = json_new_number("1");
                            json_insert_pair_into_object(segObj, "online", jsonVal);
                            jsonVal = NULL;
                        }
                        else
                        {
                            jsonVal = json_new_number("0");
                            json_insert_pair_into_object(segObj, "online", jsonVal);
                            jsonVal = NULL;                        
                        }
                    
                        jsonVal = createJsonStringVal(path);
                        json_insert_pair_into_object(segObj, "path", jsonVal);
                        jsonVal = NULL;
                    
                        /* dims */
                        dims[0] = '\0';
                        dimStr = calloc(1, szDimStr);
                        JSOC_INFO_ASSERT(dimStr, "out of memory");
                        
                        for (iaxis = 0; iaxis < seg->info->naxis; iaxis++)
                        {
                            snprintf(dims, sizeof(dims), "%d", seg->axis[iaxis]);

                            if (iaxis != 0)
                            {
                                dimStr = base_strcatalloc(dimStr, "x", &szDimStr);
                            }
                            
                            dimStr = base_strcatalloc(dimStr, dims, &szDimStr);
                        }

                        jsonVal = createJsonStringVal(dimStr);
                        json_insert_pair_into_object(segObj, "dims", jsonVal);
                        free(dimStr);
                        dimStr = NULL;
                    
                        /* cparms */
                        if (seg->cparms && strlen(seg->cparms))
                        {
                            jsonVal = createJsonStringVal(seg->cparms);
                            json_insert_pair_into_object(segObj, "cparms", jsonVal);
                            jsonVal = NULL;
                        }
                    
                        /* bzero */
                        snprintf(keyName, sizeof(keyName), "%s_bzero", segTemplate->info->name);
                        anckey = drms_keyword_lookup(rec, keyName, 1);

                        if (anckey)
                        {                        
                            fitsexport_getmappedextkeyvalue(anckey, &fitsValue);

                            if (fitsValue)
                            {
                                valOut = fitsValue;
                            }
                            else
                            {
                                valOut = MISSING_KEY_VALUE;
                            }
                
                            jsonVal = createJsonStringVal(valOut);
                            json_insert_pair_into_object(segObj, "bzero", jsonVal);
                            jsonVal = NULL;
                            
                            if (fitsValue)
                            {                
                                free(fitsValue);
                                fitsValue = NULL;
                            }
                        }
                    
                        /* bscale */
                        snprintf(keyName, sizeof(keyName), "%s_bscale", segTemplate->info->name);
                        anckey = drms_keyword_lookup(rec, keyName, 1);

                        if (anckey)
                        {                        
                            fitsexport_getmappedextkeyvalue(anckey, &fitsValue);

                            if (fitsValue)
                            {
                                valOut = fitsValue;
                            }
                            else
                            {
                                valOut = MISSING_KEY_VALUE;
                            }
                
                            jsonVal = createJsonStringVal(valOut);
                            json_insert_pair_into_object(segObj, "bscale", jsonVal);
                            jsonVal = NULL;
                            
                            if (fitsValue)
                            {                
                                free(fitsValue);
                                fitsValue = NULL;
                            }
                        }

                        /* for each segment, print the segment-specific keyword value */
                        list_llreset(reqKeys);
                        while ((lnKey = list_llnext(reqKeys)) != NULL)
                        {
                            keyTemplate = *((DRMS_Keyword_t **)(lnKey->data));
                            badKey = 0;
                            keyNameOut = NULL;
                            valOut = NULL;
                            missingKeyName = 0;
                            
                            if (isInvalidKey(keyTemplate))
                            {
                                /* bad keys have already been handled, in the record-key code; skip this one */
                                badKey = 1;
                            }
                            else
                            {
                                if (drms_keyword_getperseg(keyTemplate))
                                {
                                    DRMS_Keyword_t *keyWithVal = NULL;
                    
                                    if (!keywordArray)
                                    {
                                        keywordArray = json_new_array(); /* each record has a keyword array - an array of { "name" : "key1", "value" : "rumble"}
                                                                          * objects
                                                                          */
                                        JSOC_INFO_ASSERT(keywordArray, "out of memory");
                                        json_insert_pair_into_object(segObj, "keywords", keywordArray);
                                    }

                                    /* get keyword name from template keyword */                            
                                    if (fitsexport_getmappedextkeyname(keyTemplate, NULL, NULL, fitsName, sizeof(fitsName)) && *fitsName != '\0')
                                    {
                                        keyNameOut = fitsName;
                                    }
                                    else
                                    {
                                        fprintf(stderr, "Unable to map DRMS keyword name %s to FITS keyword name.\n", keyTemplate->info->name);
                                        missingKeyName = 1;
                                    }
                    
                                    /* key is from the jsd template, which does not include the _xxx suffix */
                                    snprintf(keyName, sizeof(keyName), "%s_%03d", keyTemplate->info->name, segNum);                            

                                    /* key is the template keyword, but we need the actual keyword from the current record; 
                                     * follow the link if the keyword is a linked keyword */
                                    keyWithVal = drms_keyword_lookup(rec, keyName, 1);
            
                                    if (keyWithVal)
                                    {
                                        JSOC_INFO_ASSERT(fitsValue == NULL, "about to leak");
                                        fitsexport_getmappedextkeyvalue(keyWithVal, &fitsValue);

                                        if (fitsValue)
                                        {
                                            valOut = fitsValue;
                                        }
                                        else
                                        {
                                            valOut = MISSING_KEY_VALUE;
                                        }
                                    }
                                    else
                                    {
                                        /* should not get here - we tested all key names for validity */
                                        valOut = MISSING_KEY_VALUE;
                                    } 
                                }
                            }

                            JSOC_INFO_ASSERT((keyNameOut != NULL && valOut != NULL) || (keyNameOut == NULL && valOut == NULL), "invalid combination");
                            
                            if (keyNameOut || missingKeyName)
                            {
                                /* skip invalid and per-segment keywords (which are !keyNameOut && !missingKeyName) */
                
                                /* make json */
                                keyObj = json_new_object();
                                JSOC_INFO_ASSERT(keyObj, "out of memory");

                                if (missingKeyName)
                                {
                                    snprintf(missingKeyNameBuf, sizeof(missingKeyNameBuf), "%s%02d", MISSING_KEY_NAME, missingKeyNumber++);
                                    jsonVal = createJsonStringVal(missingKeyNameBuf);
                                }
                                else
                                {
                                    jsonVal = createJsonStringVal(keyNameOut);
                                }
                                json_insert_pair_into_object(keyObj, "name", jsonVal);
                                jsonVal = NULL;

                                if (valOut)
                                {
                                    jsonVal = createJsonStringVal(valOut);
                                    json_insert_pair_into_object(keyObj, "value", jsonVal);
                                    jsonVal = NULL;
                                }
                    
                                if (badKey)
                                {
                                    jsonVal = json_new_true();
                                    JSOC_INFO_ASSERT(jsonVal, "out of memory");
                                    json_insert_pair_into_object(keyObj, INVALID, jsonVal);
                                    jsonVal = NULL;
                                }

                                json_insert_child(keywordArray, keyObj);
                            }

                            if (fitsValue)
                            {                
                                free(fitsValue);
                                fitsValue = NULL;
                            }
                        }
                    }
                    else
                    {
                        /* the segment is defined in this series, BUT a segment struct does NOT exist for this record because: 
                         * 1. the record-set spec had curly braces and although the segment does exist in the series, the {} filtered it out;
                         * OR
                         * 2. the linked record was not successfully resolved */

                        /* could not find segment struct (followLinks is true) */
                        DRMS_Segment_t *testSeg = NULL;
                    
                        /* could be missing target record (we followed tried following links); since we know that this segment is
                         * valid, we must find the segment struct in the parent series, unless #1 above is true */
                        testSeg = hcon_lookup_lower(&rec->segments, segTemplate->info->name);

                        if (testSeg)
                        {
                            /* #2 above */
                            jsonVal = createJsonStringVal(segTemplate->info->name);
                            json_insert_pair_into_object(segObj, "name", jsonVal);
                            
                            jsonVal = createJsonStringVal("BadSegLink");
                            json_insert_pair_into_object(segObj, "path", jsonVal);

                            jsonVal = NULL;
                        }
                        else
                        {
                            /* #1 above */
                            if (!allSegs)
                            {
                                /* the user specified a segment in the seglist that IS part of the series, but it is NOT 
                                 * in the curly braces clause */
                                jsonVal = createJsonStringVal(segTemplate->info->name);
                                json_insert_pair_into_object(segObj, "name", jsonVal);

                                jsonVal = createJsonStringVal(INVALID_SEG);
                                json_insert_pair_into_object(segObj, "path", jsonVal);
                                
                                jsonVal = createJsonStringVal("NA");
                                json_insert_pair_into_object(segObj, "dims", jsonVal);
                                jsonVal = NULL;                 
                            }
                        }
                    }
                }
                
                if (segObj->child)
                {
                    /* segment object is not empty */
                    json_insert_child(segmentArray, segObj);
                }
            }
        }
        else
        {
            /* not using FITS keyword names */
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
            
                if (rec_seg_iseg)
                {
                    int iaxis, naxis = rec_seg_iseg->info->naxis;
                    char dims[100], dimval[20];
              
                    if (rec_seg_iseg->record->suinfo && *rec_seg_iseg->record->suinfo->online_loc != '\0' && *rec_seg_iseg->record->suinfo->online_status == 'Y')
                    {
                        /* OK to use drms_segment_filename() - we already 'staged' records, so we have created rec->su */
                        drms_segment_filename(rec_seg_iseg, path);
                    }
                    else
                    {
                        snprintf(path, sizeof(path), "NoDataDirectory");
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
                else
                {
                    if (!allSegs)
                    {
                        char *nosegmsg = INVALID_SEG;

                        DRMS_Segment_t *segment = hcon_lookup_lower(&rec->segments, segs[iseg]);
                        if (segment && segment->info->islink)
                        nosegmsg = "BadSegLink";
                        jsonpath = string_to_json(nosegmsg);
                        json_insert_child(thissegval, json_new_string(jsonpath));
                        free(jsonpath);
                        jsondims = string_to_json("NA");
                        json_insert_child(thissegdim, json_new_string(jsondims));
                        free(jsondims);
                    }
                }
            }
        }

        if (useFitsKeyNames)
        {
            json_t *linkArray = NULL;
            DRMS_Link_t *linkTemplate = NULL;
            DRMS_Link_t *link = NULL;
            json_t *linkObj = NULL;
            DRMS_Record_t *linkedRec = NULL;
            char linkSpec[DRMS_MAXQUERYLEN];
            int missingLinkName = 0;
            
            linkArray = json_new_array(); /* each record has a segment array - an array of { "name" : "seg1", "value" : "rumble"}
                                           * objects
                                           */
            JSOC_INFO_ASSERT(linkArray, "out of memory");
            json_insert_pair_into_object(recobj, "links", linkArray);
            
            list_llreset(reqLinks);
            while ((lnLink = list_llnext(reqLinks)) != NULL)
            {
                linkTemplate = *((DRMS_Link_t **)(lnLink->data));
                badLink = 0;
                valOut = NULL;
                *linkSpec = '\0';
                
                linkObj = json_new_object();
                JSOC_INFO_ASSERT(linkObj, "out of memory");
                
                if (isInvalidLink(linkTemplate))
                {
                    badLink = 1;
                    
                    if (linkTemplate && linkTemplate->info && *(linkTemplate->info->name) != '\0')
                    {
                        valOut = linkTemplate->info->name;
                    }
                    else
                    {
                        missingLinkName = 1;
                    }                  
                }
                else if (!(linkTemplate->info) || *(linkTemplate->info->name) == '\0')
                {
                    missingLinkName = 1;
                    badLink = 1;
                }
                else
                {
                    valOut = linkTemplate->info->name;

                    /* unlike the case for keywords and segments, DRMS does not have a lookup function for 
                     * links */
                    link = hcon_lookup_lower(&rec->links, linkTemplate->info->name);

                    if (!link || !link->info)
                    {
                        badLink = 1;
                    }
                    else
                    {
                        linkedRec = drms_link_follow(rec, linkTemplate->info->name, &status);
                     
                        if (!linkedRec || !linkedRec->seriesinfo || *(linkedRec->seriesinfo->seriesname) == '\0')
                        {
                            badLink = 1;
                        }
                        else
                        {
                            if (link->info->type == DYNAMIC_LINK)
                            {
                                drms_sprint_rec_query(linkSpec, linkedRec);
                            }
                            else
                            {
                                snprintf(linkSpec, sizeof(linkSpec), "%s[:#%lld]", linkedRec->seriesinfo->seriesname, linkedRec->recnum);
                            }
                            
                            drms_close_record(linkedRec, DRMS_FREE_RECORD);
                        }
                    }                 
                }
                
                if (missingLinkName)
                {
                    snprintf(missingLinkNameBuf, sizeof(missingLinkNameBuf), "%s%02d", MISSING_LINK_NAME, missingLinkNumber++);
                    jsonVal = createJsonStringVal(missingLinkNameBuf);
                }
                else
                {
                    jsonVal = createJsonStringVal(valOut);                    
                }
                json_insert_pair_into_object(linkObj, "name", jsonVal);
                jsonVal = NULL;
                
                if (*linkSpec != '\0')
                {
                    jsonVal = createJsonStringVal(linkSpec);
                    json_insert_pair_into_object(linkObj, "linkedrec", jsonVal);
                    jsonVal = NULL;
                }
                
                if (badLink)
                {
                    jsonVal = json_new_true();
                    JSOC_INFO_ASSERT(jsonVal, "out of memory");
                    json_insert_pair_into_object(linkObj, INVALID, jsonVal);
                    jsonVal = NULL;
                }

                json_insert_child(linkArray, linkObj);
            } /* link loop */
        }
        else
        {
            /* not using FITS keyword names */
          /* now show desired links */
          for (ilink=0; ilink<nlinks; ilink++) 
            {
            DRMS_Link_t *rec_link = hcon_lookup_lower (&rec->links, links[ilink]); 
            DRMS_Record_t *linked_rec = drms_link_follow(rec, links[ilink], &status);
            char linkquery[DRMS_MAXQUERYLEN];
            if (linked_rec)
              {
              if (rec_link->info->type == DYNAMIC_LINK)
                drms_sprint_rec_query(linkquery, linked_rec);
              else
                sprintf(linkquery, "%s[:#%lld]", linked_rec->seriesinfo->seriesname, linked_rec->recnum);
              drms_close_record(linked_rec, DRMS_FREE_RECORD);
  
              json_t *thislinkval = linkvals[ilink]; 
              json_insert_child(thislinkval, json_new_string(linkquery));
              }
            else
              {
              json_t *thislinkval = linkvals[ilink]; 
              json_insert_child(thislinkval, json_new_string("Invalid_Link"));
              }
            }
        }

        if (useFitsKeyNames)
        {
            if (wantRecInfo && !requisition.requireSUMinfoOnline)
            {
                /* create an 'online' property in the record object only if the user did not request *online* - if they did 
                 * request *online*, then each segment has an 'online' attribute, if not then the record has an 'online' attribute */
                if (!rec->suinfo)
                {
                    jsonVal = createJsonStringVal("NA");
                }
                else
                {
                    if (strcmp("Y", rec->suinfo->online_status) == 0)
                    {
                        jsonVal = json_new_number("1");
                    }
                    else
                    {
                        jsonVal = json_new_number("0");
                    }
                }
                
                JSOC_INFO_ASSERT(jsonVal, "out of memory");
                json_insert_pair_into_object(recobj, "online", jsonVal);
                jsonVal = NULL;
            }
            
            json_insert_child(recArray, recobj);
        }
        else
        {
            /* not using FITS keyword names */
            if (!recinfo)
            {
                recinfo = json_new_array();
            }

              /* finish record info for this record */
            if (wantRecInfo)
            {
                json_insert_pair_into_object(recobj, "online", json_new_number(online ? "1" : "0"));
                json_insert_child(recinfo, recobj);
            }
        }
      } /* rec loop */
      
        /* clean up memory used by invalid keys, segs, links */
        list_llreset(reqKeys);
        while ((lnKey = list_llnext(reqKeys)) != NULL)
        {
            keyTemplate = *((DRMS_Keyword_t **)(lnKey->data));

            if (isInvalidKey(keyTemplate))
            {            
                /* remove from list so that the keyword is ignored in the per-segment code */
                list_llremove(reqKeys, lnKey);
            
                if (keyTemplate)
                {
                    if (keyTemplate->info)
                    {
                        free(keyTemplate->info);
                    }

                    free(keyTemplate); /* invalid keys were allocated */
                }
                
                list_llfreenode(&lnKey);
            }
        }
        
        list_llreset(reqSegs);
        while ((lnSeg = list_llnext(reqSegs)) != NULL)
        {
            segTemplate = *((DRMS_Segment_t **)(lnSeg->data));

            if (isInvalidSeg(segTemplate))
            {            
                /* remove from list so that the keyword is ignored in the per-segment code */
                list_llremove(reqSegs, lnSeg);
            
                if (segTemplate)
                {
                    if (segTemplate->info)
                    {
                        free(segTemplate->info);
                    }

                    free(segTemplate); /* invalid segs were allocated */
                }

                list_llfreenode(&lnSeg);
            }
        }

        list_llreset(reqLinks);
        while ((lnLink = list_llnext(reqLinks)) != NULL)
        {
            linkTemplate = *((DRMS_Link_t **)(lnLink->data));
            
            if (isInvalidLink(linkTemplate))
            {
                list_llremove(reqLinks, lnLink);

                if (linkTemplate)
                {
                    if (linkTemplate->info)
                    {
                        free(linkTemplate->info);
                    }

                    free(linkTemplate); /* invalid linkd were allocated */
                }

                list_llfreenode(&lnLink);
            }
        }
  
  /* Finished.  Clean up and exit. */
        if (useFitsKeyNames)
        {
            json_insert_pair_into_object(jroot, "recset", recArray);
        }
        else
        {
            json_t *json_keywords = json_new_array();
            json_t *json_segments = json_new_array();
            json_t *json_links = json_new_array();
        
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
                /* do not insert invalid segs if allSegs == 1 */
                if (!allSegs || segvals[iseg]->child || segdims[iseg]->child || segcparms[iseg]->child || segbzeros[iseg]->child || segbscales[iseg]->child)
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
      }

      snprintf(count, sizeof(count), "%d", nrecs);
      json_insert_pair_into_object(jroot, "count", json_new_number(count));
      json_insert_runtime(jroot, StartTime);
      json_insert_pair_into_object(jroot, "status", json_new_number("0"));
    
    drms_close_records(recordset, DRMS_FREE_RECORD);
    json_tree_to_string(jroot, &final_json);
    
    if (printHTTPHeaders)
    {
        printf("Content-type: application/json\n\n");
    }

    printf("%s\n",final_json);
    free(final_json);
    fflush(stdout);

    json_free_value(&jroot);
    
    if (log_dirs)
    {
        hcon_destroy(&log_dirs);
    }

    if (reqLinks)
    {
        list_llfree(&reqLinks);
    }
    
    if (reqSegs)
    {
        list_llfree(&reqSegs);
    }

    if (reqKeys)
    {
        list_llfree(&reqKeys);
    }    
        
    drms_destroy_jsdtemplate_record(&jsdTemplate);

    report_summary(Server, StartTime, Remote_Address, op, in, max_recs, 0);
    manage_userhandle(0, userhandle);
    return(0);
    } /* rs_list */

    manage_userhandle(0, userhandle);
    return(0);
  }

