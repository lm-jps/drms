#define DEBUG 1
#define DEBUG 0

/*
 *  jsoc_export_as_is - Generates index.XXX files for dataset export.
 *  Copied and changed from jsoc_info.c
 *
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "Not Specified", "<Operation>"},
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "seg", "**ALL**", "<comma delimited segment list>"},
  {ARG_STRING, "requestid", "Not Specified", "RequestID string for export management"},
  {ARG_STRING, "method", "url", "Export method"},
  {ARG_STRING, "protocol", "as-is", "export file protocol"},
  {ARG_STRING, "format", "json", "export communication protocol"},
  {ARG_STRING, "filenamefmt", "Not Specified", "export filename format rule"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_FLAG, "z", "0", "emit JSON output"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_END}
};

char *module_name = "jsoc_info";
int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\njsoc_info {-h} "
	"op=<command> ds=<recordset query> {n=0} {key=<keylist>} {seg=<segment_list>}\n"
        "  details are:\n"
	"op=<command> tell which ajax function to execute\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"seg=<comma delimited segment list, default is **ALL**>\n"
        "requestid= RequestID string for export management\n"
        "method = Export method, default to url\n"
        "protocol = export file protocol, default to as-is\n"
        "format = export communication protocol, default to json\n"
        "filenamefmt = export filename format rule\n"
	);
    return(1);
    }
  return (0);
  }

// jsoc_export_make_filename - creates a filename from a template.  The filename
// string should have enough room for a DRMS_MAX_PATHLEN string.
// the default template is {seriesname}.{recnum:%ld}.{segment}.
// The general form is {<word>:<format>}<text> repeated as needed where <text> will be
// copied into the filename as is.  The form above can be repeated as desiered.
// The :<format> section can be omitted.  In fact it is presently ignored except for
// the special case of recnum.  This can be changed later to allow specification of
// formats for any keyword.  Special "words" are:
//   seriesname - copies in the seriesname
//   recnum - allows format and copies in the current recnum
//   segment - copies in the segment filename which is usually the <segment name>.<protocol>.
//   <keyword> - copies in the value of the given keyword.
// The seriesname and record information is looked up from the segment.

jsoc_export_make_filename(DRMS_Segment_t *seg, const char *filenamefmt, char *filename)
  {
  char *fn = filename;
  char format[1000];
  char *fmt;
  if (filenamefmt)
    strcpy(format, filenamefmt);
  else
    strcpy(format, "{seriesname}.{recnum:%ld}.{segment}");
  fmt = format;
  *fn = '\0';
  while (*fmt)
    {
    char *last;
    if (*fmt == '{')
      {
      DRMS_Keyword_t *kw;
      char *val;
      char *p;
      char *keyname;
      char *layout;
      last = index(fmt, '}');
      if (!last)
         return;
      keyname = ++fmt;
      layout = NULL;
      *last = '\0';
      for (p=keyname; p<last; p++)
        {
        if (*p == ':')
          {
          *p++ = '\0';
          layout = p;
          }
        }
      if (*keyname)
        {
        char valstr[100];
        if (strcmp(keyname,"seriesname")==0)
          val = seg->record->seriesinfo->seriesname;
        else if (strcmp(keyname,"recnum")==0)
          {
          snprintf(valstr, 100, (layout ? layout : "%ld"), seg->record->recnum); 
          val = valstr;
          }
        else if (strcmp(keyname,"segment")==0)
          val = seg->filename;
        else
          val = drms_getkey_string(seg->record,keyname,NULL);
        if (!val)
          val = "ERROR";
        for (p=val; *p; )
          {
          *fn++ = *p++;
          }
        *fn = '\0';
        }
      fmt = last+1;
      }
    else
      *fn++ = *fmt++;
    }
  *fn = '\0';
  }

#define DIE(msg) \
  {	\
  fprintf(index_txt,"status=1\n");	\
  fprintf(index_txt, "error='%s'\n", msg);	\
  fclose(index_txt); \
  return(1);	\
  }

/* Module main function. */
int DoIt(void)
  {
  char *in;
  char *requestid;
  char *method;
  char *protocol;
  char *format;
  char *filenamefmt;
  char *seglist;
  int  keys_listed, segs_listed;

  DRMS_RecordSet_t *recordset;
  DRMS_Record_t *rec;
  char *keys[1000];
  char *segs[1000];
  int ikey, nkeys = 0;
  int iseg, nsegs = 0;
  int count;
  int status=0;
  int irec, nrecs;
  long long size;
  FILE *index_txt, *index_data;
  char buf[2*DRMS_MAXPATHLEN];
  char *cwd;

  if (nice_intro ()) return (0);

  in = cmdparams_get_str (&cmdparams, "ds", NULL);
  requestid = cmdparams_get_str (&cmdparams, "requestid", NULL);
  format = cmdparams_get_str (&cmdparams, "format", NULL);
  filenamefmt = cmdparams_get_str (&cmdparams, "filenamefmt", NULL);
  method = cmdparams_get_str (&cmdparams, "method", NULL);
  protocol = cmdparams_get_str (&cmdparams, "protocol", NULL);
  seglist = strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
  segs_listed = strcmp (seglist, "Not Specified");

  index_txt = fopen("index.txt", "w");
  fprintf(index_txt, "# JSOC Export File List\n");
  fprintf(index_txt, "version=1\n");
  fprintf(index_txt, "requestid=%s\n", requestid);
  fprintf(index_txt, "method=%s\n", method);
  fprintf(index_txt, "protocol=%s\n", protocol);
  fprintf(index_txt, "wait=0\n");

  /* Open record_set */
  recordset = drms_open_records (drms_env, in, &status);
  if (!recordset) 
    DIE(" jsoc_info: series not found.");

  nrecs = recordset->n;
  if (nrecs == 0)
    {
    fprintf(index_txt, "count=0\n");
    fprintf(index_txt, "size=0\n");
    fprintf(index_txt, "status=0\n");
    fclose(index_txt);
    return(0);
    }

  index_data = fopen("index.data", "w+");

  /* get list of segments to show for each record */
  nsegs = 0;
  if (segs_listed) 
    { /* get specified segment list */
    char *thisseg;
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
        HIterator_t hit;
        hiter_new (&hit, &recordset->records[0]->segments);
        while ((seg = (DRMS_Segment_t *)hiter_getnext (&hit)))
          segs[nsegs++] = strdup (seg->info->name);
	  }
	else
	  segs[nsegs++] = strdup(thisseg);
	}
    }
  free (seglist);

  /* loop over set of selected records */
  count = 0;
  size = 0;
  for (irec = 0; irec < nrecs; irec++) 
    {
    char recquery[DRMS_MAXQUERYLEN];
    char recpath[DRMS_MAXPATHLEN];

    rec = recordset->records[irec];  /* pointer to current record */
    drms_sprint_rec_query(recquery,rec);
    drms_record_directory (rec, recpath, 1);

    /* now get desired segments */
    for (iseg=0; iseg<nsegs; iseg++) 
      {
      DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
      char path[DRMS_MAXPATHLEN];
      char query[DRMS_MAXQUERYLEN];
      char filename[DRMS_MAXPATHLEN];
      struct stat filestat;

      // Get record query with segment name appended
      strncpy(query, recquery, DRMS_MAXQUERYLEN);
      strncat(query, "{", DRMS_MAXQUERYLEN);
      strncat(query, segs[iseg], DRMS_MAXQUERYLEN);
      strncat(query, "}", DRMS_MAXQUERYLEN);

      // Get paths to segment files
      strncpy(path, recpath, DRMS_MAXPATHLEN);
      strncat(path, "/", DRMS_MAXPATHLEN);
      strncat(path, rec_seg_iseg->filename, DRMS_MAXPATHLEN);

      // Get segment file size
      if (!stat(path, &filestat))
        size += filestat.st_size;

      /* Make a symlink for each selected file */

      jsoc_export_make_filename(rec_seg_iseg, strcmp(filenamefmt,"Not Specified") ? filenamefmt : NULL, filename);
      symlink(path,filename);

      /* write a line for each record to each output file type wanted */

      fprintf(index_data, "%s\t%s\n",query,filename);
      count += 1;
      } // segment loop
    } // record loop

/* Finished.  Clean up and exit. */

   fprintf(index_txt, "count=%d\n",count);
   fprintf(index_txt, "size=%d\n",size);
   fprintf(index_txt, "status=0\n");
   cwd = getcwd(NULL, 0);
   fprintf(index_txt,"dir=%s\n", ((strncmp("/auto", cwd,5) == 0) ? cwd+5 : cwd));
   fprintf(index_txt, "# DATA\n");
   rewind(index_data);
   while (fgets(buf, DRMS_MAXPATHLEN*2, index_data))
     fputs(buf, index_txt);
   fclose(index_txt);
   fclose(index_data);
   unlink("index.data");
  
  drms_close_records(recordset, DRMS_FREE_RECORD);
  return(0);
}
