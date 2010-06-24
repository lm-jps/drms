// #define DEBUG 1
#define DEBUG 0

/*
 *  jsoc_export_as_is - Generates index.XXX files for dataset export.
 *  Copied and changed from jsoc_info.c
 *  This program is expected to be run in a drms_run script.
 *  cwd is expected to be the export SU.
 *
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "exputil.h"

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

  in = (char *)cmdparams_get_str (&cmdparams, "ds", NULL);
  requestid = (char *)cmdparams_get_str (&cmdparams, "requestid", NULL);
  format = (char *)cmdparams_get_str (&cmdparams, "format", NULL);
  filenamefmt = (char *)cmdparams_get_str (&cmdparams, "filenamefmt", NULL);
  method = (char *)cmdparams_get_str (&cmdparams, "method", NULL);
  protocol = (char *)cmdparams_get_str (&cmdparams, "protocol", NULL);
  seglist = (char *)strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
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
    if (drms_record_directory (rec, recpath, 1))
      continue;
    if (strlen(recpath) < 10)
      continue;

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

      if (stat(path, &filestat) == 0) // only make links for existing files!
        { 
        if (S_ISDIR(filestat.st_mode))
          { // Segment is directory, get size == for now == use system "du"
          char cmd[DRMS_MAXPATHLEN+100];
          FILE *du;
          long long dirsize;
          sprintf(cmd,"/usr/bin/du -s -b %s", path);
          du = popen(cmd, "r");
          if (du)
            {
            if (fscanf(du,"%lld",&dirsize) == 1)
              size += dirsize;
            pclose(du);
            }
          }
        else
          size += filestat.st_size;

        /* Make a symlink for each selected file */

        exputl_mk_expfilename(rec_seg_iseg, strcmp(filenamefmt,"Not Specified") ? filenamefmt : NULL, filename);
        if (strcmp(method,"ftp")==0)
          {
          char tmp[DRMS_MAXPATHLEN];
          sprintf(tmp,"/export%s", path);
          symlink(tmp,filename);
          }
        else
          symlink(path,filename);

        /* write a line for each record to each output file type wanted */

        fprintf(index_data, "%s\t%s\n",query,filename);
        count += 1;
        }
      else
        fprintf(index_data, "%s\tNoDataFile\n",query);
      } // segment loop
    } // record loop

   if (size > 0 && size < 1024*1024) size = 1024*1024;
   size /= 1024*1024;

/* Finished.  Clean up and exit. */

   fprintf(index_txt, "count=%d\n",count);
   fprintf(index_txt, "size=%lld\n",size);
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
