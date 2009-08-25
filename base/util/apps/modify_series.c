/**
\defgroup modify_series modify_series - for a series, change the archive flag and/or add keywords as described in a .jsd file
@ingroup drms_util

\brief Modify a DRMS series structure.

\par Synopsis:

\code
modify_series [-GEN_FLAGS] series=<seriesname> [archive={0|1}] [jsdfile]
\endcode

Modify a series definition. It does one of the following:
 - Change archive flag
 - Add keywords as specified in jsdfile

\par GEN_FLAGS: 
Ubiquitous flags present in every module.
\ref jsoc_main

\param seriesname
\param jsdfile JSOC series definition file name. This file contains keywords to add.
\param archive 0: not to achive, 1: to archive.

\par Limitations:
 - \ref modify_series does not deal with duplications in keyword definition
 - \ref modify_series does not fill in defaultval for the newly added keywords.

\sa
create_series delete_series describe_series show_info

*/
#include "drms.h"
#include "jsoc_main.h"
#include "util.h"

/* Command line parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_END}
};

char *module_name = "modify_series";
typedef enum {ARCHIVE, ADD_KEYWORD} action_t;

int DoIt(void) {
  const int bufsz = 2048;
  action_t action = ADD_KEYWORD;
  int status = DRMS_NO_ERROR;
  int archive;
  char *series, *series_short, *namespace;
  char stmt [DRMS_MAXQUERYLEN];
  //char stmt[2048];
  DB_Text_Result_t *qres = NULL;
  char *filename;
  FILE *fp;
  struct stat file_stat;
  char *buf = 0;
  DRMS_Record_t *template;

  /* Parse command line parameters. */
  if (cmdparams_numargs (&cmdparams) < 2) goto usage;

  /* Get series name. */
  series = cmdparams_get_str(&cmdparams, "series", NULL);
  char *p = strchr(series, '.');  
  if (!p) {
    printf("Invalid series name\n");
    return 1;
  }
  namespace = ns(series);
  series_short  = strdup(p+1);

  archive = cmdparams_get_int(&cmdparams, "archive", &status);
  if (!status) {
    action = ARCHIVE;
  }

  if (action == ADD_KEYWORD) {
    filename = cmdparams_getarg(&cmdparams, 1);
    /* Read the whole series definition file into memory. */
    if (stat(filename, &file_stat)) 
      {
	printf("Can't stat file %s\n", filename);
	return 1;
      }  
    XASSERT((buf = (char *)malloc( file_stat.st_size+1 )));
    fp = fopen(filename,"r");
    fread(buf,file_stat.st_size,1,fp);
    buf[file_stat.st_size] = 0;
  }

  switch (action) {
  case ARCHIVE:
    snprintf(stmt, bufsz, "select has_table_privilege('%s', '%s.drms_series', 'update')", drms_env->session->db_handle->dbuser, namespace);
    if ( (qres = drms_query_txt(drms_env->session, stmt)) != NULL) {
      if (qres->num_rows > 0) {
	if (qres->field[0][0][0] == 'f') {
	  fprintf(stderr, "dbuser %s does not have create privilege in namespace %s.\n",
		  drms_env->session->db_handle->dbuser, namespace);
	  goto bailout;
	}
      } else {
	fprintf(stderr, "Failed: %s\n", stmt);
	goto bailout;
      }
    } else {
      fprintf(stderr, "Failed: %s\n", stmt);
      goto bailout;
    }      

    snprintf(stmt, bufsz, "update %s.drms_series set archive = %d where seriesname='%s'", namespace, archive, series);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }

    printf("Archive flag has been set to %d for series %s\n", archive, series);
    printf("Note this does not have any effect on existing records of this series.\n");
    break;
  case ADD_KEYWORD:
    // assume the new keywords do not conflict with any existing
    // ones. otherwise all hell will break loose. 

    /* Here's how this works:
     * 1. Template contains the content parsed from the "new" jsd - the one containing the new keywords.
     * 2. Remove the old template record from series_cache, but don't delete any parts of the old template record.
     *    This is achieved by temporarily setting the deep_free function to NULL, and calling drms_delete_series().
     *    cascade == 0 in the call to ensure that the original series table is not dropped. The old entries in 
     *    the drms_series, drms_keywords, etc. tables will also be deleted.
     * 3. Restore the deep_free function.
     * 4. Rename the old series table and sequence to *_tmp.
     * 5. Create the series based on the new template. This will make appropriate entries in drms_series, drms_keywords, 
     *    etc.
     * 6. Drop the newly created sequence and replace it with the *_tmp sequence saved in in step 4.
     * 7. Copy the series table data saved in step 4 to the newly created series table.
     */
 
    template = drms_template_record(drms_env, series, &status);
    drms_link_getpidx(template); 
    // save the original column names
    char *field_list = drms_field_list(template, NULL);
    if (template == NULL) {
      printf("Series '%s' does not exist. drms_template_record returned "
	     "status=%d\n",series,status);
      goto bailout;
    }
    // allocate for a record template
    status = parse_keywords(buf, template, NULL);
    free(buf);

    // Delete current series metadata without dropping the series table and
    // sequence. Take care not to free the template. Use cascade=0 in drms_delete_series to avoid dropping table.
    void (*pFn)(const void *);
    pFn = (drms_env->series_cache).deep_free; /* temp var to hold fn. */
    (drms_env->series_cache).deep_free = NULL;
    if (drms_delete_series(drms_env, series, 0)) {
      fprintf(stderr, "Failed to remove previous definitions\n");
      status = 1;
      goto bailout;
    }

    // rename series table and sequence
    snprintf(stmt,  bufsz, "alter table %s rename to %s_tmp; alter table %s_seq rename to %s_tmp_seq", series, series_short, series, series_short);
    //if ((qres = drms_query_txt(drms_env->session, stmt)) && qres->num_rows>0) {
    if(drms_dms(drms_env->session, NULL, stmt)) {
       fprintf(stderr, "Failed: %s\n", stmt);
       status = 1;
        goto bailout;
    }


    // restore deep_free function
    (drms_env->series_cache).deep_free = pFn;

    // retain the spot in the series_cache. this is a hack to undo
    // hcon_remove()
    // Enclosed the hcon_remove() in an 'if (cascade)' stmt, so can remove this hack.
    hcon_allocslot_lower(&drms_env->series_cache, series);
        drms_print_record(template);

    // create a new series
    if (drms_insert_series(drms_env->session, 0, template, 0)) {
      fprintf(stderr, "Failed to create new definitions\n");
      status = 1;
      goto bailout;
    }

    // drop newly created series sequence
    snprintf(stmt, bufsz, "drop sequence %s_seq; alter table %s_tmp_seq rename to %s_seq", series, series, series_short);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }

    // move back contents of series
    //snprintf(stmt, bufsz, "insert into %s (%s) select %s from %s_tmp", series, field_list, field_list, series);
    //buffer size could be huge, above statment doesn't have enough space.  Need to build with base_strcatalloc instead.
    char *stmt2 = malloc(DRMS_MAXQUERYLEN);
    size_t bufsize = DRMS_MAXQUERYLEN;
    memset (stmt2, 0, bufsize);
     base_strcatalloc(stmt2, "insert into ", &bufsize);
     base_strcatalloc(stmt2, series, &bufsize);
     base_strcatalloc(stmt2, " (", &bufsize);
     base_strcatalloc(stmt2, field_list, &bufsize);
     base_strcatalloc(stmt2, ") ", &bufsize);
     base_strcatalloc(stmt2, "select ", &bufsize);
     base_strcatalloc(stmt2, field_list, &bufsize);
     base_strcatalloc(stmt2, " from ", &bufsize);
     base_strcatalloc(stmt2, series, &bufsize);
     base_strcatalloc(stmt2, "_tmp", &bufsize);

    if(drms_dms(drms_env->session, NULL, stmt2)) {
      fprintf(stderr, "Failed: %s\n", stmt2);
      status = 1;
      goto bailout;
    }
    free(field_list);
    free(stmt2);

    srand((unsigned int) time(NULL)+getpid());
    snprintf(stmt, bufsz, "create table backup.%s_%s_%d as select * from %s_tmp;drop table %s_tmp", namespace, series_short, rand(), series, series);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }
    break;
  default:
    printf("Don't know what to do, give up...\n");
    status = 1;
  }

 bailout:
  free(series_short);
  free(namespace);
  if (qres) 
    db_free_text_result(qres);

  return status;

 usage:
  printf("Usage: %s archive=[1|0] series=seriesname\n", cmdparams_getarg (&cmdparams, 0));
  printf("       %s series=seriesname filename\n", cmdparams_getarg (&cmdparams, 0));
  return 1;
}
    
