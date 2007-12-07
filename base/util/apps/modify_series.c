#include "drms.h"
#include "jsoc_main.h"

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
  char stmt[bufsz];
  DB_Text_Result_t *qres = NULL;
  char *filename;
  FILE *fp;
  struct stat file_stat;
  char *buf = 0;
  DRMS_Record_t *template;

  /* Parse command line parameters. */
  if (cmdparams_numargs (&cmdparams) < 3) goto usage;

  /* Get series name. */
  series = cmdparams_get_str(&cmdparams, "series", NULL);
  char *p = strchr(series, '.');  
  if (!p) {
    printf("Invalid series name\n");
    return 1;
  }
  namespace = strndup(series, p-series);
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
    status = parse_keywords(buf, template);
    free(buf);

    // rename series table and sequence
    snprintf(stmt,  bufsz, "drop index %s_prime_idx; alter table %s rename to %s_tmp; alter table %s_seq rename to %s_tmp_seq", series, series, series_short, series, series_short);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }

    // delete current series without dropping the series table and
    // sequence. take care not to free the template.
    void (*pFn)(const void *);
    pFn = (drms_env->series_cache).deep_free; /* temp var to hold fn. */
    (drms_env->series_cache).deep_free = NULL;
    if (drms_delete_series(drms_env, series, 0)) {
      fprintf(stderr, "Failed to remove previous definitions\n");
      status = 1;
      goto bailout;
    }
    // restore deep_free function
    (drms_env->series_cache).deep_free = pFn;

    // retain the spot in the series_cache. this is a hack to undo
    // hcon_remove()
    hcon_allocslot_lower(&drms_env->series_cache, series);
    //    drms_print_record(template);

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
    snprintf(stmt, bufsz, "insert into %s (%s) select %s from %s_tmp", series, field_list, field_list, series);
    if(drms_dms(drms_env->session, NULL, stmt)) {
      fprintf(stderr, "Failed: %s\n", stmt);
      status = 1;
      goto bailout;
    }
    free(field_list);

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
    
