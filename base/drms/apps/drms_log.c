/**
\defgroup drms_log drms_log - for each record, print the log's storage-unit path
@ingroup drms_util

Query session log SU and JSOC version for given dataset names.

\par Synopsis:

\code
drms_log dsname1 [dsname2 ...] [-pP]
\endcode

\par Flags:
\c -p: print the full log storage-unit path for each record
\c -P: print the full log storage-unit path for each record but not retrieve

\par Driver flags: 
\ref jsoc_main

\sa
create_series delete_series describe_series modify_series show_info

@{
*/
#include "drms.h"
#include "jsoc_main.h"

ModuleArgs_t module_args[] = { 
  {ARG_FLAG, "p", "0", "list the record\'s log storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s log storage_unit path but no retrieve"},
  {ARG_END}
};

char *module_name = "drms_log";
/** @}*/

// to improve: cache sums results based on sessionns and sessionid
int DoIt(void) {
  int status = 0;
  int i = 1;
  char *rsname;
  char query[DRMS_MAXQUERYLEN];
  int want_path = cmdparams_get_int (&cmdparams, "P", NULL) != 0;
  int retrieve = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  while ((rsname = cmdparams_getarg (&cmdparams,i++))) {
    DRMS_RecordSet_t *rs = drms_open_records (drms_env, rsname, &status);    
    if (!status) {
      for (int j = 0; j < rs->n; j++) {
	DRMS_Record_t *rec = rs->records[j];
	printf("recnum = %lld:\n", rec->recnum);
	sprintf(query, "select jsoc_version, starttime, sunum from %s.drms_session where sessionid=%lld", rec->sessionns, rec->sessionid);
	DB_Text_Result_t *qres;
	if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0) {
	  printf("jsoc version: ");
	  if (qres->field[0][0][0]) {
	    printf("%s\n", qres->field[0][0]);
	  } else {
	    printf("Undefined\n");
	  }
	  printf("session starttime: ");
	  if (qres->field[0][1][0]) {
	    printf("%s\n", qres->field[0][1]);
	  } else {
	    printf("Unavailable\n");
	  }
	  if (qres->field[0][2][0] == '\0') {
	    printf("No log avaliable\n");
	  } else {
	    if (want_path || retrieve) {
	      DRMS_StorageUnit_t *su;
	      XASSERT(su = malloc(sizeof(DRMS_StorageUnit_t)));
	      su->sunum = atoll(qres->field[0][2]);
	      drms_env->retention = DRMS_LOG_RETENTION;
	      status = drms_su_getsudir(drms_env, su, retrieve);
	      if (!status) {
		printf("Log SU=%s\n", su->sudir);
	      }
	      free(su);
	    }
	}
	db_free_text_result(qres);
	}
      }
    }
  }
  return status;
}

