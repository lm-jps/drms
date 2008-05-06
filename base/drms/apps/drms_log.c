/**
\defgroup drms_log drms_log

Query session log SU and JSOC version for given dataset names.

\par Synopsis:

\code
drms_log dsname1 [dsname2 ...]
\endcode

\par Driver flags: 
\ref jsoc_main

\sa
create_series delete_series describe_series modify_series show_info

@{
*/
#include "drms.h"
#include "jsoc_main.h"

ModuleArgs_t module_args[] = { 
  {ARG_FLAG, "p", "0", "list the record\'s storage_unit path"},
  {ARG_END}
};

char *module_name = "drms_log";
/** @}*/
int DoIt(void) {
  int status = 0;
  int i = 1;
  char *rsname;
  char query[DRMS_MAXQUERYLEN];
  while ((rsname = cmdparams_getarg (&cmdparams,i++))) {
    DRMS_RecordSet_t *rs = drms_open_records (drms_env, rsname, &status);    
    if (!status) {
      for (int j = 0; j < rs->n; j++) {
	DRMS_Record_t *rec = rs->records[j];
	printf("recnum = %lld: ", rec->recnum);
	sprintf(query, "select online_loc, online_status, jsoc_version, b.sunum from sum_main a, %s.drms_session b where a.ds_index = b.sunum and b.sessionid=%lld", rec->sessionns, rec->sessionid);
	DB_Text_Result_t *qres;
	if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0) {
	  if (qres->field[0][1][0] == 'Y') {
	    printf("Log SU=%s\n",qres->field[0][0]);
	  } else {
	    int want_path = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
	    if (!want_path) {
	      printf("Log exists, but has gone off-line\n");
	    } else {
#ifndef DRMS_CLIENT
		DRMS_StorageUnit_t *su;
		XASSERT(su = malloc(sizeof(DRMS_StorageUnit_t)));

		su->sunum = atoll(qres->field[0][3]);
		drms_env->retention = DRMS_LOG_RETENTION;
		status = drms_su_getsudir(drms_env, su, 1);
		if (!status) {
		  printf("Log SU=%s\n", su->sudir);
		}
		free(su);
#else
	    printf("Must run in direct connection to stage log SU\n");
#endif
	    }
	  }
	  printf("jsoc version: ");
	  if (qres->field[0][2][0]) {
	    printf("%s\n", qres->field[0][2]);
	  } else {
	    printf("Undefined\n");
	  }
	} else {
	  printf("No log avaliable\n");
	}
	db_free_text_result(qres);
      }
    }
  }
  return status;
}

