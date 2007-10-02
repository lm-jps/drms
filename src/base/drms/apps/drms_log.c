#include "drms.h"
#include "jsoc_main.h"

ModuleArgs_t module_args[] = { 
  {ARG_END}
};

char *module_name = "drms_log";

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
	sprintf(query, "select online_loc, online_status, jsoc_version from sum_main a, %s.drms_session b where a.ds_index = b.sunum and b.sessionid=%lld", rec->sessionns, rec->sessionid);
	DB_Text_Result_t *qres;
	if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0) {
	  if (qres->field[0][1][0] == 'Y') {
	    printf("Log SU=%s\n",qres->field[0][0]);
	  } else {
	    printf("Log exists, but has gone off-line. This should not have happened.\n");
	  }
	} else {
	  printf("No log avaliable\n");
	}
	printf("jsoc version: ");
	if (qres->field[0][2][0]) {
	  printf("%s\n", qres->field[0][2]);
	} else {
	  printf("Undefined\n");
	}
	db_free_text_result(qres);
      }
    }
  }
  return status;
}

