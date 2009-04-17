/**
\defgroup drms_log drms_log - for each record, print the log's storage-unit path
@ingroup drms_util

\brief Query session log SU and JSOC version for given dataset names.

\par Synopsis:

\code
drms_log dsname1 [dsname2 ...] [-pP]
\endcode

\par Flags:
\c -p: print the full log storage-unit path for each record
<br>
\c -P: print the full log storage-unit path for each record but not retrieve
<br>

\par Driver flags: 
\ref jsoc_main

\sa
create_series delete_series describe_series modify_series show_info

*/
#include "drms.h"
#include "jsoc_main.h"

ModuleArgs_t module_args[] = { 
  {ARG_FLAG, "p", "0", "list the record\'s log storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s log storage_unit path but no retrieve"},
  {ARG_FLAG, "t", "0", "list the infomation as a table"},
  {ARG_FLAG, "q", "0", "list the record query"},
  {ARG_END}
};

char *module_name = "drms_log";

// to improve: cache sums results based on sessionns and sessionid
int DoIt(void)
  {
  int status = 0;
  int i = 1;
  char *rsname;
  char query[DRMS_MAXQUERYLEN];
  int want_path = cmdparams_get_int (&cmdparams, "P", NULL) != 0;
  int retrieve = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  int as_table = cmdparams_get_int (&cmdparams, "t", NULL) != 0;
  int as_query = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  if (as_table)
    {
    if (as_query) printf("RecordQuery\t");
    printf("recnum\tjsoc_version\thost\tuser\tstarttime\tendtime\tsessionid\tLogSU");
    if (want_path || retrieve)
      printf("\tLogPath");
    printf("\n");
    }
  while ((rsname = cmdparams_getarg (&cmdparams,i++)))
    {
    DRMS_RecordSet_t *rs = drms_open_records (drms_env, rsname, &status);    
    if (!status)
      {
      for (int j = 0; j < rs->n; j++)
        {
	DRMS_Record_t *rec = rs->records[j];

        if (as_query)
          {
	  if (!as_table)
	    printf("query: ");
	  drms_print_rec_query(rec);
          printf((as_table ? "\t" : "\n"));
          }

	if (!as_table)
	  printf("recnum: ");
	printf("%lld", rec->recnum);
        printf((as_table ? "\t" : "\n"));

	sprintf(query, "select jsoc_version, hostname, username, starttime, endtime, sessionid, sunum from %s.drms_session where sessionid=%lld", rec->sessionns, rec->sessionid);
	DB_Text_Result_t *qres;
	if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0)
	  {
	  if (!as_table)
	    printf("jsoc version: ");
	  if (qres->field[0][0][0])
	    printf("%s", qres->field[0][0]);
	  else
	    printf("Undefined");
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("host: ");
	  if (qres->field[0][1][0])
	    printf("%s", qres->field[0][1]);
	  else
	    printf("Undefined");
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("user: ");
	  if (qres->field[0][2][0])
	    printf("%s", qres->field[0][2]);
	  else
	    printf("Undefined");
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("session starttime: ");
	  if (qres->field[0][3][0])
	    printf("%s", qres->field[0][3]);
	  else
	    printf("Unavailable");
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("session endtime: ");
	  if (qres->field[0][4][0])
	    printf("%s", qres->field[0][4]);
	  else
	    printf("Unavailable");
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("sessionid: ");
	  if (qres->field[0][5][0])
	    printf("%s", qres->field[0][5]);
	  else
	    printf("%lld", rec->sessionid);
          printf((as_table ? "\t" : "\n"));

	  if (!as_table)
	    printf("logSU: ");
	  if (qres->field[0][6][0])
	    {
	    printf("%s", qres->field[0][6]);
	    if (want_path || retrieve)
	      {
	      DRMS_StorageUnit_t *su;
              printf((as_table ? "\t" : "\n"));
	      if (!as_table)
	        printf("logPath: ");
	      XASSERT(su = malloc(sizeof(DRMS_StorageUnit_t)));
	      su->sunum = atoll(qres->field[0][2]);
	      drms_env->retention = DRMS_LOG_RETENTION;
	      status = drms_su_getsudir(drms_env, su, retrieve);
	      if (!status) 
		printf("%s", su->sudir);
              else
	        printf("No log avaliable");
	      free(su);
	      }
            }
          else
	    printf("No log sunum avaliable");
          printf((as_table ? "" : "\n"));
	  db_free_text_result(qres);
	  }
        else
          printf("session query failed");
        printf("\n");
        }
      }
    else
      printf("query failed: %s\n", rsname);
    }
  return status;
  }

