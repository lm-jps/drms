#include "drms.h"
#include "drms_names.h"
#include "jsoc_main.h"

ModuleArgs_t module_args[] = {
  {ARG_INT, "getdir", "0"},
  {}
};

char *module_name = "drms_query";

int DoIt (void) {
  int i, j, status, getdir;
  DRMS_RecordSet_t *rs;
  DRMS_Record_t *rec;
  char *rsname, dirname[DRMS_MAXPATHLEN];

  getdir = cmdparams_get_int (&cmdparams, "getdir", NULL);
  i = 1;
  while ((rsname = cmdparams_getarg (&cmdparams,i++))) {
    rs = drms_open_records (drms_env, rsname, &status);    
    if (!status) {
      for (j=0; j<rs->n; j++) {
	rec = rs->records[j];
	if (drms_record_numsegments (rec) && getdir) {
	  drms_record_directory (rec, dirname, 1);
	  printf ("%s[:#%lld] sudir=%s\n", rec->seriesinfo->seriesname, 
		 rec->recnum,dirname);
	} else
	  printf ("%s[:#%lld]\n", rec->seriesinfo->seriesname, rec->recnum);
      }
      drms_close_records (rs, DRMS_FREE_RECORD);
    } else
      printf ("drms_open_records failed with error code %d.\n", status);
  }
  return 0;
}
