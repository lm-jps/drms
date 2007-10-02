#include "drms.h"
#include "drms_names.h"
#include "jsoc_main.h"
						 /* Default parameter values */
ModuleArgs_t module_args[] = {
  {ARG_FLOAT, "alpha", "", "multiplicative scaling"},
  {ARG_FLOAT, "beta", "",  "additive offset: data -> (alpha * data) + beta"},
  {ARG_END}
};
					    /* Module name presented to DRMS */
char *module_name = "scalesegments";
char *version_id = "1.8";

/* This module updates records in recordsets specified on the command line
   by rescaling all array segments and recomputing the array statistics. */

int DoIt () {
  char keyname[10];
  int i, j, status, k, n, count, dostat;
  char *rsname;
  DRMS_RecordSet_t *oldrs, *newrs;
  DRMS_Array_t *array;
  DRMS_Segment_t *oldseg, *newseg;
  HIterator_t newit, oldit;
  double alpha, beta, *data, sum;
  				/* Read scaling parameters from command line */
  alpha = cmdparams_get_double (&cmdparams, "alpha", &status);
  beta = cmdparams_get_double (&cmdparams, "beta", &status);

  i = 1;
  while ((rsname = cmdparams_getarg (&cmdparams, i++))) {
    /* Query DRMS for record set. */
    oldrs = drms_open_records(drms_env, rsname, &status);    

    if (oldrs) {
						    /* Create new record set */
      newrs = drms_create_records (drms_env, oldrs->n,
				   oldrs->records[0]->seriesinfo->seriesname,
				   DRMS_PERMANENT, &status);
      if (status) {
	fprintf (stderr, "Create records failed.\n");
	return status;
      }
						  /* Allocate new record set */
      if (oldrs->n > 0 ) {	
					  /* Loop over records in record set */
	for (j=0; j<oldrs->n; j++) {
					   /* Initialize container iterators */
	  hiter_new(&oldit, &((oldrs->records[j])->segments)); 
	  hiter_new(&newit, &((newrs->records[j])->segments)); 
					     /* Loop over segments in record */
	  while ((oldseg = (DRMS_Segment_t *)hiter_getnext (&oldit)) ) {
	    newseg = (DRMS_Segment_t *)hiter_getnext (&oldit);
					/* Check if we should do image stats */
	    sprintf(keyname, "datamean[%d]", newseg->info->segnum);
	    if (drms_keyword_lookup (oldrs->records[j], keyname, 1)) dostat = 1;
	    else dostat = 0;
				   /* Read old segment, converting to double */
	    array = drms_segment_read (oldseg, DRMS_TYPE_DOUBLE, &status);
	    if (status) {
	      printf("ERROR: drms_segment_read failed with status %d\n", status);
	      return status;
	    }
					     /* Get number of array elements */
	    n = drms_array_count (array);
			       /* Scale array and and compute new statistics */
	    data = array->data;
	    count = 0;
	    sum = 0.0;
	    for (k=0; k<n; k++) {
	      if (data[k] != DRMS_MISSING_DOUBLE) {
		count++;
		data[k] = alpha*data[k] + beta;
		if (dostat) sum += data[k];
	      }
	    }
					   /* Set stat keyword in new record */
	    if (dostat) {
	      sprintf (keyname, "datamean[%d]", newseg->info->segnum);
	      drms_setkey_double (newrs->records[j], keyname, sum/count);
	    }
					/* Write array to new record segment */
	    status = drms_segment_write (newseg, array,0);
	    if (status) {
	      fprintf (stderr,"ERROR: drms_segment_write failed with error "
		      "code %d.\n",status);
	      return status;
	    }
	    drms_free_array(array);
	  }
	}
	drms_close_records (oldrs, DRMS_FREE_RECORD);
	drms_close_records (newrs, DRMS_INSERT_RECORD);
      }
    }
    else printf ("drms_open_records failed with error code %d.\n", status);
  }
  return 0;
}
