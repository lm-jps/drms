/* opendsdsrecs.c */

#include "jsoc_main.h"
#include "drms_types.h"

char *module_name = "opendsrecs";

#define kRecSetIn      "recsin"
#define kOutSeries     "su_arta.TestDSDS"

ModuleArgs_t module_args[] =
{
     {ARG_STRING, kRecSetIn, "", "Input data series."},
     {ARG_END}
};

int DoIt(void) 
{
   int status = DRMS_SUCCESS;
   int error = 0;
   char *inRecQuery = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
   DRMS_RecordSet_t *inRecSet = drms_open_records(drms_env, inRecQuery, &status);

   if (status == DRMS_SUCCESS)
   {
      int nRecs = inRecSet->n;
      int iRec;

      /* create output series rec prototype */
      DRMS_Record_t *sourceRec = NULL;
      DRMS_Record_t *recout = NULL;
      DRMS_Record_t *prototype = NULL;
      DRMS_Segment_t *segout = NULL;
      DRMS_Segment_t *segin = NULL;
      DRMS_Segment_t *segproto = NULL;
      HContainer_t *matchSegNames = NULL;

      for (iRec = 0; !error && iRec < nRecs; iRec++)
      {
	 DRMS_Record_t *rec = inRecSet->records[iRec];
	 if (rec)
	 {
	    if (iRec == 0)
	    {
	       if (drms_series_exists(drms_env, kOutSeries, &status))
	       {
		  prototype = drms_create_recproto(rec, &status);
		  segproto = drms_segment_lookup(prototype, kDSDS_Segment);
		  
		  if (segproto && segproto->info->protocol == DRMS_DSDS)
		  {
		     segproto->info->protocol = DRMS_FITS;
		  }

		  XASSERT((matchSegNames = (HContainer_t *)malloc(sizeof(HContainer_t))) != NULL);
		  int compat =  drms_series_checkrecordcompat(drms_env,
							      kOutSeries, 
							      prototype, 
							      matchSegNames, 
							      &status);
		  hcon_destroy(&matchSegNames);
		  drms_destroy_recproto(&prototype);
		  prototype = NULL;
	    
		  if (!compat)
		  {
		     fprintf(stderr, 
			     "Output series %s is not compatible with output data.\n", 
			     kOutSeries);
		     status = DRMS_ERROR_INVALIDDATA;
		  }
	       }
	       else
	       {
		  /* Create a new series to hold the results of a data manipulation */
		  sourceRec = drms_template_record(drms_env, rec->seriesinfo->seriesname, &status);
		  if (sourceRec)
		  {
		     prototype = drms_create_recproto(sourceRec, &status);
		     segproto = drms_segment_lookup(prototype, kDSDS_Segment);

		     if (segproto && segproto->info->protocol == DRMS_DSDS)
		     {
			segproto->info->protocol = DRMS_FITS;
			status = drms_create_series_fromprototype(&prototype, kOutSeries, 0);
			error = (status != DRMS_SUCCESS);
		     }
		     else
		     {
			drms_destroy_recproto(&prototype);
			error = 1;
		     }
		  }
	       }
	    }
	    else
	    {
	       /* Check for compatibility of each record with existing series. */
	       prototype = drms_create_recproto(rec, &status);
	       segproto = drms_segment_lookup(prototype, kDSDS_Segment);
	       
	       if (segproto && segproto->info->protocol == DRMS_DSDS)
	       {
		  segproto->info->protocol = DRMS_FITS;
	       }

	       XASSERT((matchSegNames = (HContainer_t *)malloc(sizeof(HContainer_t))) != NULL);
	       int compat =  drms_series_checkrecordcompat(drms_env,
							   kOutSeries, 
							   prototype, 
							   matchSegNames, 
							   &status);
	       hcon_destroy(&matchSegNames);
	       drms_destroy_recproto(&prototype);
	       prototype = NULL;

	       if (!compat)
	       {
		  fprintf(stderr, 
			  "Output series %s is not compatible with output data.\n", 
			  kOutSeries);
		  status = DRMS_ERROR_INVALIDDATA;
	       }
	     
	       error = (status != DRMS_SUCCESS);
	    }

	    if (!error)
	    {
	       segin = drms_segment_lookup(rec, kDSDS_Segment);
	       if (segin)
	       {
		  DRMS_Array_t *arr = drms_segment_read(segin, segin->info->type, &status);
		  if (arr)
		  {
		     int iData;
		     void *data = arr->data;

		     /* Manipulate data. */
		     DRMS_Type_Value_t intval;
		     DRMS_Value_t val;
		     DRMS_Value_t opval;
		     DRMS_Value_t resval;
		     long long nElem = drms_array_count(arr);

		     /* convert value to target array's type before adding */
		     intval.int_val = 100;
		     drms_convert(arr->type, &(opval.value), DRMS_TYPE_INT, &intval);
		     opval.type = arr->type;

		     /* Iterate through data values. Since we don't know the data type,
		      * use generic DRMS_Type_Value functions.  
		      */
		     for (iData = 0; iData < nElem; iData++)
		     {
			DRMS_ARRAY_GETVAL(val, arr, iData);
			resval = drms_val_add(&val, &opval);
			DRMS_ARRAY_SETVAL(resval, arr, iData);
		     }

		     DRMS_RecordSet_t *rs = drms_create_records(drms_env, 
								1, 
								kOutSeries, 
								DRMS_PERMANENT, 
								&status);

		     error = (status != DRMS_SUCCESS);

		     if (!error)
		     {
			recout = rs->records[0];
		     }

		     if (recout)
		     {
			DRMS_Keyword_t *key = NULL;
			segout = drms_segment_lookup(recout, kDSDS_Segment);
			if (segout)
			{
			   HIterator_t *hit = hiter_create(&(rec->keywords));
			   if (hit)
			   {
			      while ((key = hiter_getnext(hit)) != NULL)
			      {
				 status = drms_setkey(recout, 
						      key->info->name,
						      key->info->type,
						      &(key->value));
			      }

			      hiter_destroy(&hit);
			   }

			   drms_segment_write(segout, arr, 0);
			}

			drms_close_records(rs, DRMS_INSERT_RECORD);
		     }

		     drms_free_array(arr);
		  }
	       }
	       else
	       {
		  error = 1;
	       }
	    }
	 }
      } /* iRec */

      if (inRecSet)
      {
	 drms_close_records(inRecSet, DRMS_FREE_RECORD);
      }
   }

   return error;
}
