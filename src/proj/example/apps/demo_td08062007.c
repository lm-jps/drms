/* demo_td08062007.c */

/*  The purpose of this module is to demonstrate how JSOC can input data cubes (like
 *  those output by fastrack) and transform it into a dataseries that can be used
 *  to generate power spectra.
 *
 *  The demonstration was held on August 6-9, 2007 at Stanford University.
 *
 *  Assumptions:
 *    1. One segment per record.
 *
 *
 *  Caveats:
 *    1. Run "limit stacksize unlimited" or else you'll likely get a crash.
 *
 *
 *  --Art Amezcua
 */

#include <complex.h>
#include <sys/time.h>
#include "jsoc_main.h"
#include "drms_types.h"
#include "fftw3.h"

char *module_name = "demo_td08062007";

#define kRecSetIn      "recsin"
#define kDSOut         "dsout"
#define kOutSeries     "su_arta.TestDemoTD"

ModuleArgs_t module_args[] =
{
     {ARG_STRING, kRecSetIn, "",          "Input data series."},
     {ARG_STRING, kDSOut,    kOutSeries,  "Output data series."},
     {ARG_END}
};

typedef enum
{
   kDemoError_Success,
   kDemoError_CouldntCheckSeries,
   kDemoError_CouldntCreateSeries,
   kDemoError_InputIncompatibleWithSeries,
   kDemoError_CouldntCreateRecord,
   kDemoError_SegmentNotFound,
   kDemoError_UnsupportedDimensionality,
   kDemoError_UnsupportedDataType,
   kDemoError_BadParameter,
   kDemoError_CouldntCreateArray
} DemoError_t;

/* CreateOutSeries
 *
 *   Using an input record as a prototype, create an output series.  Modifies
 *   segment dimensionality.
 *
 *   env - DRMS session object.
 *   rec - Record representing input data.
 */
static int CreateOutSeries(DRMS_Env_t *env, DRMS_Record_t *rec)
{
   int error = kDemoError_CouldntCreateSeries;
   int status = DRMS_SUCCESS;

   DRMS_Record_t *sourceRec = drms_template_record(env, rec->seriesinfo->seriesname, &status);
   DRMS_Segment_t *segproto = NULL;

   if (sourceRec)
   {
      DRMS_Record_t *prototype = drms_create_recproto(sourceRec, &status);

      if (prototype)
      {
	 DRMS_SegmentDimInfo_t di;
	 
	 if (hcon_size(&(prototype->segments)) != 1)
	 {
	    XASSERT(0);
	    fprintf(stderr, 
		    "Warning: more than one segment in input series; using first segment.\n");
	 }

	 if ((segproto = drms_segment_lookupnum(prototype, 0)) != NULL)
	 {
	    if (segproto->info->naxis != 3)
	    {
	       error = kDemoError_UnsupportedDimensionality;
	    }
	    else if (segproto->info->type != DRMS_TYPE_FLOAT && 
		     segproto->info->type != DRMS_TYPE_DOUBLE)
	    {
	       error = kDemoError_UnsupportedDataType;
	    }
	    else 
	    {
	       if (segproto->info->protocol == DRMS_DSDS)
	       {
		  /* Input is a DSDS series. */
		  segproto->info->protocol = DRMS_FITS;
	       }
	       else
	       {
		  /* Input is a true DRMS series. */
	       }

	       drms_segment_getdims(segproto, &di);
	       di.naxis = 2;
	       di.axis[0] = (segproto->axis)[1] / 2;
	       di.axis[1] = (segproto->axis)[2] / 2;
	       drms_segment_setdims(segproto, &di);
	    
	       status = drms_create_series_fromprototype(&prototype, kOutSeries, 0);
	       if (status == DRMS_SUCCESS)
	       { 
		  error = kDemoError_Success;
	       }
	    }
	 }
	 else
	 {
	    drms_destroy_recproto(&prototype);
	 }
      }
   }

   return error;
}

/* CheckCompat
 *
 *   Indicate if a record of input data is compatible with the output series.  Returns 1 
 *   if compatible, 0 otherwise.
 *	 
 *   env   - DRMS session object.
 *   rec   - Record representing data.
 *   dsout - Output series name.
 */
static int CheckCompat(DRMS_Env_t *env, DRMS_Record_t *rec, const char *dsout)
{
   int compat = 0;
   int status = DRMS_SUCCESS;

   DRMS_Record_t *prototype =  drms_create_recproto(rec, &status);
   DRMS_Segment_t *segproto = NULL;
   HContainer_t *matchSegNames = NULL;
   
   if (prototype)
   {
      if ((segproto = drms_segment_lookupnum(prototype, 0)) != NULL)
      {
	 DRMS_SegmentDimInfo_t di;

	 if (segproto->info->protocol == DRMS_DSDS)
	 {
	    /* Input is a DSDS series. */
	    segproto->info->protocol = DRMS_FITS;
	 }
	 else
	 {
	    /* Input is a true DRMS series. */
	 }

	 /* Data saved has a different format than data input. Modify prototype 
	  * to reflect this. */
	 drms_segment_getdims(segproto, &di);
	 di.naxis = 2;
	 di.axis[0] = (segproto->axis)[1] / 2;
	 di.axis[1] = (segproto->axis)[2] / 2;
	 drms_segment_setdims(segproto, &di);

	 XASSERT((matchSegNames = (HContainer_t *)malloc(sizeof(HContainer_t))) != NULL);
	 compat = drms_series_checkrecordcompat(env,
						dsout, 
						prototype, 
						matchSegNames, 
						&status);
	 
	 hcon_destroy(&matchSegNames);
	 drms_destroy_recproto(&prototype);

	 if (!compat)
	 {
	    fprintf(stderr, 
		    "Output series %s is not compatible with output data.\n", 
		    dsout);
	 }
      }     
   }

   return compat;
}

/* Crunch
 *
 *   Manipluate the 3D input data to produce a 2D power diagram as output.
 *  
 *   arrin  - 3D array of input data.
 *   arrout - 2D array of output data.
 */
DemoError_t Crunch(DRMS_Array_t *arrin, DRMS_Array_t **arrout)
{
   DemoError_t error = kDemoError_Success;
   int status = DRMS_SUCCESS;

   if (arrin && arrout)
   {
      long long arrsize = drms_array_count(arrin);
      DRMS_Type_t type = arrin->type;
      int laxis1 = drms_array_nth_axis(arrin, 0);
      int laxis2 = drms_array_nth_axis(arrin, 1);
      int laxis3 = drms_array_nth_axis(arrin, 2);
      int axisout[2] = {laxis2 / 2, laxis3 / 2};
      long long nElem = drms_array_count(arrin);

      *arrout = drms_array_create(type,
				  2,
				  axisout,
				  NULL,
				  &status);

      if (*arrout)
      {
	 fftwf_plan p;
	 fftwf_complex datc[laxis3][laxis1][laxis1 / 2 + 1];

	 /* Iterate through data values.  Works on double or float. */
	 long long iData;

	 if (type == DRMS_TYPE_FLOAT)
	 {
	    float *pDataIn = arrin->data;

	    for (iData = 0; iData < nElem; iData++)
	    {
	       pDataIn[iData] = pDataIn[iData] / (float)arrsize;
	    }
	 }
	 else if (type == DRMS_TYPE_DOUBLE)
	 {
	    double *pDataIn = arrin->data;

	    for (iData = 0; iData < nElem; iData++)
	    {
	       pDataIn[iData] = pDataIn[iData] / (double)arrsize;
	    }	   
	 }

	 p = fftwf_plan_dft_r2c_3d(laxis3,
				   laxis2,
				   laxis1,
				   arrin->data,
				   &datc[0][0][0],
				   FFTW_ESTIMATE);

	 fftwf_execute(p);
	 fftwf_destroy_plan(p);

	 int i;
	 int j;
	 int k;
	 int ii;

	 int iLim = laxis3 / 2;
	 int jLim = laxis2 / 2;
	 int kLim = laxis1 / 2;

	 memset((*arrout)->data, 0, drms_array_size(*arrout));

	 if (type == DRMS_TYPE_FLOAT)
	 {
	    float *power = (*arrout)->data;

	    for (j = 0; j < jLim; j++)
	    {
	       for (k = 0; k < kLim; k++)
	       {
		  ii = (int)(sqrtf(powf((float)j, 2.0) + powf((float)k, 2.0)));
			
		  if (ii < jLim)
		  {	    
		     for (i = 0; i < iLim; i++)
		     {
			power[i * axisout[0] + ii] +=
			  powf(cabsf(datc[i][j][k]), 2.0);
		     }
		  }
	       }
	    }
	 }
	 else if(type == DRMS_TYPE_DOUBLE)
	 {
	    double *power = (*arrout)->data;

	    for (j = 0; j < jLim; j++)
	    {
	       for (k = 0; k < kLim; k++)
	       {
		  ii = (int)(sqrtf(powf((float)j, 2.0) + powf((float)k, 2.0)));
			
		  if (ii < jLim)
		  {	    
		     for (i = 0; i < iLim; i++)
		     {
			power[i * axisout[0] + ii] +=
			  pow(cabs(datc[i][j][k]), 2.0);
		     }
		  }
	       }
	    }
	 }	   
      } /* arrout */
      else
      {
	 error = kDemoError_CouldntCreateArray;
      }
   }
   else
   {
      error = kDemoError_BadParameter;
   }

   return error;
}

/* LogTime
 *
 *   Print out to stdout a message showing elapsed time since the last time LogTime
 *   was called with the clear flag set.
 *
 *   clear - Flag to establish baseline.  Call with clear == 1 to reset timer.
 *     Then call with clear == 0 to obtain elapsed time since first call.
 *   msg   - Message to print in output.
 */
void LogTime(int clear, const char *msg)
{
   static double stTime = 0.0;
   struct timeval tv;
   double newTime = 0.0;
   double elapsed = 0.0;

   gettimeofday(&tv, NULL);
   newTime = (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;

   if (clear)
   {
      stTime = newTime;
      elapsed = 0.0;
   }
   else
   {
      elapsed = newTime - stTime;
      fprintf(stdout, 
	      "%s: done (%.3g seconds elapsed)\n", 
	      msg,
	      elapsed);
      fflush(stdout);
   }
}

/* DoIt
 *
 *   Module entry point.
 */
int DoIt(void) 
{
   int status = DRMS_SUCCESS;
   DemoError_t error = kDemoError_Success;
   char *inRecQuery = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
   char *dsout = cmdparams_get_str(&cmdparams, kDSOut, NULL);
   DRMS_RecordSet_t *inRecSet = NULL;
   int isDSDS;
   char *pQ = (*inRecQuery == '{') ? inRecQuery + 1 : inRecQuery;

   isDSDS = DSDS_IsDSDSSpec(pQ);

   if (isDSDS)
   {
      fprintf(stdout, "Fetching records from DSDS");
      fflush(stdout);
      LogTime(1, NULL);
   }

   inRecSet = drms_open_records(drms_env, inRecQuery, &status);

   if (isDSDS)
   {
      LogTime(0, "");
   }

   if (status == DRMS_SUCCESS)
   {
      int nRecs = inRecSet->n;
      int iRec;

      /* create output series rec prototype */
      DRMS_Record_t *recout = NULL;
      DRMS_Segment_t *segout = NULL;
      DRMS_Segment_t *segin = NULL;

      for (iRec = 0; !error && iRec < nRecs; iRec++)
      {
	 DRMS_Record_t *rec = inRecSet->records[iRec];
	 if (rec)
	 {
	    if (iRec == 0)
	    {
	       fprintf(stdout, "Record %d\n", iRec);
	       fflush(stdout);

	       /* Create output series if necessary. */
	       drms_series_exists(drms_env, dsout, &status);
	       if (status == DRMS_ERROR_UNKNOWNSERIES)
	       {
		  if (CreateOutSeries(drms_env, rec))
		  {
		     error = kDemoError_CouldntCreateSeries;
		  }
	       }
	       else if (status != DRMS_SUCCESS)
	       {
		  error = kDemoError_CouldntCheckSeries;
	       }
	       else
	       {
		  /* Check for compatibility of each record with existing series. */
		  if (!CheckCompat(drms_env, rec, dsout))
		  {
		     error = kDemoError_InputIncompatibleWithSeries;
		  }
	       }
	    }
	    else
	    {
	       /* Check for compatibility of each record with existing series. */
	       if (!CheckCompat(drms_env, rec, dsout))
	       {
		  error = kDemoError_InputIncompatibleWithSeries;
	       }
	    }

	    if (!error)
	    {
	       segin = drms_segment_lookupnum(rec, 0);
	       if (segin)
	       {
		  DRMS_Array_t *arrin = NULL;
		  DRMS_Array_t *arrout = NULL;

		  fprintf(stdout, "  reading data");
		  fflush(stdout);
		  LogTime(1, NULL);

		  arrin = drms_segment_read(segin, segin->info->type, &status);

		  LogTime(0, "");

		  if (arrin)
		  {
		     int naxis = drms_array_naxis(arrin);
		     DRMS_Type_t type = arrin->type;

		     if (naxis != 3)
		     {
			error = kDemoError_UnsupportedDimensionality;
		     }
		     else if (type != DRMS_TYPE_FLOAT && type != DRMS_TYPE_DOUBLE)
		     {
			error = kDemoError_UnsupportedDataType;
		     }
		   
		     if (error == kDemoError_Success)
		     {
			DRMS_RecordSet_t *rs = NULL;

			fprintf(stdout, "  processing data");
			fflush(stdout);
			LogTime(1, NULL);

			error = Crunch(arrin, &arrout);

			LogTime(0, "");

			if (error == kDemoError_Success)
			{
			   rs = drms_create_records(drms_env, 
						    1, 
						    dsout, 
						    DRMS_PERMANENT, 
						    &status);
			   
			   if (status != DRMS_SUCCESS)
			   {
			      error = kDemoError_CouldntCreateRecord;
			   }
			}

			if (!error)
			{
			   recout = rs->records[0];
			}

			if (recout)
			{
			   DRMS_Keyword_t *key = NULL;
			   segout = drms_segment_lookupnum(recout, 0);
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

			      drms_segment_write(segout, arrout, 0);
			   }

			   drms_close_records(rs, DRMS_INSERT_RECORD);
			}

			drms_free_array(arrin);
		     }
		  } /* arrin */
	       }
	       else
	       {
		  error = kDemoError_SegmentNotFound;
	       }
	    } /* !error */
	 } /* rec */
      } /* iRec */

      if (inRecSet)
      {
	 drms_close_records(inRecSet, DRMS_FREE_RECORD);
      }
   }

   return error;
}
