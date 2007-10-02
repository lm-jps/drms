/*
 *  regrid.c
 *
 *  Regrid a (two_dimensional) data set
 *
 *  Responsible:  Rick Bogart			RBogart@solar.Stanford.EDU
 *
 *  Usage:
 *    regrid in=  out= [cols= rows= ]
 *
 *  Arguments:  (type   default         description)
 *      in	Data_In   -	input data series
 *      out	Data_Out  -	output data series
 *	rows	int	orig	output columns
 *	cols	int	orig	output columsn
 *      e       flag            embed rectangular data in enclosing square.
 *
 *  Bugs:
 *    Output datasets are mal-conforming
 *    Flag argument for choice of interpolation scheme should be changed
 *	to ARG_NUME
 *    Scaling and location information is not corrected except for MDI data
 *	For MDI data, only keywords used in solar_image_params() are corrected
 *      Solar radius correction is average for x and y magnifications
 *      No correction for ellipticity parameters
 *    If embedding in a square is selected, the data are converted to
 *	floats (possibly scaled) rather than kept in native format
 *
 *  Revision history is at the end of the file.
 *
 */

#include "jsoc_main.h"
#include "astro.h"

char *module_name = "Image Regrid";

#define kRecSetIn      "recsin"
#define kSeriesOut     "dsout"
#define kCols          "cols"
#define kRows          "rows"
#define kNotSpecified  "NotSpecified"
#define kXSCALE        "XSCALE"
#define kYSCALE        "YSCALE"
#define kX0            "XO"
#define kY0            "YO"
#define kR_SUN         "R_SUN"
#define kMAGNIFY       "MAGNIFY"
#define kIM_SCALE      "IM_SCALE"
#define kQUALITY       "QUALITY"


ModuleArgs_t module_args[] =
{
     {ARG_STRING, kRecSetIn, "", "Input data series."},
     {ARG_STRING, kSeriesOut, "", "Output data series."},
     {ARG_INT, kCols, "0", "Number of output columns (default to original)"},
     {ARG_INT, kRows, "0", "Number of output rows (default to original)"},
     {ARG_NUME, "3", "0", "Cubic-convolution interpolation (default to bilinear).", "bilinear, cubic"},
     {ARG_FLAG, "e", "0", "Embed rectangular data in enclosing square.", "-1:1"},
     /* Since drms doesn't have sets of record sets, merging doesn't make sense.
      * Each segment of each in-series record should go into a corresponding 
      * segment in a single out-series record */
     /* {ARG_FLAG, "m", "0", "Merge output to single data set.", "-1:1"}, */
     {ARG_END}
};

/* In MDI, update_scale_loc() was run once per each fits file.  However, 
 * in DRMS, ALL fits files in a record must share one set of keywords. 
 * So, run this function once per record. */
/* XXX make the keys modified by update_scale_loc() all per-segment keys! */
int update_scale_loc(DRMS_Record_t *rec, double xmag, double ymag) 
{
  /* will complete even if an error occurs. */
  double dval;
  double new;
  int platform, instrument;
  int status = 0;
  int error = 0;

  if (xmag == 1.0 && ymag == 1.0) return 0;

  dval = drms_getkey_double(rec, kXSCALE, &status);
  if (status == DRMS_SUCCESS)
  {
     new = dval / xmag;
     error = (status = drms_setkey_double(rec, kXSCALE, new)) == DRMS_SUCCESS;
  }

  dval = drms_getkey_double(rec, kYSCALE, &status);
  if (status == DRMS_SUCCESS)
  {
     new = dval / ymag;
     status = drms_setkey_double(rec, kYSCALE, new);
     if (!error)
     {
	error = (status == DRMS_SUCCESS);
     }
  }

  dval = drms_getkey_double(rec, kX0, &status);
  if (status == DRMS_SUCCESS)
  {
     new = dval * xmag;
     status = drms_setkey_double(rec, kX0, new);
     
     if (!error)
     {
	error = (status == DRMS_SUCCESS);
     }
  }
  
  dval = drms_getkey_double(rec, kY0, &status);
  if (status == DRMS_SUCCESS)
  {
     new = dval * ymag;
     status = drms_setkey_double(rec, kY0, new);
     
     if (!error)
     {
	error = (status == DRMS_SUCCESS);
     }
  }
  
  if (xmag == ymag) 
  {
     dval = drms_getkey_double(rec, kR_SUN, &status);
     if (status == DRMS_SUCCESS)
     {
	new = dval * xmag;
	status = drms_setkey_double(rec, kR_SUN, new);
	
	if (!error)
	{
	   error = (status == DRMS_SUCCESS);
	}
     }
     
     new = xmag;
     
     dval = drms_getkey_double(rec, kMAGNIFY, &status);
     if (status == DRMS_SUCCESS)
     {
	new *= dval;
	status = drms_setkey_double(rec, kMAGNIFY, new);
	
	if (!error)
	{
	   error = (status == DRMS_SUCCESS);
	}
     }
     
     dval = drms_getkey_double(rec, kIM_SCALE, &status);
     if (status == DRMS_SUCCESS)
     {
	new = dval / xmag;
	status = drms_setkey_double(rec, kIM_SCALE, new);
	
	if (!error)
	{
	   error = (status == DRMS_SUCCESS);
	}
     }      
  }
  
  return error;
}

static int ValidateSeries(DRMS_Env_t *drmsEnv, 
			  const char *inSeriesName, 
			  const char *seriesOutParam,
			  char *outSeriesName, 
			  int size,
			  int cols,
			  int rows,
			  HContainer_t **matchSegNames)
{
   int status = DRMS_SUCCESS;

   if (strcmp(seriesOutParam, kNotSpecified) == 0)
   {
      /* Output series not specified, this won't work since the whole point of
       * regrid is to output data in a format different from data format of 
       * the input series (can't write output to input series). */
      
      /* Actually with cmdparams trapping required params, shouldn't get here. */
      fprintf(stderr, "Must specify an output series.\n");
      status = DRMS_ERROR_INVALIDDATA;
   }
   else if (drms_series_exists(drmsEnv, inSeriesName, &status))
   {
      char **segNames = NULL;
      HContainer_t *segProtoI = NULL;
      int idx = 0;
      int nNames;

      /* Create a prototype output record. */
      DRMS_Record_t *sourceRec = drms_template_record(drms_env, inSeriesName, &status);
      DRMS_Record_t *prototype = drms_create_recproto(sourceRec, &status);
      
      if (status == DRMS_SUCCESS)
      {
	 /* Set things like unit size, archive, retention, tape group, description. */
	 int bArchive = 0;
	 int nDaysRetention = 7; /* XXX - parameterize this */
	 char *description = "regrid module output series";
	 drms_recproto_setseriesinfo(prototype, 
				     NULL, 
				     &bArchive, 
				     &nDaysRetention, 
				     NULL, 
				     description);
	 
	 /* Set nAxis and dims. */
	 DRMS_SegmentDimInfo_t dims;
	 dims.naxis = 2;
	 dims.axis[0] = cols;
	 dims.axis[1] = rows;
	 
	 /* Get input seg names. */
	 segProtoI = drms_segment_createinfocon(drmsEnv, inSeriesName, &status);
	 nNames = hcon_size(segProtoI);
	 segNames = (char **)malloc(sizeof(char *) * nNames);
	 
	 /* Set output segment dimension info. */
	 for (; status == DRMS_SUCCESS && idx < nNames; idx++)
	 {
	    DRMS_SegmentInfo_t *aSegInfo = hcon_lookupindex(segProtoI, idx);
	    segNames[idx] = (char *)malloc(sizeof(char) * DRMS_MAXNAMELEN);
	    strncpy(segNames[idx], aSegInfo->name, DRMS_MAXNAMELEN);
	    segNames[idx][DRMS_MAXNAMELEN - 1] = '\0';

	    DRMS_Segment_t *segproto = drms_segment_lookup(prototype, segNames[idx]);
	    status = drms_segment_setdims(segproto, &dims);
	 }

	 snprintf(outSeriesName, size, seriesOutParam);
	 if (drms_series_exists(drms_env, outSeriesName, &status))
	 {
	    /* Ensure out series is compatible with in series. */
	    /* Ensure the prime keywords and at least one segment match, 
	     * and return a list of matching segments */
	    XASSERT((*matchSegNames = (HContainer_t *)malloc(sizeof(HContainer_t))) != NULL);
	    int nMatch = 0;
	    int compat =  drms_series_checkrecordcompat(drms_env,
							outSeriesName, 
							prototype, 
							*matchSegNames, 
							&status);
	    
	    if (compat != 1)
	    {
	       fprintf(stderr, "Output series %s is not compatible with output data.\n", outSeriesName);
	       status = DRMS_ERROR_INVALIDDATA;
	    }
	 }
	 else
	 {
	    /* Must create the series */
	    status = drms_create_series_fromprototype(&prototype, outSeriesName, 0);
	    if (status != DRMS_SUCCESS)
	    {
	       fprintf(stderr, "Couldn't create new series %s.\n", outSeriesName);
	    }
	    else
	    {
	       /* Must populate matchSegNames container. */
	       *matchSegNames = hcon_create(DRMS_MAXNAMELEN, 
					    DRMS_MAXNAMELEN, 
					    NULL, 
					    NULL,
					    (void **)segNames,
					    segNames,
					    nNames);
	    }
	 }
      }

      if (segNames)
      {
	 for (idx = 0; idx < nNames; idx++)
	 {
	    free(segNames[idx]);
	 }
	 
	 free(segNames);
      }
   }
   else
   {
      fprintf(stderr, "Specified input series %s does not exist.\n", inSeriesName);
   }
   
   return status;
}

static int DoMain(DRMS_Array_t **dataArr, 
		  int naxis, 
		  int *dims, 
		  int *length, 
		  int embed, 
		  LIBASTRO_Interpolation_t scheme,
		  int *status)
{
   int error = 0;
   *status = DRMS_SUCCESS;

   if (embed)
   {
      float *dat = (float *)((*dataArr)->data);
      float fill = F_NAN;
      int oc = dims[0];
      int or = dims[1];
      int c, r, liml;
      
      length[0] = length[1] = (oc > or) ? oc : or;
      
      //out = sds_construct (2, length, SDS_FLOAT, &fill);
      //sds_append_attrs (out, in);
      
      float *rdat = (float *)malloc(drms_array_size(*dataArr));
      
      if (oc > or) {
	 liml = (oc - or) / 2;
	 for (r = 0; r < or; r++)
	   for (c = 0; c < oc; c++)
	     rdat[c + oc * (r + liml)] = dat[c + oc * r];
      } 
      else 
      {
	 liml = (or - oc) / 2;
	 for (r = 0; r < or; r++)
	   for (c = 0; c < oc; c++)
	     rdat[c + liml + oc * r] = dat[c + oc * r];
      }
      
      drms_free_array(*dataArr);
      *dataArr = drms_array_create(DRMS_TYPE_FLOAT,
				  naxis, 
				  dims, 
				  rdat,
				  status);
   }

   if (*status == DRMS_SUCCESS)
   {
      /* regrids in place */
      error = (Regrid(dataArr, length, scheme) != kLIBASTRO_Success); 
   }
   
   return error;
}

int DoIt(void) 
{
  int error = 0;
  int status = 0;

  double xmag, ymag;
  int length[3];

  char *inRecQuery = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
  char *seriesOut = cmdparams_get_str(&cmdparams, kSeriesOut, NULL);
  int cols = cmdparams_get_int(&cmdparams, kCols, NULL);
  int rows = cmdparams_get_int(&cmdparams, kRows, NULL);
  LIBASTRO_Interpolation_t scheme = (LIBASTRO_Interpolation_t)(cmdparams_get_int(&cmdparams, 
										 "3", 
										 NULL));
  int embed = cmdparams_get_int(&cmdparams, "e", NULL);

  char inSeriesName[DRMS_MAXNAMELEN];
  char outSeriesName[DRMS_MAXNAMELEN];
  HContainer_t *matchSegNames = NULL; /* container of names of matching segments */

  /* No set of record sets in drms, so just loop over records in record set */
  /* Open the inSeries. */
  snprintf(inSeriesName, sizeof(inSeriesName), inRecQuery);
  char *pChar = strchr(inSeriesName, '[');
  if (pChar != NULL)
  {
     *pChar = '\0';
  }

  /* Validate input and output series, create output series if necessary, 
   * ensure output series is compatible with data to be writter. */
  status = ValidateSeries(drms_env, 
			  inSeriesName, 
			  seriesOut, 
			  outSeriesName, 
			  sizeof(outSeriesName),
			  cols, 
			  rows, 
			  &matchSegNames);
  error = (status != DRMS_SUCCESS);
  
  if (!error)
  {
     int nInRecs = 0;
     DRMS_RecordSet_t *inRecSet = drms_open_records(drms_env, inRecQuery, &status);
     error = (status != DRMS_SUCCESS);
     
     if (!error)
     {
	nInRecs = inRecSet->n;
     }

     int quality;
     
     int iRec = 0;
     DRMS_Record_t *inRec = NULL;
     DRMS_Record_t *outRec = NULL;

     /* loop over input records */
     for (; !error && iRec < nInRecs; iRec++)
     {
	inRec = inRecSet->records[iRec];
	
	quality = drms_getkey_int(inRec, kQUALITY, &status);
	if (status == DRMS_SUCCESS)
	{
	   if (quality == kALLDATAMISSING)
	   {
	      continue;
	   }
	}

	outRec = drms_create_record(drms_env, outSeriesName, DRMS_PERMANENT, &status);
	error = (status != DRMS_SUCCESS);

	/* MUST FILL IN KEYWORDS BEFORE WRITING THE SEGMENT.  The segment
	 * writing code needs bscale and bzero set, and they won't be unless
	 * the keywords are filled in already. */
	/* Write the record keywords. */
	DRMS_Keyword_t *inKey = NULL;
	DRMS_Keyword_t *outKey = NULL;
	
	HIterator_t *hit = hiter_create(&(inRec->keywords));
	error = (hit == NULL);
	
	while (!error && (inKey = (DRMS_Keyword_t *)hiter_getnext(hit)) != NULL)
	{
	   outKey = drms_keyword_lookup(outRec, inKey->info->name, 1);
	   if (outKey != NULL)
	   {
	      status = drms_setkey(outRec, 
				   outKey->info->name, 
				   outKey->info->type, 
				   &(inKey->value));
	      
	      error = (status != DRMS_SUCCESS);
	   }
	}
	
	if (hit)
	{
	   hiter_destroy(&hit);
	}

	hit = hiter_create(matchSegNames);
	char *oneSegName = NULL;
	DRMS_Array_t *dataArr = NULL;
	int naxis = 0;
	int *dims = NULL;
	DRMS_Segment_t *inSeg = NULL;
	DRMS_Segment_t *outSeg = NULL;

	error = (hit == NULL);

	while (!error && NULL != (oneSegName = (char *)hiter_getnext(hit)))
	{
	   inSeg = drms_segment_lookup(inRec, oneSegName);
	   XASSERT(inSeg != NULL);
	   outSeg = drms_segment_lookup(outRec, oneSegName);
	   XASSERT(outSeg != NULL);

	   if (inSeg != NULL && outSeg != NULL)
	   {
	      naxis = inSeg->info->naxis;
	      dims = inSeg->axis;
	      
	      if (naxis != 2)
	      {
		 continue;
	      }

	      length[0] = (cols) ? cols : dims[0];
	      length[1] = (rows) ? rows : dims[1];
	      
	      int doEmbed = 0;
	      
	      if (embed && dims[0] != dims[1]) 
	      {
		 doEmbed = 1;

		 /* In this case, must convert to float.  This was done in 
		  * the MDI code during vds_select_frec. */
		 dataArr = drms_segment_read(inSeg, DRMS_TYPE_FLOAT, &status);
		 error = (status == DRMS_SUCCESS);
	      } 
	      else 
	      {
		 /* Use the original data format. */
		 dataArr = drms_segment_read(inSeg, DRMS_TYPE_RAW, &status);
		 error = (status != DRMS_SUCCESS);
	      }

	      if (!error)
	      {
		 error = DoMain(&dataArr, naxis, dims, length, doEmbed, scheme, &status);

		 /* Write the out segment. */
		 if (!error)
		 {
		    drms_segment_write(outSeg, dataArr, 0);		   
		 }

		 // sds_set_stdhead (out, strategy_name); /* this seems irrelevant in drms */
		 // sds_calc_stats (out); /* XXX do we need this? */

		 /* XXX this function should be called per-segment, 
		  * as should the keywords therein. 
		  * Need to modify update_scale_loc() to make that happen. */
		 xmag = cols / (double) dims[0];
		 ymag = rows / (double) dims[1];
		 update_scale_loc(outRec, xmag, ymag);
	      }

	      if (dataArr)
	      {
		 drms_free_array(dataArr); /* frees rdat too. */
	      }
	   }
	} /* segment loop */

	if (hit)
	{
	   hiter_destroy(&hit);
	}

	/* Save the out record. */
	if (!error)
	{
	   drms_close_record(outRec, DRMS_INSERT_RECORD);
	}
	else
	{
	   drms_close_record(outRec, DRMS_FREE_RECORD);
	}
     } /* rec loop */
  } /* !error */

  hcon_destroy(&matchSegNames);

  return error;
}
