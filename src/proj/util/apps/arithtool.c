/* arithTool.c */

/* Mapping from vds/sds to drms:
 *   VDS (a set of datasets)     -> Set of record sets (not implemented in DRMS yet)
 *   Dataset (a set of records)  -> Record set
 *   Record (a set of SDSs)      -> Record
 *   SDS (a FITS file)           -> Segment
 *
 * VDS is implemented as an array (indexed by var number) of datasets
 *
 * <recSetIn> - Record set on which to operate
 * <withRecSet> - For binary operations, the second set of records on which to operate.
 *    The data series containing these two record sets must have equivalent primary keyword sets
 *    and overlapping segment sets.  Otherwise, the two sets are too dissimilar to operate on.
 *    The number of records in this set must be either equal to the number of records in 
 *    <recSetIn>, or it must be equal to one record.  If it is the former, then 
 *    there is a one-to-one correspondence between the records in <recSetIn> and 
 *    <withRecSet>.  The keyword-tuple that selects one record in <recSetIn> should
 *    also select one record in <withRecSet>.  Each record in <recSetIn> is paired with 
 *    its corresponding record in <withRecSet>.  The binary operation is then performed 
 *    on each pair of records.  If the number of records in <withRecSet> is equal to one,
 *    then each 
 *    record in <recSetIn> is paired with the single record in <withRecSet>.  Then 
 *    the binary operation is performed on each pair.  The binary operation is
 *    only performed between equivalently named segments.  In other words, if a record
 *    from <recSetIn> contains three segments named SegA, SegB, and SegC, and the corresponding
 *    record from <withRecSet< contains two semgnets named SegA and SegB, the binary operation is
 *    performed between <recSetIn>:SegA and <withRecSet>:SegA and between <recSetIn>:SegB
 *    and <withRecSet>:SegB.
 *
 */

#include "jsoc_main.h"

#define kRecSetIn "recSetIn"
#define kWithRecSet "withRecSet"
#define kSeriesOut "seriesOut"
#define kSegList "segments"
#define kOp "op"
#define kNotSpecified "NOT SPECIFIED"
#define kMaxSegs 1024
#define kMaxQuery 2048
#define kOutSeriesDesc "Output series for arithTool calculation."

#define TESTER 0

typedef enum
{
     kArithOpUnknown = 0,
     kArithOpMul,
     kArithOpDiv,
     kArithOpAdd,
     kArithOpSub,
     kArithOpBinaryDummy, /* Delimits binary from unary ops */
     kArithOpAbs,
     kArithOpSqrt,
     kArithOpLog,
     kArithOpPow,
     kArithOpSqr,
     kArithOpRecip,
     kArithOpSubmean,
     kArithOpLastDummy
} ArithOp_t;

ModuleArgs_t module_args[] =
{
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_STRING, kRecSetIn, "", "A set of records with segments upon which to operate."},
  {ARG_STRING, kWithRecSet, kNotSpecified, "For binary operations, the second operand."},
  {ARG_STRING, kSeriesOut, kNotSpecified, "Name of series in which to save extracted data."},
  {ARG_STRING, kSegList, kNotSpecified, "Comma-separated list of segments on which to operate."},
  {ARG_STRING, kOp, "", "The operation to perform."},
  {ARG_END}
};

char *module_name = "arithTool";

typedef struct DRMSContainer_struct
{
  HContainer_t *items;
  void (*Free)(HContainer_t *);
  HIterator_t iter;  
} DRMSContainer_t;



static int NiceIntro()
{
     int usage = cmdparams_get_int(&cmdparams, "h", NULL);
     if (usage)
     {
	  printf ("Usage:\n\tarithTool [-h] "
		  "op=<operation> recSetIn=<recspec> [withRecSet=<recspec>] [seriesOut=<seriesname>] [segList=<segments>]\n"
		  "  details are:\n"
		  "  -h: help - show this message then exit\n"
		  "  <seriesname> - fully qualified series name.\n"
		  "  <timerange> - time value range set.\n"
		  "  seriesIn defaults to sdo.fds.\n"
		  "  timeRange defaults to all records in seriesIn.\n"
		  "  seriesOut defaults to sdo.fdsStateVectors.\n"
		  "  example - extractFdsStateV seriesIn=su_arta.TestFDSData timeRange=2006.11.20_22:38:00-2006.11.20_22:45:00,2006.11.20_22:52:00. seriesOut=su_arta.TestFDSHelio\n");
	  return 1;
     }

     return 0;
}

/* XXX Implement the rest of the types. */
/* anything compared to a NaN will return false */
int IsMissingData(DRMS_Type_t type, const void *data)
{
   int ret = 0;

   switch(type)
   {
   case DRMS_TYPE_FLOAT:
      if (*((float *)data) != *((float *)data))
      {
	 /* The data is a NaN */
	 ret = 1;
      }
      else if (DRMS_MISSING_FLOAT != DRMS_MISSING_FLOAT)
      {
	 /* The missing value is a NaN, but data isn't NaN. */
	 ret = 0;
      }
      else
      {
	 /* Neither the data nor the missing value is a NaN. */
	 ret = (*((float *)data) == DRMS_MISSING_FLOAT);
      }
      break;
   case DRMS_TYPE_DOUBLE:
      if (*((double *)data) != *((double *)data))
      {
	 /* The data is a NaN */
	 ret = 1;
      }
      else if (DRMS_MISSING_DOUBLE != DRMS_MISSING_DOUBLE)
      {
	 /* The missing value is a NaN, but data isn't NaN. */
	 ret = 0;
      }
      else
      {
	 /* Neither the data nor the missing value is a NaN. */
	 ret = (*((double *)data) == DRMS_MISSING_DOUBLE);
      }
      break;
   default:
     fprintf(stderr, "Unexpected type %d.\n", type);
   }

   return ret;
}

/* No need for an associative array since this mapping will be performed one time. */
static ArithOp_t MapOp(char *opStr)
{
     ArithOp_t op = kArithOpUnknown;

     if (strncmp(opStr, "mul", 3) == 0)
     {
	  op = kArithOpMul;
     }
     else if (strncmp(opStr, "div", 3) == 0)
     {
	  op = kArithOpDiv;
     }
     else if (strncmp(opStr, "add", 3) == 0)
     {
	  op = kArithOpAdd;
     }
     else if (strncmp(opStr, "sub", 3) == 0)
     {
	  op = kArithOpSub;
     }
     else if (strncmp(opStr, "abs", 3) == 0)
     {
	  op = kArithOpAbs;
     }
     else if (strncmp(opStr, "sqrt", 4) == 0)
     {
	  op = kArithOpSqrt;
     }
     else if (strncmp(opStr, "log", 3) == 0)
     {
	  op = kArithOpLog;
     }
     else if (strncmp(opStr, "pow", 3) == 0)
     {
	  op = kArithOpPow;
     }
     else if (strncmp(opStr, "sqr", 3) == 0)
     {
	  op = kArithOpSqr;
     }
     else if (strncmp(opStr, "recip", 5) == 0)
     {
	  op = kArithOpRecip;
     }
     else if (strncmp(opStr, "nomean", 6) == 0)
     {
	  op = kArithOpSubmean;
     }

     return op;
}

/* hconFree performs non-standard cleaning up of HContainer_t. */
static int CreateDRMSPrimeKeyContainer(DRMS_Record_t *recTemplate, DRMSContainer_t *conPrimeKeys, 
				       void (*hconFree)(HContainer_t *hc))
{
     int error = 0;

     /* There is no HContainer_t of primary keys - make one. */
     
     if (conPrimeKeys != NULL)
     {
	  /* Create new HContainer_t. */
	  conPrimeKeys->items = (HContainer_t *)malloc(sizeof(HContainer_t));

	  if (conPrimeKeys->items != NULL)
	  {
	       hcon_init(conPrimeKeys->items, sizeof(DRMS_Keyword_t *), DRMS_MAXNAMELEN, NULL, NULL);
	       int iPrimeKeysMax = recTemplate->seriesinfo->pidx_num - 1;
	       int iPrimeKeys = 0;
	       
	       while (iPrimeKeys <= iPrimeKeysMax)
	       {
		    DRMS_Keyword_t *keyword = recTemplate->seriesinfo->pidx_keywords[iPrimeKeys];
		    if (keyword != NULL)
		    {
			 DRMS_Keyword_t **newKeyword = 
			   (DRMS_Keyword_t **)hcon_allocslot(conPrimeKeys->items, 
							     keyword->info->name);
			 
			 if (newKeyword != NULL)
			 {
			      *newKeyword = keyword;
			 }
			 else
			 {
			      error = 1;
			      break;
			 }
		    }
		    else
		    {
			 error = 1;
			 break;
		    }
		    
		    iPrimeKeys++;
	       }
	       
	       /* Create iterator too. */
	       conPrimeKeys->Free = hconFree;
	       hiter_new(&(conPrimeKeys->iter), conPrimeKeys->items);
	  }
     }
     else
     {
	  error = 1;
     }
     
     return error;
}

/* hconFree performs non-standard cleaning up of HContainer_t. */
static void CreateDRMSSegmentContainer(DRMS_Record_t *recTemplate, DRMSContainer_t *conSegs, 
				       void (*hconFree)(HContainer_t *hc))
{
     /* Create iterator too. */
     conSegs->items = &(recTemplate->segments);
     conSegs->Free = hconFree;
     hiter_new(&(conSegs->iter), conSegs->items);
}

static void DestroyDRMSContainer(DRMSContainer_t *pCon)
{
     if (pCon != NULL)
     {
	  if (pCon->Free != NULL && pCon->items != NULL)
	  {
	       (*(pCon->Free))(pCon->items);
	  }

	  pCon->items = NULL;
     }
}

static void ReleaseHContainer(HContainer_t *hcon)
{
     if (hcon != NULL)
     {
	  /* Standard cleaning up of HContainer_t. */
	  hcon_free(hcon);
	  free(hcon);
     }
}

static int IsValidOp(ArithOp_t op)
{
     return (op >= kArithOpUnknown && op < kArithOpLastDummy);
}

static int IsBinaryOp(ArithOp_t op)
{
     if (IsValidOp(op))
     {
	  return op < kArithOpBinaryDummy;
     }

     return 0;
}

/* Returns 0 on error. */
/* Returns 1 if keys2 is equal to keys1. */
static int KeysEqual(DRMSContainer_t *keys1, DRMSContainer_t *keys2)
{
     int ret = 1;

     /*  Iterate through keys1, looking up the keyword name in the keys2 collection. 
      *  This should be relatively efficient since the lookup is a hash function.
      */

     if (hcon_size(keys1->items) != hcon_size(keys2->items))
     {
	  ret = 0;
     }
     else
     {
	  hiter_rewind(&(keys1->iter));
	  DRMS_Keyword_t **currKey = NULL;
	  DRMS_Keyword_t **currKey2 = NULL;
	  while ((currKey = (DRMS_Keyword_t **)hiter_getnext(&(keys1->iter))) != NULL)
	  {
	       if ((currKey2 = hcon_lookup(keys2->items, (*currKey)->info->name)) == NULL)
	       {
		    ret = 0;
		    break;
	       }
	       else
	       {
		    /* Ensure matching keys are the same type. */
		    if (drms_keyword_type(*currKey) != drms_keyword_type(*currKey2))
		    {
			 ret = 0;
			 break;
		    }
	       }
	  }
     }

     return ret;
}


/* Returns the number of matching segments.  If matchSet != NULL, 
 * returns the actual segs1 segments 
 * matching in matchSet.  Returns -1 on error.
 */
static int CreateMatchingSegs(DRMSContainer_t *segs1, DRMSContainer_t *segs2, DRMSContainer_t *matchSet)
{
     int nMatch = 0;

     hiter_rewind(&(segs1->iter));
     DRMS_Segment_t *currSeg = NULL;

     while ((currSeg = (DRMS_Segment_t *)hiter_getnext(&(segs1->iter))) != NULL)
     {
	  if (segs1 == segs2 ||
	      hcon_lookup(segs2->items, currSeg->info->name) != NULL)
	  {
	       nMatch++;

	       if (matchSet != NULL)
	       {
		    if (nMatch == 1)
		    {
			 matchSet->items = (HContainer_t *)malloc(sizeof(HContainer_t));
			 if (matchSet->items != NULL)
			 {
			      hcon_init(matchSet->items, sizeof(DRMS_Segment_t *), 
					DRMS_MAXNAMELEN, 
					NULL, 
					NULL);
			 }
			 else
			 {
			      nMatch = -1;
			      break;
			 }
		    }
		    
		    DRMS_Segment_t **newSeg = 
		      (DRMS_Segment_t **)hcon_allocslot(matchSet->items, currSeg->info->name);
		    
		    if (newSeg != NULL && *newSeg != NULL)
		    {
			 *newSeg = currSeg;
		    }
		    else
		    {
			 nMatch = -1;
			 break;
		    }
	       }
	  }
     }

     if (nMatch > 0 && matchSet != NULL)
     {
	  matchSet->Free = ReleaseHContainer;
	  hiter_new(&(matchSet->iter), matchSet->items);
     }
     
     return nMatch;
}

static int ValidateBinaryOperands(DRMS_Env_t *drmsEnv, 
				  char *recSetIn, 
				  char **segNameArr, 
				  int nSegs, 
				  DRMSContainer_t *inKeys, DRMSContainer_t *inSegs, 
				  DRMSContainer_t *withKeys, DRMSContainer_t *withSegs, 
				  DRMSContainer_t *segsToProc)
{
     int error = 0;
     int status = 0;
  
     int nWithRecs = 0;
     DRMS_RecordSet_t *recSet = drms_open_records(drms_env, recSetIn, &status);
     error = (status != DRMS_SUCCESS);
     
     if (error == 0)
     {
	  nWithRecs = recSet->n;
	  drms_close_records(recSet, DRMS_FREE_RECORD);
     }

     /* If the number of records in the withSeries != 1, then the
      * primary keys must match.
      */
     if (!error)
     {
	  if (nWithRecs > 1)
	  {
	       if (!KeysEqual(inKeys, withKeys))
	       {
		    error = 1;
	       }
	  }
     }
     
     if (!error)
     {
	  int nMatch = 0;

	  if (nSegs > 0)
	  {
	       
	       /* If a segment list is specified, then both the 'in' and 'with' series 
		* must contain all items in the segment list
		*/
	       int iSeg = 0;
	       
	       for (; iSeg < nSegs; iSeg++) 
	       {
		    char *aSegName = segNameArr[iSeg];
		    
		    if (hcon_lookup(inSegs->items, aSegName) == NULL ||
			hcon_lookup(withSegs->items, aSegName) == NULL)
		    {
			 error = 1;
			 fprintf(stderr, "Segment %s not present in an input recordset.\n", 
				 aSegName);
			 break;
		    }
	       }

	       /* Create segsToProc list - a copy of segs in inSegs. */
	       if (!error)
	       {
		    nMatch = CreateMatchingSegs(inSegs, inSegs, segsToProc);
	       }
	  }
	  else
	  {
	       /* If no segments are specified, at least one segment must match */
	       nMatch = CreateMatchingSegs(inSegs, withSegs, segsToProc);
	  }

	  if (nMatch == -1)
	  {
	       DestroyDRMSContainer(segsToProc);
	       error = 1;
	  }

	  if (!error && nMatch == 0)
	  {
	       error = 1;
	  }
     }

     return error;
}

/* Must free malloc'd memory if ret != NULL. */
static char *CreateStrFromDRMSValue(DRMS_Record_t *rec, char *keyName, int *error)
{
     return drms_getkey_string(rec, keyName, error);
}

/* Gets as far as it can, filling in with MISSINGS if necessary.  Returns 1
* if any error happens. */
static int PerformOperation(ArithOp_t op, const double *pInData, const double *pWithData, 
			    int nElements, double *pOutData, double mean)
{
   int error = 0;

   int first = 1;
   int percentDone = 0;
   int update = 0;

   const double *rop = NULL;
   const double *lop = NULL;

   if (!pInData)
   {
      error = 1;
      fprintf(stderr, "Missing input data.\n");
   }
   else if (IsBinaryOp(op) && !pWithData)
   {
      error = 1;
      fprintf(stderr, "Missing input data.\n");
   }
   else
   {
      double *result = NULL;
      unsigned int index = 0;

      for (; index < nElements; index++) 
      {
	 result = &(pOutData[index]);
	 *result = DRMS_MISSING_DOUBLE;

	 if (!IsMissingData(DRMS_TYPE_DOUBLE, &(pInData[index])) && (!IsBinaryOp(op) ||
	     !IsMissingData(DRMS_TYPE_DOUBLE, &(pWithData[index])))) 
	 {
	    percentDone = index * 100/nElements;
	    
	    if (first)
	    {
	       fprintf(stdout, "%03d%% done", percentDone);
	       first = 0;
	    }
	    else if (percentDone == update)
	    {
	       update++;
	       fprintf(stdout, 
		       "\b\b\b\b\b\b\b\b\b%03d%% done", 
		       percentDone);
	       fflush(stdout);
	    }

	    lop = &(pInData[index]);
	    
	    if (IsBinaryOp(op))
	    {
	       rop = &(pWithData[index]);

	       switch (op)
	       {
	       case kArithOpMul:
		 *result = *lop * *rop;
		 break;
	       case kArithOpDiv:
		 *result = *lop / *rop;
		 break;
	       case kArithOpAdd:
		 *result = *lop + *rop;
		 break;
	       case kArithOpSub:
		 *result = *lop - *rop;
		 break;
	       default:
		 error = 1;
		 *result = DRMS_MISSING_DOUBLE;
		 fprintf(stderr, "Expecting a binary operator, got %d instead.\n", op);
	       }	       
	    }
	    else
	    {
	       switch (op)
	       {
	       case kArithOpAbs:
		 *result = fabsf(*lop);
		 break;
	       case kArithOpSqrt:
		 *result = sqrt(fabsf(*lop));
		 break;
	       case kArithOpLog:
		 *result = log10(*lop);
		 break;
	       case kArithOpPow:
		 *result = pow(10.0, *lop);
		 break;
	       case kArithOpSqr:
		 *result = *lop * *lop;
		 break;
	       case kArithOpRecip:
		 *result = ((*lop != 0.0) ? 1.0 / *lop : DRMS_MISSING_DOUBLE);
		 break;
	       case kArithOpSubmean:
		 {
		    *result = *lop - mean;
		 }
		 break;
	       default:
		 error = 1;
		 *result = DRMS_MISSING_DOUBLE;
		 fprintf(stderr, "Expecting a unary operator, got %d instead.\n", op);
	       }	       
	    }	    	    
	 }
      } /* for elements */
      
      fprintf(stdout, "\b\b\b\b\b\b\b\b\b100%% done\n");
      fflush(stdout);
   }				   
				   
   return error;
}

int DoBinaryOp(DRMS_Env_t *drmsEnv, ArithOp_t op,
	       DRMSContainer_t *inPrimeKeys, DRMSContainer_t *segsToProc, 
	       char *inSeriesName, char *inSeriesQuery, 
	       char *withSeriesName, char *withSeriesQuery, 
	       char *seriesOut)
{
     int error = 0;
     int status = 0;
     int nWithRecs = 0;
     int nInRecs = 0;
     double **pOutData = NULL;
     char **outSegNames = NULL;

     /* Open the inSeries. */
     DRMS_RecordSet_t *inRecSet = drms_open_records(drmsEnv, inSeriesQuery, &status);
     error = (status != DRMS_SUCCESS);
     DRMS_RecordSet_t *withRecSet = NULL;

     DRMS_Record_t *inRec = NULL;
     DRMS_Record_t *withRec = NULL; 

     int nSegs = segsToProc->items->num_total;

     if (!error)
     {
	  nInRecs = inRecSet->n;
	  withRecSet = drms_open_records(drmsEnv, withSeriesQuery, &status);
	  error = (status != DRMS_SUCCESS);
	  nWithRecs = withRecSet->n;

	  XASSERT(nWithRecs >= 1);

	  if (nWithRecs == 0)
	  {
	       error = 1;
	  }
	  else if (nWithRecs == 1)
	  {
	       withRec = withRecSet->records[0];
	  }
     }

     int iRec = 0;
     for (; !error && iRec < nInRecs; iRec++)
     {
	  inRec = inRecSet->records[iRec];

	  fprintf(stdout, "Processing record %lld\n", inRec->recnum);

	  if (nWithRecs != 1)
	  {
	       /* We want to choose one record from all withSeries recs.  This
		* chosen record must match the prime keys of the current inSeries
		* record.  Since the inSeries and withSeries have the same prime
		* keys, because the prime key values select one record from the inSeries,
		* they also select one record from the withSeries. 
		*
		* First close the withSeries record that matched
		* the previous inSeries record. */
	       drms_close_records(withRecSet, DRMS_FREE_RECORD);
	       withRecSet = NULL;

	       /* Query the withSeries to find the record whose prime keys match 
		* the current inSeries record's prime key values.
		*/
	       char query[kMaxQuery];
	       char buf[kMaxQuery];
	       char *pQuery = query;
	       int maxQuery = kMaxQuery;
	       
	       snprintf(buf, sizeof(buf), "%s", withSeriesName);
	       snprintf(pQuery, maxQuery, "%s", buf);
	       pQuery += strlen(buf);
	       maxQuery -= strlen(buf);
	       
	       hiter_rewind(&(inPrimeKeys->iter));
	       DRMS_Keyword_t **primeKey = NULL;
	       
	       while (maxQuery > 0 && 
		      (primeKey = hiter_getnext(&(inPrimeKeys->iter))) != NULL)
	       {
		    char *val = CreateStrFromDRMSValue(inRec, (*primeKey)->info->name, &error);
		    if (!error)
		    {
			 if (val != NULL)
			 {
			      snprintf(buf, sizeof(buf), "[%s=%s]", 
					    (*primeKey)->info->name, 
				       val);
			      snprintf(pQuery, maxQuery, "%s", buf);
			      pQuery += strlen(buf);
			      maxQuery -= strlen(buf);
			      
			      if (maxQuery <= 0)
			      {
				   error = 1;
				   break;
			      }
			      
			      free(val);
			 }
			 else
			      {
				   error = 1;
			      }
		    }
	       }
	       
	       if (!error)
	       {
		    withRecSet = drms_open_records(drmsEnv, query, &status);
		    error = (status != DRMS_SUCCESS);
		    
		    if (!error && withRecSet != NULL)
		    {
			 if (withRecSet -> n != 1)
			 {
			      error = 1;
			 }
			 else
			 {
			      withRec = withRecSet->records[0];
			 }
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	  }

	  if (!error)
	  {
	       /* Have a single record from inSeries and a matching single 
		* record from withSeries.  Get matching segments from each record. */
	       hiter_rewind(&(segsToProc->iter));
	       DRMS_Segment_t **seg = NULL;
	       DRMS_Segment_t *inSeg = NULL;
	       DRMS_Segment_t *withSeg = NULL;
	       DRMS_Array_t *inSegArray = NULL;
	       DRMS_Array_t *withSegArray = NULL;

	       pOutData = (double **)malloc(sizeof(double *) * nSegs);
	       outSegNames = (char **)malloc(sizeof(char *) * nSegs);

	       unsigned int iSeg = 0;
	       while(pOutData != NULL && 
		     outSegNames != NULL &&
		     !error && 
		     (seg = (DRMS_Segment_t **)hiter_getnext(&(segsToProc->iter))) != NULL)
	       {
		    fprintf(stdout, "  processing segment %s:  ", (*seg)->info->name);
		    
		    inSeg = drms_segment_lookup(inRec, (*seg)->info->name);
		    withSeg = drms_segment_lookup(withRec, (*seg)->info->name);


		    XASSERT(inSeg != NULL && withSeg != NULL);
		    if (inSeg != NULL && withSeg != NULL)
		    {
			 inSegArray = drms_segment_read(inSeg, DRMS_TYPE_DOUBLE, &status);
			 error = (status != DRMS_SUCCESS);
			 if (!error)
			 {
			      withSegArray = drms_segment_read(withSeg, DRMS_TYPE_DOUBLE, &status);
			      error = (status != DRMS_SUCCESS);
			 }
		    }

		    if (!error)
		    {
			 /* Have inSeries seg data and withSeries seg data - do binary op. */
			 int nElementsIn = drms_array_count(inSegArray);
			 int nElementsWith = drms_array_count(withSegArray);
			 double *pInData = inSegArray->data;
			 double *pWithData = withSegArray->data;

			 if (nElementsIn == nElementsWith)
			 {
			      pOutData[iSeg] = (double *)malloc(sizeof(double) * nElementsIn);
			      outSegNames[iSeg] = strdup((*seg)->info->name);

			      if (pOutData[iSeg] != NULL && outSegNames[iSeg] != NULL)
			      {
				 /* Shouldn't return an error - all params checked */
				 PerformOperation(op, pInData, pWithData, nElementsIn, pOutData[iSeg], 0.0);
				 iSeg++;
			      }
			      else
			      {
				   error = 1;
			      }
			 }
			 else
			 {
			      error = 1;
			 }
		    }
	       } /* while */
	  }

	  if (!error)
	  {
	       // CreateRecord(drmsEnv, inRec, outSegNames, pOutData, seriesOut, seriesOutExists);
	       /* If a record with the same primary key already exists, the new record will be
		* a newer version of the original one.
		*/
	       DRMS_Record_t *rec = drms_create_record(drmsEnv, seriesOut, DRMS_PERMANENT, &status);
	       error = (status != DRMS_SUCCESS);
	       if (rec != NULL && !error)
	       {
		    /* Write out outSeries keys - fill them in with values from inSeries. */
		    HIterator_t hit;
		    hiter_new(&hit, &(rec->keywords));
		    DRMS_Keyword_t *outKey = NULL;
		    DRMS_Keyword_t *inKey = NULL;

		    while ((outKey = (DRMS_Keyword_t *)hiter_getnext(&hit)) != NULL)
		    {
			 inKey = drms_keyword_lookup(inRec, outKey->info->name, 1);
			 if (inKey != NULL)
			 {
			      status = drms_setkey(rec, 
						   outKey->info->name, 
						   outKey->info->type, 
						   &(inKey->value));

			      error = (status != DRMS_SUCCESS);
			 }
		    }

		    /* Write segments. */
		    unsigned int index = 0;
		    for (; index < nSegs; index++)
		    {
			 if (pOutData != NULL && outSegNames != NULL)
			 {

			      DRMS_Segment_t *inSeg = drms_segment_lookup(inRec, 
									  outSegNames[index]);
			      DRMS_Segment_t *seg = drms_segment_lookup(rec, outSegNames[index]);
			      if (inSeg != NULL && seg != NULL)
			      {
				   /* segArray now owns pOutData[index]. */
				   DRMS_Array_t *segArray = drms_array_create(DRMS_TYPE_DOUBLE,
									      inSeg->info->naxis, 
									      inSeg->axis, 
									      pOutData[index],
									      &status);
				   error = (status != DRMS_SUCCESS);

				   if (!error)
				   {
					drms_segment_write(seg, segArray, 0);
					drms_free_array(segArray);
				   }
			      }
			      
			 }
		    }
		    
		    if (!error)
		    {
			 drms_close_record(rec, DRMS_INSERT_RECORD);
		    }
		    else
		    {
			 drms_close_record(rec, DRMS_FREE_RECORD);
		    }
	       }
	       else if (rec != NULL)
	       {
		    drms_close_record(rec, DRMS_FREE_RECORD);
	       }
	  }
	  
	  /* Clean up seg names. DO NOT clean up pOutData - this got transferred to segArray
	   * just prior to writing the segments.
	   */
	  unsigned int index = 0;
	  for (; index < nSegs; index++)
	  {
	       if (outSegNames[index] != NULL)
	       {
		    char *pName = outSegNames[index];
		    free(pName);
	       }
	  }

	  if (outSegNames != NULL)
	  {
	       free(outSegNames);
	  }
     } /* foreach (record) */

     if (inRecSet != NULL)
     {
	  drms_close_records(inRecSet, DRMS_FREE_RECORD);
     }

     if (withRecSet != NULL)
     {
	  drms_close_records(withRecSet, DRMS_FREE_RECORD);
     }

     return error;
}

static int DoUnaryOp(DRMS_Env_t *drmsEnv, ArithOp_t op,
		     DRMSContainer_t *inPrimeKeys, DRMSContainer_t *segsToProc, 
		     char *inSeriesName, char *inSeriesQuery, 
		     char *seriesOut)
{
     int error = 0;
     int status = 0;
     double **pOutData = NULL;
     char **outSegNames = NULL; 
     int nInRecs = 0;
     int nSegs = 0;

     /* Open the inSeries. */
     DRMS_RecordSet_t *inRecSet = drms_open_records(drmsEnv, inSeriesQuery, &status);
     DRMS_Record_t *inRec = NULL;
     error = (status != DRMS_SUCCESS);

     if (!error)
     {
	  nInRecs = inRecSet->n;
	  nSegs = segsToProc->items->num_total;
     }

     int iRec = 0;
     for (; !error && iRec < nInRecs; iRec++)
     {
	  inRec = inRecSet->records[iRec];

	  fprintf(stdout, "Processing record %lld\n", inRec->recnum);

	  hiter_rewind(&(segsToProc->iter));
	  DRMS_Segment_t **seg = NULL;
	  DRMS_Segment_t *inSeg = NULL;
	  DRMS_Array_t *inSegArray = NULL;
	  
	  pOutData = (double **)malloc(sizeof(double *) * nSegs);
	  outSegNames = (char **)malloc(sizeof(char *) * nSegs);

	  unsigned int iSeg = 0;
	  while(pOutData != NULL && 
		outSegNames != NULL &&
		!error && 
		(seg = (DRMS_Segment_t **)hiter_getnext(&(segsToProc->iter))) != NULL)
	  {
	       fprintf(stdout, "  processing segment %s:  ", (*seg)->info->name);
	       inSeg = drms_segment_lookup(inRec, (*seg)->info->name);
	       
	       XASSERT(inSeg != NULL);
	       if (inSeg != NULL)
	       {
		    inSegArray = drms_segment_read(inSeg, DRMS_TYPE_DOUBLE, &status);

		    error = (status != DRMS_SUCCESS);
		   
		    if (!error)
		    {
			 /* Have inSeries seg data - do unary op. */
			 int nElementsIn = drms_array_count(inSegArray);
			 double *pInData = inSegArray->data;

			 pOutData[iSeg] = (double *)malloc(sizeof(double) * nElementsIn);
			 outSegNames[iSeg] = strdup((*seg)->info->name);

			 if (pOutData[iSeg] != NULL && outSegNames[iSeg] != NULL)
			 {
			    double mean = 0.0;

			    /* calc mean, if op == submean */
			    if (op == kArithOpSubmean)
			    {
			       int n = 0;
			       double *pData = pInData;
			       
			       int iData = 0;					
			       for (; iData < nElementsIn; iData++, pData++)
			       {
				  // if (*pData != DRMS_MISSING_DOUBLE)
				  if (!IsMissingData(DRMS_TYPE_DOUBLE, pData))
				  {
				     mean += *pData;
				     n += 1;
				  }
			       }
			       
			       if (n > 0)
			       {
				  mean /= n;
			       }
			    }
			
			    PerformOperation(op, pInData, NULL, nElementsIn, pOutData[iSeg], mean);
			    iSeg++;
			 }
			 else
			 {
			      error = 1;
			 }
		    }
	       }
	  } /* while */

	  if (!error)
	  {
	       /* If a record with the same primary key already exists, the new record will be
		* a newer version of the original one.
		*/
	       DRMS_Record_t *rec = drms_create_record(drmsEnv, 
						       seriesOut, 
						       DRMS_PERMANENT, 
						       &status);
	       error = (status != DRMS_SUCCESS);

	       if (rec != NULL && !error)
	       {
		    /* Write out outSeries keys - fill them in with values from inSeries. */
		    HIterator_t hit;
		    hiter_new(&hit, &(rec->keywords));
		    DRMS_Keyword_t *outKey = NULL;
		    DRMS_Keyword_t *inKey = NULL;

		    while ((outKey = (DRMS_Keyword_t *)hiter_getnext(&hit)) != NULL)
		    {
			 inKey = drms_keyword_lookup(inRec, outKey->info->name, 1);
			 if (inKey != NULL)
			 {
			      status = drms_setkey(rec, 
						   outKey->info->name, 
						   outKey->info->type, 
						   &(inKey->value));

			      error = (status != DRMS_SUCCESS);
			 }
		    }

		    /* Write segments, and then clean up arrays holding data and seg names. */
		    unsigned int index = 0;
		    for (; index < nSegs; index++)
		    {
			 if (pOutData != NULL && outSegNames != NULL)
			 {
			      DRMS_Segment_t *inSeg = drms_segment_lookup(inRec, 
									  outSegNames[index]);
			      DRMS_Segment_t *seg = drms_segment_lookup(rec, outSegNames[index]);

			      if (inSeg != NULL && seg != NULL)
			      {
				   /* segArray now owns pOutData[index]. */
				   DRMS_Array_t *segArray = drms_array_create(DRMS_TYPE_DOUBLE,
									      inSeg->info->naxis, 
									      inSeg->axis, 
									      pOutData[index],
									      &status);

				   error = (status != DRMS_SUCCESS);

				   if (!error)
				   {
					drms_segment_write(seg, segArray, 0);
					drms_free_array(segArray);
				   }
			      }
			 }
		    }
		    
		    if (!error)
		    {
			 drms_close_record(rec, DRMS_INSERT_RECORD);
		    }
		    else
		    {
			 drms_close_record(rec, DRMS_FREE_RECORD);
		    }
	       }
	       else if (rec != NULL)
	       {
		    drms_close_record(rec, DRMS_FREE_RECORD);
	       }
	  }
	  
	  /* Clean up seg names. DO NOT clean up pOutData - this got transferred to segArray
	   * just prior to writing the segments.
	   */
	  unsigned int index = 0;
	  for (; index < nSegs; index++)
	  {
	       if (outSegNames[index] != NULL)
	       {
		    char *pName = outSegNames[index];
		    free(pName);
	       }
	  }

	  if (outSegNames != NULL)
	  {
	       free(outSegNames);
	  }

     } /* foreach (record) */

     if (inRecSet != NULL)
     {
	  drms_close_records(inRecSet, DRMS_FREE_RECORD);
     }

     return error;
}

#if TESTER
     typedef struct yammer
     {
       int a;
       char *b;
     } yammer_t;

     void FreeF(const void *v) 
     {
	  free(((yammer_t *)(v))->b);
     };

     void CopyF(const void *dst, const void *src) 
     {
	  ((yammer_t *)dst)->b = strdup(((yammer_t *)src)->b);
     };
#endif

int DoIt(void)
{

#if TESTER
     char *nameArr[4] = {"name1", "name2", "name3", "name4"};
     char *strArr[4] = {"yammer1", "yammer2", "yammer3", "yammer4"};
     yammer_t valArr[4];

     valArr[0].a = 1;
     valArr[0].b = strArr[0];
     valArr[1].a = 2;
     valArr[1].b = strArr[1];
     valArr[2].a = 3;
     valArr[2].b = strArr[2];
     valArr[3].a = 4;
     valArr[3].b = strArr[3];

     yammer_t *valArrI[4] = {&valArr[0], &valArr[1], &valArr[2], &valArr[3]};

     HContainer_t *cont = hcon_create(sizeof(yammer_t), 
				      128, 
				      FreeF, 
				      CopyF, 
				      (void **)valArrI, 
				      nameArr, 
				      4);
     if (cont)
     {
	  HIterator_t *it = hiter_create(cont);
	  if (it)
	  {
	       yammer_t *yp = NULL;
	       while ((yp = (yammer_t *)hiter_getnext(it)) != NULL)
	       {
		    printf("int val %d, str val %s\n", yp->a, yp->b);
	       }

	       hiter_destroy(&it);
	  }
	  
	  hcon_destroy(&cont);
     }

     printf("Aok\n");

     return 1;
#endif
     int error = 0;
     int status = 0;

     if (NiceIntro())
     {
	  return 0;
     }

     if (drms_env == NULL)
     {
	  error = 1;
     }
     else
     {
	  /* Check for a valid combination of parameters. */
	  int bBinaryOp = 0;
	  int bSeriesOut = 0;       /* user specified an out series, not just the default */
	  int bWithRecSet = 0;
	  int bSeriesOutExists = 0; /* the out series specified by the user exists
				     * and matches the inSeries in terms of 
				     * keywords. */
	  int bSeriesOutEqSeriesIn = 0;
	  ArithOp_t op = kArithOpUnknown;
	  char inSeriesName[DRMS_MAXNAMELEN];
	  char withSeriesName[DRMS_MAXNAMELEN];

	  char *recSetIn = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
	  char *withRecSet = cmdparams_get_str(&cmdparams, kWithRecSet, NULL);
	  char *seriesOut = cmdparams_get_str(&cmdparams, 
					      kSeriesOut, 
					      NULL); /* actual param string
						      * could be 'NOT SPECIFIED' */
	  char *segList = cmdparams_get_str(&cmdparams, kSegList, NULL);
	  char *opStr = cmdparams_get_str(&cmdparams, kOp, NULL);

	  char actualOutputSeries[DRMS_MAXNAMELEN];
	  /* xxx DO SCALILNG/OFFSET PARAMS LATER */

	  char *segNameArr[kMaxSegs];
	  bzero(segNameArr, sizeof(segNameArr));

	  int nSegs = 0;

	  DRMS_Record_t *inRecTemplate = NULL;
	  DRMS_Record_t *outRecTemplate = NULL;

	  /* op is required */
	  if (strcmp(opStr, kNotSpecified) == 0)
	  {
	       error = 1;
	       fprintf(stderr, "Operation parameters is required.\n");
	  }
	  else
	  {
	       op = MapOp(opStr);

	       if (op == kArithOpUnknown)
	       {
		    error = 1;
		    fprintf(stderr, "Invalid op %s.\n", opStr);
	       }

	       bBinaryOp = IsBinaryOp(op);
	  }

	  /* recSetIn is required */
	  if (!error && strcmp(recSetIn, kNotSpecified) == 0)
	  {
	       error = 1;
	       fprintf(stderr, "Input record set required.\n");
	  }

	  if (!error)
	  {
	       snprintf(inSeriesName, sizeof(inSeriesName), recSetIn);
	       char *pChar = strchr(inSeriesName, '[');
	       if (pChar != NULL)
	       {
		    *pChar = '\0';
	       }
	  }

	  if (!error)
	  {
	       /* Ensure inSeries exists in the database. */
	       inRecTemplate = drms_template_record(drms_env, inSeriesName, &status);
	       error = (status != 0);
	       if (error)
	       {
		    fprintf(stderr, "BAILING: inSeries does not exist\n");
	       }
	  }

	  if (!error && strcmp(seriesOut, kNotSpecified) != 0)
	  {
	       bSeriesOut = 1;

	       /* Does the specified outSeries actually exist? */
	       if (strcmp(inSeriesName, seriesOut) == 0)
	       {
		    bSeriesOutExists = 1; /* because we already verified that inSeries exists. */
		    bSeriesOutEqSeriesIn = 1;
	       }
	       else
	       {
		    outRecTemplate = drms_template_record(drms_env, 
							  seriesOut, 
							  &status);

		    if (status == DRMS_SUCCESS)
		    {
			 bSeriesOutExists = 1;
		    }
	       }
	  }

	  if (!error && strcmp(withRecSet, kNotSpecified) != 0)
	  {
	       bWithRecSet = 1;
	       snprintf(withSeriesName, sizeof(withSeriesName), withRecSet);
	       char *pChar = strchr(withSeriesName, '[');
	       if (pChar != NULL)
	       {
		    *pChar = '\0';
	       }
	  }

	  if (!error && strcmp(segList, kNotSpecified) != 0)
	  {
	       char *aSeg;
	       
	       for (aSeg = strtok(segList, ","); aSeg && nSegs < kMaxSegs; aSeg = strtok(NULL, ","))
	       {
		    segNameArr[nSegs++] = strdup(aSeg);
	       }
	  }

	  if (!error)
	  { 
	       /* main code */
	       DRMSContainer_t inSeriesPrimeKeys;
	       DRMSContainer_t inSeriesSegs;
	       DRMSContainer_t segsToProc;
	       DRMSContainer_t outSeriesPrimeKeys;
	       DRMSContainer_t outSeriesSegs;
	       
	       if (!error)
	       {
		    error = CreateDRMSPrimeKeyContainer(inRecTemplate, 
							&inSeriesPrimeKeys, 
							ReleaseHContainer);
	       }
	       
	       if (!error)
	       {
		    CreateDRMSSegmentContainer(inRecTemplate, &inSeriesSegs, NULL);
	       }

	       if (!error && bSeriesOut && bSeriesOutExists && !bSeriesOutEqSeriesIn)
	       {
		    error = CreateDRMSPrimeKeyContainer(outRecTemplate, 
							&outSeriesPrimeKeys, 
							ReleaseHContainer);
		    
		    if (!error)
		    {
			 CreateDRMSSegmentContainer(outRecTemplate, 
						    &outSeriesSegs, 
						    NULL);
		    }
	       }

	       /* Branch on whether the op is binary or not */
	       if (!error && bBinaryOp)
	       {
		    /* withRecSet is required*/
		    error = (bWithRecSet != 1);
		   
		    DRMSContainer_t withSeriesPrimeKeys;
		    DRMSContainer_t withSeriesSegs;
		    DRMS_Record_t *withRecTemplate = NULL;
		    
		    if (!error)
		    {
			 withRecTemplate = drms_template_record(drms_env, 
								withSeriesName, 
								&status);
			 error = (status != DRMS_SUCCESS);
		    }

		    if (!error)
		    {
			 error = CreateDRMSPrimeKeyContainer(withRecTemplate, 
							     &withSeriesPrimeKeys, 
							     ReleaseHContainer);
		    }
		    
		    if (!error)
		    {
			 CreateDRMSSegmentContainer(withRecTemplate, &withSeriesSegs, NULL);
		    }

		    if (!error)
		    {
			 error = ValidateBinaryOperands(drms_env, 
							recSetIn, 
							segNameArr, 
							nSegs, 
							&inSeriesPrimeKeys, 
							&inSeriesSegs, 
							&withSeriesPrimeKeys, 
							&withSeriesSegs, 
							&segsToProc);
		    }

		    DestroyDRMSContainer(&withSeriesPrimeKeys);
		    DestroyDRMSContainer(&withSeriesSegs);
	       }
	       else if (!error)
	       {
		    /* Unary op */
		   
		    int nSegsToProc = CreateMatchingSegs(&inSeriesSegs, 
							 &inSeriesSegs,
							 &segsToProc);
		    
		    if (nSegsToProc == -1)
		    {
			 error = 1;
		    }
	       }

	       /* Validate output series */
	       if (!error)
	       {
		    if (bSeriesOut && bSeriesOutExists)
		    {
			 if(!bSeriesOutEqSeriesIn)
			 {
			      /* Ensure that the existing outSeries' prime keys match 
			       * those of inSeries. */
			      if (!KeysEqual(&inSeriesPrimeKeys, &outSeriesPrimeKeys))
			      {
				   error = 1;
			      }
			      
			      /* Ensure that the existing outSeries' segments are a superset of 
			       * those of segToProc. 
			       */ 
			      if (!error)
			      {
				   int nSegsToProc = CreateMatchingSegs(&outSeriesSegs, 
									&segsToProc, 
									NULL);
				   
				   if (nSegsToProc == -1)
				   {
					error = 1;
				   }
				   else if (nSegsToProc != segsToProc.items->num_total)
				   {
					error = 1;
				   }
			      }
			 }

			 strncpy(actualOutputSeries, seriesOut, sizeof(actualOutputSeries));
		    }
		    else if (bSeriesOut && !bSeriesOutExists)
		    { 
		       /*
			 error = (DRMS_SUCCESS != 
				  drms_create_seriesfromseries(drms_env, 
							       inSeriesName, 
							       seriesOut, 
							       1, 0, 2, 1, 
							       kOutSeriesDesc,
							       1));
		       */
		       error = 1;
		       fprintf(stdout, "Need to call drms_create_seriesfromtemplate()!!!\n");
		       XASSERT(0);

			 if (!error)
			 {
			      strncpy(actualOutputSeries, seriesOut, sizeof(actualOutputSeries));
			 }
		    }
		    else
		    {
			 strncpy(actualOutputSeries, inSeriesName, sizeof(actualOutputSeries));
		    }
	       }

	       /* Do operation */
	       if (!error)
	       {
		    if (bBinaryOp == 1)
		    {
			 error = DoBinaryOp(drms_env,
					    op,
					    &inSeriesPrimeKeys, 
					    &segsToProc, 
					    inSeriesName,
					    recSetIn,
					    withSeriesName, 
					    withRecSet,
					    actualOutputSeries);
		    }
		    else
		    {
			 error = DoUnaryOp(drms_env,
					   op,
					   &inSeriesPrimeKeys, 
					   &segsToProc, 
					   inSeriesName,
					   recSetIn,
					   actualOutputSeries);
		    }

	       } /* Do operation */

	       DestroyDRMSContainer(&inSeriesPrimeKeys);
	       DestroyDRMSContainer(&inSeriesSegs);
	       DestroyDRMSContainer(&segsToProc);
	  } /* main code */
     } /* drms env exists */
     
     return error;
}
