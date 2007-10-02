/**** procFdsData.c ****/

/* Process FDS state-vector data.  This involves converting location coordinates and 
 * interpolating the location and velocity vectors generate values for a 
 * once-a-second cadence.
 */

#include "drms.h"
#include "serverdefs.h"
#include "drms_types.h"
#include "drms_record.h"
#include "drms_names.h"
#include "drms_env.h"
#include "drms_network.h"
#include "drms_keyword.h"

#define kDefaultSeries "sdo.fdsStateVectors"
#define kMaxKeys 1024

#define PI   (M_PI)



/**** ConvAndInterpFDS ****/

/* seriesName - fully qualified name of the series that contains the raw FDS state-vector
 * data.
 *
 * dateRange - UT time range, expressed as a <ValueRangeSet> 
 * (see http://jsoc.stanford.edu/trac/wiki/DrmsNames).  An example is:
 * "2006.04.30_03:13-2006.05.07_8:28,2006.08.10_00:00-2006.08.22_10:20"
 *
 *
 */


/* Print the fields of a keyword struct to stdout. */
static void KeywordSnprintval(DRMS_Keyword_t *key, char *buf, size_t size)
{
     switch(key->info->type)
     {
     case DRMS_TYPE_CHAR: 
       snprintf(buf, size, key->info->format, key->value.char_val);
       break;
     case DRMS_TYPE_SHORT:
       snprintf(buf, size, key->info->format, key->value.short_val);
       break;
     case DRMS_TYPE_INT:  
       snprintf(buf, size, key->info->format, key->value.int_val);
       break;
     case DRMS_TYPE_LONGLONG:  
       snprintf(buf, size, key->info->format, key->value.longlong_val);
       break;
     case DRMS_TYPE_FLOAT:
       snprintf(buf, size, key->info->format, key->value.float_val);
       break;
     case DRMS_TYPE_DOUBLE: 	
       snprintf(buf, size, key->info->format, key->value.double_val);
       break;
     case DRMS_TYPE_TIME: 
       {
	    char timeBuf[1024];
	    sprint_time(timeBuf, key->value.time_val, key->info->format, 0);
	    snprintf(buf, size, "%s", timeBuf);
       }
       break;
     case DRMS_TYPE_STRING: 
       snprintf(buf, size, key->info->format, key->value.string_val);
       break;
     default:
       break;
     }
}

int ConvAndInterpFDS(DRMS_Env_t *drmsEnv, char *seriesName, char *dateRange)
{
     char *series = NULL;
     char *dates = NULL;
     int error = 0;

     if (drmsEnv == NULL)
     {
	  error = 1;
     }
     else
     {
	  if (seriesName != NULL)
	  {
	       series = strdup(seriesName);
	  }
	  else
	  {
	       series = strdup(kDefaultSeries);
	  }
	  
	  if (dateRange != NULL && series != NULL)
	  {
	       /* get prime key (should only be one) */
	       int nPrime;
	       DRMS_Keyword_t **primeKeys = NULL;
	       
	       /* get series */
	       DRMS_Record_t *template = drms_template_record(drmsEnv, series, &error);
	       
	       if (template == NULL || error != 0)
	       {
		    printf("Series '%s' does not exist. drms_template_record returned "
			   "status = %d\n", series, error);
		    error = 1;
	       }
	       else
	       {
		    nPrime = template->seriesinfo->pidx_num;
		    primeKeys = template->seriesinfo->pidx_keywords;
		    
		    if (nPrime == 1)
		    {
			 size_t len = strlen(primeKeys[0]->info->name) + strlen(dateRange) + 3;
			 dates = malloc(sizeof(char) * len + 1);
			 if (dates != NULL)
			 {
			      snprintf(dates, len + 1, "[%s=%s]", primeKeys[0]->info->name, dateRange);
			 }
			 else
			 {
			      error = 1;
			 }
		    }
		    else
		    {
			 printf("Unexpected number of prime keys.\n");
			 error = 1;
		    }
	       }
	  }
	  
	  if (error == 0 && series != NULL)
	  {
	       char *recSet = NULL;
	       size_t len = strlen(series);
	       
	       if (dates != NULL)
	       {
		    len += strlen(dates);
		    recSet = malloc(sizeof(char) * len + 1);
		    if (recSet != NULL)
		    {
			 snprintf(recSet, len + 1, "%s%s", series, dates);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	       else
	       {
		    recSet = strdup(series);
	       }
	       
	       if (error == 0 && recSet != NULL)
	       {
		    DRMS_RecordSet_t *recordSet = NULL;
		    int nRecs = 0;
		    
		    recordSet = drms_open_records(drmsEnv, recSet, &error);
		    
		    if (error != 0 || recordSet == NULL) 
		    {
			 printf("drms_open_records failed, recSet=%s, error=%d.  Aborting.\n", recSet, error);
		    }
		    else
		    {
			 nRecs = recordSet->n;
			 
			 if (nRecs == 0)
			 {
			      printf ("** No records in selected data set **\n");
			 }
			 else
			 {
			      int firstRec = 0;
			      int lastRec = nRecs - 1; /* zero-based index */
			      int iRec;
			      
			      /* loop over set of selected records */
			      for (iRec = firstRec; iRec <= lastRec; iRec++) 
			      {
				   /* pointer to current record */
				   DRMS_Record_t *rec = recordSet->records[iRec];  
				   DRMS_Keyword_t *keys[kMaxKeys];
				   bzero(keys, sizeof(DRMS_Keyword_t *) * kMaxKeys);

				   int nKeys = 0;
				   
				   /* get all keys */
				   DRMS_Keyword_t *key = NULL;
				   HIterator_t hit;
				   hiter_new(&hit, &rec->keywords);
				   while ((key = (DRMS_Keyword_t *)hiter_getnext(&hit)))
				   {
					keys[nKeys++] = key;
				   }
				   
				   /* XXX - insert Jesper's conversion function here */

				   /* for now, just print out the keys to verify the right 
				    * records were  obtained
				    */
				   
				   /* print out header */
				   if (iRec == firstRec)
				   {
					int nKeysC = nKeys;
					while (nKeysC > 0)
					{
					     DRMS_Keyword_t *key = keys[nKeys - nKeysC];
					     char buf[1024];
					     KeywordSnprintval(key, buf, sizeof(buf));
					     
					     int keyValLen = strlen(buf);
					     int nSp = keyValLen - strlen(key->info->name);

					     if (nSp < 0)
					     {
						  char *trunc = strdup(key->info->name);
						  trunc[strlen(key->info->name) + nSp] = '\0';
						  printf("%s", trunc);
					     }
					     else
					     {
						  printf("%s", key->info->name);
						  nSp += 5; /* there are 5 spaces betw cols */

						  while (nSp > 0)
						  {
						       printf(" ");
						       nSp--;
						  }
					     }

					     nKeysC--;
					}

					printf("\n");
				   }
				   
				   int iKey;

				   for (iKey = 0; iKey < nKeys; iKey++) 
				   {
					if (keys[iKey])
					{
					     if (iKey > 0)
					     {
						  printf ("     ");
					     }

					     drms_keyword_printval(keys[iKey]);
					}
					else
					{
					     printf("MISSING");
					}
				   }

				   printf("\n");
			      }
			 }
		    }
		    
		    if (recordSet != NULL)
		    {
			 drms_close_records(recordSet, DRMS_FREE_RECORD);
		    }
	       }

	       if (recSet != NULL)
	       {
		    free(recSet);
	       }
	  }
     }

     if (series != NULL)
     {
	  free(series);
     }
     
     if (dates != NULL)
     {
	  free(dates);
     }
     
     return error;
}
