#include "jsoc_main.h"
#include "drms_types.h"
#include <sys/file.h>

char *module_name = "dscp";

typedef enum
{
   kDSCPErrSuccess     = 0,
   kDSCPErrBadArgs     = 1,
   kDSCPErrCantOpenRec = 2
} DSCPError_t;

#define kRecSetIn      "rsin"
#define kDSOut         "dsout"
#define kUndef         "undef"
#define kMaxChunkSize  8192
#define kGb            1073741824

ModuleArgs_t module_args[] =
{
   {ARG_STRING, kRecSetIn, kUndef,  "Input record-set specification."},
   {ARG_STRING, kDSOut,    kUndef,  "Output data series."},
   {ARG_END}
};

static int SeriesExist(DRMS_Env_t *env, const char *rsspec, char ***names, int *nseries, int *ostat)
{
   int rv = 0;
   char *allvers = NULL;
   char **sets = NULL;
   DRMS_RecordSetType_t *settypes = NULL;
   char **snames = NULL;
   int nsets = 0;
   int istat;

   if ((istat = drms_record_parserecsetspec(rsspec, &allvers, &sets, &settypes, &snames, &nsets, NULL)) == DRMS_SUCCESS)
   {
      int iseries;

      for (iseries = 0; iseries < nsets; iseries++)
      {
         rv = drms_series_exists(env, snames[iseries], &istat);
         if (istat != DRMS_SUCCESS)
         {
            fprintf(stderr, "Problems checking for series '%s' existence.\n", snames[iseries]);
            rv = 0;
            break;
         }
         else if (rv == 0)
         {
            break;
         }
      }
   }
   else
   {
      fprintf(stderr, "dscp FAILURE: invalid record-set specification %s.\n", rsspec);
      rv = 0;
   }

   if (istat == DRMS_SUCCESS)
   {
      int iseries;

      if (nseries)
      {
         *nseries = nsets;
      }

      if (names)
      {
         *names = (char **)malloc(nsets * sizeof(char *));

         for (iseries = 0; iseries < nsets; iseries++)
         {
            (*names)[iseries] = strdup(snames[iseries]);
         }
      }
   }

   drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, nsets);

   if (ostat)
   {
      *ostat = istat;
   }

   return rv;
}

/* returns 0 on error, otherwise it returns an estimated good-chunk size (the
 * number of records whose SUs comprise a GB of storage). */
static int CalcChunkSize(DRMS_Env_t *env, const char *sname)
{
   int rv = 0;
   int istat = DRMS_SUCCESS;

   if (istat)
   {
      fprintf(stderr, "Problem counting records, internal error %d.\n", istat);
   }
   else
   {
      /* Open latest record to get file sizes. Use this as a (poor) estimate of 
       * the file size of each segment. Use an SQL query, because this is the only 
       * way to get the 'last' record in a series without knowing anything about 
       * the series (i.e., the number of prime keys). */
      char query[2048];
      DB_Binary_Result_t *res = NULL;

      /* series-name case matters not */
      snprintf(query, sizeof(query), 
               "SELECT sunum FROM %s WHERE recnum = (SELECT max(recnum) FROM %s)", 
               sname, 
               sname);
      res = drms_query_bin(env->session, query);

      if (res) 
      {
         if (res->num_rows == 1 && res->num_cols == 1)
         {
            long long sunum = db_binary_field_getint(res, 0, 0);
            SUM_info_t **info = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * 1);

            /* Get file-size info from SUMS. */
            if (drms_getsuinfo(env, &sunum, 1, info) == DRMS_SUCCESS)
            {
               if (info[0]->online_loc == '\0')
               {
                  /* Not online or invalid SUNUM. Give up and use maximum chunk size. */
                  rv = kMaxChunkSize;
               }
               else
               {
                  rv =  (int)((double)kGb / info[0]->bytes);
               }
            }
            else
            {
               fprintf(stderr, "Unable to get SU size for sunum %lld.\n", sunum);
            }

            if (info)
            {
               int iinfo;

               for (iinfo = 0; iinfo < 1; iinfo++)
               {
                  if (info[iinfo])
                  {
                     free(info[iinfo]);
                     info[iinfo] = NULL;
                  }
               }

               free(info);
            }
         }
         else
         {
            fprintf(stderr, "Unexpected db response to query '%s'.\n", query);
         }

         db_free_binary_result(res);
         res = NULL;
      }
   }

   return rv;
}

static int ProcessRecord(DRMS_Record_t *recin, DRMS_Record_t *recout)
{
   HIterator_t *iter = NULL;
   char infile[DRMS_MAXPATHLEN];
   char outfile[DRMS_MAXPATHLEN];
   DRMS_Segment_t *segin = NULL;
   DRMS_Segment_t *segout = NULL;
   int rv = 0;

   /* copy keywords to output records */
   if (!drms_copykeys(recout, recin, 1, kDRMS_KeyClass_Explicit))
   {
      /* copy segment files */
      while ((segin = drms_record_nextseg(recin, &iter, 0)) != NULL)
      {
         /* Get output segment */
         segout = drms_segment_lookup(recout, segin->info->name);

         if (segout)
         {
            *infile = '\0';
            *outfile = '\0';

            if (recin->sunum != -1LL)
            {
               /* skip input records that have no SU associated with them */
               drms_segment_filename(segin, infile);
               drms_segment_filename(segout, outfile);

               if (*infile != '\0' && *outfile != '\0')
               {
                  if (copyfile(infile, outfile) != 0)
                  {
                     fprintf(stderr, "failure copying file '%s' to '%s'.\n", infile, outfile);
                     rv = 1;
                  }
               }
            }
         }
      }

      if (iter)
      {
         free(iter);
         iter = NULL;
      }
   }
   else
   {
      fprintf(stderr, "copy keys failure.\n");
      rv = 1;
   }

   return rv;
}

int DoIt(void)
{
   DSCPError_t rv = kDSCPErrSuccess;
   int status;
   
   const char *rsspecin = NULL;
   const char *dsspecout = NULL;
   char **seriesin = NULL;
   int nseriesin = 0;

   rsspecin = cmdparams_get_str(&cmdparams, kRecSetIn, NULL);
   dsspecout = cmdparams_get_str(&cmdparams, kDSOut, NULL);

   /* If both rsspecin and dsout are missing, use positional arguments. */
   if (strcasecmp(rsspecin, kUndef) == 0 && strcasecmp(dsspecout, kUndef) == 0)
   {
      /* cmdparams counts the executable as the first argument! */
      if (cmdparams_numargs(&cmdparams) >= 3)
      {
         rsspecin = cmdparams_getarg(&cmdparams, 1);
         dsspecout = cmdparams_getarg(&cmdparams, 2);
      }
      else
      {
         rv = kDSCPErrBadArgs;
      }
   }

   if (strcasecmp(rsspecin, kUndef) == 0 || strcasecmp(dsspecout, kUndef) == 0)
   {
      rv = kDSCPErrBadArgs;
   }

   if (rv == kDSCPErrSuccess)
   {
      /* Make sure input and output series exist. */
      int exists;

      rv = kDSCPErrBadArgs;
      exists = SeriesExist(drms_env, rsspecin, &seriesin, &nseriesin, &status);
      if (status)
      {
         fprintf(stderr, "LibDRMS error  %d.\n", status);
      }
      else if (exists)
      {
         exists = SeriesExist(drms_env, dsspecout, NULL, NULL, &status);
         if (status)
         {
            fprintf(stderr, "LibDRMS error  %d.\n", status);
         }
         else
         {
            rv = kDSCPErrSuccess;
         }
      }
   }

   if (rv == kDSCPErrSuccess)
   {
      /* Operate on record chunks. */
      DRMS_RecordSet_t *rsin = NULL;
      DRMS_RecordSet_t *rsout = NULL;
      DRMS_RecordSet_t *rsfinal = NULL;
      int chunksize = 0;
      int newchunk = 0;
      DRMS_RecChunking_t cstat;
      DRMS_Record_t *recin = NULL;
      DRMS_Record_t *recout = NULL;
      int irec;

      /* Determine good chunk size (based on first input series). */
      chunksize = CalcChunkSize(drms_env, seriesin[0]);
      if (drms_recordset_setchunksize(chunksize))
      {
         fprintf(stderr, "Unable to set record-set chunk size of %d; using default.\n", chunksize);
         chunksize = drms_recordset_getchunksize();
      }

      rsin = drms_open_recordset(drms_env, rsspecin, &status);
      if (status)
      {
         fprintf(stderr, "Invalid record-set specification %s.\n", rsspecin);
         rv = kDSCPErrBadArgs;
      }
      else if (rsin && rsin->n > 0)
      {
         /* Stage the records. The retrieval is actually deferred until the drms_recordset_fetchnext()
          * call opens a new record chunk. */
         drms_sortandstage_records(rsin, 1, 0, NULL); 

         /* Create a record-set that contains only "good" output records. Records for which 
          * there was some kind of failure do not get put into rsfinal. */
         rsfinal = malloc(sizeof(DRMS_RecordSet_t));
         memset(rsfinal, 0, sizeof(DRMS_RecordSet_t));
         irec = 0;

         while ((recin = drms_recordset_fetchnext(drms_env, rsin, &status, &cstat, &newchunk)) != NULL)
         {
            if (status != DRMS_SUCCESS)
            {
               fprintf(stderr, "Unable to fetch next input record.\n");
               rv = kDSCPErrCantOpenRec;
               break;
            }

            if (newchunk)
            {
               /* Close old chunk of output records (if one exists). */
               if (rsout)
               {
                  /* All "good" records will have been saved in rsfinal, so we can
                   * free all rsout records. */
                  drms_close_records(rsout, DRMS_FREE_RECORD);
                  rsout = NULL;
               }

               /* Create a chunk of output records. */
               rsout = drms_create_records(drms_env, chunksize, dsspecout, DRMS_PERMANENT, &status);
               if (status || !rsout || rsout->n != chunksize)
               {
                  fprintf(stderr, "Failure creating output records.\n");
                  break;
               }
               
               irec = 0;
            }

            recout = drms_recordset_fetchnext(drms_env, rsout, &status, NULL, NULL);
            if (status != DRMS_SUCCESS)
            {
               fprintf(stderr, "Unable to fetch next output record.\n");
               rv = kDSCPErrCantOpenRec;
               break;
            }

            status = ProcessRecord(recin, recout);

            if (status != 0)
            {
               fprintf(stderr, "Failure processing record.\n");
               break;
            }
            else
            {
               /* Successfully processed record - save output record in rsfinal. */
               drms_merge_record(rsfinal, recout);
               XASSERT(rsout->records[irec] == recout);

               /* Renounce ownership (if this isn't done, the calls to drms_close_records(rsout) and 
                * drms_close_records(rsfinal) will attempt to free the same record.). */
               rsout->records[irec] = NULL;
            }

            irec++;
         }

         /* free remaining output records not freed in while loop. */
         if (rsout)
         {
            drms_close_records(rsout, DRMS_FREE_RECORD);
            rsout = NULL;
         }

         if (rsfinal)
         {
            drms_close_records(rsfinal, DRMS_INSERT_RECORD);
         }
      }
      else
      {
         fprintf(stderr, "No records to process.\n");
      }

      /* Close input records. */
      drms_close_records(rsin, DRMS_FREE_RECORD);      
   }

   if (seriesin)
   {
      int iseries;

      for (iseries = 0; iseries < nseriesin; iseries++)
      {
         if (seriesin[iseries])
         {
            free(seriesin[iseries]);
         }
      }

      free(seriesin);
   }

   return rv;
}
