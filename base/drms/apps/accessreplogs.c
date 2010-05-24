/* accessreplogs.c 
 *   Called by archive_slon_logs to ingest into SUMS, and by retrieve_logs to retrieve from 
 *   SUMS, a set of log files. The set is identified by a range of counter numbers.
 *
 *   parameters:
 *     rsq - record-set query that identifies the log files desired
 */

/* Slony-replication log-shipping files must be stored in tar files. Each tar file contains multiple
 * log files. Embedded in the name of each tar file is the string <counter_begin>-<counter_end> that 
 * identifies the inclusive range of counters of the logs contained in the tar file. The data
 * series containing the tar files has two keywords that hold the being and end counter values.
 * The calling program, when retrieving logs, must provide this range in the beg and end parameter.
 * Internally, this program will do a check for:
 *   (cbegin < beg && cend > beg) || (cbegin > beg && cend < end) || (cbegin < end && cend > end)
 * where cbegin and cend are the names of the DRMS keywords in the log-file dataseries.
 *
 * Examples:
 *  accessreplogs logs=su_arta.slonylogs path=/home/arta/slonydev/testarchive/archive action=str regexp="slogs_([0-9]+)-([0-9]+)[.]tar[.]gz"
 *
 *  accessreplogs logs=su_arta.slonylogs path=/home/arta/slonydev/testarchive/retrieved beg=93900 end=94000 action=ret
 *
 * To read the tar, make sure you use the "-z" flag (some implementations of tar require that, some do not).
 */

/*
  Current .jsd
  
#=====General Series Information=====
Seriesname:      	su_production.slonylogs
Author:          	"Art Amezcua"
Owner:           	slony
Unitsize:        	1
Archive:         	1
Retention:       	40
Tapegroup:       	1
PrimeKeys:       	obsdate
DBIndex:                obsdate, cbegin, cend
Description:     	"This series contains ingested, pre-launch FDS product files."

#=====Keywords=====

# The date that the first file in the archive was created
Keyword:obsdate, time, ts_eq, record, DRMS_MISSING_VALUE, 0, UTC, "Date embedded in product file name"

# Currently, the tar-file generation code runs at midnight each day. So, don't align the edge of 
# a slot on midnight.
Keyword:obsdate_epoch, time, constant, record, 1993.01.01_00:00:00_UTC, 0, UTC, "MDI epoch - center slots at midnight of each day"

# Currently, the tar files are created once a day, so slots less than one-day wide should be safe (but 
# slots one-day wide are not if the slot boundary is close to the time when the files are created). However, 
# we don't know if this interval will change, and it is possible that somebody could manually run the tar-
# file generation code twice in quick succession (which could lead to a collision). With a ten-minute
# slot width, it is unlikely that such a collision could occur.
Keyword:obsdate_step, time, constant, record, 10.00000, %f, mins, "Slots are 1 day wide"
Keyword:cbegin, int, variable, record, DRMS_MISSING_VALUE, %lld, NA, "Slony counter value of the first log in the tar."
Keyword:cend, int, variable, record, DRMS_MISSING_VALUE, %lld, NA, "Slony counter value of the last log in the tar."
Keyword:DATE, time, variable, record, DRMS_MISSING_VALUE, 0, ISO, "Date of product-file ingestion; ISO 8601."
Keyword:extra, string, variable, record, "", %s, NA, "An extra field, just in case we realize we want to store something else later one."

#=====Segments=====
Data: slonylog, variable, string, 0, NA, generic, "Original slony log-replication sql file."


 */

#include "jsoc_main.h"
#include <regex.h>
#include <dirent.h>

char *module_name = "accessreplogs";

typedef enum
{
   kARLErr_Success = 0,
   kARLErr_MissingSU,
   kARLErr_Argument,
   kARLErr_LogFileSeries,
   kARLErr_FileIO,
   kARLErr_InvalidLog,
   kARLErr_InvalidRange
} ARLError_t;


#define kLogs    "logs"
#define kPath    "path"
#define kAction  "action"
#define kLFRegEx "regexp"
#define kBegin   "beg"
#define kEnd     "end"

#define kActionRetrieve  "ret"
#define kActionStore     "str"

#define kDefRegexp       "slogs_([0-9]+)-([0-9]+)[.]tar[.]gz"


/* DRMS keywords */
#define kObsDate         "obsdate"
#define kCBegin          "cbegin"
#define kCEnd            "cend"


ModuleArgs_t module_args[] =
{
   {ARG_STRING, 
    kLogs,    
    NULL,  
    "Dataseries containing the slony log files",
    NULL},

   {ARG_STRING, 
    kPath,   
    NULL,  
    "Path of log files or a single log file to ingest, or of location to copy logs to",  
    NULL},

   {ARG_STRING, 
    kAction, 
    NULL,  
    "Store, retrieve, delete, etc. sql logs from SUMS",
    NULL},

   {ARG_STRING, 
    kLFRegEx, 
    kDefRegexp,  
    "POSIX regular expression that matches the log-file file names",
    NULL},

   /*
     Don't add these, because this would cause cmdparams to generate 
     arguments even if the user didn't provide them. It would be
     better if cmdparams had some flag to ignore these - but it 
     it nice to have these here so we have a complete list of 
     possible arguments.

   {ARG_STRING, 
    kBegin, 
    NULL,  
    "Beginning of counter range of desired log files",
    NULL},

   {ARG_STRING, 
    kEnd, 
    NULL,
    "End of counter range of desired log files",
    NULL},
   */

   {ARG_END}
};

static ARLError_t GetLogPaths(DRMS_RecordSet_t *rs, char ***paths)
{
   ARLError_t err = kARLErr_Success;
   int irec;
   DRMS_Record_t *rec = NULL;
   DRMS_Segment_t *seg = NULL;
   char filepath[DRMS_MAXPATHLEN];

   if (rs && rs->n > 0 && paths)
   {
      *paths = (char **)malloc(sizeof(char *) * rs->n);

      /* Bring all SUs online and assign all rec->su's */
      if (drms_stage_records(rs, 1, 0) != DRMS_SUCCESS)
      {
         err = kARLErr_MissingSU;
      }

      for (irec = 0; irec < rs->n; irec++)
      {
         rec = rs->records[irec];
         seg = drms_segment_lookupnum(rec, 0);

         if (seg)
         {
            drms_segment_filename(seg, filepath);

            if (strlen(filepath))
            {
               (*paths)[irec] = strdup(filepath);
            }
            else
            {
               (*paths)[irec] = NULL;
            }
         }
         else
         {
            /* error */
            fprintf(stderr, "Expected segment not found.\n");
            err = kARLErr_LogFileSeries;
            break;
         }
      }
   }
   else
   {
      fprintf(stderr, "Invalid arguments to GetLogPaths().\n");
      err = kARLErr_Argument;
   }

   return err;
}

DRMS_RecordSet_t *RetrieveRecords(const char *logs, int64_t beg, int64_t end, ARLError_t *err)
{
   DRMS_RecordSet_t *rs = NULL;
   int status = DRMS_SUCCESS;

   char query[DRMS_MAXQUERYLEN];

   snprintf(query, 
            sizeof(query), 
            "%s[? (%s <= %lld AND %s >= %lld) OR (%s > %lld AND %s < %lld) OR (%s <= %lld AND %s >= %lld) ?]", 
            logs,
            kCBegin, (long long)beg, kCEnd, (long long)beg, 
            kCBegin, (long long)beg, kCEnd, (long long)end, 
            kCBegin, (long long)end, kCEnd, (long long)end);

   rs = drms_open_records(drms_env, query, &status);

   if (status || !rs || rs->n == 0)
   {
      if (err)
      {
         *err = kARLErr_InvalidRange;
      }
   }

   return rs;
}

ARLError_t IngestFile(const char *path, const char *basefile, DRMS_Record_t *orec, regex_t *regexp)
{
   ARLError_t err = kARLErr_InvalidLog;

   char dirEntry[PATH_MAX] = {0};
   regmatch_t matches[3]; /* index 0 - the entire string */
   char *bcountertxt = NULL;
   char *ecountertxt = NULL;
   long long bcounter;
   long long ecounter;
   DRMS_Segment_t *seg = NULL;
   int indx;
   char *filetmp = NULL;
   struct stat stBuf;
   DRMS_RecordSet_t *rs = NULL;

   filetmp = strdup(basefile);

   /* store a single match */
   if (regexec(regexp, filetmp, 3, matches, 0) == 0)
   {
      /* begin counter number part of log-file name */
      filetmp[matches[1].rm_eo] = '\0';
      indx = matches[1].rm_so;
      bcountertxt = strdup(filetmp + indx);
      sscanf(bcountertxt, "%lld", &bcounter);

      /* end counter number part of log-file name */
      filetmp[matches[2].rm_eo] = '\0';
      indx = matches[2].rm_so;
      ecountertxt = strdup(filetmp + indx);
      sscanf(ecountertxt, "%lld", &ecounter);

      snprintf(dirEntry, 
               sizeof(dirEntry), 
               "%s%s%s", 
               path, 
               path[strlen(path) - 1] == '/' ? "" : "/",
               basefile);

      if (*dirEntry != '\0' && !stat(dirEntry, &stBuf));
      {
         if (S_ISREG(stBuf.st_mode))
         {
            fprintf(stdout, "Archiving '%s'.\n", dirEntry);

            /* Ensure that the counter hasn't already been ingested */
            rs = RetrieveRecords(orec->seriesinfo->seriesname, bcounter, ecounter, &err);

            if (err != kARLErr_InvalidRange)
            {
               fprintf(stderr, "The file to be ingested (%s) has content from files that has been previously ingested. The previous content will be hidden (must use [! !] query to recover).\n", dirEntry);
            }

            if (rs)
            {
               drms_close_records(rs, DRMS_FREE_RECORD);
            }

            err = kARLErr_InvalidLog;

            /* Copy the file into SUMS */
            seg = drms_segment_lookupnum(orec, 0);

            if (seg)
            {
               char filepath[DRMS_MAXPATHLEN];
               TIME modtime;

               snprintf(seg->filename, DRMS_MAXSEGFILENAME, "%s", basefile);
               drms_segment_filename(seg, filepath);
               copyfile(dirEntry, filepath);
            
               /* Set the keywords */
               modtime = stBuf.st_mtime + UNIX_EPOCH;

               drms_setkey_time(orec, kObsDate, modtime);
               drms_setkey_longlong(orec, kCBegin, bcounter);
               drms_setkey_longlong(orec, kCEnd, ecounter);
               drms_keyword_setdate(orec);

               err = kARLErr_Success;
            }
         }
         else
         {
            fprintf(stderr, "Not archiving '%s'; it doesn't look like a log file.\n", dirEntry);
         }
      }
   }

   if (bcountertxt)
   {
      free(bcountertxt);
   }

   if (ecountertxt)
   {
      free(ecountertxt);
   }

   if (filetmp)
   {
      free(filetmp);
   }

   return err;
}

int DoIt(void) 
{
   ARLError_t err = kARLErr_Success;

   const char *logs = cmdparams_get_str(&cmdparams, kLogs, NULL);
   const char *path = cmdparams_get_str(&cmdparams, kPath, NULL);
   const char *action = cmdparams_get_str(&cmdparams, kAction, NULL);
   const char *pat = cmdparams_get_str(&cmdparams, kLFRegEx, NULL);
   int64_t beg = cmdparams_get_int64(&cmdparams, kBegin, NULL);
   int64_t end = cmdparams_get_int64(&cmdparams, kEnd, NULL);

   DRMS_RecordSet_t *rs = NULL;
   char **paths = NULL;
   int ipath;
   int irec;

   struct stat stBuf;

   int status = DRMS_SUCCESS;

   /* branch on action */
   if (strcasecmp(action, kActionRetrieve) == 0)
   {
      rs = RetrieveRecords(logs, beg, end, &err);
      
      if (!err && rs && rs->n > 0)
      {
         if ((err = GetLogPaths(rs, &paths)) == kARLErr_Success)
         {
            /* path identifies a directory, or file, to write the log file to */
            if (!stat(path, &stBuf))
            {
               if (S_ISREG(stBuf.st_mode))
               {
                  if (rs->n == 1)
                  {
                     /* Copy file to path */
                     if (copyfile(paths[0], path) != 0)
                     {
                        fprintf(stderr, "Error copying file from '%s' to '%s'.\n", paths[0], path);
                        err = kARLErr_FileIO;
                     }
                  }
                  else
                  {
                     /* tar file - to be implemented (use libtar)*/
                     fprintf(stderr, "Archiving log files into a tar file not supported yet.\n");
                  }
               }
               else if (S_ISDIR(stBuf.st_mode))
               {
                  char outpath[PATH_MAX];
                  char *pbase = NULL;

                  /* copy all files to a directory */
                  for (irec = 0; irec < rs->n; irec++)
                  {
                     pbase = strrchr(paths[irec], '/');
                     if (pbase)
                     {
                        snprintf(outpath, sizeof(outpath), "%s/%s", path, pbase + 1);
                     }
                     else
                     {
                        snprintf(outpath, sizeof(outpath), "%s/%s", path, paths[irec]);
                     }

                     if (copyfile(paths[irec], outpath) != 0)
                     {
                        fprintf(stderr, "Error copying file from '%s' to '%s'.\n", paths[irec], outpath);
                        err = kARLErr_FileIO;
                        break;
                     }
                  }
               }
               else
               {
                  /* error */
                  fprintf(stderr, "Unsupported file type for argument '%s'.\n", path);
                  err = kARLErr_Argument;
               }
            }
            else
            {
               /* error - not a valid directory or file */
               fprintf(stderr, "File '%s' is not a valid directory or regular file.\n", path);
               err = kARLErr_FileIO;
            }
         }
         else
         {
            fprintf(stderr, "Error fetching log files from SUMS.\n");
         }
      }
      else
      {
         /* error - log-file seriesname misspelled or the series is empty */
         fprintf(stderr, "Invalid log-file dataseries (or series is empty).\n");
         err = kARLErr_LogFileSeries;
      }

      if (paths)
      {
         for (ipath = 0; ipath < rs->n; ipath++)
         {
            if (paths[ipath])
            {
               free(paths[ipath]);
            }
         }

         free(paths);
      }

      if (rs)
      {
         drms_close_records(rs, DRMS_FREE_RECORD);
      }
   }
   else if (strcasecmp(action, kActionStore) == 0)
   {
      /* path identifies a single log-file to ingest, or a directory of log-files to ingest */

      if (!stat(path, &stBuf))
      {
         if (S_ISREG(stBuf.st_mode))
         {
               
         }
         else if (S_ISDIR(stBuf.st_mode))
         {
            /* Count the number of files in the directory */
            struct dirent **filelist = NULL;
            struct dirent *entry = NULL;
            int nfiles = -1;
            int ifile;
            char *oneFile = NULL;
            DRMS_RecordSet_t *rsouttmp = NULL;
            DRMS_RecordSet_t *rsout = NULL;
            regex_t regexp;

            if ((nfiles = scandir(path, &filelist, NULL, NULL)) > 0 && 
                filelist != NULL)
            {
               /* This may be more records than needed - use drms_record_merge() to 
                * remove unwanted ones. */
               char *logstmp = strdup(logs);

               rsouttmp = drms_create_records(drms_env, nfiles, logstmp, DRMS_PERMANENT, &status);
               rsout = malloc(sizeof(DRMS_RecordSet_t));
               memset(rsout, 0, sizeof(DRMS_RecordSet_t));

               free(logstmp);

               /* regular expression that pulls out the counter index */
               if (regcomp(&regexp, pat, (REG_EXTENDED | REG_ICASE)) != 0)
               {
                  fprintf(stderr, "Bad regular expression '%s'.\n", pat);
                  err = kARLErr_Argument;
               }
               else
               {
                  irec = 0;
                  for (ifile = 0; ifile < nfiles; ifile++)
                  {
                     entry = filelist[ifile];
                     if (entry != NULL)
                     {
                        DRMS_Record_t *orec = rsouttmp->records[irec];
                        ARLError_t ret;

                        oneFile = entry->d_name;
                        ret = IngestFile(path, oneFile, orec, &regexp);
                        if (ret == kARLErr_Success)
                        {
                           drms_merge_record(rsout, rsouttmp->records[irec]);
                           rsouttmp->records[irec] = NULL; /* relinquish ownership */
                           irec++;
                        }

                        free(entry);
                     }
                  }
               }

               free(filelist);
            }

            if (rsout)
            {
               drms_close_records(rsout, DRMS_INSERT_RECORD);
            }

            if (rsouttmp)
            {
               drms_close_records(rsouttmp, DRMS_FREE_RECORD);
            }

            regfree(&regexp);
         }
         else
         {
            /* error */
            fprintf(stderr, "Unsupported file type for argument '%s'.\n", path);
            err = kARLErr_Argument;
         }
      }
      else
      {
         /* error - not a valid directory or file */
         fprintf(stderr, "File '%s' is not a valid directory or regular file.\n", path);
         err = kARLErr_FileIO;
      }
   }
   else
   {
      /* Unsupported action */
      
   }

   return err;
}
