#include "jsoc_main.h"
#include "drms_types.h"
#include "drms_storageunit.h"
#include "exputil.h"

char *module_name = "export";

typedef enum
{
   kMymodErr_Success = 0,
   kMymodErr_CantRegisterDefs,
   kMymodErr_MissingArg,
   kMymodErr_BadRequestID, 
   kMymodErr_BadRecSetQuery,
   kMymodErr_BadFilenameFmt,
   kMymodErr_ExportFailed,
   kMymodErr_CantOpenPackfile,
   kMymodErr_PackfileFailure,
   kMymodErr_UnsupportedPLRecType,
   kMymodErr_DRMS
} MymodError_t;

typedef enum
{
   kPL_metadata,
   kPL_content
} PLRecType_t;

#define kNotSpecified    "NOT SPECIFIED"
#define kArg_reqid       "reqid"
#define kArg_version     "version"
#define kArg_method      "method"
#define kArg_protocol    "protocol"
#define kArg_rsquery     "rsquery"
#define kArg_expSeries   "expseries"
#define kArg_ffmt        "ffmt"
#define kArg_path        "path"
#define kArg_clname      "kmclass"
#define kArg_kmfile      "kmfile"


#define kDef_expSeries   "jsoc.exports"

#define kPWD             "PWD"


/* If rsquery is provided as a cmd-line argument, then jsoc_export does not 
 * save the output data files to an export series.  Instead the caller
 * MUST provide a filename format string (which is a template that 
 * describes how to create the output file names).  The caller may also
 * provide the path argument to specify the directory into which the 
 * output files are to be written.  If the path argument is not specified,
 * then the output files are written to the current working directory.
 * If requery is omitted from the cmd-line, then an export series
 * must either be provided on the cmd-line, or the default series is used.
 * The query that determines what files are to be exported is found
 * in the export series by searching for the record with a matching
 * reqid.
 */
ModuleArgs_t module_args[] =
{
     {ARG_STRING, kArg_version, "",  "jsoc export version."},
     {ARG_STRING, kArg_reqid, "", 
        "Export series primary key value that identifies the output record."},
     {ARG_STRING, kArg_method, "",  "jsoc export method (eg, url or ftp)."},
     {ARG_STRING, kArg_protocol, "",  "file conversion method (eg, convert to fits)."},
     {ARG_STRING, kArg_rsquery, kNotSpecified, 
        "Record-set query that specifies data to be exported."},
     {ARG_STRING, kArg_expSeries, kDef_expSeries, "Series to which exported data are saved."},
     {ARG_STRING, kArg_ffmt, kNotSpecified, "Export filename template."},
     {ARG_STRING, kArg_path, kNotSpecified, "Path to which fits files are output."},
     {ARG_STRING, kArg_clname, kNotSpecified,  "Export key map class."},
     {ARG_STRING, kArg_kmfile, kNotSpecified,  "Export key map file."},
     {ARG_END}
};

char gDefBuf[PATH_MAX] = {0};

MymodError_t WritePListRecord(PLRecType_t rectype, FILE *pkfile, const char *f1, const char *f2)
{
   MymodError_t err = kMymodErr_Success;

   switch (rectype)
   {
      case kPL_metadata:
        fprintf(pkfile, "%s=%s\n", f1, f2);
        break;
      case kPL_content:
        fprintf(pkfile, "%s\t%s\n", f1, f2);
        break;
      default:
        fprintf(stderr, "Unsupported packing-list record type '%d'.\n", (int)rectype);
        err = kMymodErr_UnsupportedPLRecType;
   }

   return err;
}

/* Assumes tcount is zero on the first call.  This function adds 
 * the number of files exported to tcount on each call. */
static int MapexportRecordToDir(DRMS_Record_t *recin,
                                const char *ffmt,
                                const char *outpath,
                                FILE *pklist,
                                const char *classname, 
                                const char *mapfile,
                                int *tcount, 
                                MymodError_t *status)
{
   int drmsstat = DRMS_SUCCESS;
   MymodError_t modstat = kMymodErr_Success;
   HIterator_t *hit = NULL;
   DRMS_Segment_t *segin = NULL;
   unsigned long long tsize = 0;
   char dir[DRMS_MAXPATHLEN];
   char fmtname[DRMS_MAXPATHLEN];
   char fullfname[DRMS_MAXPATHLEN];
   char query[DRMS_MAXQUERYLEN];
   struct stat filestat;

   drms_record_directory(recin, dir, 1); /* This fetches the input data from SUMS. */

   /* Must create query from series name and prime keyword values */
   char buf[1028];
   int nkeys = recin->seriesinfo->dbidx_num;
   int snext = 0;
   int ikey = 0;

   snprintf(query, sizeof(query), "%s", recin->seriesinfo->seriesname);
   snext = strlen(query);

   for (ikey = 0; ikey < nkeys; ikey++)
   {
      drms_keyword_snprintfval(recin->seriesinfo->dbidx_keywords[ikey], buf, sizeof(buf));
      snprintf(query + snext, sizeof(query) - snext, "[%s]", buf);
      snext += strlen(buf) + 2;
   }

   /* The input rs query can specify a subset of all the series' segments - 
    * this is encapsulated in recin. */
   hit = hiter_create(&(recin->segments));
   while ((segin = hiter_getnext(hit)) != NULL)
   {
      if (exputl_mk_expfilename(segin, ffmt, fmtname) == kExpUtlStat_Success)
      {
         snprintf(fullfname, sizeof(fullfname), "%s/%s", outpath, fmtname);
      }
      else
      {
         modstat = kMymodErr_BadFilenameFmt;
         break;
      }

      //drmsstat = drms_segment_mapexport_tofile(segin, classname, mapfile, fullfname);
      DRMS_Array_t *arrout = drms_segment_read(segin, DRMS_TYPE_RAW, &drmsstat);
      drmsstat = drms_mapexport_tofitsfile(arrout, 
                                           &segin->record->keywords, 
                                           segin->cparms, 
                                           classname, 
                                           mapfile, 
                                           fullfname);
      if (drmsstat != DRMS_SUCCESS || stat(fullfname, &filestat))
      {
         modstat = kMymodErr_ExportFailed;
         fprintf(stderr, "Failure exporting segment '%s'.\n", segin->info->name);
         break;
      }
      else 
      {
         if (tcount)
         {
            ++*tcount;
         }

         tsize += filestat.st_size;
         WritePListRecord(kPL_content, pklist, query, fmtname);
      }
   }

   hiter_destroy(&hit);

   if (status)
   {
      *status = modstat;
   }

   return tsize;
}

static int MapexportToDir(DRMS_Env_t *env, 
                          const char *rsinquery, 
                          const char *ffmt,
                          const char *outpath, 
                          FILE *pklist, 
                          const char *classname, 
                          const char *mapfile,
                          int *tcount,
                          TIME *exptime,
                          MymodError_t *status)
{
   int stat = DRMS_SUCCESS;
   MymodError_t modstat = kMymodErr_Success;
   DRMS_RecordSet_t *rsin = NULL;
   DRMS_Record_t *recin = NULL;
   int iRec = 0;
   int nSets = 0;
   int iSet = 0;
   int nRecs = 0;
   unsigned long long tsize = 0;

   /* Sigh - some day have to fix the query param to open_records */
   char *tmpq = NULL;

   if (rsinquery)
   {
      tmpq = strdup(rsinquery);
   }

   if (tmpq && (rsin = drms_open_records(env, tmpq, &stat)))
   {
      nSets = rsin->ss_n;

      for (iSet = 0; 
           stat == DRMS_SUCCESS && modstat == kMymodErr_Success && iSet < nSets; 
           iSet++)
      {
         nRecs = drms_recordset_getssnrecs(rsin, iSet, &stat);

         for (iRec = 0; modstat == kMymodErr_Success && iRec < nRecs; iRec++)
         {
            recin = rsin->records[(rsin->ss_starts)[iSet] + iRec];
            tsize += MapexportRecordToDir(recin,
                                          ffmt,
                                          outpath,
                                          pklist, 
                                          classname, 
                                          mapfile, 
                                          tcount, 
                                          &modstat);
         }
      }
	 
      if (stat != DRMS_SUCCESS || modstat != kMymodErr_Success)
      {
         fprintf(stderr, "Export halted due to DRMS failure.\n");
      }
      else if (exptime)
      {
         *exptime = CURRENT_SYSTEM_TIME;
      }
   }
   else
   {
      fprintf(stderr, "Record-set query '%s' is not valid.\n", rsinquery);
      modstat = kMymodErr_BadRecSetQuery; 
   }

   if (status)
   {
      *status = modstat;
   }
   
   return tsize;
}

static MymodError_t CallExportToFile(DRMS_Segment_t *segout, 
                                     DRMS_Segment_t *segin, 
                                     const char *clname,
                                     const char *mapfile,
                                     const char *ffmt,
                                     unsigned long long *szout,
                                     char *filewritten)
{
   int status = DRMS_SUCCESS;
   MymodError_t err = kMymodErr_Success;
   char fileout[DRMS_MAXPATHLEN];
   char filein[DRMS_MAXPATHLEN];
   char basename[DRMS_MAXPATHLEN];
   unsigned long long size = 0;
   struct stat filestat;

   if (segout)
   {
      drms_segment_filename(segin, filein); /* input seg file name */
      if (!stat(filein, &filestat))
      {
	 size = filestat.st_size;

         if (exputl_mk_expfilename(segin, ffmt, basename) == kExpUtlStat_Success)
         {
            CHECKSNPRINTF(snprintf(segout->filename, DRMS_MAXSEGFILENAME, "%s", basename), DRMS_MAXSEGFILENAME);
            drms_segment_filename(segout, fileout);

            status = drms_segment_mapexport_tofile(segin, clname, mapfile, fileout);
            if (status != DRMS_SUCCESS)
            {
               err = kMymodErr_ExportFailed;
               fprintf(stderr, "Failed to export segment '%s' to '%s'.\n", segin->info->name, fileout);
            }
         }
         else
         {
            err = kMymodErr_BadFilenameFmt;
         }
      }
      else
      {
	 fprintf(stderr, "Unable to open source file '%s'.\n", filein);
         err = kMymodErr_ExportFailed;
      }
   }

   *szout = 0;
   if (err == kMymodErr_Success)
   {
      *szout = size;
      snprintf(filewritten, DRMS_MAXPATHLEN, "%s", basename);
   }

   return err;
}

/* recout is the export series' record to which the exported data will be saved. */
static int MapexportRecord(DRMS_Record_t *recout,
                           DRMS_Record_t *recin,
                           const char *classname, 
                           const char *mapfile,
                           const char *pklfilename, 
                           int *tcount,
                           const char *ffmt, 
                           char **outpath,
                           FILE **pklist,
                           MymodError_t *status)
{
   MymodError_t err = kMymodErr_Success;
   HIterator_t *hit = NULL;
   DRMS_Segment_t *segout = NULL;
   DRMS_Segment_t *segin = NULL;
   unsigned long long size = 0;
   unsigned long long tsize = 0;
   char dir[DRMS_MAXPATHLEN];

   segout = drms_segment_lookupnum(recout, 0);

   drms_record_directory(recin, dir, 1); /* This fetches the input data from SUMS. */

   if (segout)
   {
      char glom[128];
      snprintf(glom, sizeof(glom), "%%s/%s", DRMS_SLOTDIR_FORMAT);
      char path[DRMS_MAXPATHLEN];
      snprintf(path, sizeof(path), glom, segout->record->su->sudir, segout->record->slotnum);

      if (outpath)
      {
         *outpath = strdup(path);
      }

      if (pklist)
      {
         char pkpath[DRMS_MAXPATHLEN];
         snprintf(pkpath, sizeof(pkpath), "%s/%s", path, pklfilename);
         *pklist = fopen(pkpath, "w+");
      }

      char fname[DRMS_MAXPATHLEN];
      char query[DRMS_MAXQUERYLEN];

      /* Must create query from series name and prime keyword values */
      char buf[1028];
      int nkeys = recin->seriesinfo->dbidx_num;
      int snext = 0;
      int ikey = 0;

      snprintf(query, sizeof(query), "%s", recin->seriesinfo->seriesname);
      snext = strlen(query);

      for (ikey = 0; ikey < nkeys; ikey++)
      {
         drms_keyword_snprintfval(recin->seriesinfo->dbidx_keywords[ikey], buf, sizeof(buf));
         snprintf(query + snext, sizeof(query) - snext, "[%s]", buf);
         snext += strlen(buf) + 2;
      }

      /* The input rs query can specify a subset of all the series' segemnts - 
       * this is encapsulated in recin. */
      hit = hiter_create(&(recin->segments));
      while ((segin = hiter_getnext(hit)) != NULL)
      {
	 size = 0;
	 err = CallExportToFile(segout, segin, classname, mapfile, ffmt, &size, fname);
	 if (err != kMymodErr_Success)
	 {
	    fprintf(stderr, "Failure exporting segment '%s'.\n", segin->info->name);
	    break;
	 }
	 else
	 {
            if (tcount)
            {
               ++*tcount;
            }

            tsize += size;
            if (pklist && *pklist)
            {
               WritePListRecord(kPL_content, *pklist, query, fname);
            }
	 }
      }

      hiter_destroy(&hit);
   }
   else
   {
      fprintf(stderr, "Export series contains no segment!\n");
      err = kMymodErr_ExportFailed;
   }

   if (status)
   {
      *status = err;
   }

   return tsize;
}		

/* recout is the export series' record to which the exported data will be saved. 
 * reqid is the primary key in the export series 
 */
static int Mapexport(DRMS_Env_t *env,
                     const char *reqid,
                     const char *classname, 
                     const char *mapfile,
                     const char *expseries,
                     const char *pklfilename, 
                     int *tcount, 
                     char **outpath, 
                     TIME *exptime, 
                     FILE **pklist, 
                     MymodError_t *status)
{
   int stat;
   MymodError_t err = kMymodErr_Success;
   int nSets = 0;
   int iSet = 0;
   int nRecs = 0;
   int iRec = 0;
   unsigned long long tsize = 0;
   DRMS_Record_t *recout = NULL;
   DRMS_Record_t *recin = NULL;
   DRMS_RecordSet_t *rs = NULL;
   DRMS_RecordSet_t *rsout = NULL;
   DRMS_RecordSet_t *rsin = NULL;
   char rsoutquery[DRMS_MAXQUERYLEN];
   char *rsinquery = NULL;

   snprintf(rsoutquery, sizeof(rsoutquery), "%s[%s]", expseries, reqid);

   rs = drms_open_records(env, rsoutquery, &stat);

   if (rs)
   {
      /* XXX Change to DRMS_REPLACE_SEGMENTS (which isn't implemented yet). */
      rsout = drms_clone_records(rs, DRMS_PERMANENT, DRMS_COPY_SEGMENTS, &stat);
      drms_close_records(rs, DRMS_FREE_RECORD);
   }
   
   if (rsout && rsout->n == 1)
   {
      /* Filename Format comes from export series */
      char *ffmt = NULL;
      char *kval = NULL;

      recout = rsout->records[0];
      rsinquery = drms_getkey_string(recout, drms_defs_getval("kExportKW_Request"), &stat);
      
      kval = drms_getkey_string(recout, drms_defs_getval("kExportKW_FileNameFormat"), &stat);
      if (kval)
      {
         if (*kval == '\0')
         {
            ffmt = NULL;
         }
         else
         {
            ffmt = strdup(kval);
         }

         free(kval);
      }

      if (rsinquery && (rsin = drms_open_records(env, rsinquery, &stat)))
      {
	 nSets = rsin->ss_n;

	 for (iSet = 0; 
              stat == DRMS_SUCCESS && err == kMymodErr_Success && iSet < nSets; 
              iSet++)
	 {
	    /* Perhaps we will need this in the future?
	     * request = rsin->ss_queries[iSet];
	     */
	    nRecs = drms_recordset_getssnrecs(rsin, iSet, &stat);

	    for (iRec = 0; 
                 stat == DRMS_SUCCESS && err == kMymodErr_Success &&iRec < nRecs; 
                 iRec++)
	    {
	       recin = rsin->records[(rsin->ss_starts)[iSet] + iRec];
	       tsize += MapexportRecord(recout, 
                                        recin,
                                        classname, 
                                        mapfile, 
                                        pklfilename,
                                        tcount, 
                                        ffmt, 
                                        outpath, 
                                        pklist, 
                                        &err);
	    }
	 }
	 
	 if (stat != DRMS_SUCCESS || err != kMymodErr_Success)
	 {
	    fprintf(stderr, "Export halted due to DRMS failure.\n");
	 }
         else if (exptime)
         {
            *exptime = CURRENT_SYSTEM_TIME;
         }
      }
      else
      {
	 fprintf(stderr, 
		 "Export series keyword '%s' did not contain a valid recordset query.\n", 
                 drms_defs_getval("kExportKW_Request"));
         err = kMymodErr_BadRecSetQuery; 
      }

      if (ffmt)
      {
         free(ffmt);
      }
   }
   else
   {
      fprintf(stderr, "Could not open export destination record set with request id '%s'.\n", reqid);
      err = kMymodErr_BadRequestID; 
   }

   /* Set output record keywords. */
   if (err == kMymodErr_Success)
   {
      drms_setkey_time(recout, drms_defs_getval("kExportKW_ExpTime"), CURRENT_SYSTEM_TIME);
      drms_setkey_int(recout, drms_defs_getval("kExportKW_DataSize"), tsize);
   }

   if (rsout)
   {
      drms_close_records(rsout, DRMS_INSERT_RECORD);
   }

   if (rsinquery)
   {
      free(rsinquery);
   }

   if (status)
   {
      *status = err;
   }
   
   return tsize;
}

static char *GenErrMsg(const char *fmt, ...)
{
   char *msgout = NULL;
   char errmsg[4096];

   va_list ap;
   va_start(ap, fmt);
   vsnprintf(errmsg, sizeof(errmsg), fmt, ap);

   msgout = strdup(errmsg);
   fprintf(stderr, errmsg);
   
   va_end (ap);

   return msgout;
}

static MymodError_t AppendContent(FILE *dst, FILE *src)
{
   MymodError_t err = kMymodErr_Success;
   char buf[32768];
   int nread = 0;
   int dstfd;
   int srcfd;

   if (dst && src)
   {
      dstfd = fileno(dst);
      srcfd = fileno(src);

      while(1)
      {
         nread = read(srcfd, buf, sizeof(buf));
         if (nread > 0)
         {
            if (write(dstfd, buf, nread) == -1)
            {
               err = kMymodErr_PackfileFailure;
               fprintf(stderr, "Failure writing packing-list file.\n");
               break;
            }
         }
         else
         {
             if (nread == -1)
             {
                err = kMymodErr_PackfileFailure;
                fprintf(stderr, "Failure reading packing-list file.\n");
             }

             break;
         }
      }
   }

   return err;
}

//#undef DEFS_MKPATH
//#define FRED(X, Y) X##Y
//#define DEFS_MKPATH(Y) FRED(CDIR, Y)
//#define DEFS_MKPATH(Y) CDIR##Y

int DoIt(void) 
{
   MymodError_t err = kMymodErr_Success;
   int drmsstat = DRMS_SUCCESS;
   long long tsize = 0;
   int tcount = 0;
   TIME exptime = DRMS_MISSING_TIME;
   FILE *pklist = NULL;
   FILE *pklistTMP = NULL;
   char pklistfname[PATH_MAX];
   char pklistfnameTMP[PATH_MAX];
   char pklistpath[PATH_MAX];
   char pklistpathTMP[PATH_MAX];

   const char *version = NULL;
   const char *reqid = NULL;
   const char *method = NULL;
   const char *protocol = NULL;
   const char *rsquery = NULL;
   const char *clname = NULL;
   const char *mapfile = NULL;

   /* "packing list" header/metadata */
   char *md_version = NULL;
   char *md_reqid = NULL;
   char *md_method = NULL;
   char *md_protocol = NULL;
   char *md_count = NULL; /* number of files exported */
   char *md_size = NULL; /* total bytes exported */
   char *md_exptime = NULL;
   char *md_dir = NULL; /* same as outpath, the /SUMX path that contains the exported files
                         * before they are downloaded by the user */
   char *md_status = NULL; 
   char *md_error = NULL;

   if (drms_defs_register("/home/arta/Dev/cvs/JSOC/proj/export/apps/data/export.defs"))
   {
      md_error = GenErrMsg("jsoc_export failure - missing definition file.\n");
      err = kMymodErr_CantRegisterDefs;
   }
   else
   {
      snprintf(pklistfname, sizeof(pklistfname), "%s", drms_defs_getval("kPackListFileName"));
      snprintf(pklistfnameTMP, sizeof(pklistfnameTMP), "%s.tmp", pklistfname);

      version = cmdparams_get_str(&cmdparams, kArg_version, &drmsstat);
      reqid = cmdparams_get_str(&cmdparams, kArg_reqid, &drmsstat);
      method = cmdparams_get_str(&cmdparams, kArg_method, &drmsstat);
      protocol = cmdparams_get_str(&cmdparams, kArg_protocol, &drmsstat);

      rsquery = cmdparams_get_str(&cmdparams, kArg_rsquery, &drmsstat);

      clname = cmdparams_get_str(&cmdparams, kArg_clname, &drmsstat);
      if (drmsstat != DRMS_SUCCESS || !strcmp(clname, kNotSpecified))
      {
         clname = NULL;
      }

      mapfile = cmdparams_get_str(&cmdparams, kArg_kmfile, &drmsstat);
      if (drmsstat != DRMS_SUCCESS || !strcmp(mapfile, kNotSpecified))
      {
         mapfile = NULL;
      }

      md_version = strdup(version);
      md_reqid = strdup(reqid);
      md_method = strdup(method);
      md_protocol = strdup(protocol);

      if (strcmp(rsquery, kNotSpecified) == 0)
      {
         /* Packing list items come from the export series. */
         char *outpath = NULL;

         /* No record-set query provided - must use series to get rsquery */
         const char *expseries = NULL;

         expseries = cmdparams_get_str(&cmdparams, kArg_expSeries, &drmsstat);

         /* Open packing-list file */         
         tsize = Mapexport(drms_env, 
                           reqid, 
                           clname, 
                           mapfile, 
                           expseries, 
                           pklistfnameTMP, 
                           &tcount, 
                           &outpath, 
                           &exptime, 
                           &pklistTMP,
                           &err);

         if (err != kMymodErr_Success)
         {
            md_error = GenErrMsg("Failure occurred while processing export Request ID '%s'.\n", reqid);
            err = kMymodErr_ExportFailed;
         }
         else
         {
            snprintf(pklistpathTMP, sizeof(pklistpathTMP), "%s/%s", outpath, pklistfnameTMP);

            /* Set packing list info */
            md_dir = strdup(outpath);
         }

         if (outpath)
         {
            free(outpath);
         }
      }
      else
      {
         /* Packing list items that come from the cmd-line arguments. */
         const char *outpath = NULL;

         /* Filename Format comes from cmd-line argument */
         const char *ffmt = NULL;
      
         outpath = cmdparams_get_str(&cmdparams, kArg_path, &drmsstat);
         if (strcmp(outpath, kNotSpecified) == 0)
         {
            /* Use current working directory by default */
            outpath = getenv(kPWD);
         }

         ffmt = cmdparams_get_str(&cmdparams, kArg_ffmt, &drmsstat);
         if (strcmp(ffmt, kNotSpecified) == 0 || *ffmt == '\0')
         {
            /* Assume the user wants the default filename format - set to NULL */
            ffmt = NULL;
         }

         /* Open tmp packing-list file */
         snprintf(pklistpathTMP, sizeof(pklistpathTMP), "%s/%s", outpath, pklistfnameTMP);

         pklistTMP = fopen(pklistpathTMP, "w+");
         if (pklistTMP)
         {
            /* Call export code, filling in tsize, tcount, and exptime */
            tsize = MapexportToDir(drms_env, 
                                   rsquery, 
                                   ffmt, 
                                   outpath, 
                                   pklistTMP, 
                                   clname, 
                                   mapfile, 
                                   &tcount, 
                                   &exptime, 
                                   &err);
         }
         else
         {
            err = kMymodErr_CantOpenPackfile;
            md_error = GenErrMsg("Couldn't open packing-list file '%s'.\n",  pklistpathTMP);
         }

         if (err != kMymodErr_Success)
         {
            md_error = GenErrMsg("Failure occurred while processing export Request ID '%s'.\n", reqid);
            err = kMymodErr_ExportFailed;
         }
         else
         {
            /* Set packing list info */
            md_dir = strdup(outpath);
         }
      }

      if (drmsstat != DRMS_SUCCESS)
      {
         md_error = GenErrMsg("DRMS error '%d'.\n", drmsstat);
      }
      else if (err == kMymodErr_Success)
      {
         char tstr[64];
         int strsize = 0;
         sprint_time(tstr, exptime, "UT", 0);

         /* Set packing list info not already set */
         strsize = 64;
         md_size = malloc(strsize);
         snprintf(md_size, strsize, "%lld", tsize);
         md_count = malloc(strsize);
         snprintf(md_count, strsize, "%d", tcount);
         md_exptime = strdup(tstr);
      }

      fprintf(stdout, "'%lld' bytes exported.\n", tsize); 
   }

   /* open 'real' pack list */
   if (md_dir)
   {
      snprintf(pklistpath, sizeof(pklistpath), "%s/%s", md_dir, pklistfname);
      pklist = fopen(pklistpath, "w+");
   }

   if (pklist)
   {
      if (fseek(pklistTMP, 0, SEEK_SET))
      {
         md_error = GenErrMsg("Failure accessing packing-list file '%s'.\n", pklistfnameTMP);
         err = kMymodErr_PackfileFailure;
      }
   }
   else
   {
      md_error = GenErrMsg("Failure opening packing-list file '%s'.\n", pklistfname);
      err = kMymodErr_PackfileFailure;
   }

   /* For now, there is just success/failure in the packing-list file */
   if (err == kMymodErr_Success)
   {
      md_status = strdup(drms_defs_getval("kMDStatus_Good"));
   }
   else
   {
      md_status = strdup(drms_defs_getval("kMDStatus_Bad"));
   }

   if (pklist)
   {
      /* write packing list */
      fprintf(pklist, "# JSOC \n");
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Version"), md_version);
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_RequestID"), md_reqid);
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Method"), md_method);
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Protocol"), md_protocol);
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Count"), md_count ? md_count : "-1");
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Size"), md_size ? md_size : "-1");
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_ExpTime"), md_exptime ? md_exptime : "");
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Dir"), md_dir ? md_dir : "");
      WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Status"), md_status);

      fflush(pklist);

      if (err == kMymodErr_Success)
      {
         /* Copy the content from the temporary pklist into the real pklist */
         fprintf(pklist, "# DATA \n");
         fflush(pklist);
         err = AppendContent(pklist, pklistTMP);
      }

      if (err != kMymodErr_Success)
      {
         fflush(pklist);

         /* If there is an error, write out the error message. */
         WritePListRecord(kPL_metadata, pklist, drms_defs_getval("kMD_Error"), md_error ? md_error : "");
      }
   }



   if (pklistTMP)
   {
      /* close tmp packing-list file */
      fclose(pklistTMP);
      pklistTMP = NULL;

      /* delete temporary file */
      unlink(pklistpathTMP);
   }

   if (pklist)
   {
      /* close actual packing-list file */
      fclose(pklist);
      pklist = NULL;
   }

   if (md_version)
   {
      free(md_version);
   }
   if (md_reqid)
   {
      free(md_reqid);
   }
   if (md_method)
   {
      free(md_method);
   }
   if (md_protocol)
   {
      free(md_protocol);
   }
   if (md_count)
   {
      free(md_count);
   }
   if (md_size)
   {
      free(md_size);
   }
   if (md_exptime)
   {
      free(md_exptime);
   }
   if (md_dir)
   {
      free(md_dir);
   }
   if (md_status)
   {
      free(md_status);
   }
   if (md_error)
   {
      free(md_error);
   }

   return err;
}
