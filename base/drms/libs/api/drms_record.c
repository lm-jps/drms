//#define DEBUG

#include <dirent.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <strings.h>
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "drms_dsdsapi.h"
#include "keymap.h"
#include "fitsexport.h"

#ifndef DEBUG
#undef TIME
#define TIME(code) code
#endif

#define kMAXRSETSPEC (DRMS_MAXSERIESNAMELEN + DRMS_MAXQUERYLEN + 128)

static void *ghDSDS = NULL;
static int gAttemptedDSDS = 0;
static unsigned int gRSChunkSize = 128;

/* Cache the summary-table check */
static HContainer_t *gSummcon = NULL;

typedef enum
{
   /* Begin parsing dataset string. */
   kRSParseState_Begin = 0,
   /* The start of a new dataset element (may be an RS or an @file). */
   kRSParseState_BeginElem,
   /* Parsing the series name of a DRMS record set. */
   kRSParseState_DRMS,
   /* Parsing a DRMS record-set filter (stuff between '[' and ']'). */
   kRSParseState_DRMSFilt,
   /* Parsing a DRMS SQL record-set filter (a "[?" seen). */
   kRSParseState_DRMSFiltSQL,
   /* Parsing a DRMS SQL record-set filter with no 'prime-key logic' (a "[!" seen).*/
   kRSParseState_DRMSFiltAllVersSQL,
   /* Parsing a DRMS segment-list specifier (a '{' seen). */
   kRSParseState_DRMSSeglist,
   /* Parsing 'prog' (DSDS) specification */
   kRSParseState_DSDS,
   /* Parsing 'vot' (VOT) specification */
   kRSParseState_VOT,
   /* Parsing a DRMS series that has generic segment directories that conform to the 
    * DSDS VDS standard. */
   kRSParseState_DSDSPort,
   /* Parsing "@file". */
   kRSParseState_AtFile,
   /* Parsed end of filename, now creating sub-recordsets. */
   kRSParseState_EndAtFile,
   /* Parsing a file or directory that will be read into memory. */
   kRSParseState_Plainfile,
   /* The previous DS elem is done, either by a delimeter or end of non-ws input. */
   kRSParseState_EndElem,
   /* No more ds elements or non-ws text, but there may be ws */
   kRSParseState_End,
   /* Parsing ends with either Success or one of the following errors. */
   kRSParseState_Success,
   /* Generic parsing error - input invalid in some way.
    * To do - make more specfic errors. */
   kRSParseState_Error,
} RSParseState_t;

#define kDSElemParseWS    " \t\b"
#define kDSElemParseDelim ",;\n"
#define kOverviewFits     "overview.fits"

#define kQUERYNFUDGE      (4);

static void FreeSummaryChkCache()
{
   if (gSummcon)
   {
      hcon_destroy(&gSummcon);
   }
}

static void drms_record_summchkcache_term(void *data)
{
   FreeSummaryChkCache();
}

static int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopySegments(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyLinks(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyKeywords(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyPrimaryIndex(DRMS_Record_t *target, DRMS_Record_t *source);
static int ParseRecSetDesc(const char *recsetsStr, 
                           char **allvers, 
			   char ***sets, 
			   DRMS_RecordSetType_t **types, 
                           char ***snames,
			   int *nsets,
                           DRMS_RecQueryInfo_t *info);
static int FreeRecSetDescArr(char **allvers, char ***sets, DRMS_RecordSetType_t **types, char ***snames, int nsets);

/* drms_open_records() helpers */
static int IsValidPlainFileSpec(const char *recSetSpec, 
				DSDS_KeyList_t ***klarrout, 
				DRMS_Segment_t **segarrout,
				int *nRecsout,
				char **pkeysout,
				int *status);
static void AddLocalPrimekey(DRMS_Record_t *template, int *status);
static int CreateRecordProtoFromFitsAgg(DRMS_Env_t *env,
					DSDS_KeyList_t **keylistarr, 
					DRMS_Segment_t *segarr,
					int nRecs,
					char **pkeyarr,
					int nPKeys,
					Exputl_KeyMapClass_t fitsclass,
					DRMS_Record_t **proto,
					DRMS_Segment_t **segout,
					int *status);
static void AdjustRecordProtoSeriesInfo(DRMS_Env_t *env, 
					DRMS_Record_t *proto, 
					const char *seriesName,
					unsigned int unitsize);
static void AllocRecordProtoSeg(DRMS_Record_t *template, DRMS_Segment_t *seg, int *status);
static void SetRecordProtoPKeys(DRMS_Record_t *template, 
				char **pkeyarr, 
				int nkeys, 
				int *status);
static DRMS_Record_t *CacheRecordProto(DRMS_Env_t *env, 
				       DRMS_Record_t *proto, 
				       const char *seriesName, 
				       int *status);
static DRMS_RecordSet_t *CreateRecordsFromDSDSKeylist(DRMS_Env_t *env,
						      int nRecs,
						      DRMS_Record_t *cached,
						      DSDS_KeyList_t **klarr,
						      DRMS_Segment_t *segarr,
						      int pkeysSpecified,
						      Exputl_KeyMapClass_t fitsclass,
						      int *status);
static DRMS_RecordSet_t *OpenPlainFileRecords(DRMS_Env_t *env, 
					      DSDS_KeyList_t ***klarr,
					      DRMS_Segment_t **segarr,
					      int nRecs,
					      char **pkeysout,
					      int *status);

static void RSFree(const void *val);
/* end drms_open_records() helpers */


/* A valid local spec is:
 *   ( <file> | <directory> ) { '[' <keyword1>, <keyword2>, <...> ']' }
 *
 * where () denotes grouping, {} denotes optional, and '' denotes literal
 */
static int IsValidPlainFileSpec(const char *recSetSpecIn, 
				DSDS_KeyList_t ***klarrout, 
				DRMS_Segment_t **segarrout,
				int *nRecsout,
				char **pkeysout,
				int *status)
{
   int isLocSpec = 0;
   struct stat stBuf;
   int lstat = DRMS_SUCCESS;
   char *lbrack = NULL;
   int keylistSize = sizeof(char) * (strlen(recSetSpecIn) + 1);
   char *keylist;
   char *recSetSpec = strdup(recSetSpecIn);

   /* There could be an optional clause at the end of the file/directory spec. */
   if ((lbrack = strchr(recSetSpec, '[')) != NULL)
   {
      if (strchr(lbrack, ']'))
      {
	 *lbrack = '\0';
	 keylist = malloc(keylistSize);
	 snprintf(keylist, keylistSize, "%s", lbrack + 1);
	 *(strchr(keylist, ']')) = '\0';
	 *pkeysout = strdup(keylist);
	 free(keylist);
      }
   }
   else
   {
      *pkeysout = NULL;
   }

   /* Basically, if recSetSpec is a FITS file, directory of FITS files, or 
    * a link to either of these, then it refers to a set of local files */
   if (recSetSpec && !stat(recSetSpec, &stBuf) && klarrout && segarrout)
   {
      if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode) || S_ISDIR(stBuf.st_mode))
      {
	 /* ensure these files are all fits files - okay to use libdsds.so for now 
	  * but this should be divorced from Stanford-specific code in the future */
	 if (!gAttemptedDSDS && !ghDSDS)
	 {
	    kDSDS_Stat_t dsdsstat;
	    ghDSDS = DSDS_GetLibHandle(kLIBDSDS, &dsdsstat);
	    if (dsdsstat != kDSDS_Stat_Success)
	    {
	       lstat = DRMS_ERROR_CANTOPENLIBRARY;
	    }

	    gAttemptedDSDS = 1;
	 }

	 if (lstat == DRMS_SUCCESS && ghDSDS)
	 {
	    DSDS_KeyList_t *kl = NULL;
	    DRMS_Segment_t *seg = NULL;
	    int iRec = 0;

	    pDSDSFn_DSDS_read_fitsheader_t pFn_DSDS_read_fitsheader = 
	      (pDSDSFn_DSDS_read_fitsheader_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_READ_FITSHEADER);
	    pDSDSFn_DSDS_free_keylist_t pFn_DSDS_free_keylist = 
	      (pDSDSFn_DSDS_free_keylist_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLIST);
	    pDSDSFn_DSDS_steal_seginfo_t pFn_DSDS_steal_seginfo =
	      (pDSDSFn_DSDS_steal_seginfo_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_STEAL_SEGINFO);
	    pDSDSFn_DSDS_free_seg_t pFn_DSDS_free_seg = 
	      (pDSDSFn_DSDS_free_seg_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_SEG);

	    /* for each file, check for a valid FITS header */
	    if (pFn_DSDS_read_fitsheader && pFn_DSDS_free_keylist &&
		pFn_DSDS_steal_seginfo && pFn_DSDS_free_seg)
	    {
	       kDSDS_Stat_t dsdsStat;
	       int fitshead = 1;

	       if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
	       {
		  (*pFn_DSDS_read_fitsheader)(recSetSpec, &kl, &seg, kLocalSegName, &dsdsStat);

		  if (dsdsStat == kDSDS_Stat_Success)
		  {
		     *klarrout = (DSDS_KeyList_t **)malloc(sizeof(DSDS_KeyList_t *));
		     *segarrout = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t));

		     (*klarrout)[0] = kl;
		     (*pFn_DSDS_steal_seginfo)(&((*segarrout)[0]), seg);
		     /* For local data, add filepath for use with later calls to 
		      * drms_segment_read(). */
		     snprintf((*segarrout)[0].filename, 
			      DRMS_MAXSEGFILENAME, 
			      "%s", 
			      recSetSpec);
		     (*pFn_DSDS_free_seg)(&seg);
		     *nRecsout = 1;
		  }
		  else
		  {
		     fitshead = 0;
		  }
	       }
	       else 
	       {
		  struct dirent **fileList = NULL;
		  int nFiles = -1;

		  if ((nFiles = scandir(recSetSpec, &fileList, NULL, NULL)) > 0 && 
		      fileList != NULL)
		  {
		     int fileIndex = 0;
		     iRec = 0;

		     /* nFiles is the maximum number of fits files possible */
		     *klarrout = (DSDS_KeyList_t **)malloc(sizeof(DSDS_KeyList_t *) * nFiles);
		     *segarrout = (DRMS_Segment_t *)malloc(sizeof(DRMS_Segment_t) * nFiles);

		     while (fileIndex < nFiles)
		     {
			struct dirent *entry = fileList[fileIndex];
			if (entry != NULL)
			{
			   char *oneFile = entry->d_name;
			   char dirEntry[PATH_MAX] = {0};
			   snprintf(dirEntry, 
				    sizeof(dirEntry), 
				    "%s%s%s", 
				    recSetSpec, 
				    recSetSpec[strlen(recSetSpec) - 1] == '/' ? "" : "/",
				    oneFile);
			   if (*dirEntry !=  '\0' && !stat(dirEntry, &stBuf));
			   {
			      if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
			      {
				 (*pFn_DSDS_read_fitsheader)(dirEntry, 
							     &kl, 
							     &seg, 
							     kLocalSegName, 
							     &dsdsStat);

				 if (dsdsStat == kDSDS_Stat_Success)
				 {
				    (*klarrout)[iRec] = kl;
				    (*pFn_DSDS_steal_seginfo)(&((*segarrout)[iRec]), seg);
				    /* For local data, add filepath for use with later calls to 
				     * drms_segment_read(). */
				    snprintf((*segarrout)[iRec].filename, 
					     DRMS_MAXSEGFILENAME, 
					     "%s", 
					     dirEntry);
				    (*pFn_DSDS_free_seg)(&seg);
				    iRec++;
				 }
				 else
				 {
				    fitshead = 0;
				    break;
				 }
			      }
			   }

			   free(entry);
			}
		 
			fileIndex++;
		     } /* while */

		     if (nRecsout)
		     {
			*nRecsout = iRec;
		     }
	      
		     free(fileList);
		  }
	       }

	       isLocSpec = fitshead;
	    }
	 }
      }	 
   }
   

   if (recSetSpec)
   {
      free(recSetSpec);
   }

   if (status)
   {
      *status = lstat;
   }

   return isLocSpec;
}

static void AddLocalPrimekey(DRMS_Record_t *template, int *status)
{
   int stat = DRMS_SUCCESS;
   char drmsKeyName[DRMS_MAXKEYNAMELEN];
   DRMS_Keyword_t *tKey = NULL;

   snprintf(drmsKeyName, sizeof(drmsKeyName), kLocalPrimekey);

   /* insert into template */
   tKey = hcon_allocslot_lower(&(template->keywords), drmsKeyName);
   XASSERT(tKey);
   memset(tKey, 0, sizeof(DRMS_Keyword_t));
   tKey->info = malloc(sizeof(DRMS_KeywordInfo_t));
   XASSERT(tKey->info);
   memset(tKey->info, 0, sizeof(DRMS_KeywordInfo_t));
	    
   if (tKey && tKey->info)
   {
      /* record */
      tKey->record = template;

      /* keyword info */
	      
      //memcpy(tKey->info, sKey->info, sizeof(DRMS_KeywordInfo_t));
      snprintf(tKey->info->name, 
	       DRMS_MAXKEYNAMELEN,
	       "%s",
	       drmsKeyName);

      tKey->info->type = DRMS_TYPE_LONGLONG;
      strcpy(tKey->info->format, "%lld");

      /* default value - missing */
      drms_missing(tKey->info->type, &(tKey->value));
   }
   else
   {
      stat = DRMS_ERROR_OUTOFMEMORY;
   }

   if (status)
   {
      *status = stat;
   }
}

static int IsWS(const char *str)
{
   int ret = 1;
   char *lasts;
   char *buf = strdup(str);

   if (strtok_r(buf, " \t\b", &lasts))
   {
      ret = 0;
   }

   free(buf);

   return ret;
}

static int CreateRecordProtoFromFitsAgg(DRMS_Env_t *env,
					DSDS_KeyList_t **keylistarr, 
					DRMS_Segment_t *segarr,
					int nRecs,
					char **pkeyarr,
					int nPKeys,
					Exputl_KeyMapClass_t fitsclass,
					DRMS_Record_t **proto,
					DRMS_Segment_t **segout,
					int *status)
{
   DRMS_Record_t *template = NULL;
   DRMS_Segment_t *seg = NULL;
   int iRec = 0;
   int stat = DRMS_SUCCESS;
   int pkeysSpecified = (pkeyarr != NULL);
	   
   if (stat == DRMS_SUCCESS)
   {
      template = calloc(1, sizeof(DRMS_Record_t));
      XASSERT(template);
      template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t));
      XASSERT(template->seriesinfo);
   }

   if (template && template->seriesinfo)
   {
      char drmsKeyName[DRMS_MAXKEYNAMELEN];
      DRMS_Keyword_t *sKey = NULL;
      DRMS_Keyword_t *tKey = NULL;

      template->env = env;
      template->init = 1;
      template->recnum = 0;
      template->sunum = -1;
      template->sessionid = 0;
      template->sessionns = NULL;
      template->su = NULL;
      
      /* Initialize container structure. */
      hcon_init(&template->segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_segment_struct, 
		(void (*)(const void *, const void *)) drms_copy_segment_struct);
      /* Initialize container structures for links. */
      hcon_init(&template->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_link_struct, 
		(void (*)(const void *, const void *)) drms_copy_link_struct);
      /* Initialize container structure. */
      hcon_init(&template->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_keyword_struct, 
		(void (*)(const void *, const void *)) drms_copy_keyword_struct);

      /* Loop through DSDS records - put a superset of keywords in rec->keywords and 
       * ensure segments match */
      for (iRec = 0; iRec < nRecs; iRec++)
      {
	 /* multiple keywords per record - walk keylistarr */
	 DSDS_KeyList_t *kl = keylistarr[iRec];
	 DRMS_Segment_t *oneSeg = &(segarr[iRec]); 

	 if (kl && oneSeg)
	 {
	    while (kl != NULL && ((sKey = kl->elem) != NULL))
	    {
	       /* skip keywords that are empty or ws strings - they 
		* don't provide any information, and DSDS represents
		* missing values this way. */
	        if (sKey->info->type == DRMS_TYPE_STRING &&
		    (sKey->value.string_val == NULL || 
		    IsWS(sKey->value.string_val)))
		{
		   kl = kl->next;
		   continue;
		}
	       
                /* generate a valid drms keyword (fits might not be valid) */
                if (!fitsexport_getmappedintkeyname(sKey->info->name, 
                                                    exputl_keymap_getclname(fitsclass),
                                                    NULL, 
                                                    drmsKeyName, 
                                                    sizeof(drmsKeyName)))
                {
                   *drmsKeyName = '\0';
                   stat = DRMS_ERROR_INVALIDDATA;
		  break;
	       }

	       if (!(tKey = hcon_lookup_lower(&(template->keywords), drmsKeyName)))
	       {
		  /* insert into template */
                  tKey = hcon_allocslot_lower(&(template->keywords), drmsKeyName);
                  XASSERT(tKey);
		  memset(tKey, 0, sizeof(DRMS_Keyword_t));
                  tKey->info = malloc(sizeof(DRMS_KeywordInfo_t));
                  XASSERT(tKey->info);
		  memset(tKey->info, 0, sizeof(DRMS_KeywordInfo_t));
	    
		  if (tKey && tKey->info)
		  {
		     /* record */
		     tKey->record = template;

		     /* keyword info */
		     memcpy(tKey->info, sKey->info, sizeof(DRMS_KeywordInfo_t));
		     snprintf(tKey->info->name, 
			      DRMS_MAXKEYNAMELEN,
			      "%s",
			      drmsKeyName);

		     /* default value - missing */
		     drms_missing(tKey->info->type, &(tKey->value));
		  }
		  else
		  {
		     stat = DRMS_ERROR_OUTOFMEMORY;
		  }
	       }
	       else if (sKey->info->type != tKey->info->type &&
			tKey->info->type != DRMS_TYPE_STRING)
	       {
		  /* If the keyword already exists, and the type of 
		   * the current keyword doesn't match the type of the
		   * existing template keyword, and the current keyword
		   * isn't the empty string or a whitespace string, 
		   * then make the template keyword's data type be string. */
		  if (sKey->info->type != DRMS_TYPE_STRING ||
		      (sKey->value.string_val != NULL && 
		       !IsWS(sKey->value.string_val)))
		  {
		     tKey->info->type = DRMS_TYPE_STRING;
		     tKey->info->format[0] = '%';
		     tKey->info->format[1] = 's';
		     tKey->info->format[2] = '\0';
		     /* The following uses copy_string(), which frees the value,
		      * unless the value is zero. So set it to zero. */
		     tKey->value.string_val = NULL;
		     drms_missing(DRMS_TYPE_STRING, &(tKey->value));
		  }		  
	       }
		     
	       kl = kl->next;
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* one segment per record - but some records might now have a segment.
                * Records that don't have segments will still have a DRMS_Segment_t 
                * in the segarr, but the structure is zeroed out. */
               if (oneSeg && oneSeg->info)
               {
                  if (!seg)
                  {
                     seg = oneSeg;
                  }
                  else if (!drms_segment_segsmatch(seg, oneSeg))
                  {
                     stat = DRMS_ERROR_INVALIDDATA;
                  }
               }
	    }
	 }
      } /* iRec */

      /* Add a keyword that will serve as primary key - not sure what to use here. 
       * What about an increasing integer? */
      if (fitsclass == kKEYMAPCLASS_LOCAL)
      {
	 if (!pkeysSpecified)
	 {
	    AddLocalPrimekey(template, &stat);
	 }
	 else
	 {
	    /* pkeysSpecified - ensure they exist; if kLocalPrimekey is specified, but
	     * doesn't exist, add it */
	    int iKey;
	    for (iKey = 0; iKey < nPKeys && stat == DRMS_SUCCESS; iKey++)
	    {
	       if (!hcon_member_lower(&(template->keywords), pkeyarr[iKey]))
	       {
		  if (strcasecmp(pkeyarr[iKey], kLocalPrimekey) == 0)
		  {
		     /* user specified kLocalPrimekey, but it doesn't 
		      * exist in the local data being read in.  This is 
		      * a special keyword that may have gotten added the last 
		      * time this code was run, so add it now. */
		     AddLocalPrimekey(template, &stat);
		  }
		  else
		  {
		     /* error - user specified a prime keyword that doesn't exist
		      * in the local data being read in. */
		     stat = DRMS_ERROR_INVALIDDATA;
		     fprintf(stderr, 
			     "keyword %s doesn't exist in fits file header\n", 
			     pkeyarr[iKey]);
		  }
	       }
	    }
	 }
      }
   }

   if (status)
   {
      *status = stat;
   }

   if (stat == DRMS_SUCCESS)
   {
      *proto = template;
      *segout = seg;
   }
   else
   {
      drms_destroy_recproto(&template);
   }

   return iRec;
}

static void AdjustRecordProtoSeriesInfo(DRMS_Env_t *env, 
					DRMS_Record_t *proto, 
					const char *seriesName,
					unsigned int unitsize) 
{
   /* series info */
   char *user = getenv("USER");
   snprintf(proto->seriesinfo->seriesname,
	    DRMS_MAXSERIESNAMELEN,
	    "%s",
	    seriesName);

   strcpy(proto->seriesinfo->author, "unknown");
   strcpy(proto->seriesinfo->owner, "unknown");

   if (user)
   {
      if (strlen(user) < DRMS_MAXCOMMENTLEN)
      {
	 strcpy(proto->seriesinfo->author, user);
      }
		    
      if (strlen(user) < DRMS_MAXOWNERLEN)
      {
	 strcpy(proto->seriesinfo->owner, user);
      }
   }

   /* discard "Owner", fill it with the dbuser */
   if (env->session->db_direct) 
   {
      strcpy(proto->seriesinfo->owner, env->session->db_handle->dbuser);
   }

   if (unitsize > 0)
   {
      proto->seriesinfo->unitsize = unitsize;
   }
   else
   {
      fprintf(stderr, 
	      "The series unit size must be at least 1, but it is %ud.\n",
	      unitsize);
   }
}

static void AllocRecordProtoSeg(DRMS_Record_t *template, DRMS_Segment_t *seg, int *status)
{
   int stat = DRMS_SUCCESS;

   if (seg)
   {
      DRMS_Segment_t *tSeg = NULL;

      tSeg = hcon_allocslot_lower(&(template->segments), seg->info->name);
      XASSERT(tSeg);
      memset(tSeg, 0, sizeof(DRMS_Segment_t));
      tSeg->info = malloc(sizeof(DRMS_SegmentInfo_t));
      XASSERT(tSeg->info);
      memset(tSeg->info, 0, sizeof(DRMS_SegmentInfo_t));

      if (tSeg && tSeg->info)
      {
         /* record */
         tSeg->record = template;
	       
         /* segment info*/
         memcpy(tSeg->info, seg->info, sizeof(DRMS_SegmentInfo_t));

         /* axis is allocated as static array */
         memcpy(tSeg->axis, seg->axis, sizeof(int) * DRMS_MAXRANK);
      }
      else
      {
         stat = DRMS_ERROR_OUTOFMEMORY;
      }
   }

   if (status)
   {
      *status = stat;
   }
}

static void SetRecordProtoPKeys(DRMS_Record_t *template, 
				char **pkeyarr, 
				int nkeys, 
				int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_Keyword_t *pkey = NULL;
   int ikey = 0;

   for (; ikey < nkeys; ikey++)
   {
      pkey = hcon_lookup_lower(&(template->keywords), pkeyarr[ikey]);
      if (pkey != NULL)
      {
	 template->seriesinfo->pidx_keywords[ikey] = pkey;
         drms_keyword_setintprime(pkey);
      }
      else
      {
	 stat = DRMS_ERROR_INVALIDKEYWORD;
	 break;
      }
   }

   template->seriesinfo->pidx_num = ikey;

   if (status)
   {
      *status = stat;
   }
}

static DRMS_Record_t *CacheRecordProto(DRMS_Env_t *env, 
				       DRMS_Record_t *proto, 
				       const char *seriesName, 
				       int *status)
{
   int stat = DRMS_SUCCESS;
   DRMS_Record_t *cached = NULL;

   /* seriesName might actually exist, but not be in the series_cache, because we now cache the series 
    * on-demand. */
   if (drms_template_record(env, seriesName, &stat) != NULL)
   {
      fprintf(stderr,"drms_open_dsdsrecords(): "
	      "ERROR: Series '%s' already exists.\n", seriesName);
      drms_free_template_record_struct(proto);
      stat = DRMS_ERROR_INVALIDDATA;
   }
   else
   {
      stat = DRMS_SUCCESS; /* ingore drms_template_record status, which might have been 'unknown series' */
      cached = (DRMS_Record_t *)hcon_allocslot_lower(&(env->series_cache), seriesName);
      drms_copy_record_struct(cached, proto);  

      for (int i = 0; i < cached->seriesinfo->pidx_num; i++) 
      {
	 cached->seriesinfo->pidx_keywords[i] = 
	   drms_keyword_lookup(cached, 
			       proto->seriesinfo->pidx_keywords[i]->info->name, 
			       0);
      }

      drms_free_record_struct(proto);
   }

   if (status)
   {
      *status = stat;
   }
   
   return cached;
}

static DRMS_RecordSet_t *CreateRecordsFromDSDSKeylist(DRMS_Env_t *env,
						      int nRecs,
						      DRMS_Record_t *cached,
						      DSDS_KeyList_t **klarr,
						      DRMS_Segment_t *segarr,
						      int setPrimeKey,
						      Exputl_KeyMapClass_t fitsclass,
						      int *status)
{
   int stat = DRMS_SUCCESS;
   int iRec;
   DRMS_RecordSet_t *rset = (DRMS_RecordSet_t *)malloc(sizeof(DRMS_RecordSet_t));
   rset->n = nRecs;
   rset->records = (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nRecs);
   memset(rset->records, 0, sizeof(DRMS_Record_t *) * nRecs);
   rset->ss_n = 0;
   rset->ss_queries = NULL;
   rset->ss_types = NULL;
   rset->ss_starts = NULL;
   rset->ss_currentrecs = NULL;
   rset->cursor = NULL;

   int iNewRec = 1;
   int primekeyCnt = 0;
   for (iRec = 0; stat == DRMS_SUCCESS && iRec < nRecs; iRec++)
   {
      DSDS_KeyList_t *kl = klarr[iRec];
      DRMS_Segment_t *sSeg = &(segarr[iRec]);
      DRMS_Segment_t *tSeg = NULL;

      if (kl)
      {
	 rset->records[iRec] = drms_alloc_record2(cached, iNewRec, &stat);
	 if (stat == DRMS_SUCCESS)
	 {
	    rset->records[iRec]->sessionid = env->session->sessionid;  
	    rset->records[iRec]->sessionns = strdup(env->session->sessionns);
	    rset->records[iRec]->lifetime = DRMS_TRANSIENT;
	 }

	 /* populate the new records with information from keylistarr and segarr */

	 DRMS_Keyword_t *sKey = NULL;
	 char drmsKeyName[DRMS_MAXKEYNAMELEN];
	 int iKey = 0;

	 while (stat == DRMS_SUCCESS && kl && ((sKey = kl->elem) != NULL))
	 {
	    if (!fitsexport_getmappedintkeyname(sKey->info->name, 
                                                exputl_keymap_getclname(fitsclass),
                                                NULL, 
                                                drmsKeyName, 
                                                sizeof(drmsKeyName)))
	    {
	       *drmsKeyName = '\0';
	       stat = DRMS_ERROR_INVALIDDATA;
	       break;
	    }

	    /* Essentially a DSDS keyword missing value - ignore.
	     * When the template was created, such keywords were NOT
	     * used. */
	    if (sKey->info->type != DRMS_TYPE_STRING ||
		(sKey->value.string_val != NULL && 
		 !IsWS(sKey->value.string_val)))
	    {
	       stat = drms_setkey(rset->records[iRec], 
				  drmsKeyName,
				  sKey->info->type,
				  &(sKey->value));

	       if (stat != DRMS_SUCCESS)
	       {
		  fprintf(stderr, "Couldn't set keyword '%s'.\n", drmsKeyName);
	       }
	    }

	    kl = kl->next;
	    iKey++;
	 } /* while */

	 if (fitsclass == kKEYMAPCLASS_LOCAL)
	 {
	    /* set and increment primekey (if no prime key specified by user ) */
	    if (setPrimeKey)
	    {
	       stat = drms_setkey_longlong(rset->records[iRec], 
					   kLocalPrimekey,
					   primekeyCnt);
	       primekeyCnt++;
	    }

	    /* enter segment file names (kKEYMAPCLASS_LOCAL) */
            tSeg = hcon_lookup_lower(&(rset->records[iRec]->segments), kLocalSegName);
         }
         else if (fitsclass == kKEYMAPCLASS_DSDS)
         {
            tSeg = drms_segment_lookupnum(rset->records[iRec], 0);
         }

         if (tSeg != NULL)
         {
            if (sSeg->info)
            {
               /* Not all DSDS records have a data file. This will be used if the protocol
                * is DRMS_LOCAL.  In this case, the filename is the path to the file(s)
                * that formed the rec-set query.  If the protocol is DRMS_DSDS, then filename 
                * is the DSDS path to a fits file. */
               snprintf(tSeg->filename, DRMS_MAXSEGFILENAME, "%s", sSeg->filename);
            }
         }
         else
         {
            stat = DRMS_ERROR_INVALIDDATA;
            break;
         }

	 rset->records[iRec]->readonly = 1;
	 iNewRec++;
      }
   } /* for iRec */

   if (status)
   {
      *status = stat;
   }

   return rset;
}

static DRMS_RecordSet_t *OpenPlainFileRecords(DRMS_Env_t *env, 
					      DSDS_KeyList_t ***klarr,
					      DRMS_Segment_t **segarr,
					      int nRecs,
					      char **pkeys,
					      int *status)
{
   DRMS_RecordSet_t *rset = NULL;
   int stat = DRMS_SUCCESS;

   if (!gAttemptedDSDS && !ghDSDS)
   {
      kDSDS_Stat_t dsdsstat;
      ghDSDS = DSDS_GetLibHandle(kLIBDSDS, &dsdsstat);
      if (dsdsstat != kDSDS_Stat_Success)
      {
	 stat = DRMS_ERROR_CANTOPENLIBRARY;
      }

      gAttemptedDSDS = 1;
   }

   if (stat == DRMS_SUCCESS && ghDSDS)
   {
      pDSDSFn_DSDS_free_keylistarr_t pFn_DSDS_free_keylistarr = 
	(pDSDSFn_DSDS_free_keylistarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLISTARR);

      if (pFn_DSDS_free_keylistarr)
      {
	 char seriesName[DRMS_MAXSERIESNAMELEN];
	 int nPkeys = 0;
	
	 /* make record prototype from this morass of information */
	 DRMS_Record_t *proto = NULL;
	 DRMS_Segment_t *seg = NULL;
	 DRMS_Record_t *cached = NULL;


	 Exputl_KeyMapClass_t fitsclass = kKEYMAPCLASS_LOCAL;
	 char drmsKeyName[DRMS_MAXKEYNAMELEN];
	 char *pkeyarr[DRMS_MAXPRIMIDX] = {0};
	 int setPrimeKey = 0;

	 if (pkeys && *pkeys)
	 {
	    char *pkeyname = strtok(*pkeys, ",");
	    nPkeys = 0;
	    while(pkeyname != NULL)
	    {
	       if (!fitsexport_getmappedintkeyname(pkeyname, 
                                                   exputl_keymap_getclname(fitsclass),
                                                   NULL, 
                                                   drmsKeyName, 
                                                   sizeof(drmsKeyName)))
	       {
		  *drmsKeyName = '\0';
		  stat = DRMS_ERROR_INVALIDDATA;
		  break;
	       }

	       if (strcasecmp(drmsKeyName, kLocalPrimekey) == 0)
	       {
		  setPrimeKey = 1;
	       }

	       pkeyname = strtok(NULL, ",");
	       pkeyarr[nPkeys] = strdup(drmsKeyName);
	       nPkeys++;
	    }
	 }
	 else
	 {
	    pkeyarr[0] = strdup(kLocalPrimekey);
	    nPkeys = 1;
	    setPrimeKey = 1;
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    CreateRecordProtoFromFitsAgg(env, 
					 *klarr, 
					 *segarr, 
					 nRecs, 
					 pkeyarr,
					 nPkeys,
					 kKEYMAPCLASS_LOCAL,
					 &proto,
					 &seg,
					 &stat);
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* Get series name - this should be "dsdsing.<GUID>.  The GUID should
	     * be stored in libdrmsserver.a.  It should be a monotonically
	     * increasing integer that lives for the life of libdrmsserver.a. 
	     */

	    long long guid = -1;
            const char *dsdsNsPrefix = NULL;

            /* Use the same prefix used for dsds temporary series. These series do NOT live in PSQL. 
             * There is simply an entry in the series_cache for each one of them, so they must
             * be handled differently than regular series. */
            dsdsNsPrefix = DSDS_GetNsPrefix();

#ifdef DRMS_CLIENT
	       drms_send_commandcode(env->session->sockfd, DRMS_GETTMPGUID);
	       guid = Readlonglong(env->session->sockfd);
#else
	       /* has direct access to drms_server.c. */
	       guid = drms_server_gettmpguid(NULL);
#endif /* DRMS_CLIENT */

	       snprintf(seriesName, sizeof(seriesName), "%s.%lld", dsdsNsPrefix, guid);

	    /* Adjust seriesinfo */
	    AdjustRecordProtoSeriesInfo(env, proto, seriesName, 32);
	      
	    /* alloc segments */
            if (seg)
            {
               /* Not all DSDS records have a data file */
               AllocRecordProtoSeg(proto, seg, &stat);
            }
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* primary index */
	    SetRecordProtoPKeys(proto, pkeyarr, nPkeys, &stat);
	 }

	 if (stat == DRMS_SUCCESS)
	 {
	    /* place proto in cache */
	    cached = CacheRecordProto(env, proto, seriesName, &stat);

            /* Don't free proto here - already done in CacheRecordProto() */
	 }

	 /* create a new record (read-only) for each record */
	 if (stat == DRMS_SUCCESS)
	 {
	    rset = CreateRecordsFromDSDSKeylist(env,
						nRecs, 
						cached, 
						*klarr,
						*segarr,
						setPrimeKey,
						kKEYMAPCLASS_LOCAL,
						&stat);
	 }

	 /* libdsds created the keylists in the array, it needs to clean them */
	 (*pFn_DSDS_free_keylistarr)(klarr, nRecs);

	 /* segarr was created by DRMS, clean segments here */
	 if (segarr && *segarr)
	 {
	    int i;
	    for (i = 0; i < nRecs; i++)
	    {
	       seg = (*segarr + i);

	       /* need to free malloc'd mem within each segment */
	       if (seg->info)
	       {
		  free(seg->info);
	       }
	    }

	    free(*segarr);
	 }

	 *segarr = NULL;
	 
	 /* free primary key list */
	 if (pkeys && *pkeys)
	 {
	    free(*pkeys);
	    *pkeys = NULL;
	 }

	 for (nPkeys = 0; nPkeys < DRMS_MAXPRIMIDX; nPkeys++)
	 if (pkeyarr[nPkeys])
	 {
	    free(pkeyarr[nPkeys]);
	 }
      }
   }
   else
   {
      fprintf(stdout, "Your JSOC environment does not support DSDS database access.\n");
      stat = DRMS_ERROR_NODSDSSUPPORT;
   }

   if (status)
   {
      *status = stat;
   }

   /* Clean-up in the case of an error happens in calling function */

   return rset;
}

/* Deep-frees recordsets in realSets container */
static void RSFree(const void *val)
{
   DRMS_RecordSet_t **realPtr = (DRMS_RecordSet_t **)val;
   DRMS_RecordSet_t *theRS = NULL;

   if (realPtr)
   {
      theRS = *realPtr;
      if (theRS)
      {
	 drms_close_records(theRS, DRMS_FREE_RECORD);
	 /* free(theRS); - don't free, drms_close_records() already does this! */
      }
   }
}

/*********************** Public functions *********************/

DRMS_RecordSet_t *drms_open_localrecords(DRMS_Env_t *env, const char *dsRecSet, int *status)
{
   DRMS_RecordSet_t *rs = NULL;
   int stat = DRMS_SUCCESS;
   DSDS_KeyList_t **klarr = NULL;
   DRMS_Segment_t *segarr = NULL;
   int nRecsLocal = 0;
   char *pkeys = NULL;

   if (IsValidPlainFileSpec(dsRecSet, &klarr, &segarr, &nRecsLocal, &pkeys, &stat))
   {
      rs = OpenPlainFileRecords(env, &klarr, &segarr, nRecsLocal, &pkeys, &stat);
   }

   if (status)
   {
      *status = stat;
   }

   return rs;
}

/* dsRecSet may resolve into more than one record (fits file) */
DRMS_RecordSet_t *drms_open_dsdsrecords(DRMS_Env_t *env, const char *dsRecSet, int *status)
{
   DRMS_RecordSet_t *rset = NULL;
   int stat = DRMS_SUCCESS;

   if (!gAttemptedDSDS && !ghDSDS)
   {
      /* Get handle to libdsds.so */
      kDSDS_Stat_t dsdsstat;
      ghDSDS = DSDS_GetLibHandle(kLIBDSDS, &dsdsstat);
      if (dsdsstat != kDSDS_Stat_Success)
      {
	 stat = DRMS_ERROR_CANTOPENLIBRARY;
      }

      gAttemptedDSDS = 1;
   }

   if (stat == DRMS_SUCCESS && ghDSDS)
   {
      pDSDSFn_DSDS_open_records_t pFn_DSDS_open_records = 
	(pDSDSFn_DSDS_open_records_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_OPEN_RECORDS);
      pDSDSFn_DSDS_free_keylistarr_t pFn_DSDS_free_keylistarr = 
	(pDSDSFn_DSDS_free_keylistarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_KEYLISTARR);
      pDSDSFn_DSDS_free_segarr_t pFn_DSDS_free_segarr = 
	(pDSDSFn_DSDS_free_segarr_t)DSDS_GetFPtr(ghDSDS, kDSDS_DSDS_FREE_SEGARR);

      if (pFn_DSDS_open_records && pFn_DSDS_free_keylistarr && pFn_DSDS_free_segarr)
      {
	 char seriesName[DRMS_MAXSERIESNAMELEN];
	 DSDS_Handle_t hparams;
	 DSDS_KeyList_t **keylistarr;
	 DRMS_Segment_t *segarr;
	 kDSDS_Stat_t dsdsStat;
	 long long nRecs = 0; /* info returned from libdsds.so */

	 /* Returns one keylist per record and one segment per record. Even though
	  * this could involve a lot of alloc'd memory, and it isn't 100% necessary 
	  * to do this to pass the needed information from libdsds.so here, it
	  * does make keyword and segment analysis easier since the functions for 
	  * doing the analysis are available in the module, but not in libdsds.so.
	  * And, eventually it IS necessary to have n keywords and one segment 
	  * per record - that is the way DRMS works. */
	 nRecs = (*pFn_DSDS_open_records)(dsRecSet, 
					  seriesName, 
					  &hparams, 
					  &keylistarr, 
					  &segarr, 
					  &dsdsStat);
	 if (dsdsStat == kDSDS_Stat_Success)
	 {
	    /* make record prototype from this morass of information */
	    DRMS_Record_t *template = NULL;
	    DRMS_Segment_t *seg = NULL;
	    DRMS_Record_t *cached = NULL;

	    CreateRecordProtoFromFitsAgg(env, 
					 keylistarr, 
					 segarr, 
					 nRecs, 
					 NULL,
					 0,
					 kKEYMAPCLASS_DSDS,
					 &template,
					 &seg,
					 &stat);

	    if (stat == DRMS_SUCCESS)
	    {
	       /* Adjust seriesinfo */
	       AdjustRecordProtoSeriesInfo(env, template, seriesName, 32);
	       DSDS_SetDSDSParams(ghDSDS, template->seriesinfo, hparams);
	      
	       /* alloc segments */
               if (seg)
               {
                  /* Not all DSDS records have a segment */
                  AllocRecordProtoSeg(template, seg, &stat);
               }
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* primary index - series_num and rn */
	       char *pkeyarr[2] = {kDSDS_SERIES_NUM, kDSDS_RN};
	       SetRecordProtoPKeys(template, pkeyarr, 2, &stat);
	    }

	    if (stat == DRMS_SUCCESS)
	    {
	       /* place proto in cache */
	       cached = CacheRecordProto(env, template, seriesName, &stat);
	    }

	    /* create a new record (read-only) for each record */
	    rset = CreateRecordsFromDSDSKeylist(env,
						nRecs, 
						cached, 
						keylistarr,
						segarr,
						0,
						kKEYMAPCLASS_DSDS,
						&stat);

	    /* clean up - let libdsds clean up the stuff it created */
	    (*pFn_DSDS_free_keylistarr)(&keylistarr, nRecs);
	    (*pFn_DSDS_free_segarr)(&segarr, nRecs);
	 }
	 else
	 {
	    stat = DRMS_ERROR_LIBDSDS;

            if (dsdsStat == kDSDS_Stat_DSDSOffline)
            {
               stat = DRMS_ERROR_DSDSOFFLINE;
            }
	 }
      }
   }
   else
   {
      fprintf(stdout, "Your JSOC environment does not support DSDS database access.\n");
      stat = DRMS_ERROR_NODSDSSUPPORT;
   }

   if (status)
   {
      *status = stat;
   }

   /* Clean-up in the case of an error happens in calling function */

   return rset;
}

static void QFree(void *data)
{
   char **realdata = (char **)data;
   if (realdata)
   {
      free(*realdata);
   }
}

/* recordsetname is a comma-separated list of recordsets.  
 * Must surround DSDS queries with '{' and '}'. 
 * 
 * allversout is an array of int flags, one for each record-subset, indicating
 * if the query for that subset is for all records with the prime-key value, 
 * of if the query yields just one, unique record for the prime-key value.
 */
DRMS_RecordSet_t *drms_open_records_internal(DRMS_Env_t *env, 
					     const char *recordsetname, 
					     int retrieverecs, 
					     LinkedList_t **llistout,
                                             char **allversout,
                                             int nrecslimit, 
					     int *status)
{
  DRMS_RecordSet_t *rs = NULL;
  DRMS_RecordSet_t *ret = NULL;
  int i, filter, mixed;
  char *query=0, *seriesname=0;
  HContainer_t *realSets = NULL;
  int nRecs = 0;
  int j = 0;
  char buf[64];
  DSDS_KeyList_t **klarr = NULL;
  DRMS_Segment_t *segarr = NULL;
  int nRecsLocal = 0;
  char *pkeys = NULL;
  char *seglist = NULL;
  char *actualSet = NULL;
  char *psl = NULL;
  char *lasts = NULL;
  char *ans = NULL;
  HContainer_t *goodsegcont = NULL;
  char *countquery = NULL;
  DB_Text_Result_t *tres = NULL;;

  /* Must save SELECT statements if saving the query is desired (retreiverecs == 0) */
  LinkedList_t *llist = NULL;

  if (llistout)
  {
     llist = list_llcreate(sizeof(char *), QFree);
     *llistout = llist;
  }

  /* conflict with stat var in this scope */
  int (*filestat)(const char *, struct stat *buf) = stat;

  /* recordsetname is a list of comma-separated record sets
   * commas may appear within record sets, so need to use a parsing 
   * mechanism more sophisticated than strtok() */
  char **sets = NULL;
  DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
  char **snames = NULL;
  int *setstarts = NULL;
  
  int nsets = 0;
  char *allvers = NULL; /* If 'y', then don't do a 'group by' on the primekey value.
                         * The rationale for this is to allow users to get all versions
                         * of the requested DRMS records */
  DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
  int stat = ParseRecSetDesc(recordsetname, &allvers, &sets, &settypes, &snames, &nsets, &rsinfo);

  if (stat == DRMS_SUCCESS)
  {
     int iSet;

     if (nsets > 0)
     {
        if (allversout)
        {
           *allversout = strdup(allvers);
        }
     }

     CHECKNULL_STAT(env,status);

     if (nsets > 0)
     {
        setstarts = (int *)malloc(sizeof(int) * nsets);
     }

     for (iSet = 0; stat == DRMS_SUCCESS && iSet < nsets; iSet++)
     {
	char *oneSet = sets[iSet];

	if (oneSet && strlen(oneSet) > 0)
	{
	   if (settypes[iSet] == kRecordSetType_PlainFile)
	   {
	      char pbuf[DRMS_MAXPATHLEN];
	      struct stat stBuf;
	      int foundOV = 0;
           

#if !defined(DSDS_SUPPORT) || !DSDS_SUPPORT
           stat = DRMS_ERROR_NODSDSSUPPORT;
           goto failure;
#endif

	      if (!(*filestat)(oneSet, &stBuf) && S_ISDIR(stBuf.st_mode))
	      {
		 /* Append '/' if necessary */
		 snprintf(pbuf, sizeof(pbuf), "%s", oneSet);

		 if (oneSet[strlen(oneSet) - 1] == '/')
		 {
		    snprintf(pbuf, sizeof(pbuf), "%s", oneSet);
		 }
		 else
		 {
		    snprintf(pbuf, sizeof(pbuf), "%s/", oneSet);
		 }

		 /* Ack - have to examine each file in the dir and figure out
		  * if any filenames have "overview.fits" int them.  Ignore
		  * subdirs. */
		 struct dirent **fileList = NULL;
		 int nFiles = -1;
		    
		 if ((nFiles = scandir(pbuf, &fileList, NULL, NULL)) > 0 && 
		     fileList != NULL)
		 {
		    int fileIndex = 0;

		    while (fileIndex < nFiles)
		    {
		       struct dirent *entry = fileList[fileIndex];
		       if (entry != NULL)
		       {
			  char *oneFile = entry->d_name;
			  char dirEntry[PATH_MAX] = {0};
			  snprintf(dirEntry, 
				   sizeof(dirEntry), 
				   "%s%s", 
				   pbuf,
				   oneFile);
			  if (*dirEntry !=  '\0' && 
			      !(*filestat)(dirEntry, &stBuf) &&
			      S_ISREG(stBuf.st_mode))
			  {
			     /* Finally, check to see if the file name has
			      * "overview.fits" in it */
			     if (strstr(dirEntry, kOverviewFits))
			     {
				foundOV = 1;
				break;
			     }
			  }

			  free(entry);
		       }

		       fileIndex++;
		    }
		 }	
	      }

	      if (foundOV)
	      {
		 rs = drms_open_dsdsrecords(env, pbuf, &stat);
		 if (stat)
		   goto failure; 
	      }
	      else
	      {
		 if (IsValidPlainFileSpec(oneSet, &klarr, &segarr, &nRecsLocal, &pkeys, &stat))
		 {
		    rs = OpenPlainFileRecords(env, &klarr, &segarr, nRecsLocal, &pkeys, &stat);
		    if (stat)
		      goto failure;
		 }
		 else
		 {
		    fprintf(stderr, "Invalid plain file record-set specification %s.\n", oneSet);
		    goto failure;
		 }
	      }
	   } /* Plain File */
	   else if (settypes[iSet] == kRecordSetType_DSDS)
	   {
#if !defined(DSDS_SUPPORT) || !DSDS_SUPPORT
           stat = DRMS_ERROR_NODSDSSUPPORT;
           goto failure;
#endif
           
	      rs = drms_open_dsdsrecords(env, oneSet, &stat);
	      if (stat)
              {
                 if (stat == DRMS_ERROR_DSDSOFFLINE)
                 {
                    fprintf(stderr, 
                            "Series '{%s}' data files are offline.\nBring them online with \"peq -A \'%s\'\".\n",
                            oneSet,
                            oneSet);
                 }
                 goto failure; 
              }
	   } /* DSDS */
	   else if (settypes[iSet] == kRecordSetType_VOT)
	   {
	      /* TBD */
	      fprintf(stderr, "VOT record-set specification not implemented.\n");
	   } /* VOT */
           else if (settypes[iSet] == kRecordSetType_DSDSPort)
           {
              /* Issue: if the data are offline, then there MIGHT be a failure. 
               * You can't create a temporary series unless the data are online
               * (the DRMS series, eg., ds_mdi.XXX, doesn't have keyword or segment
               * information.  That information lives in the FITS files, and if they
               * are offline, you can't return a valid record set.  So, you could 
               * call 'peq -A' (peq should know about ds_mdi.XXX and dsds.XXX)
               * and then when the data are put back online you can create your temporary
               * series and a recordset.  OR, you could simply fail.  The action
               * taken is determined inside libdsds.so.  
               *
               * Currently (8/20/2008), if the data are offline, a failure will occur.
               */
#if !defined(DSDS_SUPPORT) || !DSDS_SUPPORT
               stat = DRMS_ERROR_NODSDSSUPPORT;
               goto failure;
#endif
               
              rs = drms_open_dsdsrecords(env, oneSet, &stat);
	      if (stat)
              {
                 if (stat == DRMS_ERROR_DSDSOFFLINE)
                 {
                    fprintf(stderr, 
                            "Series '{%s}' data files are offline.\nBring them online with \"peq -A \'%s\'\".\n",
                            oneSet,
                            oneSet);
                 }
                 goto failure; 
              }
              
           } /* DSDSPort */
	   else if (settypes[iSet] == kRecordSetType_DRMS)
	   {
	      /* oneSet may have a segement specifier - strip that off and 
	       * generate the HContainer_t that contains the requested segment 
	       * names. */
	      actualSet = strdup(oneSet);
	      if (actualSet)
	      {
		 psl = strchr(actualSet, '{');
		 if (psl)
		 {
		    seglist = strdup(psl);
		    *psl = '\0';
		 }
	      
		 TIME(stat = drms_recordset_query(env, actualSet, &query, &seriesname, 
					       &filter, &mixed, NULL));

                 if (actualSet)
                 {
                    free(actualSet);
                    actualSet = NULL;
                 }
	      }
	      else
		goto failure;

	      if (stat)
		goto failure;

#ifdef DEBUG  
	      printf("seriesname = %s\n",seriesname);
	      printf("query = %s\n",query);
#else
              if (env->verbose)
              {
                 printf("seriesname = %s\n",seriesname);
                 printf("where clause = %s\n",query);
              }
#endif

	      if (seglist)
	      {
                 char aseg[DRMS_MAXSEGNAMELEN];
		 goodsegcont = hcon_create(DRMS_MAXSEGNAMELEN, 
					   DRMS_MAXSEGNAMELEN,
					   NULL,
					   NULL,
					   NULL,
					   NULL,
					   0);

		 ans = strtok_r(seglist, " ,;:{}", &lasts);

		 do
		 {
		    /* ans is a segment name */
                    snprintf(aseg, sizeof(aseg), "%s", ans);
		    hcon_insert_lower(goodsegcont, aseg, aseg);
		 }
		 while ((ans = strtok_r(NULL, " ,;:{}", &lasts)) != NULL);

		 free(seglist);
                 seglist = NULL;
	      }

	      if (retrieverecs)
	      {
                 XASSERT(allvers[iSet] != '\0');
		 TIME(rs = drms_retrieve_records(env, 
						 seriesname, 
						 query, 
						 filter, 
						 mixed, 
						 goodsegcont, 
                                                 allvers[iSet] == 'y',
                                                 nrecslimit, 
						 &stat));
                 /* Remove unrequested segments now */
	      }
	      else
	      {
                 /* Don't retrieve recs, because record-chunking 
                  * functions will.  Instead, make an 
                  * empty recordset and rs->n empty records. */
		 rs = (DRMS_RecordSet_t *)malloc(sizeof(DRMS_RecordSet_t));
                 memset(rs, 0, sizeof(DRMS_RecordSet_t));

                 countquery = drms_query_string(env, 
                                                seriesname, 
                                                query, 
                                                filter, 
                                                mixed, 
                                                DRMS_QUERY_COUNT, 
                                                NULL, 
                                                NULL,
                                                allvers[iSet] == 'y');
                 if (!countquery)
                   goto failure;

                 tres = drms_query_txt(env->session, countquery);

                 if (countquery)
                 {
                    free(countquery);
                    countquery = NULL;
                 }

              if (!tres)
              {
                  stat = DRMS_ERROR_QUERYFAILED;
                  goto failure;
              }
              
                 if (tres->num_rows == 1 && tres->num_cols == 1)
                 {
                    rs->n = atoi(tres->field[0][0]);
                 }
                 else
                 {
                    goto failure;
                 }

                 db_free_text_result(tres);

		 if (rs->n > 0)
		 {
		    rs->records = (DRMS_Record_t **)calloc(rs->n, sizeof(DRMS_Record_t *));
		 }
		 else
		 {
		    rs->records = NULL;
		 }

		 /* The following will be assigned later in this function */
		 rs->ss_n = 0;
		 rs->ss_queries = NULL;
		 rs->ss_types = NULL;
		 rs->ss_starts = NULL;
		 rs->ss_currentrecs = NULL;
		 rs->cursor = NULL;		
	      }

	      if (llist)
	      {
                 /* one query per query set (sets are delimited by commas) */
		 char *selquery = drms_query_string(env, 
						    seriesname, 
						    query, 
						    filter, 
						    mixed, 
						    DRMS_QUERY_ALL, 
                                                    NULL, 
						    NULL,
                                                    allvers[iSet] == 'y');
		 list_llinserttail(llist, &selquery);
	      }

	      if (goodsegcont)
	      {
                 hcon_destroy(&goodsegcont);
	      }

#ifdef DEBUG
	      printf("rs=%p, env=%p, seriesname=%s, filter=%d, stat=%d\n  query=%s\n",rs,env,seriesname,filter,stat,query);
#endif
	      if (stat)
		goto failure;

	      free(query);
	      query = NULL;
	      free(seriesname); 
	      seriesname = NULL;
	      
	      for (i=0; i<rs->n; i++)
	      {
                 if (rs->records[i])
                 {
                    rs->records[i]->lifetime = DRMS_PERMANENT; 
                 }
	      }
	   } /* DRMS */
	   else
	   {
	      fprintf(stderr, "Unexpected record-set specification %s.\n", oneSet);
	   }

	   if (stat)
	   {
	      goto failure;
	   }

	   if (nsets == 1)
	   {
	      /* optimize */
	      ret = rs;
	   }
	   else
	   {
	      if (!realSets)
	      {
		 realSets = hcon_create(sizeof(DRMS_RecordSet_t *), 
					64,
					NULL,
					NULL,
					NULL,
					NULL,
					0);
	      }
	   
	      /* save rs - combine at the end */
	      snprintf(buf, sizeof(buf), "%d", iSet);
	      hcon_insert(realSets, buf, &rs);
	      nRecs += rs->n;
	   }

	   rs = NULL;
	}
     } /* iSet */

     /* create the record set structure to return if necessary */
     if (!ret)
     {
	ret = (DRMS_RecordSet_t *)malloc(sizeof(DRMS_RecordSet_t));
	if (ret)
	{
	   ret->n = 0;
	   ret->records = NULL;
	   ret->ss_n = 0;
	   ret->ss_queries = NULL;
	   ret->ss_types = NULL;
	   ret->ss_starts = NULL;
	   ret->ss_currentrecs = NULL;
	   ret->cursor = NULL;
	}
     }

     if (ret)
     {
	if (nsets > 1)
	{
	   if (realSets && realSets->num_total > 0)
	   {
              /* merge sets, if more than one set requested */
	      if (nRecs > 0)
	      {
		 ret->records = 
		   (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nRecs);
		 ret->n = nRecs;

		 /* retain record set order */
		 j = 0;
		 DRMS_RecordSet_t **prs = NULL;
		 DRMS_RecordSet_t *oners = NULL;

		 for (iSet = 0; iSet < nsets; iSet++)
		 {
		    snprintf(buf, sizeof(buf), "%d", iSet);
		    if ((prs = hcon_lookup(realSets, buf)) != NULL)
		    {
		       oners = *prs;
		       if (oners)
		       {
			  if (oners->n > 0)
			  {
			     /* Save the number of records in a set into setstarts */
			     setstarts[iSet] = j;
			  }
			  else
			  {
			     setstarts[iSet] = -1; /* indicates this query led to no records */
			  }

			  /* Move oners to return RecordSet */
			  for (i = 0; i < oners->n; i++)
			  {
			     ret->records[j] = oners->records[i];
			     j++;
			  }
		       }

                       /* oners's records have been transferred to ret, but the record-set struct has not been freed. */
                       free(oners->records);
                       free(oners);
		       oners = NULL;
		    }
		    else
		    {
		       stat = DRMS_ERROR_INVALIDDATA;
		       goto failure;
		    }
		 } /* iSet */
	      }
	   }
	}
	else if (nsets > 0)
	{
	   /* One set only - save the number of records in a set into setstarts */
	   if (ret->n > 0)
	   {
	      setstarts[0] = 0;
	   }
	   else
	   {
	      /* All record queries are saved, even ones that produced
	       * no records (no records matching query criteria).
	       * If a query produced no records, then set the pointer
	       * to the first record to NULL.
	       */
	      setstarts[0] = -1;
	   }
	}

	/* Add fields that are used to track record-set sources */
	ret->ss_n = nsets;
	ret->cursor = NULL;

        if (nsets > 0)
        {
           ret->ss_starts = setstarts; /* ret assumes ownership */
           setstarts = NULL;
           ret->ss_currentrecs = (int *)malloc(sizeof(int) * nsets);

           /* ret can't assume ownership of sets or settypes */
           ret->ss_queries = (char **)malloc(sizeof(char *) * nsets);
           ret->ss_types = (DRMS_RecordSetType_t *)malloc(sizeof(DRMS_RecordSetType_t) * nsets);
           if (ret->ss_currentrecs && ret->ss_queries && ret->ss_types)
           {
              for (iSet = 0; iSet < nsets; iSet++)
              {
                 ret->ss_queries[iSet] = strdup(sets[iSet]);
                 ret->ss_types[iSet] = settypes[iSet];
                 ret->ss_currentrecs[iSet] = -1;
              }
           }
           else
           {
              stat = DRMS_ERROR_OUTOFMEMORY;
              goto failure;
           }
        }
     }
     else
     {
	stat = DRMS_ERROR_OUTOFMEMORY;
	goto failure;
     }

     if (realSets)
     {	
	hcon_destroy(&realSets);
     }

     if (setstarts)
     {
	free(setstarts);
     }

     FreeRecSetDescArr(&allvers, &sets, &settypes, &snames, nsets);

     if (status)
       *status = stat;

     return ret;
  }

 failure:
  if (query)
  {
     free(query);
  }

  if (seriesname)
  {
     free(seriesname);
  }

  if (setstarts)
  {
     free(setstarts);
  }
  if (actualSet)
  {
     free(actualSet);
  }

  if (seglist)
  {
     free(seglist);
  }

  FreeRecSetDescArr(&allvers, &sets, &settypes, &snames, nsets);

  if (rs)
  {
     RSFree(&rs);
  }

  if (ret)
  {
     RSFree(&ret);
  }
  
  if (realSets)
  {
     hcon_map(realSets, RSFree);
     hcon_destroy(&realSets);
  }

  if (countquery)
  {
     free(countquery);
  }

  if (tres)
  {
     db_free_text_result(tres);
  }

  if (status)
    *status = stat;
  return NULL;
}


DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, const char *recordsetname, 
				    int *status)
{
   char *allvers = NULL;
   DRMS_RecordSet_t *ret = NULL;

   ret = drms_open_records_internal(env, recordsetname, 1, NULL, &allvers, 0, status);
   if (allvers)
   {
      free(allvers);
   }

   return ret;
}

DRMS_RecordSet_t *drms_open_nrecords(DRMS_Env_t *env, 
                                     const char *recordsetname, 
                                     int n,
                                     int *status)
{
   char *allvers = NULL;
   DRMS_RecordSet_t *rs = NULL;
   DRMS_Record_t **recs = NULL;
   int statint = DRMS_SUCCESS;
   int iset;
   int irec;
   int nrecs = 0;
   DRMS_Record_t *rec = NULL;

   rs = drms_open_records_internal(env, recordsetname, 1, NULL, NULL, n, &statint);

   if (statint == DRMS_SUCCESS && n < 0)
   {
      /* loop through each record subset, and reverse the records (which are in descending prime-key order) */
      for (iset = 0; iset < rs->ss_n; iset++)
      {
         nrecs = drms_recordset_getssnrecs(rs, iset, &statint);
         recs = (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nrecs);
         if (recs)
         {
            memset(recs, 0, sizeof(DRMS_Record_t *) * nrecs);

            for (irec = 0; irec < nrecs; irec++)
            {
               rec = rs->records[(rs->ss_starts)[iset] + irec];
               recs[nrecs - irec - 1] = rec;
            }
         }
         else
         {
            statint = DRMS_ERROR_OUTOFMEMORY;
            break;
         }
      }
      
      if (statint == DRMS_SUCCESS)
      {
         /* now copy the record ptrs back to rs->records */
         memcpy(rs->records, recs, sizeof(DRMS_Record_t *) * nrecs);
      }

      if (recs)
      {
         free(recs);
      }
   }

   if (status)
   {
      *status = statint;
   }

   return rs;
}

/* Create n new records by calling drms_create_record n times.  */
DRMS_RecordSet_t *drms_create_records(DRMS_Env_t *env, int n, char *series,
				      DRMS_RecLifetime_t lifetime, int *status)
{
  DRMS_Record_t *template;

  /* Get template record for the series. */
  if ((template = drms_template_record(env, series, status)) == NULL)
    return NULL;
  return drms_create_records_fromtemplate(env, n, template, lifetime, status);
}

static void CountNonLinkedSegs(const void *value, void *data)
{
   int *count = (int *)data;
   DRMS_Segment_t *seg = (DRMS_Segment_t *)value;

   if (seg && count)
   {
      if (!seg->info->islink)
      {
         (*count)++;
      }
   }
}

/* Create n new records by calling drms_create_record n times.  */
DRMS_RecordSet_t *drms_create_records_fromtemplate(DRMS_Env_t *env, int n,  
						   DRMS_Record_t *template,     
						   DRMS_RecLifetime_t lifetime, 
						   int *status)
{
  int i=0, stat;
  DRMS_RecordSet_t *rs=NULL;
  long long *recnum=NULL;
  HIterator_t hit;
  DRMS_StorageUnit_t **su;
  DRMS_Segment_t *seg;
  int *slotnum;
  char filename[DRMS_MAXPATHLEN];
  char *series;
  int createslotdirs = 1;

  CHECKNULL_STAT(env,status);
  CHECKNULL_STAT(template,status);
  
  int summaryexists = -1;
  int canupdatesummaries = -1;

  int nnonlinkedsegs = 0;

  /* Cache the summary-table check */
  if (!gSummcon)
  {
     gSummcon = hcon_create(sizeof(char), DRMS_MAXSERIESNAMELEN, NULL, NULL, NULL, NULL, 0);

     if (!gSummcon)
     {
        stat = DRMS_ERROR_OUTOFMEMORY;
        goto failure;
     }

     BASE_Cleanup_t cu;
     cu.item = NULL;
     cu.free = drms_record_summchkcache_term;
     base_cleanup_register("summarytablechkcache", &cu);
  }

  if (gSummcon)
  {
     char *disp = (char *)hcon_lookup_lower(gSummcon, template->seriesinfo->seriesname);
     if (disp)
     {
        /* summary-table check has been performed, and results cached. */
        if (*disp == 'y')
        {
           /* summary table exists for this series. */
           summaryexists = 1;
        }
        else
        {
           /* summary table does not for this series. */
           summaryexists = 0;
        }
     }
     else
     {
        /* perform check. */
        summaryexists = drms_series_summaryexists(env, template->seriesinfo->seriesname, &stat);

        if (stat == DRMS_SUCCESS)
        {
           char res;

           if (summaryexists)
           {
              res = 'y';

              /* No need to check on the ability to update the summary table if the summary table 
               * does exist. */
              canupdatesummaries = drms_series_canupdatesummaries(env, template->seriesinfo->seriesname, &stat);
           }
           else
           {
              res = 'n';
           }

           /* cache result. */
           hcon_insert(gSummcon, template->seriesinfo->seriesname, &res);
        }

        if (stat == DRMS_SUCCESS)
        {
           if (summaryexists && !canupdatesummaries)
           {
              fprintf(stderr, "You must update your DRMS libraries before you can add records to series '%s'.\n", template->seriesinfo->seriesname);
              stat = DRMS_ERROR_CANTCREATERECORD;
              goto failure;
           }
        }
        else
        {
           goto failure;
        }
     }
  }


  if (n<1)
  {
    if (status)
      *status = DRMS_ERROR_BADRECORDCOUNT;
    return NULL;
  }

  /* Allocate the outer record set structure. */
  rs = malloc(sizeof(DRMS_RecordSet_t));
  XASSERT(rs);
  rs->records = malloc(n*sizeof(DRMS_Record_t *));
  XASSERT(rs->records);
  memset(rs->records, 0, sizeof(DRMS_Record_t *) * n);
  rs->n = n;
  rs->ss_n = 0;
  rs->ss_queries = NULL;
  rs->ss_types = NULL;
  rs->ss_starts = NULL;

  /* Need to malloc (used by drms_record_fetchnext()). */
  rs->ss_currentrecs = (int *)malloc(sizeof(int));
  *rs->ss_currentrecs = -1;
  rs->cursor = NULL;

  series = template->seriesinfo->seriesname;

  /* Get unique sequence numbers from the database server. */
  if ((recnum = drms_alloc_recnum(env, series, lifetime, n)) == NULL)
  {
    stat = DRMS_ERROR_BADSEQUENCE;
    goto failure;
  }

  /* Allocate data structures and populate them with default values
     from the series template. */
  for (i=0; i<n; i++)
  {
    if ((
	 rs->records[i] = drms_alloc_record2(template, recnum[i], &stat)
	 ) == NULL)
      goto failure;    

    rs->records[i]->readonly = 0;
    rs->records[i]->sessionid = env->session->sessionid;  
    rs->records[i]->sessionns = strdup(env->session->sessionns);
    rs->records[i]->lifetime = lifetime;
  }

  /* If this series has data segments associated with it then allocate 
     a storage unit slot for each record to hold them. */

  hcon_map_ext(&template->segments, CountNonLinkedSegs, &nnonlinkedsegs);

  //  if ( drms_record_numsegments(rs->records[0]) )
  if (nnonlinkedsegs > 0)
  {
    su = malloc(n*sizeof(DRMS_StorageUnit_t *));
    XASSERT(su);
    slotnum = malloc(n*sizeof(int));
    XASSERT(slotnum);
    
    /* If all segments are TAS segments, then there is no need to create 
     * slot dirs as all data will go into the SU */
    HIterator_t *seghit = hiter_create(&(template->segments));
    if (seghit)
    {
       createslotdirs = 0;
       while((seg = (DRMS_Segment_t *)hiter_getnext(seghit)))
       {
          if (seg->info->protocol != DRMS_TAS)
          {
             createslotdirs = 1;
          }
       }
       hiter_destroy(&seghit);
    }

    if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, su, createslotdirs)))
      goto failure;    
    
    for (i=0; i<n; i++)      
    {      
      rs->records[i]->slotnum = slotnum[i];
      rs->records[i]->su = su[i];
      su[i]->refcount++;
#ifdef DEBUG
      printf("record[%d]->su = %p\n",i,su[i]);
#endif
      rs->records[i]->sunum = rs->records[i]->su->sunum;
      if (slotnum[i]==0)
      {
	/* This is the first record in a new storage unit. Create empty TAS
	   files for each data segment stored in TAS format. */
	hiter_new(&hit, &rs->records[i]->segments);
	while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
	{
	  if (seg->info->protocol == DRMS_TAS)
	  {
	    drms_segment_filename(seg, filename);
	    seg->axis[seg->info->naxis] = rs->records[i]->seriesinfo->unitsize;
	    seg->blocksize[seg->info->naxis] = 1; 
#ifdef DEBUG
	    printf("creating new tasfile '%s'\n",filename);
#endif

            drms_fitstas_create(env,
                                filename, 
                                seg->cparms,
                                seg->info->type, 
				seg->info->naxis+1, 
                                seg->axis,
                                seg->bzero,
                                seg->bscale);

	    seg->axis[seg->info->naxis] = 0;
	    seg->blocksize[seg->info->naxis] = 0; 
	  }
	}

        hiter_free(&hit);
      }
    }
    free(su);
    free(slotnum);
  }
  else
    for (i=0; i<n; i++)      
       rs->records[i]->su = NULL;
  

  if (status)
    *status = 0;
  free(recnum);
  return rs;

 failure:
  if (rs)
    drms_close_records(rs, DRMS_FREE_RECORD);
  if (recnum)
    free(recnum);
  if (status)
    *status = stat;
  return NULL;
}

/* This copies an existing set of records, but ensures that there are no 
 * pointers between these records and anything else in DRMS (like the record
 * cache).  The created record structures must be cleaned up by calling 
 * either drms_free_template_record_struct() or drms_free_record_struct().
 * Use the former in all cases, except if you pass the records to the
 * drms_create_series() call.  If drms_create_series() succeeds, then clean
 * up the passed in record by calling drms_free_record_struct().
 */
DRMS_RecordSet_t *drms_create_recprotos(DRMS_RecordSet_t *recset, int *status)
{
   DRMS_RecordSet_t *detached;
   DRMS_Record_t *recSource = NULL;
   DRMS_Record_t *recTarget = NULL;

   int nRecs = recset->n;

   detached = malloc(sizeof(DRMS_RecordSet_t));
   XASSERT(detached);
   detached->records = malloc(nRecs * sizeof(DRMS_Record_t *));
   XASSERT(detached->records);

   if (detached && detached->records)
   {
      *status = DRMS_SUCCESS;
      detached->n = nRecs;
      detached->ss_n = 0;
      detached->ss_queries = NULL;
      detached->ss_types = NULL;
      detached->ss_starts = NULL;
      detached->ss_currentrecs = NULL;
      detached->cursor = NULL;
   }
   else
   {
      *status = DRMS_ERROR_OUTOFMEMORY;
   }

   int idx = 0;
   for (; *status == DRMS_SUCCESS && idx < nRecs; idx++) 
   {
      recSource = recset->records[idx];
      recTarget = drms_create_recproto(recSource, status);

      if (*status == DRMS_SUCCESS)
      {
	 /* Insert record into return set. */
	 detached->records[idx] = recTarget;
      }
   }
   
   return detached;
}

void drms_destroy_recprotos (DRMS_RecordSet_t **protos) {
  if (protos && *protos) {
    int idx, nRecs = (*protos)->n;
    for (idx = 0; idx < nRecs; idx++) 
      drms_destroy_recproto ((DRMS_Record_t **)((*protos)->records)[idx]);
  }
}

DRMS_Record_t *drms_create_recproto(DRMS_Record_t *recSource, int *status)
{
   DRMS_Record_t *detached = NULL;
   DRMS_Record_t *recTarget = NULL;

   recTarget = calloc(1, sizeof(DRMS_Record_t));
   XASSERT(recTarget);
   recTarget->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t));
   XASSERT(recTarget->seriesinfo);

   if (recTarget && recTarget->seriesinfo)
   {
      recTarget->env = recSource->env;
      recTarget->init = 1;
      recTarget->recnum = 0;
      recTarget->sunum = -1;
      recTarget->sessionid = 0;
      recTarget->sessionns = NULL;
      recTarget->su = NULL;
      
      /* Initialize container structure. */
      hcon_init(&recTarget->segments, sizeof(DRMS_Segment_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_segment_struct, 
		(void (*)(const void *, const void *)) drms_copy_segment_struct);
      /* Initialize container structures for links. */
      hcon_init(&recTarget->links, sizeof(DRMS_Link_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_link_struct, 
		(void (*)(const void *, const void *)) drms_copy_link_struct);
      /* Initialize container structure. */
      hcon_init(&recTarget->keywords, sizeof(DRMS_Keyword_t), DRMS_MAXHASHKEYLEN, 
		(void (*)(const void *)) drms_free_keyword_struct, 
		(void (*)(const void *, const void *)) drms_copy_keyword_struct);
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopySeriesInfo(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to create series info.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopySegments(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy segments.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyLinks(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy links.\n");
	 }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyKeywords(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy keywords.\n");
	 }

         /* The series info must refer to The template */
         for (int i = 0; i < recTarget->seriesinfo->pidx_num; i++) 
         {
            recTarget->seriesinfo->pidx_keywords[i] = 
              drms_keyword_lookup(recTarget, recSource->seriesinfo->pidx_keywords[i]->info->name, 0);
         }

         for (int i = 0; i < recTarget->seriesinfo->dbidx_num; i++) 
         {
            recTarget->seriesinfo->dbidx_keywords[i] = 
              drms_keyword_lookup(recTarget, recSource->seriesinfo->dbidx_keywords[i]->info->name, 0);
         }
      }
      
      if (*status == DRMS_SUCCESS)
      {
	 if ((*status = CopyPrimaryIndex(recTarget, recSource)) != DRMS_SUCCESS)
	 {
	    fprintf(stderr,"Failed to copy primary index.\n");
	 }
      }
      
      if (*status != DRMS_SUCCESS)
      {
	 hcon_free(&recTarget->segments);
	 hcon_free(&recTarget->links);
	 hcon_free(&recTarget->keywords);
	 free(recTarget);
      }
      else
      {
	 detached = recTarget;
      }
   }
   else
   {
      *status = DRMS_ERROR_OUTOFMEMORY;
   }

   return detached;
}

void drms_destroy_recproto(DRMS_Record_t **proto)
{
   /* If proto is in series cache, shallow-free, else deep-free. */
   if (proto)
   {
      DRMS_Record_t *prototype = *proto;
      char *series = prototype->seriesinfo->seriesname;
      DRMS_Env_t *env = prototype->env;

      /* This is the definitive way to know if a series has been cached in series_cache. */
      DRMS_Record_t *rec = hcon_lookup_lower(&(env->series_cache), series);
      int deep = 1;

      if (rec)
      {
	 if (rec->seriesinfo == prototype->seriesinfo)
	 {
	    /* prototype has been cached */
	    deep = 0;
	 }
      }

      if (deep)
      {
	 drms_free_template_record_struct(prototype);
      }
      else
      {
	 drms_free_record_struct(prototype);
      }

      free(prototype);

      *proto = NULL;
   }
}


/* Create n new records by calling drms_create_record n times.  */

/* ART - now that we have a flag to control whether we talk to sums, drms_record_directory calls
 * may fail to retrieve a directory. Must check for NULL/empty directory strings. */
static DRMS_RecordSet_t *drms_clone_records_internal(DRMS_RecordSet_t *rs_in, 
                                                     DRMS_RecLifetime_t lifetime,  
                                                     DRMS_CloneAction_t mode, 
                                                     int gotosums,
                                                     int *status)
{
  int i, stat=0, first, last, n, n_total;
  DRMS_RecordSet_t *rs_out=NULL;
  DRMS_Record_t *rec_in, *rec_out;
  long long *recnum=NULL;
  HIterator_t hit_in,hit_out;
  DRMS_StorageUnit_t **su;
  DRMS_Segment_t *seg_in, *seg_out;
  int *slotnum;
  char dir_in[DRMS_MAXPATHLEN], dir_out[DRMS_MAXPATHLEN];
  char command[2*DRMS_MAXPATHLEN+20], filename[DRMS_MAXPATHLEN];
  char *series;
  DRMS_Env_t *env;
  DRMS_Array_t *arr;
  int createslotdirs = 1;
  HIterator_t *seghit = NULL;
  DRMS_Record_t *therec = NULL;

  CHECKNULL_STAT(rs_in,status);
  n_total = rs_in->n;
  if (n_total<1)
  {
    if (status)
      *status = DRMS_ERROR_BADRECORDCOUNT;
    return NULL;
  }
  CHECKNULL_STAT(rs_in->records,status);
  CHECKNULL_STAT(rs_in->records[0],status);
  env = rs_in->records[0]->env;
  
  /* Allocate the outer record set structure. */
  rs_out = malloc(sizeof(DRMS_RecordSet_t));
  XASSERT(rs_out);
  rs_out->records = malloc(n_total*sizeof(DRMS_Record_t *));
  XASSERT(rs_out->records);
  rs_out->n = n_total;
  rs_out->ss_n = 0;
  rs_out->ss_queries = NULL;
  rs_out->ss_types = NULL;
  rs_out->ss_starts = NULL;
  rs_out->ss_currentrecs = NULL;
  rs_out->cursor = NULL;
  
  /* Outer loop over runs of input records from the same series. */
  first = 0;
  while(first<n_total)
  {
    series = rs_in->records[first]->seriesinfo->seriesname;
    last = first+1;
    while(last<n_total && 
	  !strcmp(series, rs_in->records[last]->seriesinfo->seriesname))
      ++last;
    n = last-first;
    
    /* Get unique sequence numbers from the database server. */
    if ((recnum = drms_alloc_recnum(env, series, lifetime, n)) == NULL)
    {
      stat = DRMS_ERROR_BADSEQUENCE;
      goto failure;
    }

    /* Allocate data structures and populate them with values from the 
       input records. */
    for (i=0; i<n; i++)
    {
      rs_out->records[first+i] = drms_alloc_record2(rs_in->records[first+i], 
						    recnum[i], &stat);
      if (rs_out->records[first+i] == NULL) {
	stat = 1;
	goto failure;    
      }
      
      rs_out->records[first+i]->readonly = 0;
      rs_out->records[first+i]->sessionid = env->session->sessionid;      
      rs_out->records[first+i]->sessionns = strdup(env->session->sessionns);   
      rs_out->records[first+i]->lifetime = lifetime;
    }

    /* Process nonlink data segments if this series has any associated
       with it. */ 
    if ( drms_record_num_nonlink_segments(rs_out->records[first]) )
    {
      switch(mode)
      {
      case DRMS_SHARE_SEGMENTS:
        /* look over records in the input record set in the current series. */
        /* ARTA - PERFORMANCE ISSUE : If we're going to talk to SUMS, we could batch together
         * all records and pass to SUMS a batch of SUNUMs (instead of passing a single SUNUM
         * as is done now). */
	for (i=0; i<n; i++) 
	{
           therec = rs_in->records[first+i];
           /* drms_record_directory is called simply to get the SU struct, not to obtain the SUdir. */
           if (gotosums)
           {
              /* If there is no SU associated with this record, then the following will do nothing, 
               * and it won't communicate with SUMS. */
              drms_record_directory(therec, dir_in, 1);
           }
           else
           {
              /* The whole point of the code in this statement is to get a DRMS_StorageUnit_t struct. 
               * We can do that with a drms_getunit() call instead of drms_record_directory(). */
              if (therec->sunum != -1LL && therec->su == NULL) 
              {
                 /* drms_getunit_nosums() cannot return the sudir because that requires SUMS access. */
                 if ((therec->su = drms_getunit_nosums(therec->env, therec->seriesinfo->seriesname,
                                                       therec->sunum, &stat)) == NULL) 
                 {
                    if (stat) 
                    {
                       fprintf (stderr, "ERROR in drms_clone_records_internal: stat = %d\n", stat);
                       goto failure;
                    }
                 }
                 else
                 {
                    therec->su->refcount++;
                 }
              }
           }

	   if (therec->su)
	   {
	      rs_out->records[first+i]->su = therec->su;
	      rs_out->records[first+i]->su->refcount++;
	   }
	}
	break;
      case DRMS_COPY_SEGMENTS:
        su = malloc(n*sizeof(DRMS_StorageUnit_t *));
        XASSERT(su);
        slotnum = malloc(n*sizeof(int));
        XASSERT(slotnum);

        /* If all segments are TAS segments, then there is no need to create 
         * slot dirs as all data will go into the SU */
        seghit = hiter_create(&((rs_out->records[0])->segments));
        if (seghit)
        {
           createslotdirs = 0;
           while((seg_out = (DRMS_Segment_t *)hiter_getnext(seghit)))
           {
              if (seg_out->info->protocol != DRMS_TAS)
              {
                 createslotdirs = 1;
              }
           }
           hiter_destroy(&seghit);
        }
      
	/* Allocate new SU slots for copies of data segments. */
        /* This call MAY end up calling SUM_alloc(), but it may not. If there are a sufficient number
         * of slots available to accommodate the new records, then SUMS will not be called. If gotosums
         * is 0, and there are not a sufficient number of slots, then drms_newslots() should return an 
         * error. */

        if (gotosums)
        {
           if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, su, createslotdirs))) 
           {
              stat = 1;
              goto failure;    
           }
        }
        else
        {
           if ((stat = drms_newslots_nosums(env, n, series, recnum, lifetime, slotnum, su, createslotdirs))) 
           {
              if (stat == DRMS_ERROR_NEEDSUMS)
              {
                 /* The caller prohibited SUMS access, but drms_newslots_nosums() required SUMS access
                  * to create new slots (because there wasn't any room left in any SU for new slots). */
                 fprintf(stderr, "Cannot obtain new slot dirs without making a SUMS request.\n");
              }
              stat = 1;
              goto failure;    
           }
        }
      
	/* Copy record directories and TAS data segments (slices)
	   record-by-record. */
	for (i=0; i<n; i++)      
	{     
	  rec_in = rs_in->records[first+i];
	  rec_out = rs_out->records[first+i];
	    
	  rec_out->slotnum = slotnum[i];
	  rec_out->su = su[i];
	  su[i]->refcount++;
	  rec_out->sunum = su[i]->sunum;

	  /* Copy record directory. */
          if (gotosums)
          {
             drms_record_directory(rec_in, dir_in, 1);
             drms_record_directory(rec_out, dir_out, 1);
          }
          else
          {
             if (drms_record_directory_nosums(rec_in, dir_in, sizeof(dir_in)) == DRMS_ERROR_NEEDSUMS ||
                 drms_record_directory_nosums(rec_out, dir_out, sizeof(dir_out)) == DRMS_ERROR_NEEDSUMS)
             {
                fprintf(stderr, "Cannot obtain the input/output record directory without making a SUMS request.\n");
                stat = 1;
                goto failure;
             }
          }

          if (*dir_in == '\0' || *dir_out == '\0')
          {
             fprintf(stderr, "Unable to obtain the input/output directory.\n");
             stat = 1;
             goto failure;
          }

	  DIR *dp;
	  struct dirent *dirp;
	  if ((dp = opendir(dir_in)) == NULL) {
	    fprintf(stderr, "Can't open %s\n", dir_in);
	    stat = 1;
	    goto failure;
	  }
	  while ((dirp = readdir(dp)) != NULL) {
	    if (!strcmp(dirp->d_name, ".") ||
		!strcmp(dirp->d_name, "..")) 
	      continue;
	    sprintf(command,"cp -rf %s/* %s", dir_in, dir_out);
#ifdef DEBUG
	    printf("executing %s\n",command);
#endif
	    if(system(command))
	      {
		fprintf(stderr, "ERROR: system command to copy record directory"
			" failed.\n");
		stat = 1;
		goto failure;
	      }
	    break;
	  }
	  closedir(dp);
	  /* Loop over segments and copy any TAS segments one by one. */
	  hiter_new_sort(&hit_in, &rec_in->segments, drms_segment_ranksort);
	  hiter_new_sort(&hit_out, &rec_out->segments, drms_segment_ranksort);
	  while ((seg_in = (DRMS_Segment_t *)hiter_getnext(&hit_in)) && 
		 (seg_out = (DRMS_Segment_t *)hiter_getnext(&hit_out)))
	  {
	    if (seg_out->info->protocol == DRMS_TAS)
	    {
	      if (slotnum[i]==0)
	      {
		/* This is the first record in a new storage unit. 
		   Create an empty TAS file. */

                 /* drms_segment_filename() will make a SUMS request if the SU struct 
                  * hasn't been initialized. BUT we know that the SU struct has been
                  * intialized because execution will not reach this block of code
                  * otherwise (we successfully obtained the output SUdir already.) */
                drms_segment_filename(seg_out, filename);
		seg_out->axis[seg_out->info->naxis] = rec_out->seriesinfo->unitsize;
		seg_out->blocksize[seg_out->info->naxis] = 1; 

                drms_fitstas_create(env,
                                    filename, 
                                    seg_out->cparms,
                                    seg_out->info->type, 
                                    seg_out->info->naxis+1, 
                                    seg_out->axis,
                                    seg_out->bzero,
                                    seg_out->bscale);
	
		seg_out->axis[seg_out->info->naxis] = 0;
		seg_out->blocksize[seg_out->info->naxis] = 0; 
	      }

              /* drms_segment_read() will make a SUMS request if the SU struct 
               * hasn't been initialized. BUT we know that the SU struct has been
               * intialized because execution will not reach this block of code
               * otherwise (we successfully obtained the input SUdir already.) */
	      if ((arr = drms_segment_read(seg_in,DRMS_TYPE_RAW,&stat))==NULL)
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to read "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_in->info->name, rec_in->seriesinfo->seriesname, 
			rec_in->recnum);
		stat = 1;
		goto failure;
	      }

              /* drms_segment_write() will make a SUMS request if the SU struct 
               * hasn't been initialized. BUT we know that the SU struct has been
               * intialized because execution will not reach this block of code
               * otherwise (we successfully obtained the output SUdir already.) */
	      if (drms_segment_write(seg_out, arr, 0))
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to write "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_out->info->name, rec_out->seriesinfo->seriesname, 
			rec_out->recnum);
		stat = 1;
		goto failure;
	      }
	      drms_free_array(arr);
	    }
	  }
	}
	free(su);
	free(slotnum);
        /* END DRMS_COPY_SEGMENTS*/
	break;
      }
    }
    else
      for (i=0; i<n; i++)      
	rs_out->records[first+i]->su = NULL;
    first = last;
  }
  

  if (status)
    *status = 0;
  free(recnum);
  return rs_out;

 failure:
  fprintf(stderr,"FAILURE in clone_records.\n");
  drms_close_records(rs_out, DRMS_FREE_RECORD);
  if (recnum)
    free(recnum);
  if (status)
    *status = stat;
  return NULL;
}

DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *rs_in, 
				     DRMS_RecLifetime_t lifetime,  
				     DRMS_CloneAction_t mode, int *status)
{
   /* This call will result in a SUMS requests in some cases (if the mode is to share segments, for example). */
   return drms_clone_records_internal(rs_in, lifetime, mode, 1, status);
}

DRMS_RecordSet_t *drms_clone_records_nosums(DRMS_RecordSet_t *rs_in, 
                                            DRMS_RecLifetime_t lifetime,  
                                            DRMS_CloneAction_t mode, 
                                            int *status)
{
   /* Does not allow SUMS access (if SUMS access is required for successful completion, then an error
    * code is returned). */
   return drms_clone_records_internal(rs_in, lifetime, mode, 0, status);
}


/* Call drms_close_record for each record in a record set. */
int drms_close_records(DRMS_RecordSet_t *rs, int action)
{
  int i, status=0;
  DRMS_Record_t *rec;
  
  CHECKNULL(rs);

  status = 0;
  switch(action)
  {
  case DRMS_FREE_RECORD:
    for (i=0; i<rs->n; i++)
    {
      rec = rs->records[i];
      /* If this record was temporarily created by this session then
	 free its storage unit slot. */
      /* if (rec && !rec->readonly && rec->su) 
       * Checking refcount isn't correct here. recount is used only by direct-connect modules, and 
       * it is the refcount on the entire SU. We don't refcount slots, which is what we should be
       * checking here (i.e., slotrefcount == 1).
       */
      if (rec && !rec->readonly && rec->su && rec->su->refcount==1)
      {
	if ((status = drms_freeslot(rec->env, rec->seriesinfo->seriesname, 
				    rec->su->sunum, rec->slotnum)))
	{
	  fprintf(stderr,"ERROR in drms_close_record: drms_freeslot failed with "
		  "error code %d\n", status);
	  break;
	}
      }
    }
    break;
  case DRMS_INSERT_RECORD:
    for (i=0; i<rs->n; i++)
    {
      if (rs->records[i]->readonly)
      {
	fprintf(stderr,"ERROR in drms_close_record: trying to commit a "
		"read-only record.\n");
	status = DRMS_ERROR_COMMITREADONLY;
	break;
      }
    }
    if (!status)
    {
#ifdef DEBUG
      printf("Inserting records...\n");
#endif
      if ((status = drms_insert_records(rs)))
      {
	fprintf(stderr,"ERROR in drms_close_record: Could not insert record "
		"in the database.\ndrms_insert_records failed with error "
		"code %d\n",status);
      }
    }
    break;
  default:
    fprintf(stderr,"ERROR in drms_close_record: Action flag (%d) "
	    "is invalid.\n",action);
    status = DRMS_ERROR_INVALIDACTION;
  }

  if (rs->cursor && rs->cursor->currentrec >= 0)
  {
     rs->cursor->currentchunk = -1;
     rs->cursor->currentrec = -1;
  }

  drms_free_records(rs);
  return status;
}

static void FreeSumsInfo(const void *value)
{
   SUM_info_t *tofree = *((SUM_info_t **)value);
   if (tofree)
   {
      free(tofree);
   }
}

static int SumsInfoSort(const void *he1, const void *he2)
{
   SUM_info_t **pi1 = (SUM_info_t **)hcon_getval(*((HContainerElement_t **)he1));
   SUM_info_t **pi2 = (SUM_info_t **)hcon_getval(*((HContainerElement_t **)he2));
   
   XASSERT(pi1 && pi2);

   /* Some SUs might be online already. Those SUs should sort before the offline
    * ones, in increasing SUNUM order. Then sort the offline SUs by
    * by tapeid (SUM_info_t::arch_tape), then by filenum (SUM_info_t::arch_tape_fn). */
   SUM_info_t *i1 = *pi1;
   SUM_info_t *i2 = *pi2;
   int cval;

   /* if the online_loc is empty, then the SUNUM was invalid (eg., data aged off and not archived). */
   int i1invalid = (*i1->online_loc == '\0');
   int i2invalid = (*i2->online_loc == '\0');
   int i1offline = (!i1invalid && 
                    strcasecmp(i1->archive_status, "Y") == 0 && 
                    strcasecmp(i1->online_status, "N") == 0 &&
                    *i1->arch_tape != '\0' && 
                    strcasecmp(i1->arch_tape, "N/A") != 0);
   int i2offline = (!i2invalid && 
                    strcasecmp(i2->archive_status, "Y") == 0 && 
                    strcasecmp(i2->online_status, "N") == 0 &&
                    *i2->arch_tape != '\0' &&
                    strcasecmp(i2->arch_tape, "N/A") != 0);

   /* Sort in this order: an invalid or online SU sorts before an offline SU. If both SUs are invalid or online, sort
    * by SUNUM. If both are offline, sort by tapeid/filenum. */

   if (i1offline && i2offline)
   {
      /* both SUs are offline */
      cval = strcmp(i1->arch_tape, i2->arch_tape);

      if (cval != 0)
      {
         return cval;
      }
      else
      {
         /* tapeids are the same, sort by filenum */
         return (i1->arch_tape_fn < i2->arch_tape_fn) ? -1 : (i1->arch_tape_fn > i2->arch_tape_fn ? 1 : 0);
      }
   }
   else if (i2offline)
   {
      /* i1 invalid/online, i2 offline */
      return -1;
   }
   else if (i1offline)
   {
      /* i1 offline, i2 invalid/online */
      return 1;
   }
   else
   {
      /* both invalid/online - sort by sunum */
      return (i1->sunum < i2->sunum) ? -1 : 1; /* can't be the same - these are different SUs. */
   }
}

static void SuInfoCopyMap(const void *key, const void *value, const void *data)
{
   HContainer_t *dst = (HContainer_t *)(data);
   HContainerElement_t *elem = (HContainerElement_t *)value;
   SUM_info_t *src = NULL;
   SUM_info_t *dup = NULL;

   /* Duplicate source SUM_info_t (elem->val). */
   if (elem->val && *((SUM_info_t **)elem->val))
   {
      src = *((SUM_info_t **)elem->val);
      dup = malloc(sizeof(SUM_info_t));
      *dup = *src;
      hcon_insert(dst, key, &dup);
   }
}

/* Does not take ownership of suinfo, copies it. */
static int drms_stage_records_internal(DRMS_RecordSet_t *rs, int retrieve, int dontwait, HContainer_t **suinfo) 
{

  if (!rs) {
    return DRMS_SUCCESS;
  }

  int status = 0;

  DRMS_Record_t *rec = NULL;
  int nSets = rs->ss_n;
  int nRecs = 0;
  int iSet;
  int iRec;
  int bail = 0;

  DRMS_SuAndSeries_t *sunum = NULL;
  int cnt;
  HContainer_t *scon = NULL;

  int sortalso = 0;
  SUM_info_t **ponesuinfo = NULL;
  char key[128];
  HContainer_t *suinfoauth = NULL;
  long long *sunumreqinfo = NULL;
  char **seriesreqinfo = NULL;
  int cntreqinfo;
  SUM_info_t **infostructs = NULL;
  int iinfo;
  DRMS_Env_t *env = NULL;
  SUM_info_t *dup = NULL;

  SUM_info_t *dummy = NULL;

  sortalso = (suinfo != NULL);

  /* if recordset is result of drms_open_recordset with a cursor simply remember
     that stage_records has been called.  Actual staging will happen when each
     chunk is fetched. drms_record_fetchnext() will call this function again, 
     but with a new, clean recordset. Then it will copy this recordset back
     over the original recordset.
  */
  if (rs->cursor)
  {
     rs->cursor->staging_needed = sortalso ? 2: 1; // 2 signifies "sort also"
     rs->cursor->retrieve = retrieve;
     rs->cursor->dontwait = dontwait;
     if (sortalso)
     {
        /* Save the SUM_info_t data for later use in drms_open_recordchunk(). Copy
         * the suinfo passed in because drms_close_records() will free the one in 
         * rs->cursor->suinfo. */
        if (*suinfo && hcon_size(*suinfo) > 0)
        {
           if (!rs->cursor->suinfo)
           {
              rs->cursor->suinfo = hcon_create(sizeof(SUM_info_t *), 128, FreeSumsInfo, NULL, NULL, NULL, 0);
           }

           /* Can't use hcon_copy() because the result would be the sharing of SUM_info_t between
            * the original and the copy. But we can use a new copy function that allocates 
            * new SUM_info_ts. */
           hash_map_data(&((*suinfo)->hash), SuInfoCopyMap, rs->cursor->suinfo);
        }
     }
     return(DRMS_SUCCESS);
  }

  if (rs->n >= 1)
  {
     env = rs->records[0]->env;
     dummy = (SUM_info_t *)NULL;

     /* Collect list of SUs to stage. */
     if (!sortalso)
     {
        sunum = (DRMS_SuAndSeries_t *)malloc(rs->n * sizeof(DRMS_SuAndSeries_t));
        XASSERT(sunum);
        cnt = 0;
     }

     /* Collect list of SUs to get SUM_info_t for. */
     sunumreqinfo = (long long *)malloc(rs->n * sizeof(long long));
     XASSERT(sunumreqinfo);
     seriesreqinfo = (char **)malloc(rs->n * sizeof(char *));
     XASSERT(seriesreqinfo);
     cntreqinfo = 0;

     for (iSet = 0; !bail && iSet < nSets; iSet++)
     {
        nRecs = drms_recordset_getssnrecs(rs, iSet, &status);

        if (status != DRMS_SUCCESS)
        {
           bail = 1;
           break;
        }

        for (iRec = 0; iRec < nRecs; iRec++)
        {
           rec = rs->records[(rs->ss_starts)[iSet] + iRec];

           /* Include only SUs that are not already online. */
           if (rec->sunum != -1LL && rec->su == NULL) 
           {
              if (!suinfoauth)
              {
                 /* Need to ensure that for every SUNUM, we have a SUM_info_t which 
                  * contains the tapeid/filenum of that SUNUM. Create a new container 
                  * of SUM_info_ts, keyed by SUNUM. This will be used below to 
                  * fill in the DRMS_Record_t::suinfo field for each record. */

                 /* Since two or more records may refer to the same SUM_info_t, we will
                  * need to dupe each SUM_info_t into the DRMS_Record_t::suinfo field, 
                  * then delete the originals in suinfoauth - use FreeSumsInfo() to do the freeing. */
                 suinfoauth = (HContainer_t *)hcon_create(sizeof(SUM_info_t *), 128, FreeSumsInfo, NULL, NULL, NULL, 0);
              }

              snprintf(key, sizeof(key), "%lld", rec->sunum);

              if (sortalso)
              {
                 /* Populate DRMS_Record_t::suinfo */
                 if (!hcon_member(suinfoauth, key))
                 {
                    if (!rec->suinfo)
                    {
                       if (*suinfo && (ponesuinfo = (SUM_info_t **)hcon_lookup(*suinfo, key)))
                       {
                          /* The SU info was passed in to this function. */
                          dup = (SUM_info_t *)malloc(sizeof(SUM_info_t));
                          *dup = **ponesuinfo;
                          
                          /* Manually copy series name - rec->seriesinfo->seriesname 
                           * is more definitive than what was provided in suinfo. */
                          snprintf(dup->owning_series, 
                                   sizeof(dup->owning_series), 
                                   "%s",
                                   rec->seriesinfo->seriesname);
                          hcon_insert(suinfoauth, key, &dup);
                       }
                       else
                       {
                          /* There is no SU info available - add to list of SUNUMs that
                           * will be sent to SUM_InfoEx(). The results of SUM_InfoEx() 
                           * will be pushed back into the suinfoauth container. This container 
                           * will then be used to populate the DRMS_Record_t::suinfo fields. */
                          sunumreqinfo[cntreqinfo] = rec->sunum;
                          seriesreqinfo[cntreqinfo] = rec->seriesinfo->seriesname;
                          cntreqinfo++;
                       
                          /* Insert dummy into suinfoauth so that we don't request the same
                           * sunum from SUM_InfoEx() more than once. When this SUM_info_t
                           * gets freed by FreeSumsInfo(), there will be a call to free(NULL)
                           * which generally is a nop. */
                          hcon_insert(suinfoauth, key, &dummy);
                       }
                    }
                    else
                    {
                       /* Put the existing SUM_info_t into suinfoauth. We want all info in this 
                        * one container, then we sort the container. */
                       dup = (SUM_info_t *)malloc(sizeof(SUM_info_t));
                       *dup = *rec->suinfo;

                       /* Manually copy series name - rec->seriesinfo->seriesname 
                        * is more definitive than what was provided in suinfo. */
                       snprintf(dup->owning_series, 
                                sizeof(dup->owning_series), 
                                "%s",
                                rec->seriesinfo->seriesname);
                       hcon_insert(suinfoauth, key, &dup);
                    }
                 }
              }
              else
              {
                 if (!hcon_member(suinfoauth, key))
                 {
                    sunum[cnt].sunum = rec->sunum;
                    sunum[cnt].series = rec->seriesinfo->seriesname;
                    cnt++;

                    /* Re-purpose suinfoauth to track whether an SUNUM has been seen or not. */
                    hcon_insert(suinfoauth, key, &dummy);
                 }
              }
           }
        } /* iRec */
     } /* iSet */

     if (!bail && suinfoauth && hcon_size(suinfoauth) > 0)
     {
        if (sortalso)
        {
           if (cntreqinfo > 0)
           {
              /* Call SUM_InfoEx(). */
              infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * cntreqinfo);
              memset(infostructs, 0, sizeof(SUM_info_t *) * cntreqinfo);
              status = drms_getsuinfo(rs->records[0]->env, sunumreqinfo, cntreqinfo, infostructs);

              if (status != DRMS_SUCCESS)
              {
                 fprintf(stderr, "drms_stage_records_internal(): failure calling drms_getsuinfo(), error code %d.\n", status);
                 bail = 1;
              }
              else
              {
                 /* Populate suinfoauth with the results. suinfoauth will own all infostructs. */
                 for (iinfo = 0 ; iinfo < cntreqinfo; iinfo++)
                 {
                    snprintf(key, sizeof(key), "%llu", (unsigned long long)infostructs[iinfo]->sunum);

                    /* If SUM_infoEx() encounters an invalid SUNUM, or if it cannot retrieve an SU
                     * from tape because retrieve == 0, then the online_loc field of the returned 
                     * SUM_info_t will be the empty string. Substitute in the series name provided 
                     * by the record that was used to original obtain the SUNUM from. */
                    snprintf(infostructs[iinfo]->owning_series, 
                             sizeof(infostructs[iinfo]->owning_series), 
                             "%s",
                             seriesreqinfo[iinfo]);

                    /* There was a dummy inserted into suinfoauth earlier - remove that. */
                    hcon_remove(suinfoauth, key);
                    hcon_insert(suinfoauth, key, &(infostructs[iinfo]));
                 }
              }
           }

           /* Time to sort by tapeid/filenum. All sort criteria are in the 
            * SUM_info_ts in suinfoauth. */
           HIterator_t hit;
           hiter_new_sort(&hit, suinfoauth, SumsInfoSort);
           cnt = 0;
           sunum = malloc(hcon_size(suinfoauth) * sizeof(DRMS_SuAndSeries_t));
           XASSERT(sunum);

           if (env->verbose)
           {
              fprintf(stdout, "Sorted (by tapeid/filename) SUNUMs (sunum, tapeid, filenum):\n");
           }

           while ((ponesuinfo = hiter_getnext(&hit)) != NULL)
           {
              if (env->verbose)
              {
                 fprintf(stdout, 
                         "%llu\t%s\t%d\n", 
                         (unsigned long long)(*ponesuinfo)->sunum, 
                         (*ponesuinfo)->arch_tape, 
                         (*ponesuinfo)->arch_tape_fn);
              }

              sunum[cnt].sunum = (*ponesuinfo)->sunum;
              sunum[cnt].series = (*ponesuinfo)->owning_series;
              cnt++;
           }
        }
        
        /* Provide NULL as second argument. The SUNUMs in the list of SUs to 
         * fetch do not necessarily belong to the same series. By setting 
         * this argument to NULL, all series su caches are searched before 
         * SUM_get() is called. */
        if (!bail)
        {
           status = drms_getunits_ex(rec->env, cnt, sunum, retrieve, dontwait);
        }

        if (!status) 
        {
           if (!dontwait) 
           {
              // matching to each record is done by lookup the SU cache
              for (iRec = 0; iRec < nRecs; iRec++) 
              {
                 rec = rs->records[iRec];
                 rec->su = drms_su_lookup(rec->env, rec->seriesinfo->seriesname, rec->sunum, &scon);

                 /* ART (2/11/2010) - Previously, the refcount was not incremented (I think this was a bug) */
                 if (rec->su)
                 {
                    /* Invalid SUNUMs will result in a NULL su. These SUNUMs could have been deleted, 
                     * because the series was deleted, or because the SUs expired. */
                    rec->su->refcount++;
                 
                    /* rec->su could be NULL: if the original SUNUM was invalid (for example, the SU it
                     * refers to could have aged off, and archive could be zero), then drms_getunits() 
                     * will ensure that there is no SU in the SU cache for that SUNUM. */

                    /* Copy SUM_info_t to rec->suinfo, but only if we have a valid SUM_info_t, which
                     * is true only if rec->su != NULL (drms_getunits() will remove the SU from
                     * the SU cache if the SUNUM was invalid, causing DRMS_StorageUnit_t::sudir to
                     * be the empty string). */
                    if (sortalso && !rec->suinfo)
                    {
                       /* If the caller wants to sort by tapeid/filenum, then the SUM_info_t structs 
                        * obtained in the sorting process should be saved back into the 
                        * DRMS_Record_t::suinfo field. */
                       snprintf(key, sizeof(key), "%llu", (unsigned long long)rec->sunum);
                       if ((ponesuinfo = (SUM_info_t **)hcon_lookup(suinfoauth, key)) != NULL)
                       {
                          /* Multiple records may share the same SUNUM - each record gets a copy 
                           * of the SUM_info_t. When suinfo is destroyed, the source SUM_info_t 
                           * is deleted. */
                          rec->suinfo = (SUM_info_t *)malloc(sizeof(SUM_info_t));
                          *(rec->suinfo) = **ponesuinfo;
                       }
                       else
                       {
                          fprintf(stderr, "Missing DRMS storage unit during initialization.\n");
                          status = DRMS_ERROR_UNKNOWNSU;
                          break;
                       }
                    }
                 }
              }
           }
        }
     }

     if (infostructs)
     {
        free(infostructs);
        infostructs = NULL;
     }

     if (suinfoauth)
     {
        hcon_destroy(&suinfoauth);
     }
      
      if (seriesreqinfo)
      {
          free(seriesreqinfo);
          seriesreqinfo = NULL;
      }

     if (sunumreqinfo)
     {
        free(sunumreqinfo);
        sunumreqinfo = NULL;
     }

     if (sunum)
     {
        free(sunum);
        sunum = NULL;
     }
  }

  return status;
}

int drms_stage_records(DRMS_RecordSet_t *rs, int retrieve, int dontwait) 
{
   return drms_stage_records_internal(rs, retrieve, dontwait, NULL);
}

int drms_sortandstage_records(DRMS_RecordSet_t *rs, int retrieve, int dontwait, HContainer_t **suinfo) 
{
   /* Sort records by tapeid, filenumber first. */

   /* To sort records for staging, calls to SUM_InfoEx() must be made. The records in the 
    * record array in rs may already have SUM_info_t present (the module calling 
    * drms_sortandstage_records() could have explicitly called SUM_InfoEx() or
    * drms_sortandstage_records() could have been called previously). In addition,
    * the SUM_info_t structs could be available, but not yet copied into the 
    * suinfo field of the DRMS_Record_t structs. The suinfo-container parameter 
    * can be used to pass these SUM_info_t structs to this function. */
   
   /* If the SUM_info_t information is present in the records' suinfo fields, 
    * then use those structs for sorting the records. If the SUM_info_t information
    * is not present, or the records are not present (for example, if the record-set
    * was created by calling drms_open_recordset()), then use the information in the 
    * suinfo parameter. If there is no SUM_info_t struct for a record, then 
    * call SUM_InfoEx() to obtain that information. */

   return drms_stage_records_internal(rs, retrieve, dontwait, suinfo);   
}

/* ART - This should be modified to call drms_getsuinfo() only on records that do 
 * not already have a non-NULL suinfo field. */
int drms_record_getinfo(DRMS_RecordSet_t *rs)
{
   int status = DRMS_SUCCESS;

   DRMS_Record_t *rec = NULL;
   int nSets = rs->ss_n;
   int nRecs = 0;
   int iSet;
   int iRec;
   int bail = 0;
   long long *sunums = NULL;
   SUM_info_t **infostructs = NULL;

   if (rs->cursor)
   {
      /* This is a chunked recordset - defer the SUM_infoEx() call until the chunk is opened. */
      rs->cursor->infoneeded = 1;
      return(DRMS_SUCCESS);
   }

  if (rs->n >= 1)
  {
     for (iSet = 0; !bail && iSet < nSets; iSet++)
     {
        nRecs = drms_recordset_getssnrecs(rs, iSet, &status);

        if (status != DRMS_SUCCESS)
        {
           bail = 1;
           break;
        }

        infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nRecs);
        sunums = (long long *)malloc(sizeof(long long) * nRecs);

        for (iRec = 0; iRec < nRecs; iRec++)
        {
           rec = rs->records[(rs->ss_starts)[iSet] + iRec];
           sunums[iRec] = rec->sunum;
        } /* iRec */

        /* Insert results an array of structs - will be inserted back into
         * the record structs when all drms_getsuinfo() calls have completed. */
        status = drms_getsuinfo(rec->env, sunums, nRecs, infostructs);

        if (sunums)
        {
           free(sunums);
           sunums = NULL;
        }

        if (status != DRMS_SUCCESS)
        {
           fprintf(stderr, "drms_record_getinfo(): failure calling drms_getsuinfo(), error code %d.\n", status);
           bail = 1;
        }

        /* Place the returned SUM_info_t structs back into the record structs.
         * The allocated SUM_info_t must be freed in drms_free_records(). */
        if (!bail)
        {
           for (iRec = 0; iRec < nRecs; iRec++)
           {
              rec = rs->records[(rs->ss_starts)[iSet] + iRec];

              if (rec->suinfo)
              {
                 /* Must free existing SUM_info_t - drms_getsuinfo() was called more than once. */
                 free(rec->suinfo);
              }

              rec->suinfo = infostructs[iRec];
              rec->suinfo->next = NULL; /* just make sure */
           }
        }

        if (infostructs)
        {
           free(infostructs);
           infostructs = NULL;
        }

        if (sunums)
        {
           free(sunums);
           sunums = NULL;
        }
        
     } /* iSet */
  }

  return status;
}

/* Call drms_free_record for each record in a record set. */
void drms_free_records(DRMS_RecordSet_t *rs)
{
  int i;

  if (!rs)
    return;
  for (i=0; i<rs->n; i++)
    if (rs->records[i]) {
      drms_free_record(rs->records[i]);
    }
  if (rs->n>0 && rs->records)
    free(rs->records);
  rs->n = 0;

  /* Must free record-subset stuff too */
  if (rs->ss_queries)
  {
     int iSet;
     for (iSet = 0; iSet < rs->ss_n; iSet++)
     {
	if (rs->ss_queries[iSet])
	{
	   free(rs->ss_queries[iSet]);
	}
     }
     free(rs->ss_queries);
     rs->ss_queries = NULL;
  }

  if (rs->ss_types)
  {
     free(rs->ss_types);
     rs->ss_types = NULL;
  }
  if (rs->ss_starts)
  {
     free(rs->ss_starts);
     rs->ss_starts = NULL;
  }
  if (rs->ss_currentrecs)
  {
     free(rs->ss_currentrecs);
     rs->ss_currentrecs = NULL;
  }
  if (rs->cursor)
  {
     drms_free_cursor(&(rs->cursor));
  }
  /* Must NOT set ss_n to 0 before calling drms_free_cursor(). */
  rs->ss_n = 0;

  free(rs);
}

/*********************** Primary functions *********************/

/* Create a new record from the specified series, assign it a new unique 
   record number and a storage unit slot, possibly allocating a new storage 
   unit if no free slots exist. The keywors, links and data segments will
   be initialized with default values from the series template. */
DRMS_Record_t *drms_create_record(DRMS_Env_t *env, char *series, 
				  DRMS_RecLifetime_t lifetime, int *status)
{
  DRMS_RecordSet_t *rs;
  DRMS_Record_t *rec;
  
  rs = drms_create_records(env, 1, series, lifetime, status);
  if (rs && (rs->n==1))
  {
    rec = rs->records[0]; 
    free(rs->records);
    free(rs);
    
    /* rs->ss_currentrecs will have been alloc'd, but no longer needed. Free. */
    if (rs->ss_currentrecs)
    {
       free(rs->ss_currentrecs);
       rs->ss_currentrecs = NULL;
    }

    return rec;
  }
  else
    return NULL;
}

/*  Create a new record by cloning the record pointed to by "oldrec", 
    and assign it a new unique record number. The keywords, links will be 
    initialized with values from the oldrec. If mode=DRMS_SHARE_SEGMENTS 
    the record will inherit the storage unit slot of the old record "oldrec" 
    and consequently all files that might be in its storage unit directory. 
    If mode=DRMS_COPY_SEGMENTS a new storage unit slot is allocated for the 
    new record and all files in the storage unit od "oldrec" are copied
    to the new storage unit directory. */
DRMS_Record_t *drms_clone_record(DRMS_Record_t *oldrec, 
				 DRMS_RecLifetime_t lifetime, 
				 DRMS_CloneAction_t mode, int *status)
{
  DRMS_RecordSet_t rs_old,*rs;
  DRMS_Record_t *rec;

  rs_old.n = 1;
  rs_old.records = alloca(sizeof(DRMS_Record_t *));
  rs_old.records[0] = oldrec;
  rs_old.ss_n = 0;
  rs_old.ss_queries = NULL;
  rs_old.ss_types = NULL;
  rs_old.ss_starts = NULL;
  rs_old.ss_currentrecs = NULL;
  rs_old.cursor = NULL;

  rs = drms_clone_records(&rs_old, lifetime, mode, status);
  if (rs && (rs->n==1))
  {
    rec = rs->records[0]; 
    free(rs->records);
    free(rs);
    return rec;
  }
  else
    return NULL;
}



/* Close the record pointed to by "rec". 

   If action=DRMS_FREE_RECORD the storage unit slot reserved for the 
   record is freed and record meta-data is discarded. 

   If action=DRMS_INSERT_RECORD the records's  meta-data is inserted into 
   the database. The next time drms_commit is called (e.g. when the DRMS 
   session finishes successfully) the meta-data will be commited to the
   database and the data-segment files  in the record's storage unit will 
   be commited to SUMS for archiving (if applicable).

   In all cases the record data structure is freed from memory.
*/
int drms_close_record(DRMS_Record_t *rec, int action)
{
  DRMS_RecordSet_t *rs_old = malloc(sizeof(DRMS_RecordSet_t));

  rs_old->n = 1;
  rs_old->records = malloc(sizeof(DRMS_Record_t *));
  rs_old->records[0] = rec;
  rs_old->ss_n = 0;
  rs_old->ss_queries = NULL;
  rs_old->ss_types = NULL;
  rs_old->ss_starts = NULL;
  rs_old->ss_currentrecs = NULL;
  rs_old->cursor = NULL;

  return drms_close_records(rs_old, action);
}


/* Call drms_close_record for all records in the record cache. */
int drms_closeall_records(DRMS_Env_t *env, int action)
{
  HIterator_t hit;
  DRMS_Record_t *rec;
  int status;

  CHECKNULL(env);
  status = 0;
  hiter_new(&hit, &env->record_cache); 
  while( (rec = (DRMS_Record_t *)hiter_getnext(&hit)) )
  {   
    if (action == DRMS_INSERT_RECORD && !rec->readonly)
      status = drms_close_record(rec, action);
    else
      drms_free_record(rec);
     if (status)
      break;
  }
  return status;
}


/* Remove a record from the record cache and free any memory 
   allocated for it. */
void drms_free_record(DRMS_Record_t *rec)
{
   char hashkey[DRMS_MAXHASHKEYLEN];
   XASSERT(rec);
#ifdef DEBUG
   printf("freeing '%s':%lld\n", rec->seriesinfo->seriesname, rec->recnum);
#endif
   if (rec->seriesinfo) {
     drms_make_hashkey(hashkey, rec->seriesinfo->seriesname, rec->recnum);
     /* NOTICE: refcount on rec->su will be decremented when hcon_remove calls
	drms_free_record_struct via its deep_free callback. */

     /* Caller removed a reference to the record. Decrement reference counter. */
     --rec->refcount;

     if (rec->refcount == 0)
     {
        hcon_remove(&rec->env->record_cache, hashkey); 
     }
   }
}



/* Retrieve a data records with known series and unique record number. 
   If it is already in the dataset cache, simply return a pointer to its 
   data structure, otherwise retrieve it from the database. In the latter 
   case, add it to the record cache and hash tables for fast future
   retrieval. */
DRMS_Record_t *drms_retrieve_record(DRMS_Env_t *env, const char *seriesname, 
				    long long recnum, 
				    HContainer_t *goodsegcont,
				    int *status)
{
  int stat;
  DRMS_Record_t *rec;
  char hashkey[DRMS_MAXHASHKEYLEN];
  HIterator_t *hit = NULL;
  const char *hkey = NULL;

  CHECKNULL_STAT(env,status);
  
#ifdef DEBUG
  printf("[Trying to retrieve dataset (series=%s, recnum=%lld).\n",seriesname, recnum);
#endif
  drms_make_hashkey(hashkey, seriesname, recnum);

  if ( (rec = hcon_lookup(&env->record_cache, hashkey)) == NULL )
  {
    /* Set up data structure for dataset based on series template. */
    if ((rec = drms_alloc_record(env, seriesname, recnum, &stat)) == NULL)
    {
      if (status)
	*status = stat;
      return NULL;
    }

#ifdef DEBUG
    printf("Allocated the following template for dataset (%s,%lld):\n",
	   seriesname, recnum);
    drms_print_record(rec);
#endif

    /* Fill result into dataset structure. */
    if ((stat = drms_populate_record(env, rec, recnum)))
    {
      if (status)
	*status = stat;      
      goto bailout; /* Query result was inconsistent with series template. */
    }

    /* Remove unrequested segments */
    if (goodsegcont)
    {
       hit = hiter_create(&(rec->segments));
       if (hit)
       {
	  while (hiter_extgetnext(hit, &hkey) != NULL)
	  {
	     if (!hcon_lookup(goodsegcont, hkey))
	     {
		hcon_remove(&(rec->segments), hkey);
	     }
	  }
	   
	  hiter_destroy(&hit);
       }
    }

    /* Mark as read-only. */
    rec->readonly = 1;

    if (status)
      *status = DRMS_SUCCESS;
    return rec;
  }
  else
  {
#ifdef DEBUG
    printf("Found dataset in cache.\n");    
#endif

    /* The record was in the record cache already. Increment reference counter. */
    ++rec->refcount;

    if (status)
      *status = DRMS_SUCCESS;
    return rec;
  }

  bailout:
  drms_free_record(rec);
  return NULL;  
}




/* Retrieve a set of data records from a series satisfying the condition
   given in the string "where", which must be valid SQL WHERE clause wrt. 
   the main record table for the series.
  */
static DRMS_RecordSet_t *drms_retrieve_records_internal(DRMS_Env_t *env, 
                                                        const char *seriesname, 
                                                        char *where, int filter, int mixed,
                                                        HContainer_t *goodsegcont,
                                                        const char *qoverride,
                                                        int allvers, 
                                                        int nrecs, 
                                                        int *status)
{
  int i,throttled;
  int stat = 0;
  long long recnum;
  DRMS_RecordSet_t *rs;
  DB_Binary_Result_t *qres;
  DRMS_Record_t *template;
  char hashkey[DRMS_MAXHASHKEYLEN];
  char *series_lower;
  long long recsize, limit;
  HIterator_t *hit = NULL;
  const char *hkey = NULL;
  
  CHECKNULL_STAT(env,status);
  
  if ((template = drms_template_record(env,seriesname,status)) == NULL)
    return NULL;
  drms_link_getpidx(template); /* Make sure links have pidx's set. */
  recsize = drms_record_memsize(template);
  limit  = (long long)((0.4e6*env->query_mem)/recsize);
#ifdef DEBUG
  printf("limit  = (%f / %lld) = %lld\n",0.4e6*env->query_mem, recsize, limit);
#endif
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  char *query = qoverride ? 
    strdup(qoverride) : drms_query_string(env, 
                                          seriesname, 
                                          where, 
                                          filter, 
                                          mixed, 
                                          nrecs == 0 ? DRMS_QUERY_ALL : DRMS_QUERY_N, 
                                          &nrecs, 
                                          NULL, 
                                          allvers);
#ifdef DEBUG
  printf("ENTER drms_retrieve_records, env=%p, status=%p\n",env,status);
#endif

#ifdef DEBUG
  printf("query = '%s'\n",query);
  printf("\nMemory used = %Zu\n\n",xmem_recenthighwater());
#endif

  TIME(qres = drms_query_bin(env->session, query));
  if (qres == NULL)
  {
    stat = DRMS_ERROR_QUERYFAILED;
    fprintf(stderr, "Failed in drms_retrieve_records, query = '%s'\n",query);
    goto bailout1;
  }
#ifdef DEBUG
  db_print_binary_result(qres);
  printf("\nMemory used after query = %Zu\n\n",xmem_recenthighwater());
  printf("number of record returned = %d\n",qres->num_rows);
#endif
  throttled = (qres->num_rows == limit);

  /* Filter query result and initialize record data structures 
     from template. */  
  rs = malloc(sizeof(DRMS_RecordSet_t));
  XASSERT(rs);
  if (qres->num_rows<1)
  {
    rs->n = 0;
    rs->records = NULL;
  }
  else
  {
    /* Allocate data structures and copy default values from template. */
#ifdef DEBUG
    PushTimer();
#endif
    rs->n = qres->num_rows;
    rs->records=malloc(rs->n*sizeof(DRMS_Record_t*));
    XASSERT(rs->records);
    for (i=0; i<rs->n; i++)
    {
#ifdef DEBUG
      printf("Memory used = %Zu\n",xmem_recenthighwater());      
#endif
      recnum = db_binary_field_getlonglong(qres, i, 0);
      drms_make_hashkey(hashkey, seriesname, recnum);
      
      if ( (rs->records[i] = hcon_lookup(&env->record_cache, hashkey)) == NULL )
      {
	/* Allocate a slot in the hash indexed record cache. */
	rs->records[i] = hcon_allocslot(&env->record_cache, hashkey);

	/* Populate the slot with values from the template. */
	drms_copy_record_struct(rs->records[i], template);

       /* Set refcount to initial value of 1. */
        if (rs->records[i])
        {
           rs->records[i]->refcount = 1;
        }

	/* Set pidx in links */
	drms_link_getpidx(rs->records[i]);
	/* Set new unique record number. */
	rs->records[i]->recnum = recnum;

        /* The suinfo field is allocated on-demand, and must be set to NULL so that there is 
         * no attempt to free an unallocated suinfo pointer in drms_free_record(). */
        rs->records[i]->suinfo = NULL;
      } else {
	// the old record is going to be overridden

         /* The record was in the record cache already. Increment reference counter. */
         if (rs->records[i])
         {
            ++rs->records[i]->refcount;
         }

	free(rs->records[i]->sessionns);
      }
      rs->records[i]->readonly = 1;
    }


    /* Populate dataset structures with data from query result. */
#ifdef DEBUG
    printf("Time to allocate record structures = %f\n",PopTimer());
    printf("\nMemory used before populate= %Zu\n\n",xmem_recenthighwater());
#endif
    TIME(if ((stat = drms_populate_records(env, rs, qres)))
    {
      goto bailout; /* Query result was inconsistent with series template. */
    } );
#ifdef DEBUG
    printf("\nMemory used after populate= %Zu\n\n",xmem_recenthighwater());
#endif
  }

  if (goodsegcont && rs->n > 0)
  {
     const char **keynames = NULL;
     int nsegs = 0;
     int iseg;
   
     for (i=0; i < rs->n; i++)
     {
        /* Iterate through records, removing unrequested segments */ 
        nsegs = hcon_size(&(rs->records[i]->segments));

        if (nsegs > 0)
        {
           keynames = (const char **)malloc(sizeof(const char *) * nsegs);

           if (keynames)
           {
              hit = hiter_create(&(rs->records[i]->segments));
              if (hit)
              {
                 iseg = 0;
                 while (hiter_extgetnext(hit, &hkey) != NULL)
                 {
                    /* Put the names in a list - can't delete from a container while 
                     * iterating through it. */
                    if (!hcon_lookup(goodsegcont, hkey))
                    {
                       keynames[iseg] = hkey;
                       iseg++;
                    }
                 }

                 nsegs = iseg;
                 hiter_destroy(&hit);
              }
           }
        
           for (iseg = 0; iseg < nsegs; iseg++)
           {
              hkey = keynames[iseg];
              hcon_remove(&(rs->records[i]->segments), hkey);
           }

           if (keynames)
           {
              free(keynames);
              keynames = NULL;
           }
        }
     }
  }

  /* Initialize subset information */
  rs->ss_n = 0;
  rs->ss_queries = NULL;
  rs->ss_types = NULL;
  rs->ss_starts = NULL;
  rs->ss_currentrecs = NULL;
  rs->cursor = NULL;

  db_free_binary_result(qres);   
  free(query);
  free(series_lower);
  if (status)
  {
    if (!throttled)
      *status = DRMS_SUCCESS;
    else {
      fprintf(stderr, "Query truncated\n");
      *status = DRMS_QUERY_TRUNCATED;
    }
  }
  return rs;
 bailout:
  db_free_binary_result(qres);   
  for (i=0;i<rs->n;i++)
    drms_free_records(rs);
  free(rs);
 bailout1:
  free(series_lower);
  free(query);
  if (status)
    *status = stat;
  return NULL;  
}

DRMS_RecordSet_t *drms_retrieve_records(DRMS_Env_t *env, 
                                        const char *seriesname, 
                                        char *where, int filter, int mixed,
                                        HContainer_t *goodsegcont,
                                        int allvers, 
                                        int nrecs, 
                                        int *status)
{
   return drms_retrieve_records_internal(env, 
                                         seriesname, 
                                         where, 
                                         filter, 
                                         mixed, 
                                         goodsegcont, 
                                         NULL, 
                                         allvers, 
                                         nrecs, 
                                         status);
}

char *drms_query_string(DRMS_Env_t *env, 
			const char *seriesname,
			char *where, int filter, int mixed,
			DRMS_QueryType_t qtype, 
                        void *data, /* specific to qtype */
                        const char *fl,
                        int allvers) {
  DRMS_Record_t *template;
  char *field_list, *query=0;
  char *series_lower;
  long long recsize, limit;
  char pidx_names[1024]; // comma separated pidx keyword names
  char pidx_names_desc[1200]; // comma separated pidx keyword names with DESC
  char pidx_names_n[1024]; // comma separated pidx keyword names in 'limited' table
  char pidx_names_bare[1024]; // comma separated pidx keyword names in 'limited' table
  char pidx_names_desc_bare[1024]; // comma separated pidx keyword names in 'limited' table
  char limitedtable[1024]; // for DRMS_QUERY_N, innermost table that has 4 * n rows
  char *p;
  int nrecs = 0;
  int unique = 0;

  int status = 0;

  CHECKNULL_STAT(env,&status);

  if ((template = drms_template_record(env,seriesname,&status)) == NULL)
    return NULL;
  drms_link_getpidx(template); /* Make sure links have pidx's set. */

  switch (qtype) {
  case DRMS_QUERY_COUNT:
    field_list = strdup("count(recnum)");
    break;
  case DRMS_QUERY_FL:
    field_list = strdup(fl);
    recsize = drms_keylist_memsize(template, field_list);
    if (!recsize) {
      goto bailout;
    }
    unique = *(int *)(data);
    break;
  case DRMS_QUERY_ALL:
    field_list = drms_field_list(template, NULL);
    recsize = drms_record_memsize(template);
    break;
  case DRMS_QUERY_N:
    field_list = drms_field_list(template, NULL);
    recsize = drms_record_memsize(template);
    nrecs = *(int *)(data);
    break;
  default:
    printf("Unknown query type: %d\n", (int)qtype);
    return NULL;
  }

  limit  = (long long)((0.4e6*env->query_mem)/recsize);
#ifdef DEBUG
  printf("limit  = (%f / %lld) = %lld\n",0.4e6*env->query_mem, recsize, limit);
#endif
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  if (template->seriesinfo->pidx_num>0) {
    char *pdesc = pidx_names_desc;
    char *p_n = pidx_names_n;
    char *p_bare = pidx_names_bare;
    char *pdesc_bare = pidx_names_desc_bare;

    p = pidx_names;

    p += sprintf(p, "%s.%s", series_lower, template->seriesinfo->pidx_keywords[0]->info->name);

    pdesc += sprintf(pdesc, "%s.%s DESC", series_lower, template->seriesinfo->pidx_keywords[0]->info->name);

    /* limited case */
    p_n += sprintf(p_n, 
                   "limited.%s", 
                   template->seriesinfo->pidx_keywords[0]->info->name);
    
    p_bare += sprintf(p_bare, 
                      "%s", 
                      template->seriesinfo->pidx_keywords[0]->info->name);

    pdesc_bare += sprintf(pdesc_bare, 
                          "%s DESC", 
                          template->seriesinfo->pidx_keywords[0]->info->name);
    
    
    for (int i = 1; i < template->seriesinfo->pidx_num; i++) 
    {
       p += sprintf(p, ", %s.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name);

      pdesc += sprintf(pdesc, ", %s.%s DESC", series_lower, template->seriesinfo->pidx_keywords[i]->info->name);

       p_n += sprintf(p_n, 
                      ", limited.%s", 
                      template->seriesinfo->pidx_keywords[i]->info->name);

       p_bare += sprintf(p_bare, 
                         ", %s", 
                         template->seriesinfo->pidx_keywords[i]->info->name);

       pdesc_bare += sprintf(pdesc_bare, 
                            ", %s DESC", 
                            template->seriesinfo->pidx_keywords[i]->info->name);
    }
  }


  if (qtype == DRMS_QUERY_N)
  {
     char *plimtab = limitedtable;
     int fudge = kQUERYNFUDGE;

     plimtab += snprintf(limitedtable, 
                         sizeof(limitedtable),
                         "select recnum,%s from %s where 1=1",
                         pidx_names_bare,
                         series_lower);
     if (where && *where)
     {
        plimtab += sprintf(plimtab, " and %s", where);

     }

     plimtab += sprintf(plimtab, 
                        " order by %s limit %d",
                        nrecs > 0 ? pidx_names_bare : pidx_names_desc_bare,
                        abs(nrecs) * fudge);
  }

  /* Do query to retrieve record meta-data. */
  query = malloc(strlen(field_list)+DRMS_MAXQUERYLEN);
  XASSERT(query);
  p = query;


  /* If this is a [! ... !] query, then we want to get rid of the 'group by' clause and replace
   * the max(recnum) in the subquery with simply recnum. We can do that by just forcing the
   * last query - the onen that just selects on the series with the where clause and no
   * group by clause
   */
  if (filter && template->seriesinfo->pidx_num>0 && !allvers) { // query on the lastest version
    if (mixed) {
      // query on both prime and non-prime keys
       /* break into DRMS_QUERY_N vs. other types of queries - too hard to combine all into one
        * sprintf() */
       if (qtype != DRMS_QUERY_N)
       {
          p += sprintf(p, "select %s from %s, (select q2.max1 as max from  (select max(recnum) as max1, min(q1.max) as max2 from %s, (select %s, max(recnum) from %s where %s group by %s) as q1 where %s.%s = q1.%s", field_list, series_lower, series_lower, pidx_names, series_lower, where, pidx_names, series_lower, template->seriesinfo->pidx_keywords[0]->info->name, template->seriesinfo->pidx_keywords[0]->info->name);
          for (int i = 1; i < template->seriesinfo->pidx_num; i++) {
             p += sprintf(p, " and %s.%s = q1.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name, template->seriesinfo->pidx_keywords[i]->info->name);
          }
          p += sprintf(p, " group by %s) as q2 where max1 = max2) as q3 where %s.recnum = q3.max", pidx_names, series_lower);
       }
       else
       {
          /* DRMS_QUERY_N */
          p += sprintf(p, 
                       "select %s from %s, (select q2.max1 as max from  (select max(recnum) as max1, min(q1.max) as max2 from %s, (select %s, max(recnum) from (%s) as limited group by %s) as q1 where %s.%s = q1.%s",
                       field_list, 
                       series_lower, 
                       series_lower, 
                       pidx_names_n, 
                       limitedtable, 
                       pidx_names_n, 
                       series_lower, 
                       template->seriesinfo->pidx_keywords[0]->info->name, 
                       template->seriesinfo->pidx_keywords[0]->info->name);

          for (int i = 1; i < template->seriesinfo->pidx_num; i++) 
          {
             p += sprintf(p, 
                          " and %s.%s = q1.%s", 
                          series_lower, 
                          template->seriesinfo->pidx_keywords[i]->info->name, 
                          template->seriesinfo->pidx_keywords[i]->info->name);
          }

          p += sprintf(p, 
                       " group by %s) as q2 where max1 = max2) as q3 where %s.recnum = q3.max", 
                       pidx_names, 
                       series_lower);
       }
    } 
    else 
    {
      // query only on prime keys
       /* break into DRMS_QUERY_N vs. other types of queries - too hard to combine all into one
        * sprintf() */
       if (qtype != DRMS_QUERY_N)
       {
          p += sprintf(p, "select %s from %s where recnum in (select max(recnum) from %s where 1=1 ", field_list, series_lower, series_lower);
          if (where && *where) 
          {
             p += sprintf(p, " and %s", where);
          }

          p += sprintf(p, " group by %s )", pidx_names);
       }
       else
       {
          /* DRMS_QUERY_N */
          p += sprintf(p, 
                       "select %s from %s where recnum in (select max(recnum) from (%s) as limited group by %s)", 
                       field_list, 
                       series_lower, 
                       limitedtable, 
                       pidx_names_n);

       }
    }
  } else { // query on all records including all versions
     /* same for  DRMS_QUERY_N and other types of queries */
     p += sprintf(p, "select %s from %s where 1 = 1", field_list, series_lower);
     if (where && *where) {
        p += sprintf(p, " and %s", where);
     }    
  }
  if (qtype != DRMS_QUERY_COUNT) 
  {
     if (qtype == DRMS_QUERY_N)
     {
        if (template->seriesinfo->pidx_num > 0) 
        {
           p += sprintf(p, " order by %s", nrecs > 0 ? pidx_names : pidx_names_desc);
        }

        if (abs(nrecs) < limit)
        {
           limit = abs(nrecs);
        }

        p += sprintf(p, " limit %lld", limit);
     }
     else
     {
        if (template->seriesinfo->pidx_num > 0) {
           p += sprintf(p, " order by %s", pidx_names);
        }
        p += sprintf(p, " limit %lld", limit);
     }
  }

  if (qtype == DRMS_QUERY_FL && unique)
  {
     int qsize = strlen(query) + DRMS_MAXQUERYLEN;
     char *modquery = NULL;
     modquery = malloc(qsize);
     XASSERT(modquery);
     snprintf(modquery, 
              qsize, 
              "select %s from (%s) as subfoo group by %s order by %s", 
              field_list, 
              query, 
              field_list,
              field_list);
     if (query)
     {
        free(query);
     }

     query = modquery;
  }

  free(series_lower);
 bailout:
  free(field_list);

  if (env->verbose)
  {
     printf("query (the big enchilada): %s\n", query);
  }

  return query;
}




/* Allocate a new record data structure and initialize it with
   meta-data from the given series template. "recnum" must contain
   a unique record number to be assigned to the new record. 
*/
DRMS_Record_t *drms_alloc_record2(DRMS_Record_t *template,
				  long long recnum, int *status)
{
  DRMS_Record_t *rec;
  char hashkey[DRMS_MAXHASHKEYLEN];
  char *series;
  DRMS_Env_t *env;

  CHECKNULL_STAT(template,status);
  env = template->env;
  series = template->seriesinfo->seriesname;

  /* Build hashkey <seriesname>_<recnum> */
  drms_make_hashkey(hashkey, series, recnum);

  /* Allocate a slot in the hash indexed record cache. */
  rec = (DRMS_Record_t *)hcon_allocslot(&env->record_cache, hashkey);

  rec->su = NULL;
  /* Populate the slot with values from the template. */
  drms_copy_record_struct(rec, template);

  /* Set refcount to initial value of 1. */
  if (rec)
  {
     rec->refcount = 1;
  }
  
  /* Set pidx in links */
  drms_link_getpidx(rec);
  
  /* Set new unique record number. */
  rec->recnum = recnum;
  if (status)
    *status = DRMS_SUCCESS;

  return rec;
}


/* Just a wrapper for drms_alloc_record2 that looks up the 
   template from the series name. */
DRMS_Record_t *drms_alloc_record(DRMS_Env_t *env, const char *series, 
				 long long recnum, int *status)
{
  DRMS_Record_t *template;

  CHECKNULL_STAT(env,status);
  /* Get template record for the series. */
  if ((template = drms_template_record(env, series,status)) == NULL)
    return NULL;
  return drms_alloc_record2(template, recnum, status);
}



/* 
   Return a template record for the given series. If one already exist
   in the series_cache return that one. Otherwise, build a template record 
   the given series from scratch, i.e. directly from the series meta-data 
   contained in the global database tables DRMS_MASTER_SERIES_TABLE, 
   DRMS_MASTER_KEYWORD_TABLE, DRMS_MASTER_LINK_TABLE, and 
   DRMS_MASTER_SEGMENT_TABLE (see drms_env.h for macro definition). 
   The data structure is allocated and populated with default values 
   from the database.
*/
static DRMS_Record_t *drms_template_record_int(DRMS_Env_t *env, 
                                               const char *seriesname, 
                                               int jsd,
                                               int *status)
{
  int stat;
  DB_Binary_Result_t *qres = NULL;
  DRMS_Record_t *template;
  char *p, *q, query[DRMS_MAXQUERYLEN], buf[DRMS_MAXPRIMIDX*DRMS_MAXKEYNAMELEN];
  DRMS_Keyword_t *kw;
  int dsdsing = 0;
  char *colnames = NULL;

  XASSERT(env);
  XASSERT(seriesname);

  char *lcseries = strdup(seriesname);

  if (!lcseries)
  {
     stat = DRMS_ERROR_OUTOFMEMORY;
     goto bailout;
  }

  strtolower(lcseries);

 /* This function has parts that are conditional on the series version being 
  * greater than or equal to version 2.0. */
  DRMS_SeriesVersion_t vers2_0 = {"2.0", ""};
  DRMS_SeriesVersion_t vers2_1 = {"2.1", ""};

#ifdef DEBUG
    printf("Getting template for series '%s'\n",seriesname);
#endif

  /* Presumably, there won't be many calls to obtain the jsd template during 
   * modules execution (should be just one).  So, no need to cache the 
   * template. */
  const char *dsdsNsPrefix = DSDS_GetNsPrefix();

  if (strstr(seriesname, dsdsNsPrefix) == seriesname)
  {
     dsdsing = 1;
  }
  /* Gotta special-case data ingested from DSDS - you can't populate an 
   * empty record template since the record data are all in memory and
   * don't reside in pqsl. THIS ASSUMES THAT THERE ARE NO PER-SEGMENT 
   * KEYWORDS IN INGESTED DSDS DATA (THERE SHOULD NOT BE) */

  if (!jsd || dsdsing)
  {
     /* We no longer cache all series in series_cache at module startup. So, we MAY need to query 
      * the dbase here if the series isn't in the cache. */
     if ( (template = hcon_lookup_lower(&env->series_cache, seriesname)) == NULL )
     {
        /* check series directly */
        char qry[DRMS_MAXQUERYLEN];
        char *nspace = ns(lcseries);
        int serr;
        DB_Text_Result_t *tqres = NULL;

        serr = 0;

        if (nspace)
        {
           /* First check for a valid namespace. If you just do the query with an invalid namespace, 
            * the current transaction will get aborted. */
           snprintf(qry, sizeof(qry), "select name from admin.ns where name = '%s'", nspace); /* name is lc */
           tqres = drms_query_txt(env->session, qry);
           if (tqres == NULL)
           {
              serr = 1;
           }
           else if (tqres->num_rows != 1)
           {
              serr = 1;
              db_free_text_result(tqres);
              tqres = NULL;
           }
           else
           {
              db_free_text_result(tqres);
              tqres = NULL;

              /* There is no template->seriesinfo->seriesname at this point - must use lc of seriesname 
               * passed into this function. */


              snprintf(qry, sizeof(qry), "select seriesname from %s.drms_series where lower(seriesname) = '%s'", nspace, lcseries);
              free(nspace);

              if ((tqres = drms_query_txt(env->session, qry)) != NULL && tqres->num_rows == 1)
              {
                 template = 
                   (DRMS_Record_t *)hcon_allocslot_lower(&env->series_cache, tqres->field[0][0]);
                 memset(template,0,sizeof(DRMS_Record_t));
                 template->init = 0;
              }
              else
              {
                 serr = 1;
              }

              db_free_text_result(tqres);
              tqres = NULL;
           }
        }

        if (serr)
        {
           if (status) 
           {
              *status = DRMS_ERROR_UNKNOWNSERIES;
           }

           free(lcseries);
           return NULL;
        }
     }
  }
  else
  {
     template = malloc(sizeof(DRMS_Record_t));
     memset(template, 0, sizeof(DRMS_Record_t));
  }

  if (template->init == 0 || (jsd && !dsdsing))
  {
#ifdef DEBUG
    printf("Building new template for series '%s'\n",seriesname);
#endif
    /* The series template for this series was not initialized yet.
       Retrieve its info and insert it in the the hash table
       for future reference. */

    /* Set up top level fields. */
    template->env = env;
    template->init = 1;
    template->recnum = 0;
    template->sunum = -1LL;
    template->sessionid = 0;
    template->sessionns = NULL;
    template->su = NULL;
    template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t));
    XASSERT(template->seriesinfo);
    /* Populate series info part */
    char *namespace = ns(seriesname);

    /* There is no template->seriesinfo->seriesname at this point - must use lc of seriesname 
     * passed into this function. */


    sprintf(query, "select seriesname, description, author, owner, "
	    "unitsize, archive, retention, tapegroup, primary_idx, dbidx, version "
	    "from %s.%s where lower(seriesname) = '%s'", 
	    namespace, DRMS_MASTER_SERIES_TABLE, lcseries);
    free(namespace);
#ifdef DEBUG
    printf("query string '%s'\n",query);
#endif
    
    if ((qres = drms_query_bin(env->session, query)) == NULL)
    {
      printf("Failed to retrieve series information for series %s.\n",
	     seriesname);
      stat = DRMS_ERROR_QUERYFAILED;
      goto bailout;
    }
    if (qres->num_cols != 11 || qres->num_rows != 1)
    {
      printf("Invalid sized query result for global series information for"
	     " series %s.\n", seriesname);      
      db_free_binary_result(qres);
      stat = DRMS_ERROR_BADQUERYRESULT;
      goto bailout;
    }
    db_binary_field_getstr(qres, 0, 0, DRMS_MAXSERIESNAMELEN, 
			   template->seriesinfo->seriesname);
    db_binary_field_getstr(qres, 0, 1, DRMS_MAXCOMMENTLEN, 
			   template->seriesinfo->description);
    db_binary_field_getstr(qres, 0, 2, DRMS_MAXCOMMENTLEN, 
			   template->seriesinfo->author);
    db_binary_field_getstr(qres, 0, 3, DRMS_MAXOWNERLEN, 
			   template->seriesinfo->owner);
    template->seriesinfo->unitsize = db_binary_field_getint(qres, 0, 4);
    template->seriesinfo->archive = db_binary_field_getint(qres, 0, 5);
    template->seriesinfo->retention = db_binary_field_getint(qres, 0, 6);
    template->seriesinfo->retention_perm = 0; // default
    template->seriesinfo->tapegroup = db_binary_field_getint(qres, 0, 7);

    /* Need the version early on, so go out of order. */
    if ( !db_binary_field_is_null(qres, 0, 10) ) {
       db_binary_field_getstr(qres, 0, 10, DRMS_MAXSERIESVERSION, 
			      template->seriesinfo->version);
    }

    char *ns = NULL;
    char *table = NULL;
    char *oid = NULL;
    int err = 0;
    char *serieslwr = strdup(template->seriesinfo->seriesname);

    strtolower(serieslwr);
    get_namespace(serieslwr, &ns, &table);

    if (serieslwr)
    {
       free(serieslwr);
    }

    err = GetTableOID(env, ns, table, &oid);

    if (ns)
    {
       free(ns);
    }

    if (table)
    {
       free(table);
    }

    if (!err)
    {
       err = GetColumnNames(env, oid, &colnames);
    }

    if (oid)
    {
       free(oid);
    }

    /* Populate series info segments, keywords, and links part */
    if ((stat=drms_template_segments(template)))
      goto bailout;
    if ((stat=drms_template_links(template)))
      goto bailout;
    if ((stat=drms_template_keywords_int(template, !jsd, colnames)))
      goto bailout;

    /* If any segments present, lookup pemission to set retention */
    if (template->segments.num_total > 0)
    {
        template->seriesinfo->retention_perm = drms_series_isdbowner(env, template->seriesinfo->seriesname, &stat);
        
        if (stat)
        {
            goto bailout;
        }
        
        template->seriesinfo->retention_perm = (template->seriesinfo->retention_perm || drms_client_isproduser(env, &stat));

        if (stat)
        {
            goto bailout;
        }
      
#ifdef DEBUG
      printf("retention_perm=%d\n",template->seriesinfo->retention_perm);
#endif
    }

    /* Set up primary index list. */
    if ( !db_binary_field_is_null(qres, 0, 8) )
    {
      db_binary_field_getstr(qres, 0, 8, DRMS_MAXPRIMIDX*DRMS_MAXKEYNAMELEN, buf);
      p = buf;
#ifdef DEBUG
      printf("Primary index string = '%s'\n",p);
#endif
      template->seriesinfo->pidx_num = 0;
      while(*p)
      {
	XASSERT(template->seriesinfo->pidx_num < DRMS_MAXPRIMIDX);
	while(*p && isspace(*p))
	  ++p;
	q = p;
	while(*p && !isspace(*p) && *p!=',')
	  ++p;	       
	*p++ = 0;
	
#ifdef DEBUG
	printf("adding primary key '%s'\n",q);
#endif
	kw = hcon_lookup_lower(&template->keywords,q);
	XASSERT(kw);

	template->seriesinfo->pidx_keywords[(template->seriesinfo->pidx_num)++] =
	kw; 

        /* For vers 2.1 and greater, slotted key intprime and extprime are stored in 
         * the persegment field of the drms_keyword table. For earlier versions, 
         * all series' primary index keywords are intprime, and index keywords 
         * (for time slotting) are all extprime. And anything that is not an
         * index keyword is extprime.
         */
        if (!drms_series_isvers(template->seriesinfo, &vers2_1))
        {
           drms_keyword_setintprime(kw);
           if (drms_keyword_isindex(kw))
           {
              DRMS_Keyword_t *slotkey = drms_keyword_slotfromindex(kw);
              drms_keyword_setextprime(slotkey);
           }
           else
           {
              drms_keyword_setextprime(kw);
           }
        }
      }
#ifdef DEBUG
      { int i;
	printf("Primary indices: ");
	for (i=0; i<template->seriesinfo->pidx_num; i++)
	  printf("'%s' ",(template->seriesinfo->pidx_keywords[i])->info->name); 
      }
      printf("\n");    
#endif
    }
    else
      template->seriesinfo->pidx_num = 0;

    /* Set up db index list. */
    if ( !db_binary_field_is_null(qres, 0, 9) ) {
      db_binary_field_getstr(qres, 0, 9, DRMS_MAXDBIDX*DRMS_MAXKEYNAMELEN, buf);
      p = buf;
#ifdef DEBUG
      printf("DB index string = '%s'\n",p);
#endif
      template->seriesinfo->dbidx_num = 0;
      while(*p) {
	XASSERT(template->seriesinfo->dbidx_num < DRMS_MAXDBIDX);
	while(*p && isspace(*p))
	  ++p;
	q = p;
	while(*p && !isspace(*p) && *p!=',')
	  ++p;	       
	*p++ = 0;
	
#ifdef DEBUG
	printf("adding db index '%s'\n",q);
#endif
	kw = hcon_lookup_lower(&template->keywords,q);
	XASSERT(kw);
	template->seriesinfo->dbidx_keywords[(template->seriesinfo->dbidx_num)++] = kw; 
      }
#ifdef DEBUG
      { int i;
	printf("DB indices: ");
	for (i=0; i<template->seriesinfo->dbidx_num; i++)
	  printf("'%s' ",(template->seriesinfo->dbidx_keywords[i])->info->name); 
      }
      printf("\n");    
#endif
    } else {
      template->seriesinfo->dbidx_num = 0;
    }

    /* Use the implicit, per-segment, keyword variables, cparms_sgXXX to populate the segment 
     * part of the template. */
    if (drms_series_isvers(template->seriesinfo, &vers2_0))
    {
       HIterator_t *hit = hiter_create(&(template->segments));
       if (hit)
       {
	  DRMS_Segment_t *seg = NULL;
	  while ((seg = hiter_getnext(hit)) != NULL)
	  {
	     /* compression parameters are stored as keywords cparms_sgXXX - these
	      * keywords should have been populated in the keywords section just above. */
	     char kbuf[DRMS_MAXKEYNAMELEN];
	     snprintf(kbuf, sizeof(kbuf), "cparms_sg%03d", seg->info->segnum);

	     DRMS_Keyword_t *segkey = hcon_lookup_lower(&(template->keywords), kbuf);
	     if (segkey)
	     {
		snprintf(seg->cparms, DRMS_MAXCPARMS, "%s", segkey->value.string_val);
	     }

             if (drms_series_isvers(template->seriesinfo, &vers2_1))
             {
                /* segment-specific bzero and bscale keywords are used to populate 
                 * the segment structure */
                snprintf(kbuf, sizeof(kbuf), "%s_bzero", seg->info->name);

                segkey = hcon_lookup_lower(&(template->keywords), kbuf);
                if (segkey)
                {
                   seg->bzero = segkey->value.double_val;
                }

                snprintf(kbuf, sizeof(kbuf), "%s_bscale", seg->info->name);

                segkey = hcon_lookup_lower(&(template->keywords), kbuf);
                if (segkey)
                {
                   seg->bscale = segkey->value.double_val;
                }
             }
	  }

          hiter_destroy(&hit);
       }
    }

    db_free_binary_result(qres);   
  }

  if (colnames)
  {
     free(colnames);
  }

  if (status)
    *status = DRMS_SUCCESS;
  free(lcseries);
  return template;

 bailout:
  if (colnames)
  {
     free(colnames);
  }

  if (lcseries)
  {
     free(lcseries);
  }

  if (status)
    *status = stat;
  return NULL;
}

DRMS_Record_t *drms_template_record(DRMS_Env_t *env, const char *seriesname, 
                                    int *status)
{
   return drms_template_record_int(env, seriesname, 0, status);
}

/* Caller must free record returned. */
DRMS_Record_t *drms_create_jsdtemplate_record(DRMS_Env_t *env, 
                                              const char *seriesname, 
                                              int *status)
{
   return drms_template_record_int(env, seriesname, 1, status);  
}

void drms_destroy_jsdtemplate_record(DRMS_Record_t **rec)
{
   /* Don't free the record structure IF this record is from a dsds-ingested series. In that
    * case, the record IS the template structure cached into the series cache, and 
    * will be freed upon session close. */
   if (rec)
   {
      const char *dsdsNsPrefix = DSDS_GetNsPrefix();
      const char *seriesname = (*rec)->seriesinfo->seriesname;

      if (strstr(seriesname, dsdsNsPrefix) != seriesname)
      {
         drms_free_template_record_struct(*rec);
      }

      *rec = NULL;
   }
}

/* Free the body of a template record data structure. */
void drms_free_template_record_struct(DRMS_Record_t *rec)
{
  /* Free malloc'ed data segments, links and keywords. */
  XASSERT(rec);
  if ( rec->init == 1 ) /* Don't try to free uninitialized templates. */
  {
    (rec->links).deep_free = (void (*)(const void *)) drms_free_template_link_struct;
    hcon_free(&rec->links);

    (rec->segments).deep_free = (void (*)(const void *)) drms_free_template_segment_struct;
    hcon_free(&rec->segments);

    (rec->keywords).deep_free = (void (*)(const void *)) drms_free_template_keyword_struct;
    hcon_free(&rec->keywords);

    free(rec->seriesinfo);
  }
}

/* Free the body of a record data structure. */
void drms_free_record_struct(DRMS_Record_t *rec)
{
  /* Free malloc'ed data segments, links and keywords. */
  XASSERT(rec);
  if ( rec->init == 1 ) /* Don't try to free uninitialized templates. */
  {
    hcon_free(&rec->links);
    hcon_free(&rec->segments);
    hcon_free(&rec->keywords);
    free(rec->sessionns);

    if (rec->suinfo)
    {
       free(rec->suinfo);
       rec->suinfo = NULL;
    }

    if (rec->env && !rec->env->session->db_direct && rec->su)
    {
      --rec->su->refcount;
      if (rec->su->refcount == 0)
      {
#ifdef DEBUG
	printf("drms_free_record_struct: freeing unit sunum=%lld, sudir='%s'\n",
	       rec->su->sunum,rec->su->sudir);
#endif
	drms_freeunit(rec->env, rec->su);
	rec->su = NULL;
      }
    }
  }
}

/* Copy the body of a record structure. */
void drms_copy_record_struct(DRMS_Record_t *dst, DRMS_Record_t *src)
{
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Segment_t *seg;

  XASSERT(dst && src);
  /* Copy fields in the main structure and 
     series info. */
  *dst = *src;
  /* Copy fields in segments, links and keywords. */
  hcon_copy(&dst->keywords, &src->keywords);
  hcon_copy(&dst->links, &src->links);
  hcon_copy(&dst->segments, &src->segments);

  /* Set parent pointers. */
  hiter_new(&hit, &dst->links);
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
    link->record = dst;
  hiter_free(&hit);
  hiter_new(&hit, &dst->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
    key->record = dst;
  hiter_free(&hit);
  hiter_new(&hit, &dst->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
    seg->record = dst;
  hiter_free(&hit);

  /* FIXME: Should we copy su struct here? */
}


/* Populate the keyword, link, and data descriptors in the
   record structure with the result from a database query. */ 
int drms_populate_record(DRMS_Env_t *env, DRMS_Record_t *rec, long long recnum)
{
  int stat;
  char *query, *field_list, *seriesname;
  DB_Binary_Result_t *qres;
  DRMS_RecordSet_t rs;
  char *series_lower;

  CHECKNULL(rec);
  seriesname = rec->seriesinfo->seriesname;
  field_list = drms_field_list(rec, NULL);
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  /* Do query. */
  query = malloc(strlen(field_list)+10*DRMS_MAXSERIESNAMELEN);
  XASSERT(query);
  sprintf(query, "select %s from %s where recnum=%lld", 
	  field_list, series_lower, recnum);

  qres = drms_query_bin(env->session, query);
  if (qres == NULL)
  {
    stat = DRMS_ERROR_QUERYFAILED;
    goto bailout1;
  }
  else if (qres->num_rows != 1)
  {
    stat = DRMS_ERROR_BADQUERYRESULT;
    goto bailout2;
  }
  rs.records=malloc(sizeof(DRMS_Record_t*));
  XASSERT(rs.records);
  rs.n = 1;
  rs.records[0] = rec;
  rs.ss_n = 0;
  rs.ss_queries = NULL;
  rs.ss_types = NULL;
  rs.ss_starts = NULL;
  rs.ss_currentrecs = NULL;
  rs.cursor = NULL;

  stat = drms_populate_records(env, &rs,qres);
  db_free_binary_result(qres);      
  free(query);
  free(field_list);
  free(rs.records);
  free(series_lower);
  return stat;
  
 bailout2:
  db_free_binary_result(qres);      
 bailout1:
  free(series_lower);
  free(query);
  free(field_list);
  return 1;
}
/*
 *  Input: Recordset with initialized record templates, query result.
 *  Populate the keyword, link, and data descriptors in the
 *  record structure with the result from a database query
 */ 
int drms_populate_records (DRMS_Env_t *env, DRMS_RecordSet_t *rs, DB_Binary_Result_t *qres) {
  int row, col, i;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Segment_t *seg;
  DRMS_Record_t *rec;
  DB_Type_t column_type;
  int segnum;
  char *record_value;
  HIterator_t *last = NULL;

  CHECKNULL(rs);
  CHECKNULL(qres);

  if (rs->n != qres->num_rows)
    return DRMS_ERROR_BADQUERYRESULT;

  /* Believe it or not, every use of a record SHOULD occur only after obtaining 
   * the server lock. This is because records are cached in the record cache, which 
   * is a structure that lives in the DRMS_Env_t struct. */
#ifndef DRMS_CLIENT
  drms_lock_server(env);
#endif
  for (row=0; row<qres->num_rows; row++) {
    rec = rs->records[row];
    col = 0;
    /* Absolute record number. */
    rec->recnum = db_binary_field_getlonglong(qres, row, col++);
    /* Set up storageunit info. */
    rec->sunum = db_binary_field_getlonglong(qres, row, col++);
    /* Storage unit slot number */
    rec->slotnum = db_binary_field_getint(qres, row, col++);

    /* Session ID of creating session. */
    rec->sessionid = db_binary_field_getint(qres, row, col++);

    /* Session namespace of creating session.*/
    rec->sessionns = strdup(db_binary_field_get(qres, row, col++));

    /* Populate Links. */
    if (hcon_size(&rec->links) > 0) {
       while ((link = drms_record_nextlink(rec, &last))) {
	link->record = rec; /* parent link. */
	if (link->info->type == STATIC_LINK)
	  link->recnum = db_binary_field_getlonglong(qres, row, col++);
	else {
					      /*  Oh crap! A dynamic link... */
	  link->isset = db_binary_field_getint(qres, row, col);
	  col++;
		/*  There is a field for each keyword in the primary index
				   of the target series...walk through them  */
	  for (i = 0; i < link->info->pidx_num; i++, col++) {
	    column_type = db_binary_column_type (qres, col);
	    record_value = db_binary_field_get (qres, row, col);
	    drms_copy_db2drms (link->info->pidx_type[i], &link->pidx_value[i],
		column_type, record_value);
	  }
	}
      }

       if (last)
       {
          hiter_destroy(&last);
       }
    }

    /* Populate keywords. */
    if (hcon_size(&rec->keywords) > 0) {

       while ((key = drms_record_nextkey(rec, &last, 0)) ) {
	key->record = rec; /* parent link. */
	if (likely (!key->info->islink && !drms_keyword_isconstant(key))) {
		/*  Copy value only if not null. Otherwise use default value
					already set from the record template  */
	  if (!qres->column[col].is_null[row]) {
	    column_type = db_binary_column_type (qres, col);
	    record_value = db_binary_field_get (qres, row, col);
	    drms_copy_db2drms (key->info->type, &key->value, 
		column_type, record_value);
	  }
	  col++;
	}
      }
       if (last)
       {
          hiter_destroy(&last);
       }
    }

    /* Segment fields. */
    if (hcon_size(&rec->segments) > 0)
    {
       /* This function has parts that are conditional on the series version being 
	* greater than or equal to version 2.0. */
      DRMS_SeriesVersion_t vers2_0 = {"2.0", ""};
      DRMS_SeriesVersion_t vers2_1 = {"2.1", ""};
      char kbuf[DRMS_MAXKEYNAMELEN];
      DRMS_Keyword_t *segkey = NULL;

      while( (seg = drms_record_nextseg(rec, &last, 0)) )
      {
         segnum = seg->info->segnum;
	seg->record = rec; /* parent link. */
	/* Segment file name stored as column "sg_XXX_file */
	db_binary_field_getstr(qres, row, col++, DRMS_MAXSEGFILENAME, seg->filename);
	if (seg->info->scope==DRMS_VARDIM)
	{
	  /* segment dim names are stored as columns "sg_XXX_axisXXX" */	
	  for (i=0; i<seg->info->naxis; i++)
	    seg->axis[i] = db_binary_field_getint(qres, row, col++);
	}
	if (drms_series_isvers(rec->seriesinfo, &vers2_0))
	{
	   /* compression parameters are stored as keywords cparms_sgXXX - these
	    * keywords should have been populated in the keywords section just above. */
	  
	   snprintf(kbuf, sizeof(kbuf), "cparms_sg%03d", segnum);

	   segkey = hcon_lookup_lower(&(rec->keywords), kbuf);
	   if (segkey)
	   {
	      snprintf(seg->cparms, DRMS_MAXCPARMS, "%s", segkey->value.string_val);
	   }
	}

        if (drms_series_isvers(rec->seriesinfo, &vers2_1))
        {
           /* segment-specific bzero and bscale keywords are used to populate 
            * the segment structure */
           snprintf(kbuf, sizeof(kbuf), "%s_bzero", seg->info->name);

           segkey = hcon_lookup_lower(&(rec->keywords), kbuf);
           if (segkey)
           {
              seg->bzero = segkey->value.double_val;
           }

           snprintf(kbuf, sizeof(kbuf), "%s_bscale", seg->info->name);

           segkey = hcon_lookup_lower(&(rec->keywords), kbuf);
           if (segkey)
           {
              seg->bscale = segkey->value.double_val;
           }
        }
      }

      if (last)
      {
         hiter_destroy(&last);
      }
    }
  }

#ifndef DRMS_CLIENT
  drms_unlock_server(env);
#endif

  return 0;
}

/* 
   Build a list of fields corresponding to all the columns in
   the main series table in which the attributes of the record
   is stored. The fields are stored listed in the following order:

   recnum, sumid, slotnum, <keywords names>, <link names>

*/
char *drms_field_list(DRMS_Record_t *rec, int *num_cols) 
{
  int i,len=0;
  char *buf, *p;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  int ncol=0, segnum;
  DRMS_Segment_t *seg;
  HIterator_t *last = NULL;

  XASSERT(rec);
  /**** First get length of string buffer required. ****/
  /* Fixed fields. */
  ncol=0;
  len = strlen("recnum");
  len += strlen("sunum")+2;  
  len += strlen("slotnum")+2;
  len += strlen("sessionid")+2; 
  len += strlen("sessionns")+2;
  ncol += 5;

  /* Link fields. */
  while( (link = drms_record_nextlink(rec, &last)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      len += strlen(link->info->name)+5;
      ++ncol;
    }
    else /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	len += strlen(link->info->name) + 11;
	++ncol;
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++) {
	len += strlen (link->info->name);
	len += strlen (link->info->pidx_name[i]) + 6;
	++ncol;
      }
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Keyword fields. */
  while( (key = drms_record_nextkey(rec, &last, 0)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      len += strlen(key->info->name)+5;
      ++ncol;
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Segment fields. */
  while( (seg = drms_record_nextseg(rec, &last, 0)) )
  {
    /* Segment file name stored as column "sg_XXX_file */
    len += 13;
    ++ncol; 
    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sg_XXX_axisXXX" */	
      len += 16*seg->info->naxis;      
      ncol += seg->info->naxis;
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Malloc string buffer. */
  buf = malloc(len+1);
  XASSERT(buf);
  //  printf("ncol = %d\n",ncol);
  /**** Finally construct string by copying names of segments, links,
	and keywords into the string buffer. ****/
  p = buf;
  /* Fixed fields. */
  p += sprintf(p,"%s","recnum"); 
  p += sprintf(p,", %s","sunum"); 
  p += sprintf(p,", %s","slotnum"); 
  p += sprintf(p,", %s","sessionid"); 
  p += sprintf(p,", %s","sessionns");
 
 /* Link fields. */  
  while( (link = drms_record_nextlink(rec, &last)) )
  {
    if (link->info->type == STATIC_LINK)
      p += sprintf(p,", ln_%s",link->info->name);
    else  /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	p += sprintf(p,", ln_%s_isset", link->info->name);
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
	p += sprintf(p,", ln_%s_%s",link->info->name, link->info->pidx_name[i]);
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Keyword fields. */
  while( (key = drms_record_nextkey(rec, &last, 0)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
      p += sprintf(p,", %s",key->info->name);
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Segment fields. */
  while( (seg = drms_record_nextseg(rec, &last, 0)) )
  {
     segnum = seg->info->segnum;
    p += sprintf(p, ", sg_%03d_file", segnum);
    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)	
	p += sprintf(p,", sg_%03d_axis%03d",segnum,i);
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  buf[len] = 0; /* Hopefully we got the length right! */

  if (num_cols)
    *num_cols = ncol;
#ifdef DEBUG
  printf("Constructed field list '%s'\n",buf);
#endif
  return buf;
}



/* Insert multiple records via bulk insert interface. */
int drms_insert_records(DRMS_RecordSet_t *recset)
{
  DRMS_Env_t *env;
  int col,row,i,num_args, num_rows;
  char *query, *field_list, *seriesname, *p, *series_lower;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Record_t *rec;
  DRMS_Segment_t *seg;
  DB_Type_t *intype;
  char **argin;
  int status;
  int *sz;
  HIterator_t *last = NULL;

  CHECKNULL(recset);

  /* Variables to save typing... */
  if ((num_rows = recset->n) < 1)
    return 0;

  /* Use the first record to get series wide information. */
  rec = recset->records[0];
  env = rec->env;
  seriesname = rec->seriesinfo->seriesname;
  series_lower = strdup(seriesname);
  strtolower(series_lower);
  /* Get list of column names. */
  field_list = drms_field_list(rec, &num_args);


  /* Construct SQL string. */
  query = malloc(strlen(field_list)+10*DRMS_MAXSERIESNAMELEN);
  XASSERT(query);
  p = query;
  p += sprintf(p, "%s (%s)",  series_lower, field_list);
#ifdef DEBUG
  printf("Table and column specifier for bulk insert = '%s'\n",query);
#endif

  argin = malloc(num_args*sizeof(void *));
  XASSERT(argin);
  sz = malloc(num_args*sizeof(int));
  XASSERT(sz);
  intype = malloc(num_args*sizeof(DB_Type_t));
  XASSERT(intype);
    
  /* Get type and size information for record attributes. */
  col = 0;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* recnum */
  argin[col] = malloc(num_rows*db_sizeof(intype[col]));
  XASSERT(argin[col]);
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sunum */
  argin[col] = malloc(num_rows*db_sizeof(intype[col]));
  XASSERT(argin[col]);
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_INT);  /* slotnum */
  argin[col] = malloc(num_rows*db_sizeof(intype[col]));
  XASSERT(argin[col]);
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sessionid */
  argin[col] = malloc(num_rows*db_sizeof(intype[col]));
  XASSERT(argin[col]);
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_STRING);   /* sessionns */
  argin[col] = malloc(num_rows*sizeof(char *));
  XASSERT(argin[col]);
  col++;

  /* Loop through Links. */
  while( (link = drms_record_nextlink(rec, &last)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      intype[col]  = drms2dbtype(DRMS_TYPE_LONGLONG);
      argin[col] = malloc(num_rows*db_sizeof(intype[col]));
      XASSERT(argin[col]);
      col++;
    }
    else /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
        argin[col] = malloc(num_rows*db_sizeof(intype[col]));
        XASSERT(argin[col]);
	col++;
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
      {
	intype[col] = drms2dbtype(link->info->pidx_type[i]);
	if (link->info->pidx_type[i] == DRMS_TYPE_STRING )
	{
          argin[col] = malloc(num_rows*sizeof(char *));
          XASSERT(argin[col]);
          col++;
	}
	else
	{
          argin[col] = malloc(num_rows*db_sizeof(intype[col]));
          XASSERT(argin[col]);
	  col++;
	}
      }
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Loop through Keywords. */
  while( (key = drms_record_nextkey(rec, &last, 0)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      intype[col] = drms2dbtype(key->info->type);
      if ( key->info->type == DRMS_TYPE_STRING )
      {
        argin[col] = malloc(num_rows*sizeof(char *));
        XASSERT(argin[col]);
        col++;
      }
      else
      {
        argin[col] = malloc(num_rows*db_sizeof(intype[col]));
        XASSERT(argin[col]);
	col++;
      }
    }    
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  /* Loop through Segment fields. */
  while( (seg = drms_record_nextseg(rec, &last, 0)) )
  {
    /* segment names are stored as columns "sg_XXX_file */	
    intype[col] = drms2dbtype(DRMS_TYPE_STRING);
    argin[col] = malloc(num_rows*sizeof(char *));
    XASSERT(argin[col]);
    col++;

    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      /* segment dim values are stored in columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
      {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
        argin[col] = malloc(num_rows*db_sizeof(intype[col]));
        XASSERT(argin[col]);
	col++;
      }
    }
  }

  if (last)
  {
     hiter_destroy(&last);
  }

  for (col=0; col<num_args; col++)
    sz[col] = db_sizeof(intype[col]);
    

  /* Now copy the actual values from the records into the argument arrays. */
  for (row=0; row<num_rows; row++)
  {
    rec = recset->records[row];
    col = 0;
    memcpy(argin[col]+row*sz[col], &rec->recnum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sunum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->slotnum, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sessionid, sz[col]);
    col++;
    memcpy(argin[col]+row*sz[col], &rec->sessionns, sz[col]);
    col++;

    /* Loop through Links. */
    while( (link = drms_record_nextlink(rec, &last)) )
    {
      if (link->info->type == STATIC_LINK)
      {
	memcpy(argin[col]+row*sz[col], &link->recnum, sz[col]);
	col++;
      }
      else /* Oh crap! A dynamic link... */
      {
	memcpy(argin[col]+row*sz[col], &link->isset, sz[col]);
	col++;
	/* There is a field for each keyword in the primary index
	   of the target series...walk through them. */
	for (i=0; i<link->info->pidx_num; i++)
	{
	  if (link->info->pidx_type[i] == DRMS_TYPE_STRING )
	  {
	    memcpy(argin[col]+row*sz[col], &link->pidx_value[i].string_val, sz[col]);
	    col++;
	  }
	  else
	  {
	    memcpy(argin[col]+row*sz[col], 
		   drms_addr(link->info->pidx_type[i], &link->pidx_value[i]), sz[col]);
	    col++;
	  }
	}
      }
    }

    if (last)
    {
       hiter_destroy(&last);
    }

    /* Loop through Keywords. */
    while( (key = drms_record_nextkey(rec, &last, 0)) )
    {
      if (!key->info->islink && !drms_keyword_isconstant(key))
      {
	if (key->info->type == DRMS_TYPE_STRING )
	{
	  memcpy(argin[col]+row*sz[col], &key->value.string_val, sz[col]);
	  col++;
	}
	else
	{
	  memcpy(argin[col]+row*sz[col], 
		 drms_addr(key->info->type, &key->value), sz[col]);
	  col++;
	}
      }    
    }
    
    if (last)
    {
       hiter_destroy(&last);
    }

    /* Loop through Segment fields. */
    while( (seg = drms_record_nextseg(rec, &last, 0)) )
    {
      // This is a hack to get around the problem that
      // seg->filename is an array, not a pointer.  
      ((char **)argin[col])[row] = seg->filename;
      col++;
      if (seg->info->scope==DRMS_VARDIM)
      {
	/* segment dim names are stored as columns "sgXXX_axisXXX" */	
	/* segment dim values are stored in columns "sgXXX_axisXXX" */	
	for (i=0; i<seg->info->naxis; i++)
	{
	  memcpy(argin[col]+row*sz[col], &seg->axis[i], sz[col]);
	  col++;
	}
      }
    }

    if (last)
    {
       hiter_destroy(&last);
    }
  }
#ifdef DEBUG
  printf("col = %d, num_args=%d\n",col,num_args);
#endif
  XASSERT(col==num_args);
  status = drms_bulk_insert_array(env->session, query, num_rows, num_args,
				  intype, (void **)argin);

  for (col=0; col<num_args; col++)
    free(argin[col]);
  free(argin);
  free(intype);
  free(query);
  free(sz);
  free(field_list);
  free(series_lower);
  return status;
}




/* Return an estimate of the size of a record's data segment files in bytes. */
long long drms_record_size(DRMS_Record_t *rec)
{
  long long  size;
  DRMS_Segment_t *seg;
  HIterator_t hit; 
  
  CHECKNULL(rec);
  size = 0;
  /* Sum up the sizes of the data segments.  */
  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    //    if (seg->info->scope != DRMS_CONSTANT)
    size +=  drms_segment_size(seg,NULL);
    
  }
  hiter_free(&hit);
  return size;
}

/* "Pretty" print the fields of a record structure and its keywords, links, 
   and data segments. */
void drms_print_record(DRMS_Record_t *rec)
   {
         drms_fprint_record(stdout, rec);
   }

/* "Pretty" prints the fields of a record structure and its keywords, links and data
	segments to a file. */
void drms_fprint_record(FILE *keyfile, DRMS_Record_t *rec)
{
  int i;
  const int fwidth=17;
  HIterator_t hit;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;

  if (rec==NULL)
  {
    fprintf(stderr,"NULL pointer in drms_print_record.\n");
    return;
  }
  fprintf(keyfile, "================================================================================\n");
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Series name",rec->seriesinfo->seriesname);
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Version",rec->seriesinfo->version);
  fprintf(keyfile, "%-*s:\t%lld\n",fwidth,"Record #",rec->recnum);
  fprintf(keyfile, "%-*s:\t%lld\n",fwidth,"Storage unit #",rec->sunum);
  if (rec->su)
  {
    fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Storage unit dir",rec->su->sudir);
  }
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Storage unit slot #",rec->slotnum);
  fprintf(keyfile, "%-*s:\t%lld\n",fwidth,"Session ID",rec->sessionid);
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Session Namespace",rec->sessionns);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Readonly",rec->readonly);
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Description",rec->seriesinfo->description);
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Author",rec->seriesinfo->author);
  fprintf(keyfile, "%-*s:\t%s\n",fwidth,"Owner",rec->seriesinfo->owner);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Unitsize",rec->seriesinfo->unitsize);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Archive",rec->seriesinfo->archive);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Retention",rec->seriesinfo->retention);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Retention_perm",rec->seriesinfo->retention_perm);
  fprintf(keyfile, "%-*s:\t%d\n",fwidth,"Tapegroup",rec->seriesinfo->tapegroup);

  for (i=0; i<rec->seriesinfo->pidx_num; i++)
    fprintf(keyfile, "%-*s %d:\t%s\n",fwidth,"Internal primary index",i,
	   (rec->seriesinfo->pidx_keywords[i])->info->name);

  int npkeys = 0;
  char **extpkeys = 
    drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &npkeys, NULL);
  if (extpkeys && npkeys > 0)
  {
     for (i=0; i<npkeys; i++)
       fprintf(keyfile, "%-*s %d:\t%s\n",fwidth,"External primary index",i,
	       extpkeys[i]);
  }

  if (extpkeys)
  {
     drms_series_destroypkeyarray(&extpkeys, npkeys);
  }

  for (i=0; i<rec->seriesinfo->dbidx_num; i++)
    fprintf(keyfile, "%-*s %d:\t%s\n",fwidth,"DB index",i,
	   (rec->seriesinfo->dbidx_keywords[i])->info->name);

  hiter_new_sort(&hit, &rec->keywords, drms_keyword_ranksort);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    fprintf(keyfile, "%-*s '%s':\n",13,"Keyword",key->info->name);
    drms_keyword_fprint(keyfile, key);
  }

  hiter_new_sort(&hit, &rec->links, drms_link_ranksort); 
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    fprintf(keyfile, "%-*s '%s':\n",13,"Link",link->info->name);
    drms_link_fprint(keyfile, link);
  }

  hiter_new_sort(&hit, &rec->segments, drms_segment_ranksort);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    fprintf(keyfile, "%-*s '%s':\n",fwidth,"Segment",seg->info->name);
      drms_segment_fprint(keyfile, seg);
  }
  fprintf(keyfile, "================================================================================\n");
}

int drms_record_numkeywords(DRMS_Record_t *rec)
{
  if (rec)
      return hcon_size(&rec->keywords); 
  else
    return 0;
}

int drms_record_numlinks(DRMS_Record_t *rec)
{
  if (rec)
    return hcon_size(&rec->links); 
  else
    return 0;
}

int drms_record_numsegments(DRMS_Record_t *rec)
{
  if (rec)
    return hcon_size(&rec->segments); 
  else
      return 0;
}

int drms_record_num_nonlink_segments(DRMS_Record_t *rec)
{
  HIterator_t hit;
  DRMS_Segment_t *seg;
  int count = 0;
  if (rec) {
    hiter_new(&hit, &rec->segments); 
    while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) ) {
      if (!seg->info->islink) {
	count++;
      }
    }

    hiter_free(&hit);
  }
  return count;
}

/* Return the path to the Storage unit slot directory associateed with
   a record. If no storage unit slot has been assigned to the record yet,
   an empty string is returned. */
int drms_record_directory (DRMS_Record_t *rec, char *dirname, int retrieve) {
  int stat;
  if (drms_record_numsegments (rec) <= 0) {
#ifdef DEBUG
    fprintf (stderr, "ERROR: Calling drms_record_directory is valid only for "
	"records containing data segments.");
#endif
    dirname[0] = 0;
    return(DRMS_ERROR_NOSEGMENT);
  }

  /* If all segments are protocol DRMS_DSDS, then there will be no record
   * directory, because the fits files are stored in DSDS dirs.  If a segment
   * is protocol DRMS_DSDS, then there is only one segment. */
  if (drms_record_isdsds(rec))
  {
     /* Can't call drms_record_directory() on a record that contains a DRMS_DSDS 
      * segment */
     fprintf(stderr, "ERROR: Cannot call drms_record_directory() on a record that contains a DRMS_DSDS segment.\n");
     return(DRMS_ERROR_NODSDSSUPPORT);
  }

  if (rec->sunum != -1LL && rec->su == NULL) {
#ifdef DEBUG
    printf ("Getting SU for record %s:#%lld, sunum=%lld\n",
	   rec->seriesinfo->seriesname,rec->recnum, rec->sunum);
#endif
    if ((rec->su = drms_getunit (rec->env, rec->seriesinfo->seriesname,
	rec->sunum, retrieve, &stat)) == NULL) {
      if (stat) 
        {
	fprintf (stderr, "ERROR in drms_record_directory: Cannot retrieve "
	    "storage unit. stat = %d\n", stat);
        return(DRMS_ERROR_SUMGET);
        }
      dirname[0] = '\0';
      return(DRMS_SUCCESS); /* no SU but no SUMS error either */
    }
    rec->su->refcount++;
#ifdef DEBUG    
    printf("Retrieved unit sunum=%lld, sudir=%s\n",
	   rec->su->sunum, rec->su->sudir);
#endif
  }

  /* There can be a record directory only if the record has a storage unit. */
  if (rec->su)
  {
     /* It could be that the record contains only TAS files, in which case 
      * there are no slot directories (but there are still slots - it is just
      * that a slot maps to a slice in the TAS file, which lives at the level
      * of the sudir). */
     DRMS_Segment_t *seg = NULL;
     int hasslotdirs = 0;
     HIterator_t *seghit = hiter_create(&(rec->segments));
     if (seghit)
     {
        while((seg = (DRMS_Segment_t *)hiter_getnext(seghit)))
        {
           if (seg->info->protocol != DRMS_TAS)
           {
              hasslotdirs = 1;
              break;
           }
        }
        hiter_destroy(&seghit);
     }

     if (hasslotdirs)
     {
        CHECKSNPRINTF(snprintf(dirname, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT, 
                               rec->su->sudir, rec->slotnum), DRMS_MAXPATHLEN);
     }
     else
     {
        CHECKSNPRINTF(snprintf(dirname, DRMS_MAXPATHLEN, "%s", 
                               rec->su->sudir), DRMS_MAXPATHLEN);  
     }
  }

  return(DRMS_SUCCESS);
}

/* Will not make a SUMS request - if the rec does not have a pointer to the SUdir, then the dirout 
 * returned will be the empty string. */
int drms_record_directory_nosums(DRMS_Record_t *rec, char *dirout, int size)
{
   int rv = DRMS_SUCCESS;

   if (dirout && size >= 1)
   {
      if (drms_record_numsegments(rec) <= 0)
      {
         *dirout = '\0';
         rv = DRMS_ERROR_NOSEGMENT;
      }
      else if (drms_record_isdsds(rec))
      {
         /* Can't call drms_record_directory() on a record that contains a DRMS_DSDS segment. */
         fprintf(stderr, "ERROR: Cannot call drms_record_directory_nosums() on a record that contains a DRMS_DSDS segment.\n");
         *dirout = '\0';
         rv = DRMS_ERROR_NODSDSSUPPORT;
      }
      else
      {
         /* There can be a record directory only if the record has a storage unit. */
         if (rec->su && *rec->su->sudir != '\0')
         {
            /* It could be that the record contains only TAS files, in which case 
             * there are no slot directories (but there are still slots - it is just
             * that a slot maps to a slice in the TAS file, which lives at the level
             * of the sudir). */
            DRMS_Segment_t *seg = NULL;
            int hasslotdirs = 0;
            HIterator_t *seghit = hiter_create(&(rec->segments));

            if (seghit)
            {
               while((seg = (DRMS_Segment_t *)hiter_getnext(seghit)))
               {
                  if (seg->info->protocol != DRMS_TAS)
                  {
                     hasslotdirs = 1;
                     break;
                  }
               }
               hiter_destroy(&seghit);
            }

            if (hasslotdirs)
            {
               CHECKSNPRINTF(snprintf(dirout, size, "%s/" DRMS_SLOTDIR_FORMAT, rec->su->sudir, rec->slotnum), size);
            }
            else
            {
               CHECKSNPRINTF(snprintf(dirout, size, "%s", rec->su->sudir), size);  
            }
         }
         else
         {
            if (rec->sunum != -1LL && rec->su == NULL)
            {
               /* There was never a SUM_get() performed on this SU, so we can't answer the question 
                * "what is the SUdir for this record?" */
               rv = DRMS_ERROR_NEEDSUMS;
            }

            *dirout = '\0';
         }
      }
   }
   else
   {
      rv = DRMS_ERROR_INVALIDDATA;
   }

   return rv;
}

/* Ask DRMS to open a file in the storage unit slot directory associated with
   a data record. If mode="w" or mode="a" and the record has not been assigned 
   a storage unit slot, one is allocated. It is an error to call with 
   mode="r" if the record has not been assigned a storage unit slot. 
   In this case a NULL pointer is returned. */

FILE *drms_record_fopen(DRMS_Record_t *rec, char *filename, const char *mode)
{
  int stat;
  int newslot = 0;
  char path[DRMS_MAXPATHLEN];
  FILE *fp;
  int createslotdirs = 1;
  DRMS_Segment_t *seg = NULL;
    
  if (drms_record_numsegments(rec) < 0)
  {
    fprintf(stderr,"ERROR: Calling drms_record_fopen is only for records "
	    "containing data segments.");
    return NULL;
  }
  switch(mode[0])
  {
  case 'w':
  case 'a':
    if (rec->su==NULL)
    {
       /* If all segments are TAS segments, then there is no need to create 
        * slot dirs as all data will go into the SU */
       HIterator_t *seghit = hiter_create(&(rec->segments));
       if (seghit)
       {
          createslotdirs = 0;
          while((seg = (DRMS_Segment_t *)hiter_getnext(seghit)))
          {
             if (seg->info->protocol != DRMS_TAS)
             {
                createslotdirs = 1;
             }
          }
          hiter_destroy(&seghit);
       }

       if ((stat = drms_newslots(rec->env, 1, rec->seriesinfo->seriesname, 
				&rec->recnum, rec->lifetime, &rec->slotnum, 
				&rec->su, createslotdirs)))
      {
	fprintf(stderr,"ERROR in drms_record_fopen: drms_newslot"
		" failed with error code %d.\n",stat);
	return NULL;
      }
      rec->sunum = rec->su->sunum;
      newslot = 1;
    }
    break;
  case 'r':
    /* First check if there is a SU associated with this record. 
       If so get it from SUMS if that has not already been done. */  
    if (rec->sunum != -1LL && rec->su==NULL)
    {
      if ((rec->su = drms_getunit(rec->env, rec->seriesinfo->seriesname, 
				  rec->sunum, 1, &stat)) == NULL)
      {
	if (stat)
	  fprintf(stderr,"ERROR in drms_record_fopen: Cannot open file for "
		  "reading in non-existent storage unit slot. stat = %d\n", stat);
	return NULL;
      }
      rec->su->refcount++;
    }
  }

  CHECKSNPRINTF(snprintf(path,DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT "/%s",
			 rec->su->sudir, rec->slotnum, filename), DRMS_MAXPATHLEN);

  if (!(fp = fopen(path, mode)))
  {
    perror("ERROR in drms_record_fopen: fopen failed with");
    if (newslot) /* Ooops! Never mind about that slot! */
      drms_freeslot(rec->env, rec->seriesinfo->seriesname, 
		    rec->su->sunum, rec->slotnum);
  }
  return fp;
}



/* Estimate size used by record meta-data. */
long long drms_record_memsize( DRMS_Record_t *rec)
{
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  //  DRMS_Segment_t *seg;
  long long size;

  size = sizeof(DRMS_Record_t) + DRMS_MAXHASHKEYLEN;
  //  printf("struct size = %lld\n",size);
  /* Get type and size information for record attributes. */
  /* Loop through Links. */
  hiter_new(&hit, &rec->links);/* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    size += sizeof(DRMS_Link_t) +  DRMS_MAXLINKNAMELEN;
    if (link->info->type != STATIC_LINK)
      size += link->info->pidx_num*10;/* NOTE: Just a wild guess. */
  }

  hiter_free(&hit);
  //  printf("link size = %lld\n",size);

  /* Loop through Keywords. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    size += sizeof(DRMS_Keyword_t) +  DRMS_MAXKEYNAMELEN + 1;
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      if ( key->info->type == DRMS_TYPE_STRING )
      {
	if (key->value.string_val)
	  size += strlen(key->value.string_val); 
	else 
	  size += 40;
      }    
    }
  }

  hiter_free(&hit);
  //printf("keyword size = %lld\n",size);

  /* Loop through Segment fields. */
  size += hcon_size(&rec->segments) * (sizeof(DRMS_Segment_t) + DRMS_MAXSEGNAMELEN);
  //printf("segment size = %lld\n",size);

  return size;
}

// estimate the size of keyword lists. input: comma separated keyword names
// do not check duplicate
long long drms_keylist_memsize(DRMS_Record_t *rec, char *keylist) {

  int size = 0;
  char *key = NULL;
  char *list = strdup(keylist);

  // remove whitespaces in list
  char *src, *dst;
  src = dst = list;
  while (*src != '\0') {
    if (*src != ' ') {
      *dst = *src;
      dst++;
    } 
    src++;
  }
  *dst = '\0';

  char *p = list;
  while (*p != '\0') {
    char *start = p;
    int len = 0;
    while (*p != ',' && *p != '\0') {
      len++;
      p++;
    }
    key = malloc (len + 1);
    snprintf (key, len + 1, "%s", start);
    if (strcmp(key, "recnum") == 0 || 
        strcmp(key, "sunum") == 0 ||
        strcmp(key, "slotnum") == 0 ||
        strcmp(key, "sessionid") == 0 ||
        strcmp(key, "sessionns") == 0) {
      size  += sizeof(DRMS_Keyword_t) +  DRMS_MAXKEYNAMELEN + 1;
      size += 20;
    } else {
      DRMS_Keyword_t *keyword = drms_keyword_lookup(rec, key, 0);
      if (keyword) {
	size += sizeof(DRMS_Keyword_t) +  DRMS_MAXKEYNAMELEN + 1;
	if (!keyword->info->islink && !drms_keyword_isconstant(keyword)) {
	  if ( keyword->info->type == DRMS_TYPE_STRING ) {
	    if (keyword->value.string_val)
	      size += strlen(keyword->value.string_val); 
	    else 
	      size += 40;
	  }    
	}
      } else {
	printf("Unknown keyword: %s\n", key);
	size = 0;
	goto bailout;
      }
    }
    // skip the comma
    if (*p != '\0') {
      p++;
    }

    if (key)
    {
       free(key);
       key = NULL;
    }
  }

 bailout:
  if (key)
  {
     free(key);
     key = NULL;
  }
  free(list);
  return size;
}

int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source)
{
   memcpy(target->seriesinfo, source->seriesinfo, sizeof(DRMS_SeriesInfo_t));
   memset(target->seriesinfo->pidx_keywords, 0, sizeof(DRMS_Keyword_t *) * DRMS_MAXPRIMIDX);
   memset(target->seriesinfo->dbidx_keywords, 0, sizeof(DRMS_Keyword_t *) * DRMS_MAXPRIMIDX);

   return DRMS_SUCCESS;
}

int CopySegments(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_segment_prototypes(target, source, &status);

   return status;
}

int CopyLinks(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_link_prototypes(target, source, &status);

   return status;
}

int CopyKeywords(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;
   drms_create_keyword_prototypes(target, source, &status);

   return status;
}

/* target->keywords must exist at this point! */
/* target->seriesinfo must exist at this point! */
int CopyPrimaryIndex(DRMS_Record_t *target, DRMS_Record_t *source)
{
   int status = DRMS_SUCCESS;

   if (target != NULL && target->seriesinfo != NULL)
   {
      target->seriesinfo->pidx_num = source->seriesinfo->pidx_num;

      int idx = 0;
      for (; status == DRMS_SUCCESS && idx < source->seriesinfo->pidx_num; idx++)
      {
	 char *name = source->seriesinfo->pidx_keywords[idx]->info->name;
	 DRMS_Keyword_t *key = hcon_lookup_lower(&target->keywords, name);
	 if (key != NULL)
	 {
	    target->seriesinfo->pidx_keywords[idx] = key;
	 }
	 else
	 {
	    status = DRMS_ERROR_INVALIDKEYWORD;
	 }
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDRECORD;
   }
      
   return status;
}

static int DSElem_IsWS(const char **c)
{
   const char *pC = *c;
   const char *pWS = kDSElemParseWS;

   while (*pWS)
   {
      if (*pC == *pWS)
      {
	 break;
      }

      pWS++;
   }

   if (*pWS)
   {
      return 1;
   }

   return 0;
}

static int DSElem_IsDelim(const char **c)
{
   const char *pDel = kDSElemParseDelim;

   while (*pDel)
   {
      if (**c == *pDel)
      {
	 break;
      }

      pDel++;
   }

   if (*pDel)
   {
      return 1;
   }

   return 0;
}

static int DSElem_IsComment(const char **c)
{
   return (**c == '#');
}

/* Advance to next non-whitespace char, if one exists before NULL.
 * Returns 1 if found a non-ws char before NULL, 0 otherwise. */
static int DSElem_SkipWS(char **c)
{
   if (**c == '\0')
   {
      return 0;
   }
   else if (!DSElem_IsWS((const char **)c))
   {
      return 1;
   }
   else
   {
      char *pC = *c;

      while (*pC)
      {
	 const char *pWS = kDSElemParseWS;

	 while (*pWS)
	 {
	    if (*pC == *pWS)
	    {
	       pC++;
	       break;
	    }

	    pWS++;
	 }

	 if (!(*pWS))
	 {
	    *c = pC;
	    return 1;
	 }
      }

      return 0;
   }
}

static int DSElem_SkipComment(char **c)
{
   char *endc = NULL;

   if (**c == '#')
   {
      endc = strchr((*c + 1), '#');
 
      if (endc)
      {
	 *c = endc + 1;
      }
      else
      {
	 *c = strchr(*c, '\0');
      }

      return 1;
   }
   else
   {
      return 0;
   }
}

/* ParseRecSetDesc() minimally parses record-set queries - it parses to the degree necessary
 * to distinguish between different types of queries requiring different types of processing.
 * The real parsing gets done in drms_names.c, but the problem is that the parsing in drms_names.c
 * happens too late. For example, before drms_names.c is called, we must know whether 
 * the query is a plain-flie query, a dsds rec-set query, or a drms rec-set query, so 
 * ParseRecSetDesc() figures this out.  Ideally, the query would be parsed once, at the 
 * location where ParseRecSetDesc() is first called - but too late for that.
 */
/* Caller owns sets. */
int ParseRecSetDesc(const char *recsetsStr, 
                    char **allvers, 
                    char ***sets, 
                    DRMS_RecordSetType_t **settypes, 
                    char ***snames,
                    int *nsets, 
                    DRMS_RecQueryInfo_t *info)
{
   int status = DRMS_SUCCESS;
   RSParseState_t state = kRSParseState_Begin;
   RSParseState_t oldstate;
   char *rsstr = strdup(recsetsStr);
   char *pc = rsstr;
   LinkedList_t *intSets = NULL;
   LinkedList_t *intSettypes = NULL;
   LinkedList_t *intAllVers = NULL;
   LinkedList_t *intSnames = NULL;
   char *pset = NULL;
   char *sname = NULL;
   int count = 0;
   char buf[kMAXRSETSPEC] = {0};
   char *pcBuf = buf;
   LinkedList_t *multiRSQueries = NULL;
   LinkedList_t *multiRSTypes = NULL;
   LinkedList_t *multiRSAllVers = NULL;
   LinkedList_t *multiRSSnames = NULL;
   DRMS_RecordSetType_t currSettype;
   char currAllVers = 'n';
   int countMultiRS = 0;
   char *endInput = rsstr + strlen(rsstr); /* points to null terminator */
   int nfilter = 0;
   int recnumrsseen = 0;
   DRMS_RecQueryInfo_t intinfo = 0;

   /* Test for an empty string. */
   int empty = 0;
   char *ptest = NULL;

   ptest = rsstr;
   empty = (DSElem_SkipWS(&ptest) == 0);

   *nsets = 0;

   if (rsstr && !empty)
   {
      while (pc && pc <= endInput && state != kRSParseState_Error)
      {
	 switch (state)
	 {
	    case kRSParseState_Begin:
	      intSets = list_llcreate(sizeof(char *), NULL);
	      intSettypes = list_llcreate(sizeof(DRMS_RecordSetType_t), NULL);
              intAllVers = list_llcreate(sizeof(char), NULL);
              intSnames = list_llcreate(sizeof(char *), NULL);
	      if (!intSets || !intSettypes || !intAllVers || !intSnames)
	      {
		 state = kRSParseState_Error;
		 status = DRMS_ERROR_OUTOFMEMORY;
	      }
	      else
	      {
		 state = kRSParseState_BeginElem;
	      }
	      break;
	    case kRSParseState_BeginElem:
	      /* There may be whitespace at the beginning. */
	      if (pc < endInput)
	      {
		 if (DSElem_IsWS((const char **)&pc))
		 {
		    /* skip whitespace */
		    pc++;
		 }
		 else if (*pc == '[' || *pc == ']' || *pc == ',' || 
			  *pc == ';' || *pc == '#')
		 {
		    state = kRSParseState_Error;
		 }
		 else
		 {
		    if (*pc == '{')
		    {
		       pc++;
		       if (DSElem_SkipWS(&pc))
		       {
                          if (DSDS_IsDSDSSpec(pc))
			  {
			     state = kRSParseState_DSDS;
			  }
			  else if (strstr(pc, "vot:") == pc)
			  {
			     state = kRSParseState_VOT;
			  }
                          else if (DSDS_IsDSDSPort(pc))
                          {
                             state = kRSParseState_DSDSPort;
                          }
			  else
			  {
			     fprintf(stderr, 
				     "Unexpected record-set specification within curly brackets.\n" );
			     state = kRSParseState_Error;
			  }
		       }
		       else
		       {
			  state = kRSParseState_Error;
		       }
		    }
		    else if (*pc == '@')
		    {
		       /* text file that contains one or more recset queries */
		       /* recursively parse those queries! */
		       state = kRSParseState_AtFile;

                       /* Mark the '@file' flag in the record-set spec info returned to caller. */
                       intinfo |= kAtFile;
		       pc++;
		    }
		    else if (*pc == '/' || *pc == '.')
		    {
		       state = kRSParseState_Plainfile;
		    }
		    else
		    {
		       state = kRSParseState_DRMS;
		    }
		 }
	      }
	      break;
	    case kRSParseState_DRMS:
	      /* first char is not ws */
	      /* not parsing a DRMS RS filter (yet) */
              recnumrsseen = 0; // reset watch for multiple recnum filters
	      if (pc < endInput)
	      {
		 if (*pc == ']')
		 {
		    /* chars not allowed in a DRMS RS */
		    state = kRSParseState_Error;
		 }
		 else if (*pc == '[')
		 {
                    *pcBuf++ = *pc++;

                    if (pc < endInput && (*pc == '?' || *pc == '!'))
                    {
                       if (*pc == '!')
                       {
                          state = kRSParseState_DRMSFiltAllVersSQL;
                       }
                       else
                       {
                          state = kRSParseState_DRMSFiltSQL;
                       }

                       *pcBuf++ = *pc++;
                    }
                    else
                    {
                       state = kRSParseState_DRMSFilt;
                    }

                    /* Mark the 'filters' flag in the record-set spec info returned to caller. */
                    intinfo |= kFilters;
		 }
		 else if (*pc == '{')
		 {
		    *pcBuf++ = *pc++;
		    state = kRSParseState_DRMSSeglist;
		 }
		 else if (DSElem_IsDelim((const char **)&pc))
		 {
                    /* Pointing to a delimiter after series name. */
                    if (sname == NULL)
                    {
                       size_t len = strlen(buf);
                       sname = strdup(buf);
                       *(sname + len - 1) = '\0';
                    }

		    pc++;
		    state = kRSParseState_EndElem;
		 }
		 else if (DSElem_IsComment((const char **)&pc))
		 {
                    /* Pointing to # after series name. */
                    if (sname == NULL)
                    {
                       size_t len = strlen(buf);
                       sname = strdup(buf);
                       *(sname + len - 1) = '\0';
                    }

		    DSElem_SkipComment(&pc);
		    state = kRSParseState_EndElem;
		 }
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* whitespace between the series name and a filter is allowed */
		    if (DSElem_SkipWS(&pc))
		    {
		       if (*pc == '[')
		       {
			  *pcBuf++ = *pc++;

                          if (pc < endInput && (*pc == '?' || *pc == '!'))
                          {
                             if (*pc == '!')
                             {
                                state = kRSParseState_DRMSFiltAllVersSQL;
                             }
                             else
                             {
                                state = kRSParseState_DRMSFiltSQL;
                             }

                             *pcBuf++ = *pc++;
                          }
                          else
                          {
                             state = kRSParseState_DRMSFilt;
                          }
		       }
		       else if (*pc == '{')
		       {
			  *pcBuf++ = *pc++;
			  state = kRSParseState_DRMSSeglist;
		       }
		       else if (DSElem_IsDelim((const char **)&pc))
		       {
                          /* Pointing to delimiter AFTER whitespace AFTER seriesname. */
                          if (sname == NULL)
                          {
                             size_t len = strlen(buf);
                             char *pchar = buf;

                             sname = strdup(buf);
                             *(sname + len - 1) = '\0';

                             /* Now strip off trailing whitespace. */
                             while (*pchar)
                             {
                                if (DSElem_IsWS((const char **)&pchar))
                                {
                                   /* found whitespace. */
                                   *pchar = '\0';
                                   break;
                                }
                             }
                          }

			  pc++;
			  state = kRSParseState_EndElem;
		       }
		       else if (DSElem_IsComment((const char **)&pc))
		       {
                          /* Pointing to # AFTER whitespace AFTER seriesname.*/
                          if (sname == NULL)
                          {
                             size_t len = strlen(buf);
                             char *pchar = buf;

                             sname = strdup(buf);
                             *(sname + len - 1) = '\0';

                             /* Now strip off trailing whitespace. */
                             while (*pchar)
                             {
                                if (DSElem_IsWS((const char **)&pchar))
                                {
                                   /* found whitespace. */
                                   *pchar = '\0';
                                   break;
                                }
                             }
                          }

			  DSElem_SkipComment(&pc);
			  state = kRSParseState_EndElem;
		       }
		       else
		       {
			  state = kRSParseState_Error;
		       }
		    }
		    else
		    {
                       /* whitespace AFTER series name, but nothing following this whitespace. */
                       if (sname == NULL)
                       {
                          char *pchar = buf;

                          sname = strdup(buf);

                          /* Now strip off trailing whitespace. */
                          while (*pchar)
                          {
                             if (DSElem_IsWS((const char **)&pchar))
                             {
                                /* found whitespace. */
                                *pchar = '\0';
                                break;
                             }
                          }
                       }

		       state = kRSParseState_EndElem;
		    }
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
                 if (sname == NULL)
                 {
                    sname = strdup(buf);
                 }

		 state = kRSParseState_EndElem;
	      }

	      if (state == kRSParseState_EndElem)
	      {
		 currSettype = kRecordSetType_DRMS;
	      }
	      break;
	    case kRSParseState_DRMSFilt:
	      /* inside '[' and ']' */
	      if (pc < endInput)
	      {
                 if (sname == NULL)
                 {
                    /* We know we've seen a '[', the first char in a filter. buf has the preceding series name, 
                     * plus a trailing '['. */
                    size_t len = strlen(buf);
                    sname = strdup(buf);
                    *(sname + len - 1) = '\0';
                 }

                 /* If a recnumrangeset has been seen already, then it makes
                  * no sense to have a second filter. 
                  */
                 if (*pc == ':' && recnumrsseen)
                 {
                    state = kRSParseState_Error;
                    fprintf(stderr, "Only one recnum list filter is allowed.\n");
                    break;
                 }
                 else if (*pc == ':')
                 {
                    recnumrsseen = 1;
                 }

                 /* just do one pass now */
                 /* check for SQL filt */
                 if (*pc == '?')
                 {
                    *pcBuf++ = *pc++;
                    state = kRSParseState_DRMSFiltSQL;
                 }
                 else if (*pc == '!')
                 {
                    *pcBuf++ = *pc++;
                    state = kRSParseState_DRMSFiltAllVersSQL;
                 }
                 else
                 {
                    /* Assume the filter value is a string or set of strings */
                    while (*pc != ']' && state != kRSParseState_Error)
                    {
                       DRMS_Type_Value_t val;
                       memset(&val, 0, sizeof(DRMS_Type_Value_t));
                       int rlen = drms_sscanf_str(pc, "]", &val);
                       int ilen = 0;

                       /* Don't need string - just strlen */
                       DRMS_Value_t dummy = {DRMS_TYPE_STRING, val};
                       drms_value_free(&dummy);

                       /* if ending ']' was found, then pc + rlen is ']', otherwise
                        * it is the next char after end quote */
                       while (ilen < rlen)
                       {
                          *pcBuf++ = *pc++;

                          if (pc == endInput)
                          {
                             state = kRSParseState_Error;
                          }

                          ilen++;
                       }
                    }

                    /* skip any ws between last char and right bracket */
                    //DSElem_SkipWS(&pc);

                    if (*pc == ']' && state != kRSParseState_Error)
                    {
                       *pcBuf++ = *pc++;
                       if (DSElem_SkipWS(&pc))
                       {
                          if (*pc == '[')
                          {
                             *pcBuf++ = *pc++;

                             if (pc < endInput && (*pc == '?' || *pc == '!'))
                             {
                                if (*pc == '!')
                                {
                                   state = kRSParseState_DRMSFiltAllVersSQL;
                                }
                                else
                                {
                                   state = kRSParseState_DRMSFiltSQL;
                                }

                                *pcBuf++ = *pc++;
                             }
                             else
                             {
                                state = kRSParseState_DRMSFilt;
                             }
                          }
                          else if (*pc == '{')
                          {
                             *pcBuf++ = *pc++;
                             state = kRSParseState_DRMSSeglist;
                          }
                          else if (DSElem_IsDelim((const char **)&pc))
                          {
                             pc++;
                             state = kRSParseState_EndElem;
                          }
                          else if (DSElem_IsComment((const char **)&pc))
                          {
                             DSElem_SkipComment(&pc);
                             state = kRSParseState_EndElem;
                          }
                          else
                          {
                             state = kRSParseState_Error;
                          }
                       }
                       else
                       {
                          state= kRSParseState_EndElem;
                       }
                    }
                    else
                    {
                       state = kRSParseState_Error;
                    }
                 }
	      }
	      else
	      {
		 /* didn't finish filter */
		 state = kRSParseState_Error;
	      }

	      if (state == kRSParseState_EndElem)
	      {
		 currSettype = kRecordSetType_DRMS;
	      }

              nfilter++;

	      break;
            case kRSParseState_DRMSFiltAllVersSQL:
              currAllVers = 'y';
              /* intentional fall through */
	    case kRSParseState_DRMSFiltSQL:
	      if (pc < endInput)
	      {
                 if (sname == NULL)
                 {
                    /* We know we've seen a "[?" or a "[!", the first 2 chars in a filter. buf has 
                     * the preceding series name, plus a trailing "[?" or "[!". */
                    size_t len = strlen(buf);

                    sname = strdup(buf);
                    *(sname + len - 2) = '\0';
                 }

                 if (*pc == '"' || *pc == '\'')
                 {
                    /* skip quoted strings */
                    DRMS_Type_Value_t val;
                    memset(&val, 0, sizeof(DRMS_Type_Value_t));
                    int rlen = drms_sscanf_str(pc, NULL, &val);
                    int ilen = 0;

                    /* Don't need string - just strlen */
                    DRMS_Value_t dummy = {DRMS_TYPE_STRING, val};
                    drms_value_free(&dummy);

                    if (rlen == -1)
                    {
                       /* There was a problem with the quoted string (like a missing end quote). */
                       fprintf(stderr, "Invalid quoted string '%s'.\n", pc);
                       state = kRSParseState_Error;
                    }
                    else
                    {
                       /* if ending ']' was found, then pc + rlen is ']', otherwise
                        * it is the next char after end quote */
                       while (ilen < rlen)
                       {
                          *pcBuf++ = *pc++;
                          ilen++;
                       }
                    }
                 }
// put catching of time_convert flag X here, also in parse_record_query in drms_names.c
                 else if (*pc == '$' && *(pc+1) == '(')
                 {
                    /* A form of '$(xxxx)' is taken to be a DRMS preprocessing function
                     * which is evaluated prior to submitting the query to psql.  The
                     * result of the function must be a valid SQL operand or expression.
                     * the only DRMS preprocessing function at the moment is to
                     * convert an explicit time constant into an internal DRMS TIME
                     * expressed as a double constant. */
                     char *rparen = strchr(pc+2, ')');
                     char temptime[100];
                     if (!rparen || rparen - pc > 40) // leave room for microsecs
                     {
                        fprintf(stderr,"Time conversion error starting at %s\n",pc+2);
		        state = kRSParseState_Error;
                     }
                     else
                     {
                        /* pick function here, if ever more than time conversion */
                        TIME t; 
#ifdef DEBUG
                        int consumed;
#endif

                        strncpy(temptime,pc+2,rparen-pc-2);

#ifdef DEBUG
                        consumed = 
#endif
                          sscan_time_ext(temptime, &t);


                        if (time_is_invalid(t))
                            fprintf(stderr,"Warning: invalid time from %s\n",temptime);
#ifdef DEBUG
fprintf(stderr,"XXXXXXX original in drms_record, convert time %s uses %d chars, gives %f\n",temptime, consumed,t);
#endif
                        pc = rparen + 1;
                        pcBuf += sprintf(pcBuf, "%16.6f", t);
                     }
                 }
		 else if ((*pc == '?' && state == kRSParseState_DRMSFiltSQL) ||
                          (*pc == '!' && state == kRSParseState_DRMSFiltAllVersSQL))
		 {
		    *pcBuf++ = *pc++;
		    if ((pc < endInput) && (*pc == ']'))
		    {
		       state = kRSParseState_DRMSFilt;
		    }
		 }
		 else
		 {
		    /* simply copy query as is, whitespace okay in sql query 
		     *   see drms_names.c */
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 /* didn't finish query */
		 state = kRSParseState_Error;
	      }

              /* We exit this state by either erroring out, or going to state kRSParseState_DRMSFilt. */
	      break;
	    case kRSParseState_DRMSSeglist:
	      /* first char after '{' */
	      if (pc < endInput)
	      {
              if (sname == NULL)
              {
                  /* We know we've seen a '{', the first char in a seglist. buf has 
                   * the preceding series name, plus a trailing '{'. We also know 
                   * that there was NO filter, otherwise sname would have been 
                   * set in the kRSParseState_DRMSFilt-case code block. */
                  size_t len = strlen(buf);
                  
                  sname = strdup(buf);
                  *(sname + len - 1) = '\0';
              }
              
		 DSElem_SkipWS(&pc); /* ingore ws, if any */
		 
		 while (*pc != '}' && pc < endInput)
		 {
		    if (DSElem_IsWS((const char **)&pc))
		    {
		       DSElem_SkipWS(&pc);
		    }
		    else if (*pc == ',' || *pc == ':' || *pc == ';')
		    {
		       DSElem_SkipWS(&pc);
		       if (*pc == '}')
		       {
			  state = kRSParseState_Error;
			  break;
		       }
                       
                       *pcBuf++ = *pc++;
		    }
		    else
		    {
		       *pcBuf++ = *pc++;
		    }
		 }

		 if (state != kRSParseState_Error && *pc != '}')
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 /* didn't finish seglist */
		 state = kRSParseState_Error;
	      }

	      if (state != kRSParseState_Error)
	      {
		 /* *pc == '}' */
		 *pcBuf++ = *pc++;
	      }

	      if (DSElem_SkipWS(&pc))
	      {
		 if (DSElem_IsDelim((const char **)&pc))
		 {
		    pc++;
		    state = kRSParseState_EndElem;
		 }
		 else if (DSElem_IsComment((const char **)&pc))
		 {
		    DSElem_SkipComment(&pc);
		    state = kRSParseState_EndElem;
		 }
		 else
		 {
		    state = kRSParseState_Error;
		 }
	      }
	      else
	      {
		 state= kRSParseState_EndElem;
	      }

	      if (state == kRSParseState_EndElem)
	      {
		 currSettype = kRecordSetType_DRMS;
	      }
	      break;
	    case kRSParseState_DSDS:
            case kRSParseState_DSDSPort:
	      /* first non-ws after '{' */
              oldstate = state;

	      if (pc < endInput)
	      {
		 if (*pc == '{')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* Allow WS anywhere within '{' and '}', but eliminate */
		    pc++;
		 }
		 else if (*pc == '}')
		 {
		    pc++;

		    if (pc < endInput)
		    {
		       if (DSElem_IsWS((const char **)&pc))
		       {
			  if (!DSElem_SkipWS(&pc))
			  {
			     state = kRSParseState_EndElem;
			  }
		       }

		       if (state != kRSParseState_EndElem)
		       {
			  if (DSElem_IsDelim((const char **)&pc))
			  {
			     pc++;
			     state = kRSParseState_EndElem;
			  }
			  else if (DSElem_IsComment((const char **)&pc))
			  {
			     DSElem_SkipComment(&pc);
			     state = kRSParseState_EndElem;
			  }
			  else
			  {
			     /* there is something after '}' that isn't
			      * ws or a delimeter */
			     state = kRSParseState_Error;
			  }
		       }
		    }
		    else
		    {
		       state = kRSParseState_EndElem;
		    }
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }

	      if (state == kRSParseState_EndElem)
	      {
                 if (oldstate == kRSParseState_DSDS)
                 {
                    currSettype = kRecordSetType_DSDS;
                 }
                 else if (oldstate == kRSParseState_DSDSPort)
                 {
                    currSettype = kRecordSetType_DSDSPort;
                 }
                 else
                 {
                    state = kRSParseState_Error;
                 }
	      }
	      break;
	    case kRSParseState_VOT:
	      /* first non-ws after '{' */
	      if (pc < endInput)
	      {
		 if (*pc == '{')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* Allow WS anywhere within '{' and '}', but eliminate */
		    pc++;
		 }
		 else if (*pc == '}')
		 {
		    pc++;

		    if (pc < endInput)
		    {
		       if (DSElem_IsWS((const char **)&pc))
		       {
			  if (!DSElem_SkipWS(&pc))
			  {
			     state = kRSParseState_EndElem;
			  }
		       }

		       if (DSElem_IsDelim((const char **)&pc))
		       {
			  pc++;
			  state = kRSParseState_EndElem;
		       }
		       else if (DSElem_IsComment((const char **)&pc))
		       {
			  DSElem_SkipComment(&pc);
			  state = kRSParseState_EndElem;
		       }
		       else
		       {
			  /* there is something after '}' that isn't
			   * ws or a delimeter */
			  state = kRSParseState_Error;
		       }
		    }
		    else
		    {
		       state = kRSParseState_EndElem;
		    }
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_Error;
	      }

	      if (state == kRSParseState_EndElem)
	      {
		 currSettype = kRecordSetType_VOT;
	      }
	      break;
	    case kRSParseState_AtFile:
	      if (pc < endInput)
	      {
		 if (DSElem_IsDelim((const char **)&pc))
		 {
		    pc++;
		    state = kRSParseState_EndAtFile;
		 }
		 else if (DSElem_IsComment((const char **)&pc))
		 {
		    DSElem_SkipComment(&pc);
		    state = kRSParseState_EndAtFile;
		 }
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* Don't allow ws in filenames! Print an error 
		     * message if that happens.
		     */
		    if (DSElem_SkipWS(&pc))
		    {
		       /* Found non-ws */
		       if (DSElem_IsDelim((const char **)&pc))
		       {
			  pc++;
			  state = kRSParseState_EndAtFile;
		       }
		       else if (DSElem_IsComment((const char **)&pc))
		       {
			  DSElem_SkipComment(&pc);
			  state = kRSParseState_EndAtFile;
		       }
		       else
		       {
			  fprintf(stderr, 
				  "'@' files containing whitespace are not allowed.\n");
			  state = kRSParseState_Error;
		       }
		    }
		    else
		    {
		       /* All ws after the filename */
		       state = kRSParseState_EndAtFile;
		    }
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_EndAtFile;
	      }
	      break;
	    case kRSParseState_EndAtFile:
	      {
              char lineBuf[LINE_MAX];
              char *fullline = NULL;
              char **queriesAtFile = NULL;
              DRMS_RecordSetType_t *typesAtFile = NULL;
              char **snamesAtFile = NULL;
              char *allversAtFile = NULL;
              int nsetsAtFile = 0;
              int iSet = 0;
              struct stat stBuf;
              FILE *atfile = NULL;
              
              /* finished reading an AtFile filename - read file one line at a time,
               * parsing each line recursively. */
              if (multiRSQueries)
              {
                  state = kRSParseState_Error;
                  break;
              }
              else
              {
                  multiRSQueries = list_llcreate(sizeof(char *), NULL);
                  multiRSTypes = list_llcreate(sizeof(DRMS_RecordSetType_t), NULL);
                  multiRSAllVers = list_llcreate(sizeof(char), NULL);
                  multiRSSnames = list_llcreate(sizeof(char *), NULL);
                  
                  /* buf has filename */
                  *pcBuf = '\0';
                  if (buf && !stat(buf, &stBuf))
                  {
                      if (S_ISREG(stBuf.st_mode))
                      {
                          /* read a line */
                          if ((atfile = fopen(buf, "r")) == NULL)
                          {
                              fprintf(stderr, "Cannot open @file %s for reading, skipping.\n", buf);
                          }
                          else
                          {
                              int len = 0;
                              while (!(fgets(lineBuf, LINE_MAX, atfile) == NULL))
                              {
                                  /* strip \n from end of lineBuf */
                                  len = strlen(lineBuf);
                                  
                                  fullline = strdup(lineBuf);
                                  
                                  if (len == LINE_MAX - 1)
                                  {
                                      /* may be more on this line */
                                      while (!(fgets(lineBuf, LINE_MAX, atfile) == NULL))
                                      {
                                          fullline = realloc(fullline, strlen(fullline) + strlen(lineBuf) + 1);
                                          snprintf(fullline + strlen(fullline), 
                                                   strlen(lineBuf) + 1, 
                                                   "%s",
                                                   lineBuf);
                                          if (strlen(lineBuf) > 1 && lineBuf[strlen(lineBuf) - 1] == '\n')
                                          {
                                              break;
                                          }
                                      }
                                  }
                                  
                                  len = strlen(fullline);
                                  
                                  /* skip empty lines*/
                                  if (len > 1)
                                  {
                                      DRMS_RecQueryInfo_t infoAtFile;
                                      
                                      if (fullline[len - 1] == '\n')
                                      {
                                          fullline[len - 1] = '\0';
                                      }
                                      
                                      status = ParseRecSetDesc(fullline, 
                                                               &allversAtFile, 
                                                               &queriesAtFile, 
                                                               &typesAtFile, 
                                                               &snamesAtFile,
                                                               &nsetsAtFile,
                                                               &infoAtFile);
                                      
                                      if (status == DRMS_SUCCESS)
                                      {
                                          /* add all nsetsAtFile recordsets to multiRSQueries 
                                           * NOTE: nsetsAtFile is the number of record-sets on
                                           * the current line.
                                           */
                                          for (iSet = 0; iSet < nsetsAtFile; iSet++)
                                          {
                                              /* dupe this string because FreeRecSetDescArr() will 
                                               * free the original string. */
                                              pset = strdup(queriesAtFile[iSet]);
                                              list_llinserttail(multiRSQueries, &pset);
                                              list_llinserttail(multiRSTypes, &(typesAtFile[iSet]));
                                              list_llinserttail(multiRSAllVers, &(allversAtFile[iSet]));
                                              /* dupe this string because FreeRecSetDescArr() will 
                                               * free the original string. */
                                              
                                              /* There might be no series name associated with this 
                                               * atfile line (e.g., the line is a plain-file spec. If 
                                               * this is the case, then insert a null pointer. */
                                              if (snamesAtFile[iSet])
                                              {
                                                  pset = strdup(snamesAtFile[iSet]);
                                              }
                                              else
                                              {
                                                  pset = NULL;
                                              }
                                              list_llinserttail(multiRSSnames, &pset);
                                              countMultiRS++;
                                          }
                                          
                                          intinfo |= infoAtFile;
                                      }
                                      else
                                      {
                                          state = kRSParseState_Error;
                                          break;
                                      }
                                      
                                      FreeRecSetDescArr(&allversAtFile, &queriesAtFile, &typesAtFile, &snamesAtFile, nsetsAtFile);
                                  }
                                  
                                  if (fullline)
                                  {
                                      free(fullline);
                                      fullline = NULL;
                                  }
                              } /* while */
                          }
                      }
                      else
                      {
                          fprintf(stderr, "@file %s is not a regular file, skipping.\n", buf);
                      }
                  }
                  else
                  {
                      fprintf(stderr, "Cannot find @file %s, skipping.\n", buf);
                  }
              }
	      }
             
             /* Got into this state because either saw a delimiter, or end of input */
             
	      state = kRSParseState_EndElem;
	      break;
	    case kRSParseState_Plainfile:
	      /* Pointing to leading '/' or './' */
	      if (pc < endInput)
	      {
		 if (DSElem_IsDelim((const char **)&pc))
		 {
		    pc++;
		    state = kRSParseState_EndElem;
		 }
		 else if (DSElem_IsComment((const char **)&pc))
		 {
		    DSElem_SkipComment(&pc);
		    state = kRSParseState_EndElem;
		 }
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* Don't allow ws in filenames! Print an error 
		     * message if that happens.
		     */
		    if (DSElem_SkipWS(&pc))
		    {
		       /* Found non-ws */
		       if (DSElem_IsDelim((const char **)&pc))
		       {
			  pc++;
			  state = kRSParseState_EndElem;
		       }
		       else if (DSElem_IsComment((const char **)&pc))
		       {
			  DSElem_SkipComment(&pc);
			  state = kRSParseState_EndElem;
		       }
		       else
		       {
			  fprintf(stderr, 
				  "'plainfiles' containing whitespace are not allowed.\n");
			  state = kRSParseState_Error;
		       }
		    }
		    else
		    {
		       /* All ws after the filename */
		       state = kRSParseState_EndElem;
		    }
		 }
		 else
		 {
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 state = kRSParseState_EndElem;
	      }

	      if (state == kRSParseState_EndElem)
	      {
		 currSettype = kRecordSetType_PlainFile;
	      }
	      break;
	    case kRSParseState_EndElem:
	      /* pc points to whatever is after a ds delim, or trailing ws. */
	      /* could be next ds elem, ws, delim, or NULL (end of input). */
	      *pcBuf = '\0';

              /* multiRSQueries implies @filename */
	      if (!multiRSQueries)
	      {
                 pset = strdup(buf);
                 list_llinserttail(intSets, &pset);
                 list_llinserttail(intSettypes, &currSettype);
                 list_llinserttail(intAllVers, &currAllVers);
                 list_llinserttail(intSnames, &sname);
                 currAllVers = 'n';
		 count++;
	      }
	      else
	      {
		 int iSet;
                 ListNode_t *node = NULL;

		 for (iSet = 0; iSet < countMultiRS; iSet++)
		 {
                    node = list_llgethead(multiRSQueries);
                    if (node)
                    {
                       /* intSets now owns char * pointed to by node->data */
                       list_llinserttail(intSets, node->data);
                       list_llremove(multiRSQueries, node);
                       list_llfreenode(&node);
                    }

                    node = list_llgethead(multiRSTypes);
                    if (node)
                    {
                       list_llinserttail(intSettypes, node->data);
                       list_llremove(multiRSTypes, node);
                       list_llfreenode(&node);
                    }

                    node = list_llgethead(multiRSAllVers);
                    if (node)
                    {
                       list_llinserttail(intAllVers, node->data);
                       list_llremove(multiRSAllVers, node);
                       list_llfreenode(&node);
                    }

                    node = list_llgethead(multiRSSnames);
                    if (node)
                    {
                       list_llinserttail(intSnames, node->data);
                       list_llremove(multiRSSnames, node);
                       list_llfreenode(&node);
                    }

		    count++;
		 }

		 free(multiRSQueries); /* don't deep-free; intSets now owns strings */
		 multiRSQueries = NULL;
		 free(multiRSTypes);
		 multiRSTypes = NULL;
                 free(multiRSAllVers);
                 multiRSAllVers = NULL;
                 free(multiRSSnames);
                 multiRSSnames = NULL;
		 countMultiRS = 0;
	      }

	      if (pc < endInput)
	      {
		 if (DSElem_SkipWS(&pc))
		 {
		    state = kRSParseState_BeginElem;
		 }
		 else
		 {
		    state = kRSParseState_End;
		 }
	      }
	      else
	      {
		 state = kRSParseState_End;
	      }

              pcBuf = buf;
              bzero(buf, sizeof(buf));

              /* Don't forget to set sname back to NULL to prepare for next record-set subquery. sname is now 
               * owned by intSnames*/
              sname = NULL;
              
	      break;
	    case kRSParseState_End:
	      if (DSElem_SkipWS(&pc))
	      {
		 /* found non-ws at the end, not acceptable */
		 state = kRSParseState_Error;
	      }
	      else
	      {
		 pc = NULL;
		 state = kRSParseState_Success;
	      }
	      break;
	    default:
	      state = kRSParseState_Error;
	 }
      }
      
      free(rsstr);
   } /* rsstr */

   if (status == DRMS_SUCCESS && state == kRSParseState_Success && count > 0)
   {
      *sets = (char **)malloc(sizeof(char *) * count);
      *settypes = (DRMS_RecordSetType_t *)malloc(sizeof(DRMS_RecordSetType_t) * count);
      *allvers = (char *)malloc(sizeof(char) * count + 1);
      *snames = (char **)malloc(sizeof(char *) * count);

      if (*sets && *settypes && *allvers && *snames)
      {
         int iset;
         ListNode_t *node = NULL;
	 *nsets = count;

         for (iset = 0; iset < count; iset++)
         {
            node = list_llgethead(intSets);
            if (node)
            {
               /* sets now owns char * pointed to by node->data */
               memcpy(&((*sets)[iset]), node->data, sizeof(char *));
               list_llremove(intSets, node);
               list_llfreenode(&node);
            }

            node = list_llgethead(intSettypes);
            if (node)
            {
               memcpy(&((*settypes)[iset]), node->data, sizeof(DRMS_RecordSetType_t));
               list_llremove(intSettypes, node);
               list_llfreenode(&node);
            }

            node = list_llgethead(intAllVers);
            if (node)
            {
               memcpy(&((*allvers)[iset]), node->data, sizeof(char));
               list_llremove(intAllVers, node);
               list_llfreenode(&node);
            }

            node = list_llgethead(intSnames);
            if (node)
            {
               memcpy(&((*snames)[iset]), node->data, sizeof(char *));
               list_llremove(intSnames, node);
               list_llfreenode(&node);
            }
         }

         (*allvers)[count] = '\0';

         if (info)
         {
            *info = intinfo;
         }
      }
      else
      {
	 status = DRMS_ERROR_OUTOFMEMORY;
      }
   }

   if (status == DRMS_SUCCESS && state != kRSParseState_Success)
   {
      status = DRMS_ERROR_INVALIDDATA;
   }

   if (intSets)
   {
      free(intSets);
   }

   if (intSettypes)
   {
      free(intSettypes);
   }

   if (intAllVers)
   {
      free(intAllVers);
   }

   if (intSnames)
   {
      free(intSnames);
   }

   return status;
}

int FreeRecSetDescArr(char **allvers, char ***sets, DRMS_RecordSetType_t **types, char ***snames, int nsets)
{
   int error = 0;

   if (allvers && *allvers)
   {
      free(*allvers);
      allvers = NULL;
   }

   if (sets)
   {
      int iSet;
      char **setArr = *sets;

      if (setArr)
      {
	 for (iSet = 0; iSet < nsets; iSet++)
	 {
	    char *oneSet = setArr[iSet];

	    if (oneSet)
	    {
	       free(oneSet);
	    }
	 }

	 free(setArr);
      }

      *sets = NULL;
   }

   if (types && *types)
   {
      free(*types);
      types = NULL;
   }

   if (snames)
   {
      int iSet;
      char **snameArr = *snames;

      if (snameArr)
      {
	 for (iSet = 0; iSet < nsets; iSet++)
	 {
	    char *oneSname = snameArr[iSet];

	    if (oneSname)
	    {
	       free(oneSname);
	    }
	 }

	 free(snameArr);
      }

      *snames = NULL;
   }


   return error;
}

int drms_recproto_setseriesinfo(DRMS_Record_t *rec, 
				int *unitSize, 
				int *bArchive, 
				int *nDaysRetention,
				int *tapeGroup,
				const char *description)
{
   int status = DRMS_NO_ERROR;

   if (rec && rec->seriesinfo)
   {
      if (unitSize)
      {
	 rec->seriesinfo->unitsize = *unitSize;
      }

      if (bArchive)
      {
	 rec->seriesinfo->archive = *bArchive;
      }

      if (nDaysRetention)
      {
	 rec->seriesinfo->retention = *nDaysRetention;
      }
      
      if (tapeGroup)
      {
	 rec->seriesinfo->tapegroup = *tapeGroup;
      }

      if (description && strlen(description) < DRMS_MAXCOMMENTLEN)
      {
	 strcpy(rec->seriesinfo->description, description);
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDDATA;
   }

   return status;
}

static int IsFileOrDir(const char *q)
{
   int ret = 0;
   char *lbrack = NULL;
   char *query = strdup(q);
   struct stat stBuf;

   if (query)
   {
      if ((lbrack = strchr(query, '[')) != NULL)
      {
	 *lbrack = '\0';      
      }

      if (!stat(query, &stBuf))
      {
	 if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode) || S_ISDIR(stBuf.st_mode))
	 {
	    ret = 1;
	 }
      }

      free(query);
   }

   return ret;
}

DRMS_RecordSetType_t drms_record_getquerytype(const char *query)
{
   DRMS_RecordSetType_t ret;
   const char *pQ = (*query == '{') ? query + 1 : query;

   if (DSDS_IsDSDSSpec(pQ))
   {
      ret = kRecordSetType_DSDS;
   }
   else if (DSDS_IsDSDSPort(query))
   {
      ret = kRecordSetType_DSDSPort;
   }
   else if (IsFileOrDir(query))
   {
      ret = kRecordSetType_PlainFile;
   }
   else
   {
      ret = kRecordSetType_DRMS;
   }

   return ret;
}

char *drms_record_jsoc_version(DRMS_Env_t *env, DRMS_Record_t *rec) {
  char *jsocversion = 0;
  char query[DRMS_MAXQUERYLEN];
  snprintf(query, DRMS_MAXQUERYLEN, "select jsoc_version from %s.drms_session where sessionid = %lld", rec->sessionns, rec->sessionid);
  DB_Text_Result_t *qres;
  if ((qres = drms_query_txt(env->session, query)) && qres->num_rows>0) {
    if (qres->field[0][0][0]) {
      jsocversion = malloc(strlen(qres->field[0][0])+1);
      XASSERT(jsocversion);
      strcpy(jsocversion, qres->field[0][0]);
    }
  }
  db_free_text_result(qres);
  return jsocversion;
}

char *drms_recordset_acquireseriesname(const char *query)
{
   char *sn = strdup(query);
   char *pc = NULL;

   if (sn)
   {
      pc = index(sn, '[');
      if (pc)
      {
	 *pc = '\0';
	 pc = strdup(sn);
	 free(sn);
	 sn = pc;
      }
   }
   
   return sn;
}

int drms_recordset_getssnrecs(DRMS_RecordSet_t *set, unsigned int setnum, int *status)
{
   int start = 0;
   int end = 0;
   int stat = DRMS_SUCCESS;
   int res = 0;
   int notnegone = 0;

   if (setnum >= set->ss_n)
   {
      stat = DRMS_RANGE;
   }
   else 
   {
      if (set->ss_starts[setnum] == -1)
      {
	 /* no records for this set - the query returned no records */
	 res = 0;
      }
      else
      {
	 start = set->ss_starts[setnum];
         notnegone = setnum + 1;

         /* Need to find the next ss_start whose value is not -1 (which signifies a sub-rs with no records). */
         while (notnegone < set->ss_n && set->ss_starts[notnegone] == -1)
         {
            notnegone++;
         }

	 if (notnegone == set->ss_n)
	 {
            /* There was no ss_starts that was not -1; count records from setnum (an index) 
             * to set->n - 1 (last index) */
	    end = set->n - 1;
	    res = end - start + 1;
	 }
	 else
	 {
	    end = set->ss_starts[notnegone];
	    res = end - start;
	 }
      }
   }

   if (status)
   {
      *status = stat;
   }

   return res;
}

/* Add rec to rs - rs assumes ownership of rec, pushed onto the end of rs->records - 
 * Unfortunately, the DRMS_RecordSet_t design uses an array of pointers to hold records.
 * A linked list would have been much better.  So, realloc the array in chunks
 * and hope that all uses of a record in the array check to see if it is NULL first.
 * These types of recordsets have rs->ss_n == -X, where X is the current number of 
 * malloc'd records (not all malloc'd records are used).  rs->n still contains the
 * number of actual records.

 * Since rec is not part of an existing record-set, there is no information about 
 * the query used to generate the rec and no cursor information.  So discard that
 * information from the recordset - merged recordsets cannot be used in any manner
 * that requires a query or cursor.
 */
int drms_merge_record(DRMS_RecordSet_t *rs, DRMS_Record_t *rec)
{
   int iset;
   int nmerge = 0;
   int err = 0;

   if (rs && rec)
   {
      if (rs->ss_n > 0)
      {
         for (iset = 0; iset < rs->ss_n; iset++)
         {
            if (rs->ss_queries && rs->ss_queries[iset])
            {
               free(rs->ss_queries[iset]);
               rs->ss_queries[iset] = NULL;
            }
         }

         if (rs->ss_queries)
         {
            free(rs->ss_queries);
            rs->ss_queries = NULL;
         }

         if (rs->ss_types)
         {
            free(rs->ss_types);
            rs->ss_types = NULL;
         }

         if (rs->ss_starts)
         {
            free(rs->ss_starts);
            rs->ss_starts = NULL;
         }

         if (rs->ss_currentrecs)
         {
            free(rs->ss_currentrecs);
            rs->ss_currentrecs = NULL;
         }
      
         if (rs->cursor)
         {
            drms_free_cursor(&(rs->cursor));
         }
      } /* rs->ss_n > 0 */

      if (rs->records == NULL)
      {
         /* no records in rs - malloc first one */
         rs->records = malloc(sizeof(DRMS_Record_t *) * 32);
         rs->n = 0;
         rs->ss_n = -32;
      }

      if (rs->n == abs(rs->ss_n))
      {
         /* realloc */
         void *tmp = realloc(rs->records, sizeof(DRMS_Record_t *) * abs(rs->ss_n) * 2);
         if (tmp)
         {
            rs->records = tmp;
            rs->ss_n = -1 * abs(rs->ss_n) * 2;
         }
         else
         {
            err = 1;
         }
      }

      if (!err)
      {
         rs->records[rs->n] = rec;
         rs->n++;
         nmerge = 1;
      }

   } /* rs && rec */

   return nmerge;
}

int drms_recordset_setchunksize(unsigned int size)
{
   int status = DRMS_SUCCESS;

   if (size > 0 && size <= DRMS_MAXCHUNKSIZE)
   {
      gRSChunkSize = size;
   }
   else
   {
      status = DRMS_ERROR_BADCHUNKSIZE;
      fprintf(stderr, 
	      "Invalid chunk size '%u'.  Must be > 0 and <= '%d'.\n", 
	      size, 
	      DRMS_MAXCHUNKSIZE);
   }

   return status;
}

unsigned int drms_recordset_getchunksize()
{
   /* Defaults to 128. */
   return gRSChunkSize;
}
/* Returns the number of records in the chunk (which could be less than the 
 * chunk size for the last chunk */
/* pos is chunk index */
int drms_open_recordchunk(DRMS_Env_t *env,
                          DRMS_RecordSet_t *rs, 
			  DRMS_RecSetCursorSeek_t seektype, 
			  long long chunkindex,  
			  int *status)
{
   int stat = DRMS_SUCCESS;
   int nrecs;

   if (rs && rs->cursor)
   {
      long long recindex = -1; /* record seeking to, absolute index */
      DRMS_RecordSet_t *fetchedrecs = NULL;
      
      switch (seektype)
      {
	 case kRSChunk_First:
	   {
	      recindex = 0;
	      chunkindex = 0;
	   }
	   break;
	 case kRSChunk_Abs:
	   {
              recindex = chunkindex * rs->cursor->chunksize;

              if (recindex > rs->n - 1)
              {
                 fprintf(stderr, 
			 "Error in drms_open_recordchunk(): invalid chunk index '%lld'.\n",
			 chunkindex);
		 stat = DRMS_ERROR_RECSETCHUNKRANGE;
              }
	   }
	   break;
	 case kRSChunk_Next:
	   {
              if (rs->cursor->currentchunk >= 0)
              {
                 chunkindex = rs->cursor->currentchunk + 1;
                 recindex = chunkindex * rs->cursor->chunksize;
              }
              else if (rs->cursor->lastchunk >= 0)
              {
                 chunkindex = rs->cursor->lastchunk + 1;
                 recindex = chunkindex * rs->cursor->chunksize;
              }
              else
              {
                 /* Get first chunk */
                 chunkindex = 0;
                 recindex = 0;
              }

	      if (recindex > rs->n - 1)
	      {
		 fprintf(stderr, 
			 "Error in drms_open_recordchunk(): invalid chunk index '%lld'.\n",
			 chunkindex);
		 stat = DRMS_ERROR_RECSETCHUNKRANGE;
	      }
	   }
	   break;
	 default:
	   fprintf(stderr, "Unsupported seek type '%d'.\n", (int)seektype);
	   stat = DRMS_ERROR_INVALIDDATA;
      }

      if (stat == DRMS_SUCCESS && chunkindex != rs->cursor->currentchunk)
      {
	 /* Create the cursor fetch query that gets chunksize records */
         /* FETCH FORWARD nrecs <cursorname> */

	 /* A chunk may span more than one dbase cursor, because it may span
          * more than one recordset.
          */
	 long long recindex_chunkst = rs->cursor->chunksize * chunkindex;
	 long long recindex_chunkend = recindex_chunkst + rs->cursor->chunksize - 1;
	 long long recindex_ssst;
	 long long recindex_ssend;
	 int ssnrecs;
	 int iset;
         int ntofetch = 0; /* number to fetch from current subset. */
         char sqlquery[DRMS_MAXQUERYLEN];
         char *seriesname = NULL;

         if (recindex_chunkend > rs->n - 1)
         {
            recindex_chunkend = rs->n - 1;
         }

         nrecs = 0;

	 for (iset = 0; iset < rs->ss_n; iset++)
	 {
	    recindex_ssst = rs->ss_starts[iset];
	    ssnrecs = drms_recordset_getssnrecs(rs, iset, &stat);
            recindex_ssend = recindex_ssst + ssnrecs - 1;
            seriesname = drms_recordset_acquireseriesname(rs->ss_queries[iset]);
            ntofetch = 0;

            if (!seriesname)
            {
               fprintf(stderr, "Couldn't obtain seriesname from recordset subset.\n");
               stat = DRMS_ERROR_RECORDSETSUBSET;
               break;
            }

            if (recindex_ssst <= recindex_chunkst && recindex_ssend >= recindex_chunkend)
            {
               /* The chunk lies within the subset completely */
               ntofetch = recindex_chunkend - recindex_chunkst + 1;
            }
	    else if ((recindex_ssst >= recindex_chunkst && recindex_ssst <= recindex_chunkend) ||
                     recindex_ssend >= recindex_chunkst && recindex_ssend <= recindex_chunkend)
	    {
               /* This subset has at least one record that overlaps chunk*/

               //if (recindex_ssst <= recindex_chunkst && recindex_ssend >= recindex_chunkend)
               //{
               //   /* The chunk lies within the subset completely */
               //   ntofetch = recindex_chunkend - recindex_chunkst + 1;

//               }
               if (recindex_ssst < recindex_chunkst)
               {
                  /* end of set falls within chunk - take end of subset */
                  ntofetch = recindex_ssend - recindex_chunkst + 1;
               }
               else if (recindex_ssend > recindex_chunkend)
               {
                  /* beginning of set falls within chunk - take 
                   * beginning of subset */
                  ntofetch = recindex_chunkend - recindex_ssst + 1;
               }
               else 
               {
                  /* The subset lies completely within the chunk */
                  ntofetch = recindex_ssend - recindex_ssst + 1;
               }
            }

            if (ntofetch > 0)
            {
               snprintf(sqlquery, 
                        sizeof(sqlquery), 
                        "FETCH FORWARD %d FROM %s",
                        ntofetch,
                        rs->cursor->names[iset]);

               /* Don't filter out unneeded segments - too painful with our current design */
               fetchedrecs = drms_retrieve_records_internal(env, 
                                                            seriesname, 
                                                            NULL,
                                                            0, 
                                                            0, 
                                                            NULL, /* seg filter */
                                                            sqlquery,
                                                            rs->cursor->allvers[iset],
                                                            0, 
                                                            &stat);
               if (stat != DRMS_SUCCESS)
               {
                  fprintf(stderr, "Cursor query '%s' fetch failure", sqlquery);
                  break;
               }

               /* In this fetchedrecs structure, the only valid fields are n and records; the 
                * others, such as ss_n, ss_queries, etc., have not been set. */

               free(seriesname);
               seriesname = NULL;

               /* Needed by drms_stage_records() and drms_record_getinfo(), if they are called. 
                * Doesn't hurt to set these if they are not called. */
               fetchedrecs->ss_starts = (int *)malloc(sizeof(int) * 1);
               fetchedrecs->ss_starts[0] = 0;
               fetchedrecs->ss_n = 1;
                  
               /* If staging was requested, stage this chunk */
               if (rs->cursor->staging_needed)
               {
                  if (rs->cursor->staging_needed == 1)
                  {
                     /* Stage, but don't sort records by tapeid/filenum first. */
                     stat = drms_stage_records(fetchedrecs, rs->cursor->retrieve,  rs->cursor->dontwait);
                  }
                  else if (rs->cursor->staging_needed == 2)
                  {
                     /* Stage, but first sort records by tapeid/filenum. */
                     /* There will be no rec in fetchedrecs that has a non-NULL suinfo field; these
                      * records were just retrieved. The original records in rs will also not have 
                      * any SUM_info_t data since these records were never retrieved. */
                     stat = drms_sortandstage_records(fetchedrecs, rs->cursor->retrieve,  rs->cursor->dontwait, &rs->cursor->suinfo);
                  }
                  else
                  {
                     /* Unknown value for staging_needed. */
                  }

                  /* if  stat == DRMS_REMOTESUMS_TRYLATER, segment files might be available later. */
                  if (stat != DRMS_SUCCESS && stat != DRMS_REMOTESUMS_TRYLATER)
                  {
                     fprintf(stderr, "Cursor query '%s' record staging failure, status=%d.\n", sqlquery, stat);
                     break;
                  }
               }

               /* There was a previous request for a SUM_infoEx() on all SUNUMs in the recset. This
                * request got deferred until now - it should be processed on the newly openend chunk. */
               /* OK to fetch info if the info was already fetched in drms_sortandstage_records() - 
                * drms_sortandstage_records() will actually change some of the info values (like 
                * online_status). */
               if (rs->cursor->infoneeded)
               {
                  stat = drms_record_getinfo(fetchedrecs);

                  if (stat != DRMS_SUCCESS)
                  {
                     fprintf(stderr, "Failure calling drms_record_getinfo(), status=%d.\n", stat);
                     break;
                  }
               }

               /* Put the records into rs */
               int nrecs_thisset;
               for (nrecs_thisset = 0; nrecs_thisset < fetchedrecs->n; nrecs_thisset++)
               {
                  rs->records[recindex_chunkst + nrecs] = 
                    fetchedrecs->records[nrecs_thisset]; /* assumes ownership */
                  fetchedrecs->records[nrecs_thisset] = NULL;
                  nrecs++;
               }

               /* Don't free the fetchedrecs that were "taken", but free the
                * ones not used. Since the ones used were assigned NULL, 
                * the drms_free_records() call will work as desired. */
               drms_close_records(fetchedrecs, DRMS_FREE_RECORD);
            }
         } /* for iset */

         /* ART - stat may be DRMS_REMOTESUMS_TRYLATER. This is basically the same thing
          * as calling drms_open_recordchunk() without ever having called drms_stage_records()
          * on the record-set. This is an okay thing to do. */
         if (stat == DRMS_SUCCESS || stat == DRMS_REMOTESUMS_TRYLATER)
         {
            rs->cursor->currentchunk = chunkindex;
            rs->cursor->currentrec = -1; /* one record before chunk */
         }
      }
   }
   else
   {
      fprintf(stderr, "Recordset is either NULL, or is not chunked.\n");
      stat = DRMS_ERROR_INVALIDDATA;
   }

   if (status)
   {
      *status = stat;
   }

   return nrecs;
}

/* Close current record chunk.
 * This is close to drms_close_records(), but if frees only the records in rs->records
 * that reside in the chunk.  It does not free rs->records, rs->ss_starts, etc. */
int drms_close_recordchunk(DRMS_RecordSet_t *rs)
{
   int irec;
   int status = 0;
   DRMS_Record_t *rec;
   DRMS_RecordSet_t *rs_new = NULL;

   int recstart = 0; /* index (relative to first rec in rs->records) first record in chunk */
   int recend = 0;   /* index of last record in chunk */

   if (rs->cursor->currentchunk >= 0)
   {
      /* If <0, then there are no chunks in memory. */
      recstart = rs->cursor->chunksize * rs->cursor->currentchunk;
      XASSERT(recstart < rs->n);
      recend = recstart + rs->cursor->chunksize - 1;
      if (recend > rs->n - 1)
      {
         recend = rs->n - 1;
      }
  
      rs_new = malloc(sizeof(DRMS_RecordSet_t));
      memset(rs_new, 0, sizeof(DRMS_RecordSet_t));

      for (irec = recstart; irec <= recend; irec++)
      {
	 rec = rs->records[irec];
	 drms_merge_record(rs_new, rec);
         rs->records[irec] = NULL;
      }

      drms_close_records(rs_new, DRMS_FREE_RECORD);

      rs->cursor->lastchunk = rs->cursor->currentchunk;
      rs->cursor->currentchunk = -1;
      rs->cursor->currentrec = -1;
   }

   return status;
}

/* Uses a psql cursor which allows the caller to retrieve a manageable number of rows from
 * a larger query.
 * 
 * A cursor is declared as:
 *   DECLARE <cursorname> [BINARY] [[NO] SCROLL] CURSOR for <query>
 *
 * where <query> is the larger query. This is a select statement.  SCROLL allows the caller
 * to use the cursor in a non-sequential fashion (this may incur a performance penalty).
 * BINARY will cause the resonse to be in a binary format, not text.
 * 
 * Once you create a cursor, you then retrieve the subset via the FETCH command:
 *   FETCH [<direction>] <cursorname>
 *
 * where <direction> is NEXT, PRIOR, FIRST, LAST, ABSOLUTE count, RELATIVE count, ALL, FORWARD,
 * FORWARD count, FORWARD ALL, BACKWARD, BACKWARD count, BACKWARD ALL. If you want to use 
 * anything other than FETCH NEXT or FETCH FORWARD, the declaration must include SCROLL
 *
 * When the cursor is first created, it is "positioned" before the first row of the
 * larger query. After FETCH has executed, the position of the cursor is on the last
 * retrieved row.  So FETCH NEXT will first move the cursor ahead one row, then retrieve
 * that row.
 *
 * So, the declaration should be
 *   DECLARE mycursor SCROLL CURSOR FOR <query>
 * if you want to allow for searching in non-sequential directions.
 * Then to fetch n records starting at the current cursor location, the statement is 
 * FETCH FORWARD n <cursorname>
 */

DRMS_RecordSet_t *drms_open_recordset(DRMS_Env_t *env, 
				      const char *rsquery, 
				      int *status)
{
   DRMS_RecordSet_t *rs = NULL;
   static long long guid = 1;
   int stat = DRMS_SUCCESS;
   char *cursorquery = NULL;
   char cursorname[DRMS_MAXCURSORNAMELEN];
   char *seriesname = NULL;
   char *pQuery = NULL;
   char *cursorselect = NULL;
   char *pLimit = NULL;
   int iset;
   int querylen;

   if (rsquery)
   {
      /* querylist has, for each queryset, the SQL query to select all records
       * in that set (a queryset is a set of recordsets - they are comma-separated) */
      LinkedList_t *querylist;
      char *tmp = strdup(rsquery);
      char *allvers = NULL;
        
      if (tmp)
      {
	 rs = drms_open_records_internal(env, tmp, 0, &querylist, &allvers, 0, &stat);
	 free(tmp);
      }
      else
      {
         stat = DRMS_ERROR_OUTOFMEMORY;
      }

      if (rs && rs->n > 0 && querylist && allvers)
      {
         /* Create DRMS cursor, which has one psql cursor for each recordset. */
	 rs->cursor = (DRMS_RecSetCursor_t *)malloc(sizeof(DRMS_RecSetCursor_t));
	 rs->cursor->names = (char **)malloc(sizeof(char *) * rs->ss_n);
         memset(rs->cursor->names, 0, sizeof(char *) * rs->ss_n);
         rs->cursor->allvers = (int *)malloc(sizeof(int) * rs->ss_n);
         memset(rs->cursor->allvers, 0, sizeof(int) * rs->ss_n);
         /* Future staging request applies to entire record_set */
         rs->cursor->staging_needed = rs->cursor->retrieve = rs->cursor->dontwait = 0;
         rs->cursor->infoneeded = 0;
         rs->cursor->suinfo = NULL;

	 iset = 0;
         list_llreset(querylist);
         ListNode_t *ln = NULL;

	 while ((ln = (ListNode_t *)(list_llnext(querylist))) != NULL)
	 {
            pQuery = *((char **)ln->data);

            seriesname = drms_recordset_acquireseriesname(rs->ss_queries[iset]);

            if (seriesname)
            {
               char *dot = strchr(seriesname, '.');
               if (dot)
               {
                  *dot = '_';
               }
               snprintf(cursorname, sizeof(cursorname), "%s_CURSOR%lld", seriesname, guid++);

               /* pQuery has a limit statement in it - remove that else FETCH could 
                * operate on a subset of the total number of records. */
               cursorselect = strdup(pQuery);

               if (!cursorselect)
               {
                  stat = DRMS_ERROR_OUTOFMEMORY;
               }
               else
               {
                  if ((pLimit = strstr(cursorselect, "limit")) != NULL)
                  {
                     *pLimit = '\0';
                  }

                  querylen = sizeof(char) * (strlen(cursorname) + strlen(cursorselect) + 128);
                  cursorquery = malloc(querylen);

                  if (!cursorquery)
                  {
                     stat = DRMS_ERROR_OUTOFMEMORY;
                  }
                  else
                  {
                     snprintf(cursorquery, 
                              querylen, 
                              "DECLARE %s NO SCROLL CURSOR FOR (%s) FOR READ ONLY", 
                              cursorname, 
                              cursorselect);

                     /* Now, create cursor in psql */
                     if (env->verbose)
                     {
                        fprintf(stdout, "Cursor declaration ==> %s\n", cursorquery);
                     }

                     if (drms_dms(env->session, NULL, cursorquery))
                     {
                        stat = DRMS_ERROR_QUERYFAILED;
                     }
                     else
                     {
                        rs->cursor->names[iset] = strdup(cursorname);
                     }

                     free(cursorquery);
                     cursorquery = NULL;
                  }

                  free(cursorselect);
                  cursorselect = NULL;
               }

               free(seriesname);

               if (stat != DRMS_SUCCESS)
               {
                  break;
               }

               XASSERT(allvers[iset] != '\0');
               rs->cursor->allvers[iset] = (allvers[iset] == 'y');
            }
            
            iset++;
	 } /* while */

         rs->cursor->parent = rs;
         rs->cursor->env = env;
	 rs->cursor->chunksize = drms_recordset_getchunksize();
	 rs->cursor->currentchunk = -1;
         rs->cursor->lastchunk = -1;
	 rs->cursor->currentrec = -1;
      }
      else
      {
         if (rs)
         {
            rs->cursor = NULL;
         }
      }

      if (querylist)
      {
	 list_llfree(&querylist);	    
      }

      if (allvers)
      {
         free(allvers);
      }
   }

   if (stat != DRMS_SUCCESS)
   {
      /* frees cursor too */
      drms_free_records(rs);
   }

   if (status)
   {
      *status = stat;
   }

   return rs;
}

/* Returns next record in current chunk, unless no more records in current chunk.
 * In that case, open the next chunk and return the first record. */
DRMS_Record_t *drms_recordset_fetchnext(DRMS_Env_t *env, 
                                        DRMS_RecordSet_t *rs, 
                                        int *drmsstatus, 
                                        DRMS_RecChunking_t *chunkstat,
                                        int *newchunk)
{
   DRMS_Record_t *ret = NULL;
   int stat = DRMS_SUCCESS;
   DRMS_RecChunking_t cstat = kRecChunking_None;
   int neednewchunk = -1;
   
   if (newchunk)
   {
      *newchunk = 0;
   }

   if (rs && rs->cursor)
   {
      int lastchunk = (rs->n - 1) / rs->cursor->chunksize;

      if (rs->cursor->currentchunk == -1)
      {
	 /* No chunks in memory */
         if (lastchunk == rs->cursor->lastchunk)
         {
            /* no chunks in memory, but last chunk in memory was the last chunk - 
             * user has fetched all records */
            cstat = kRecChunking_NoMoreRecs;
         }
         else
         {
            neednewchunk = (int)kRSChunk_First;
         }
      }
      else 
      {
          /* Advance record pointer */
	 rs->cursor->currentrec++;

         if (rs->cursor->currentrec + rs->cursor->chunksize * rs->cursor->currentchunk == rs->n)
         {
            /* Last record in entire recordset was read last time */
            cstat = kRecChunking_NoMoreRecs;
            drms_close_recordchunk(rs);
         }
         else if (rs->cursor->currentrec == rs->cursor->chunksize)
         {
            drms_close_recordchunk(rs);
            neednewchunk = (int)kRSChunk_Next;
         }
      }

      if (neednewchunk >= 0)
      {
         drms_open_recordchunk(env, rs, (DRMS_RecSetCursorSeek_t)neednewchunk, 0, &stat);
         /* if stat == DRMS_REMOTESUMS_TRYLATER, record will be present, but segment files
          * may not be online yet. */
         if (stat != DRMS_SUCCESS && stat != DRMS_REMOTESUMS_TRYLATER)
         {
            fprintf(stderr, 
                    "Error retrieving record chunk '%d'.\n", 
                    rs->cursor->currentrec / rs->cursor->chunksize);
         }
         else
         {
            rs->cursor->currentrec++; /* currentrec in a new chunk is -1 */
            if (newchunk)
            {
               *newchunk = 1;
            }
         }
      }

      /* ART - If remote sums is running asynchronously, then this is okay, we still need to 
       * update the cursor information and return the next record. This record will not 
       * have its SU retrieved, because it is happening asynchronously, but we still have 
       * a valid record to return. */
      if ((stat == DRMS_SUCCESS || stat == DRMS_REMOTESUMS_TRYLATER) && rs->cursor->currentrec >= 0)
      {
	 /* Now, get the next record */
	 ret = rs->records[rs->cursor->currentchunk * rs->cursor->chunksize + 
			   rs->cursor->currentrec];
         
         if (rs->cursor->currentchunk * rs->cursor->chunksize + rs->cursor->currentrec == rs->n - 1)
         {
            cstat = kRecChunking_LastInRS;
         }
         else if (rs->cursor->currentrec == rs->cursor->chunksize - 1)
         {
            cstat = kRecChunking_LastInChunk;
         }
      }
   }
   else if (rs)
   {
      /* This is a non-cursored recordset. Just return the next record. */
      XASSERT(rs->ss_currentrecs); /* This should not be NULL - drms_open_records_internal() should have 
                                    * malloc'd it. */
      if (rs->ss_currentrecs)
      {
         /* If ss_currentrecs[0] is -1, this means that this is the first time drms_recordset_fetchnext()
          * has been called. When called on a non-cursored record-set, only ss_currentrecs[0] is used - it
          * contains the index of the NEXT record in the record-set, regardless how many sub-sets the 
          * recordset contains. */
         if (*rs->ss_currentrecs == -1)
         {
            *rs->ss_currentrecs = 0;
         }

         if (*rs->ss_currentrecs < rs->n)
         {
            ret = rs->records[*rs->ss_currentrecs];
            (*rs->ss_currentrecs)++;
         }
         
         /* If last record was read last time, ret will contain NULL. */
      }
   }
   else
   {
      stat = DRMS_ERROR_INVALIDDATA;
      fprintf(stderr, "Error in drms_recordset_fetchnext(): empty recordset set provided.\n");
   }

   if (drmsstatus)
   {
      *drmsstatus = stat;
   }

   if (chunkstat)
   {
      *chunkstat = cstat;
   }

   return ret;
}

/* If setnum != NULL, then the fetch is relative to the first record of the 
 * record subset specified by *setnum, otherwise, it is relative to the 
 * first record of all records in the set. */
DRMS_Record_t *drms_recordset_fetchnextinset(DRMS_Env_t *env, 
                                             DRMS_RecordSet_t *rs, 
                                             int *setnum, 
                                             int *status,
                                             DRMS_RecChunking_t *chunkstat)
{
   int stat = DRMS_SUCCESS;
   DRMS_RecChunking_t cstat = kRecChunking_None;
   DRMS_Record_t *ret = NULL;
   int newchunk = 0;

   if (!setnum)
   {
      ret = drms_recordset_fetchnext(env, rs, &stat, &cstat, &newchunk);
   }
   else
   {
      int endset = drms_recordset_getssnrecs(rs, *setnum, &stat) - 1;

      if (rs->ss_currentrecs[*setnum] == -1)
      {
	 /* start of set iteration; point to the first rec in the set */
	 long long firstinset = rs->ss_starts[*setnum];

         /* calculate chunk */
         long long chunk = firstinset / rs->cursor->chunksize;
	 drms_open_recordchunk(env, rs, kRSChunk_Abs, chunk, &stat);
	 ret = rs->records[firstinset];

	 /* Advance record pointer */
	 rs->cursor->currentrec++;

	 /* Point to the next in the subset */
	 rs->ss_currentrecs[*setnum] = 1;
      }
      else if (rs->ss_currentrecs[*setnum] > endset)
      {
	 /* read past the end of the set */
	 rs->ss_currentrecs[*setnum] = -1;
	 ret = NULL;

	 /* Last record in recordset subset was read last time */
	 drms_close_recordchunk(rs);
	 rs->cursor->currentrec = -1;
      }
      else
      {
	 /* within the recordset subset, get the next one */
	 ret = drms_recordset_fetchnext(env, rs, &stat, &cstat, &newchunk);
	 if (!stat)
	 {
	    (rs->ss_currentrecs[*setnum])++;
	 }
	 else
	 {
	    drms_close_recordchunk(rs);
	    rs->cursor->currentrec = -1;
	    rs->ss_currentrecs[*setnum] = -1;
	 }
      }
   }

   if (status)
   {
      *status = stat;
   }

   if (chunkstat)
   {
      *chunkstat = cstat;
   }

   return ret;
}
void drms_free_cursor(DRMS_RecSetCursor_t **cursor)
{
   int iname;
   char sqlquery[DRMS_MAXQUERYLEN];

   if (cursor)
   {
      if (*cursor)
      {
         if ((*cursor)->names)
         {
            for (iname = 0; iname < (*cursor)->parent->ss_n; iname++)
            {
               if ((*cursor)->names[iname])
               {
                  snprintf(sqlquery, sizeof(sqlquery), "CLOSE %s", (*cursor)->names[iname]);
                  if (drms_dms((*cursor)->env->session, NULL, sqlquery))
                  {
                     fprintf(stderr, "Failed to close cursor '%s'.\n",(*cursor)->names[iname]); 
                  }

                  free((*cursor)->names[iname]);
                  (*cursor)->names[iname] = NULL;
               }
            }

            free((*cursor)->names);
            (*cursor)->names = NULL;
         }

         if ((*cursor)->allvers)
         {
            free((*cursor)->allvers);
            (*cursor)->allvers = NULL;
         }

         if ((*cursor)->suinfo)
         {
            hcon_destroy(&((*cursor)->suinfo));
         }

         free(*cursor);
      }

      *cursor = NULL;
   }
}

int drms_count_records(DRMS_Env_t *env, char *recordsetname, int *status)
{
   int stat, filter, mixed;
   char *query=NULL, *where=NULL, *seriesname=NULL;
   int count = 0;
   DB_Text_Result_t *tres;
   int allvers = 0;

   stat = drms_recordset_query(env, recordsetname, &where, &seriesname, &filter, &mixed, &allvers);
   if (stat)
     goto failure;

   stat = 1;
   query = drms_query_string(env, seriesname, where, filter, mixed, DRMS_QUERY_COUNT, NULL, NULL, allvers);
   if (!query)
     goto failure;

   tres = drms_query_txt(env->session,  query);

   if (tres && tres->num_rows == 1 && tres->num_cols == 1)
     count = atoi(tres->field[0][0]);
   else
     goto failure;

   free(seriesname);
   free(query);
   free(where);
   *status = DRMS_SUCCESS;
   return(count);

 failure:
   if (seriesname) free(seriesname);
   if (query) free(query);
   if (where) free(where);
   *status = stat;
   return(0);
}

/* Columns are stored contiguously in DRMS_Arra_t::data */
DRMS_Array_t *drms_record_getvector(DRMS_Env_t *env, 
                                    const char *recordsetname, 
                                    const char *keylist, 
                                    DRMS_Type_t type, 
                                    int unique,
                                    int *status)
{
   int stat, filter, mixed;
   char *query=NULL, *where=NULL, *seriesname=NULL;
   int count = 0;
   int keys = 0;
   DB_Binary_Result_t *bres=NULL;
   DRMS_Array_t *vectors=NULL;
   int allvers = 0;

   stat = drms_recordset_query(env, recordsetname, &where, &seriesname, &filter, &mixed, &allvers);
   if (stat)
     goto failure;

   query = drms_query_string(env, 
                             seriesname, 
                             where, 
                             filter, 
                             mixed, 
                             DRMS_QUERY_FL, 
                             &unique, 
                             keylist, 
                             allvers);
   if (!query)
     goto failure;

   bres = drms_query_bin(env->session,  query);

   if (bres)
   {
      int col, row;
      int dims[2];
      dims[0] = keys = bres->num_cols;
      dims[1] = count = bres->num_rows;
      vectors = drms_array_create(type, 2, dims, NULL, &stat);
      if (stat) goto failure;
      drms_array2missing(vectors);
      for (col=0; col<keys; col++)
      {
         DB_Type_t db_type = bres->column[col].type;
         for (row=0; row<count; row++)
         {
            int8_t *val = (int8_t *)(vectors->data) + (count * col + row) * drms_sizeof(type);
            char *db_src = bres->column[col].data + row * bres->column[col].size;
            if (!bres->column[col].is_null[row])
              switch(type)
              {
                 case DRMS_TYPE_CHAR:
                   *(char *)val = dbtype2char(db_type,db_src);
                   break;
                 case DRMS_TYPE_SHORT:
                   *(short *)val = dbtype2short(db_type,db_src);
                   break;
                 case DRMS_TYPE_INT:
                   *(int *)val = dbtype2longlong(db_type,db_src);
                   break;
                 case DRMS_TYPE_LONGLONG:
                   *(long long *)val = dbtype2longlong(db_type,db_src);
                   break;
                 case DRMS_TYPE_FLOAT:
                   *(float *)val = dbtype2float(db_type,db_src);
                   break;
                 case DRMS_TYPE_DOUBLE:
                   *(double *)val = dbtype2double(db_type,db_src);
                   break;
                 case DRMS_TYPE_TIME:
                   *(TIME *)val = dbtype2double(db_type,db_src);
                   break;
                 case DRMS_TYPE_STRING:
                   if (db_type ==  DB_STRING || db_type ==  DB_VARCHAR)
                     *(char **)val = strdup((char *)db_src);
                   else
                   {
		      int len = db_binary_default_width(db_type);
                      *(char **)val = (char *)malloc(len);
                      XASSERT(*(char **)val);
		      dbtype2str(db_type, db_src, len, *(char **)val);
                   }
                   break;
                 default:
                   fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
                   XASSERT(0);
                   goto failure;
              } // switch
         } // row 
      } // col
      if (seriesname) free(seriesname);
      if (query) free(query);
      if (where) free(where);
      if (status) *status = DRMS_SUCCESS;
      return(vectors);
   } // bres

 failure:
   if (seriesname) free(seriesname);
   if (query) free(query);
   if (where) free(where);
   if (status) *status = stat;
   return(NULL);
}



int drms_record_isdsds(DRMS_Record_t *rec)
{
   int isdsds = 0;

   DRMS_Segment_t *seg = NULL;
   HIterator_t *hit = hiter_create(&rec->segments);

   if (hit)
   {
      while (seg = (DRMS_Segment_t *)hiter_getnext(hit))
      {
         if (seg->info->protocol == DRMS_DSDS)
         {
            isdsds = 1;
            break;
         }
      }

      hiter_destroy(&hit);
   }

   return isdsds;
}

int drms_record_islocal(DRMS_Record_t *rec)
{
   int islocal = 0;

   DRMS_Segment_t *seg = NULL;
   HIterator_t *hit = hiter_create(&rec->segments);

   if (hit)
   {
      while (seg = (DRMS_Segment_t *)hiter_getnext(hit))
      {
         if (seg->info->protocol == DRMS_LOCAL)
         {
            islocal = 1;
            break;
         }
      }

      hiter_destroy(&hit);
   }

   return islocal;
}

DRMS_Segment_t *drms_record_nextseg(DRMS_Record_t *rec, HIterator_t **last, int followlink)
{
   DRMS_Segment_t *seg = NULL;
   DRMS_Segment_t *segret = NULL;
   HIterator_t *hit = NULL;

   if (last)
   {
      if (*last)
      {
         /* This is not the first time this function was called for the record */
         hit = *last;
      }
      else
      {
         hit = *last = (HIterator_t *)malloc(sizeof(HIterator_t));
         if (hit != NULL)
         {
            hiter_new_sort(hit, &(rec->segments), drms_segment_ranksort);
         }
      }

      seg = hiter_getnext(hit);

      if (seg && followlink)
      {
         /* Because of the way links are handled, must call drms_segment_lookup() to 
          * follow links */
         segret = drms_segment_lookup(rec, seg->info->name);
      }
      else
      {
         segret = seg;
      }
   }

   return segret;
}

/* Return keywords in rank order. */
DRMS_Keyword_t *drms_record_nextkey(DRMS_Record_t *rec, HIterator_t **last, int followlink)
{
   DRMS_Keyword_t *key = NULL;
   DRMS_Keyword_t *keyret = NULL;
   HIterator_t *hit = NULL;

   if (last)
   {
      if (*last)
      {
         /* This is not the first time this function was called for the record */
         hit = *last;
      }
      else
      {
         hit = *last = (HIterator_t *)malloc(sizeof(HIterator_t));
         if (hit != NULL)
         {
            hiter_new_sort(hit, &(rec->keywords), drms_keyword_ranksort);
         }
      }

      key = hiter_getnext(hit);

      if (key)
      {
         /* Because of the way links are handled, must call drms_keyword_lookup() to 
          * follow links */
         keyret = drms_keyword_lookup(rec, key->info->name, followlink);
      }
   }

   return keyret;
}

/* Return keywords in rank order. */
DRMS_Link_t *drms_record_nextlink(DRMS_Record_t *rec, HIterator_t **last)
{
   DRMS_Link_t *lnk = NULL;
   HIterator_t *hit = NULL;

   if (last)
   {
      if (*last)
      {
         /* This is not the first time this function was called for the record */
         hit = *last;
      }
      else
      {
         hit = *last = (HIterator_t *)malloc(sizeof(HIterator_t));
         if (hit != NULL)
         {
            hiter_new_sort(hit, &(rec->links), drms_link_ranksort);
         }
      }

      lnk = hiter_getnext(hit);
   }

   return lnk;
}

int drms_record_parserecsetspec(const char *recsetsStr, 
                                char **allvers, 
                                char ***sets, 
                                DRMS_RecordSetType_t **types, 
                                char ***snames,
                                int *nsets,
                                DRMS_RecQueryInfo_t *info)
{
   return ParseRecSetDesc(recsetsStr, allvers, sets, types, snames, nsets, info);
}

int drms_record_freerecsetspecarr(char **allvers, char ***sets, DRMS_RecordSetType_t **types,  char ***snames, int nsets)
{
   return FreeRecSetDescArr(allvers, sets, types, snames, nsets);
}


