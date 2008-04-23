// #define DEBUG
#include <dirent.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <strings.h>
#include "drms.h"
#include "drms_priv.h"
#include "xmem.h"
#include "drms_dsdsapi.h"

#ifndef DEBUG
#undef TIME
#define TIME(code) code
#endif

#define kMAXRSETS 128
#define kMAXRSETSPEC (DRMS_MAXSERIESNAMELEN + DRMS_MAXQUERYLEN + 128)

static void *ghDSDS = NULL;
static int gAttemptedDSDS = 0;

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
   /* Parsing a DRMS segment-list specifier (a '{' seen). */
   kRSParseState_DRMSSeglist,
   /* Parsing 'prog' (DSDS) specification */
   kRSParseState_DSDS,
   /* Parsing 'vot' (VOT) specification */
   kRSParseState_VOT,
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

static int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopySegments(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyLinks(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyKeywords(DRMS_Record_t *target, DRMS_Record_t *source);
static int CopyPrimaryIndex(DRMS_Record_t *target, DRMS_Record_t *source);
static int ParseRecSetDesc(const char *recsetsStr, 
			   char ***sets, 
			   DRMS_RecordSetType_t **types, 
			   int *nsets);
static int FreeRecSetDescArr(char ***sets, DRMS_RecordSetType_t **types, int nsets);

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
					DRMS_KeyMapClass_t fitsclass,
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
						      DRMS_KeyMapClass_t fitsclass,
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
   XASSERT(tKey = 
	   hcon_allocslot_lower(&(template->keywords), drmsKeyName));
   memset(tKey, 0, sizeof(DRMS_Keyword_t));
   XASSERT(tKey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
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
					DRMS_KeyMapClass_t fitsclass,
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
      XASSERT(template = calloc(1, sizeof(DRMS_Record_t)));
      XASSERT(template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));
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
	       if (!drms_keyword_getintname_ext(sKey->info->name, 
						&fitsclass,
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
		  XASSERT(tKey = 
			  hcon_allocslot_lower(&(template->keywords), drmsKeyName));
		  memset(tKey, 0, sizeof(DRMS_Keyword_t));
		  XASSERT(tKey->info = malloc(sizeof(DRMS_KeywordInfo_t)));
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
	       /* one segment per record */
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
   DRMS_Segment_t *tSeg = NULL;

   XASSERT(tSeg = hcon_allocslot_lower(&(template->segments), seg->info->name));
   memset(tSeg, 0, sizeof(DRMS_Segment_t));
   XASSERT(tSeg->info = malloc(sizeof(DRMS_SegmentInfo_t)));
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
	 pkey->info->isdrmsprime = 1;
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

   if (hcon_member_lower(&(env->series_cache), seriesName))
   {
      fprintf(stderr,"drms_open_dsdsrecords(): "
	      "ERROR: Series '%s' already exists.\n", seriesName);
      drms_free_template_record_struct(proto);
      stat = DRMS_ERROR_INVALIDDATA;
   }
   else
   {
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
						      DRMS_KeyMapClass_t fitsclass,
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
	    if (!drms_keyword_getintname_ext(sKey->info->name, 
					     &fitsclass,
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
	    if ((tSeg = 
		 hcon_lookup_lower(&(rset->records[iRec]->segments), kLocalSegName)) != NULL)
	    {
	       snprintf(tSeg->filename, DRMS_MAXSEGFILENAME, "%s", sSeg->filename);
	    }
	    else
	    {
	       stat = DRMS_ERROR_INVALIDDATA;
	       break;
	    }
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


	 DRMS_KeyMapClass_t fitsclass = kKEYMAPCLASS_LOCAL;
	 char drmsKeyName[DRMS_MAXKEYNAMELEN];
	 char *pkeyarr[DRMS_MAXPRIMIDX] = {0};
	 int setPrimeKey = 0;

	 if (pkeys && *pkeys)
	 {
	    char *pkeyname = strtok(*pkeys, ",");
	    nPkeys = 0;
	    while(pkeyname != NULL)
	    {
	       if (!drms_keyword_getintname_ext(pkeyname, 
						&fitsclass,
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
	    /* xxxx Get series name - this should be "su_tmp.<GUID>.  The GUID should
	     * be stored in libdrmsserver.a.  It should be a monotonically
	     * increasing integer that lives for the life of libdrmsserver.a. 
	     *
	     * I believe this is the way to achieve this:
	     * send_string
	     */

	    long long guid = -1;

#ifdef DRMS_CLIENT
	       drms_send_commandcode(env->session->sockfd, DRMS_GETTMPGUID);
	       guid = Readlonglong(env->session->sockfd);
#else
	       /* has direct access to drms_server.c. */
	       guid = drms_server_gettmpguid(NULL);
#endif /* DRMS_CLIENT */

	       snprintf(seriesName, sizeof(seriesName), "su_tmp.%lld", guid);

	    /* Adjust seriesinfo */
	    AdjustRecordProtoSeriesInfo(env, proto, seriesName, 32);
	      
	    /* alloc segments */
	    AllocRecordProtoSeg(proto, seg, &stat);
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
	       AllocRecordProtoSeg(template, seg, &stat);
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

/* recordsetname is a comma-separated list of recordsets.  
 * Must surround DSDS queries with '{' and '}'. 
 */
DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, 
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

  /* conflict with stat var in this scope */
  int (*filestat)(const char *, struct stat *buf) = stat;

  /* recordsetname is a list of comma-separated record sets
   * commas may appear within record sets, so need to use a parsing 
   * mechanism more sophisticated than strtok() */
  char **sets = NULL;
  DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
  DRMS_Record_t **setstarts = NULL;
  
  int nsets = 0;
  int stat = ParseRecSetDesc(recordsetname, &sets, &settypes, &nsets);

  if (stat == DRMS_SUCCESS)
  {
     int iSet;

     CHECKNULL_STAT(env,status);
     setstarts = (DRMS_Record_t **)malloc(sizeof(DRMS_Record_t *) * nsets);

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
		 rs = drms_open_dsdsrecords(env, oneSet, &stat);
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
	      rs = drms_open_dsdsrecords(env, oneSet, &stat);
	      if (stat)
		goto failure; 
	   } /* DSDS */
	   else if (settypes[iSet] == kRecordSetType_VOT)
	   {
	      /* TBD */
	      fprintf(stderr, "VOT record-set specification not implemented.\n");
	   } /* VOT */
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
					       &filter, &mixed));
	      }
	      else
		goto failure;

	      if (stat)
		goto failure;

#ifdef DEBUG  
	      printf("seriesname = %s\n",seriesname);
	      printf("query = %s\n",query);
#endif

	      if (seglist)
	      {
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
		    hcon_insert(goodsegcont, ans, ans);
		 }
		 while ((ans = strtok_r(NULL, " ,;:{}", &lasts)) != NULL);

		 free(seglist);
	      }

	      TIME(rs = drms_retrieve_records(env, 
					      seriesname, 
					      query, 
					      filter, 
					      mixed, 
					      goodsegcont, 
					      &stat));

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
		 rs->records[i]->lifetime = DRMS_PERMANENT; 
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
	}
     }

     if (ret)
     {
	if (nsets > 1)
	{
	   if (realSets && realSets->num_total > 0)
	   {
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
			     setstarts[iSet] = oners->records[0];
			  }
			  else
			  {
			     setstarts[iSet] = NULL;
			  }

			  /* Move oners to return RecordSet */
			  for (i = 0; i < oners->n; i++)
			  {
			     ret->records[j] = oners->records[i];
			     j++;
			  }
		       }

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
	else
	{
	   /* One set only - save the number of records in a set into setstarts */
	   if (ret->n > 0)
	   {
	      setstarts[0] = ret->records[0];
	   }
	   else
	   {
	      /* All record queries are saved, even ones that produced
	       * no records (no records matching query criteria).
	       * If a query produced no records, then set the pointer
	       * to the first record to NULL.
	       */
	      setstarts[0] = NULL;
	   }
	}

	/* Add fields that are used to track record-set sources */
	ret->ss_n = nsets;
	ret->ss_starts = setstarts; /* ret assumes ownership */
	setstarts = NULL;
	/* ret can't assume ownership of sets or settypes */
	ret->ss_queries = (char **)malloc(sizeof(char *) * nsets);
	ret->ss_types = (DRMS_RecordSetType_t *)malloc(sizeof(DRMS_RecordSetType_t) * nsets);
	if (ret->ss_queries && ret->ss_types)
	{
	   for (iSet = 0; iSet < nsets; iSet++)
	   {
	      ret->ss_queries[iSet] = strdup(sets[iSet]);
	      ret->ss_types[iSet] = settypes[iSet];
	   }
	}
	else
	{
	   stat = DRMS_ERROR_OUTOFMEMORY;
	   goto failure;
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

     FreeRecSetDescArr(&sets, &settypes, nsets);

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

  FreeRecSetDescArr(&sets, &settypes, nsets);

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

  if (status)
    *status = stat;
  return NULL;
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

  CHECKNULL_STAT(env,status);
  CHECKNULL_STAT(template,status);

  if (n<1)
  {
    if (status)
      *status = DRMS_ERROR_BADRECORDCOUNT;
    return NULL;
  }

  /* Allocate the outer record set structure. */
  XASSERT( rs = malloc(sizeof(DRMS_RecordSet_t)) );
  XASSERT( rs->records = malloc(n*sizeof(DRMS_Record_t *)) );
  memset(rs->records, 0, sizeof(DRMS_Record_t *) * n);
  rs->n = n;
  rs->ss_n = 0;
  rs->ss_queries = NULL;
  rs->ss_types = NULL;
  rs->ss_starts = NULL;

  series = template->seriesinfo->seriesname;

  /* Get unique sequence numbers from the database server. */
  if ((recnum = drms_alloc_recnum(env->session, series, lifetime, n)) == NULL)
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
  if ( drms_record_numsegments(rs->records[0]) )
  {
    XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
    XASSERT(slotnum = malloc(n*sizeof(int)));
    
    if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, su)))
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
	    drms_tasfile_create(filename, DRMS_COMP_RICE, seg->info->type, 
				seg->info->naxis+1, seg->axis, seg->blocksize,
				NULL);
	    seg->axis[seg->info->naxis] = 0;
	    seg->blocksize[seg->info->naxis] = 0; 
	  }
	}
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

   XASSERT(detached = malloc(sizeof(DRMS_RecordSet_t)) );
   XASSERT(detached->records = malloc(nRecs * sizeof(DRMS_Record_t *)));

   if (detached && detached->records)
   {
      *status = DRMS_SUCCESS;
      detached->n = nRecs;
      detached->ss_n = 0;
      detached->ss_queries = NULL;
      detached->ss_types = NULL;
      detached->ss_starts = NULL;
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

   XASSERT(recTarget = calloc(1, sizeof(DRMS_Record_t)));
   XASSERT(recTarget->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));

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
DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *rs_in, 
				     DRMS_RecLifetime_t lifetime,  
				     DRMS_CloneAction_t mode, int *status)
{
  int i, stat, first, last, n, n_total;
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
  XASSERT( rs_out = malloc(sizeof(DRMS_RecordSet_t)) );
  XASSERT( rs_out->records = malloc(n_total*sizeof(DRMS_Record_t *)) );
  rs_out->n = n_total;
  rs_out->ss_n = 0;
  rs_out->ss_queries = NULL;
  rs_out->ss_types = NULL;
  rs_out->ss_starts = NULL;
  
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
    if ((recnum = drms_alloc_recnum(env->session, series, lifetime, n)) == NULL)
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
      if (rs_out->records[first+i] == NULL)
	goto failure;    
      
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
	for (i=0; i<n; i++) 
	{   
	   drms_record_directory(rs_in->records[first+i], dir_in, 1);
	   if ( rs_in->records[first+i]->su )
	   {
	      rs_out->records[first+i]->su = rs_in->records[first+i]->su;
	      rs_out->records[first+i]->su->refcount++;
	   }
	}
	break;
      case DRMS_COPY_SEGMENTS:
	XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
	XASSERT(slotnum = malloc(n*sizeof(int)));
      
	/* Allocate new SU slots for copies of data segments. */
	if ((stat = drms_newslots(env, n, series, recnum, lifetime, slotnum, 
				  su)))
	  goto failure;    
      
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
	  drms_record_directory(rec_in, dir_in, 1);
	  drms_record_directory(rec_out, dir_out, 1);
	  DIR *dp;
	  struct dirent *dirp;
	  if ((dp = opendir(dir_in)) == NULL) {
	    fprintf(stderr, "Can't open %s\n", dir_in);
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
		goto failure;
	      }
	    break;
	  }
	  closedir(dp);
	  /* Loop over segments and copy any TAS segments one by one. */
	  hiter_new(&hit_in, &rec_in->segments);
	  hiter_new(&hit_out, &rec_out->segments);
	  while ((seg_in = (DRMS_Segment_t *)hiter_getnext(&hit_in)) && 
		 (seg_out = (DRMS_Segment_t *)hiter_getnext(&hit_out)))
	  {
	    if (seg_out->info->protocol == DRMS_TAS)
	    {
	      if (slotnum[i]==0)
	      {
		/* This is the first record in a new storage unit. 
		   Create an empty TAS file. */
		drms_segment_filename(seg_out, filename);
		seg_out->axis[seg_out->info->naxis] = rec_out->seriesinfo->unitsize;
		seg_out->blocksize[seg_out->info->naxis] = 1; 
		drms_tasfile_create(filename, DRMS_COMP_RICE, seg_out->info->type, 
				    seg_out->info->naxis+1, seg_out->axis, 
				    seg_out->blocksize, NULL);
		seg_out->axis[seg_out->info->naxis] = 0;
		seg_out->blocksize[seg_out->info->naxis] = 0; 
	      }
	      if ((arr = drms_segment_read(seg_in,DRMS_TYPE_RAW,&stat))==NULL)
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to read "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_in->info->name, rec_in->seriesinfo->seriesname, 
			rec_in->recnum);
		goto failure;
	      }
	      if (drms_segment_write(seg_out, arr, 0))
	      {
		fprintf(stderr,"ERROR at %s, line %d: failed to write "
			"segment '%s' of record %s:#%lld\n",__FILE__,__LINE__,
			seg_out->info->name, rec_out->seriesinfo->seriesname, 
			rec_out->recnum);
		goto failure;
	      }
	      drms_free_array(arr);
	    }
	  }
	}
	free(su);
	free(slotnum);
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
  drms_free_records(rs);
  return status;
}

int drms_stage_records(DRMS_RecordSet_t *rs, int retrieve, int dontwait) {

  if (!rs) {
    return DRMS_SUCCESS;
  }

  int status = 0;
  DRMS_Record_t *record0 = rs->records[0];
  char *series = record0->seriesinfo->seriesname;
  DRMS_Env_t *env = record0->env;

  if (rs->n > 1) {
    // check all records come from the same series
    for (int i = 1; i < rs->n; i++) {
      if (strcmp(series, rs->records[i]->seriesinfo->seriesname)) {
	fprintf(stderr, "Records do not come from the same series: %s %s\n", series, rs->records[i]->seriesinfo->seriesname);
	return 1;
      }
    }
  }

  long long *sunum;
  XASSERT(sunum = malloc(rs->n*sizeof(long long)));
  int cnt = 0;
  for (int i = 0; i < rs->n; i++) {
    DRMS_Record_t *rec = rs->records[i];
    if (rec->sunum != -1LL &&
	rec->su == NULL) {
      sunum[cnt] = rec->sunum;
      cnt++;
    }
  }

  if (cnt) {
    status = drms_getunits(env, series, cnt, sunum, retrieve, dontwait);
    if (!status) {
      if (!dontwait) {
	// matching to each record is done by lookup the SU cache
	for (int i = 0; i < rs->n; i++) {
	  DRMS_Record_t *rec = rs->records[i];
	  HContainer_t *scon;
	  rec->su = drms_su_lookup(env, series, rec->sunum, &scon);
	}
      }
    }
  }

  free(sunum);
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
  rs->ss_n = 0;
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
     hcon_remove(&rec->env->record_cache, hashkey); 
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

#ifdef DEBUG
    printf("Allocated the following template for dataset (%s,%lld):\n",
	   seriesname, recnum);
    drms_print_record(rec);
#endif

    /* Fill result into dataset structure. */
    if ((stat = drms_populate_record(rec, recnum)))
    {
      if (status)
	*status = stat;      
      goto bailout; /* Query result was inconsistent with series template. */
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
DRMS_RecordSet_t *drms_retrieve_records(DRMS_Env_t *env, 
					const char *seriesname, 
					char *where, int filter, int mixed,
					HContainer_t *goodsegcont,
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

  char *query = drms_query_string(env, seriesname, where, filter, mixed, DRMS_QUERY_ALL, NULL);
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
  XASSERT(rs = malloc(sizeof(DRMS_RecordSet_t)));
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
    XASSERT(rs->records=malloc(rs->n*sizeof(DRMS_Record_t*)));
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

	/* Remove unrequested segments */
	if (goodsegcont)
	{
	   hit = hiter_create(&(rs->records[i]->segments));
	   if (hit)
	   {
	      while (hiter_extgetnext(hit, &hkey) != NULL)
	      {
		 if (!hcon_lookup(goodsegcont, hkey))
		 {
		    hcon_remove(&(rs->records[i]->segments), hkey);
		 }
	      }
	   
	      hiter_destroy(&hit);
	   }
	}

	/* Set pidx in links */
	drms_link_getpidx(rs->records[i]);
	/* Set new unique record number. */
	rs->records[i]->recnum = recnum;
      } else {
	// the old record is going to be overridden
	free(rs->records[i]->sessionns);
      }
      rs->records[i]->readonly = 1;
    }


    /* Populate dataset structures with data from query result. */
#ifdef DEBUG
    printf("Time to allocate record structures = %f\n",PopTimer());
    printf("\nMemory used before populate= %Zu\n\n",xmem_recenthighwater());
#endif
    TIME(if ((stat = drms_populate_records(rs, qres)))
    {
      goto bailout; /* Query result was inconsistent with series template. */
    } );
#ifdef DEBUG
    printf("\nMemory used after populate= %Zu\n\n",xmem_recenthighwater());
#endif
  }

  /* Initialize subset information */
  rs->ss_n = 0;
  rs->ss_queries = NULL;
  rs->ss_types = NULL;
  rs->ss_starts = NULL;

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

char *drms_query_string(DRMS_Env_t *env, 
			const char *seriesname,
			char *where, int filter, int mixed,
			DRMS_QueryType_t qtype, char *fl) {
  DRMS_Record_t *template;
  char *field_list, *query=0;
  char *series_lower;
  long long recsize, limit;
  char pidx_names[1024]; // comma separated pidx keyword names
  char *p;

  int status = 0;

  CHECKNULL_STAT(env,&status);

  if ((template = drms_template_record(env,seriesname,&status)) == NULL)
    return NULL;
  drms_link_getpidx(template); /* Make sure links have pidx's set. */

  switch (qtype) {
  case DRMS_QUERY_COUNT:
    field_list = strdup("count(*)");
    break;
  case DRMS_QUERY_FL:
    field_list = strdup(fl);
    recsize = drms_keylist_memsize(template, fl);
    if (!recsize) {
      goto bailout;
    }
    break;
  case DRMS_QUERY_ALL:
    field_list = drms_field_list(template, NULL);
    recsize = drms_record_memsize(template);
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
    p = pidx_names;
    p += sprintf(p, "%s.%s", series_lower, template->seriesinfo->pidx_keywords[0]->info->name);
    for (int i = 1; i < template->seriesinfo->pidx_num; i++) {
      p += sprintf(p, ", %s.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name);
    }
  }

  /* Do query to retrieve record meta-data. */
  XASSERT( (query = malloc(strlen(field_list)+DRMS_MAXQUERYLEN)) );
  p = query;

  if (filter && template->seriesinfo->pidx_num>0) { // query on the lastest version
    if (mixed) {
      // query on both prime and non-prime keys
      p += sprintf(p, "select %s from %s, (select q2.max1 as max from  (select max(recnum) as max1, min(q1.max) as max2 from %s, (select %s, max(recnum) from %s where %s group by %s) as q1 where %s.%s = q1.%s", field_list, series_lower, series_lower, pidx_names, series_lower, where, pidx_names, series_lower, template->seriesinfo->pidx_keywords[0]->info->name, template->seriesinfo->pidx_keywords[0]->info->name);
      for (int i = 1; i < template->seriesinfo->pidx_num; i++) {
	p += sprintf(p, " and %s.%s = q1.%s", series_lower, template->seriesinfo->pidx_keywords[i]->info->name, template->seriesinfo->pidx_keywords[i]->info->name);
      }
      p += sprintf(p, " group by %s) as q2 where max1 = max2) as q3 where %s.recnum = q3.max", pidx_names, series_lower);
    } else {
      // query only on prime keys
      p += sprintf(p, "select %s from %s where recnum in (select max(recnum) from %s where 1=1 ", field_list, series_lower, series_lower);
      if (where && *where) {
	p += sprintf(p, " and %s", where);
      }
      p += sprintf(p, " group by %s )", pidx_names);
    }
  } else { // query on all records including all versions
    p += sprintf(p, "select %s from %s where 1 = 1", field_list, series_lower);
    if (where && *where) {
      p += sprintf(p, " and %s", where);
    }
  }
  if (qtype != DRMS_QUERY_COUNT) {      
    if (template->seriesinfo->pidx_num > 0) {
      p += sprintf(p, " order by %s", pidx_names);
    }
    p += sprintf(p, " limit %lld", limit);
  }

  free(series_lower);
 bailout:
  free(field_list);
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
DRMS_Record_t *drms_template_record(DRMS_Env_t *env, const char *seriesname, 
				    int *status)
{
  int stat;
  DB_Binary_Result_t *qres;
  DRMS_Record_t *template;
  char *p, *q, query[DRMS_MAXQUERYLEN], buf[DRMS_MAXPRIMIDX*DRMS_MAXKEYNAMELEN];
  DRMS_Keyword_t *kw;
  XASSERT(env);
  XASSERT(seriesname);

#ifdef DEBUG
    printf("Getting template for series '%s'\n",seriesname);
#endif
  if ( (template = hcon_lookup_lower(&env->series_cache, seriesname)) == NULL )
  {
    if (status) {
      *status = DRMS_ERROR_UNKNOWNSERIES;
    }
    return NULL;
  }

  if (template->init == 0)
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
    XASSERT(template->seriesinfo = calloc(1, sizeof(DRMS_SeriesInfo_t)));
    /* Populate series info part */
    char *namespace = ns(seriesname);
    sprintf(query, "select seriesname, description, author, owner, "
	    "unitsize, archive, retention, tapegroup, primary_idx, dbidx, version "
	    "from %s.%s where seriesname ~~* '%s'", 
	    namespace, DRMS_MASTER_SERIES_TABLE, seriesname);
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
    template->seriesinfo->tapegroup = db_binary_field_getint(qres, 0, 7);

    /* Populate series info segments, keywords, and links part */
    if ((stat=drms_template_segments(template)))
      goto bailout;
    if ((stat=drms_template_links(template)))
      goto bailout;
    if ((stat=drms_template_keywords(template)))
      goto bailout;

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
	kw->info->isdrmsprime = 1;
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

    if ( !db_binary_field_is_null(qres, 0, 10) ) {
       db_binary_field_getstr(qres, 0, 10, DRMS_MAXSERIESVERSION, 
			      template->seriesinfo->version);
    }

    db_free_binary_result(qres);   
  }
  if (status)
    *status = DRMS_SUCCESS;
  return template;

 bailout:
  if (status)
    *status = stat;
  return NULL;
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
  hiter_new(&hit, &dst->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
    key->record = dst;
  hiter_new(&hit, &dst->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
    seg->record = dst;

  /* FIXME: Should we copy su struct here? */
}


/* Populate the keyword, link, and data descriptors in the
   record structure with the result from a database query. */ 
int drms_populate_record(DRMS_Record_t *rec, long long recnum)
{
  int stat;
  char *query, *field_list, *seriesname;
  DRMS_Env_t *env;
  DB_Binary_Result_t *qres;
  DRMS_RecordSet_t rs;
  char *series_lower;

  CHECKNULL(rec);
  env = rec->env;
  seriesname = rec->seriesinfo->seriesname;
  field_list = drms_field_list(rec, NULL);
  series_lower = strdup(seriesname);
  strtolower(series_lower);

  /* Do query. */
  XASSERT( (query = malloc(strlen(field_list)+10*DRMS_MAXSERIESNAMELEN)) );
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
  XASSERT(rs.records=malloc(sizeof(DRMS_Record_t*)));
  rs.n = 1;
  rs.records[0] = rec;
  rs.ss_n = 0;
  rs.ss_queries = NULL;
  rs.ss_types = NULL;
  rs.ss_starts = NULL;

  stat = drms_populate_records(&rs,qres);
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
int drms_populate_records (DRMS_RecordSet_t *rs, DB_Binary_Result_t *qres) {
  int row, col, i;
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Segment_t *seg;
  DRMS_Record_t *rec;
  //  DRMS_Env_t *env;
  DB_Type_t column_type;
  int segnum;
  char *record_value;

  CHECKNULL(rs);
  CHECKNULL(qres);

  if (rs->n != qres->num_rows)
    return DRMS_ERROR_BADQUERYRESULT;

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
      hiter_new(&hit, &rec->links);
      while ((link = (DRMS_Link_t *)hiter_getnext (&hit))) {
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
    }

    /* Populate keywords. */
    if (hcon_size(&rec->keywords) > 0) {
      hiter_new(&hit, &rec->keywords);
      while ((key = (DRMS_Keyword_t *)hiter_getnext(&hit)) ) {
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
    }

    /* Segment fields. */
    if (hcon_size(&rec->segments) > 0)
    {
       /* This function has parts that are conditional on the series version being 
	* greater than or equal to version 2.0. */
       DRMS_SeriesVersion_t vers = {"2.0", ""};

      hiter_new(&hit, &rec->segments); /* Iterator for segment container. */
      segnum = 0;
      while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
      {
	seg->record = rec; /* parent link. */
	/* Segment file name stored as column "sg_XXX_file */
	db_binary_field_getstr(qres, row, col++, DRMS_MAXSEGFILENAME, seg->filename);
	if (seg->info->scope==DRMS_VARDIM)
	{
	  /* segment dim names are stored as columns "sg_XXX_axisXXX" */	
	  for (i=0; i<seg->info->naxis; i++)
	    seg->axis[i] = db_binary_field_getint(qres, row, col++);
	}
	if (drms_series_isvers(rec->seriesinfo, &vers))
	{
	   /* compression parameters are stored as columns cparms_XXX */
	   db_binary_field_getstr(qres, row, col++, DRMS_MAXCPARMS, seg->cparms);
	}
	seg->info->segnum = segnum++;
      }
    }
  }
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
  HIterator_t hit;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  int ncol=0, segnum;
  DRMS_Segment_t *seg;

  /* This function has parts that are conditional on the series version being 
   * greater than or equal to version 2.0. */
  DRMS_SeriesVersion_t vers = {"2.0", ""};

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
  hiter_new(&hit, &rec->links); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
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
  /* Keyword fields. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      len += strlen(key->info->name)+5;
      ++ncol;
    }
  }
  /* Segment fields. */
  hiter_new(&hit, &rec->segments); /* Iterator for segment container. */
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
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
    if (drms_series_isvers(rec->seriesinfo, &vers))
    {
       /* compression parameters are stored as columns cparms_XXX */
       len += 12; /* It looks like you just add 2 to the strlen. */
       ++ncol;
    }
  }
  /* Malloc string buffer. */
  XASSERT( buf = malloc(len+1) );
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
  hiter_new(&hit, &rec->links); /* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
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
  /* Keyword fields. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
      p += sprintf(p,", %s",key->info->name);
  }
  /* Segment fields. */
  hiter_new(&hit, &rec->segments); /* Iterator for segment container. */

  segnum = 0;
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    p += sprintf(p, ", sg_%03d_file", segnum);
    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)	
	p += sprintf(p,", sg_%03d_axis%03d",segnum,i);
    }

    /* cparms - version 2.0 of the master series table */
    if (drms_series_isvers(rec->seriesinfo, &vers))
    {
       /* compression parameters are stored as columns cparms_XXX */
       p += sprintf(p, ", cparms_%03d", segnum);
    } 

    segnum++;
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
  HIterator_t hit;
  DRMS_Keyword_t *key;
  DRMS_Link_t *link;
  DRMS_Record_t *rec;
  DRMS_Segment_t *seg;
  DB_Type_t *intype;
  char **argin;
  int status;
  int *sz;

  /* This function has parts that are conditional on the series version being 
   * greater than or equal to version 2.0. */
  DRMS_SeriesVersion_t vers = {"2.0", ""};

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
  XASSERT( (query = malloc(strlen(field_list)+10*DRMS_MAXSERIESNAMELEN)) );
  p = query;
  p += sprintf(p, "%s (%s)",  series_lower, field_list);
#ifdef DEBUG
  printf("Table and column specifier for bulk insert = '%s'\n",query);
#endif

  XASSERT(argin = malloc(num_args*sizeof(void *)));
  XASSERT(sz = malloc(num_args*sizeof(int)));
  XASSERT(intype = malloc(num_args*sizeof(DB_Type_t)));
    
  /* Get type and size information for record attributes. */
  col = 0;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* recnum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sunum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_INT);  /* slotnum */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_LONGLONG); /* sessionid */
  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
  col++;
  intype[col] = drms2dbtype(DRMS_TYPE_STRING);   /* sessionns */
  XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));

  /* Loop through Links. */
  hiter_new(&hit, &rec->links);/* Iterator for link container. */
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    if (link->info->type == STATIC_LINK)
    {
      intype[col]  = drms2dbtype(DRMS_TYPE_LONGLONG);
      XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
      col++;
    }
    else /* Oh crap! A dynamic link... */
    {
      if (link->info->pidx_num) {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
      /* There is a field for each keyword in the primary index
	 of the target series...walk through them. */
      for (i=0; i<link->info->pidx_num; i++)
      {
	intype[col] = drms2dbtype(link->info->pidx_type[i]);
	if (link->info->pidx_type[i] == DRMS_TYPE_STRING )
	{
	  XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));
	}
	else
	{
	  XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	  col++;
	}
      }
    }
  }

  /* Loop through Keywords. */
  hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    if (!key->info->islink && !drms_keyword_isconstant(key))
    {
      intype[col] = drms2dbtype(key->info->type);
      if ( key->info->type == DRMS_TYPE_STRING )
      {
	XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));
      }
      else
      {
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
    }    
  }

  /* Loop through Segment fields. */
  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
  {
    /* segment names are stored as columns "sg_XXX_file */	
    intype[col] = drms2dbtype(DRMS_TYPE_STRING);
    XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));

    if (seg->info->scope==DRMS_VARDIM)
    {
      /* segment dim names are stored as columns "sgXXX_axisXXX" */	
      /* segment dim values are stored in columns "sgXXX_axisXXX" */	
      for (i=0; i<seg->info->naxis; i++)
      {
	intype[col] = drms2dbtype(DRMS_TYPE_INT);
	XASSERT(argin[col] = malloc(num_rows*db_sizeof(intype[col])));
	col++;
      }
    }

    if (drms_series_isvers(rec->seriesinfo, &vers))
    {
       /* compression parameters are stored as columns cparms_XXX */
       intype[col] = drms2dbtype(DRMS_TYPE_STRING);
       XASSERT(argin[col++] = malloc(num_rows*sizeof(char *)));
    }
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
    hiter_new(&hit, &rec->links);/* Iterator for link container. */
    while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
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

    /* Loop through Keywords. */
    hiter_new(&hit, &rec->keywords); /* Iterator for keyword container. */
    while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
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

    /* Loop through Segment fields. */
    hiter_new(&hit, &rec->segments);
    while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
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

  hiter_new(&hit, &rec->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
  {
    fprintf(keyfile, "%-*s '%s':\n",13,"Keyword",key->info->name);
    drms_keyword_fprint(keyfile, key);
  }

  hiter_new(&hit, &rec->links); 
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
  {
    fprintf(keyfile, "%-*s '%s':\n",13,"Link",link->info->name);
    drms_link_fprint(keyfile, link);
  }

  hiter_new(&hit, &rec->segments);
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
  }
  return count;
}

/* Return the path to the Storage unit slot directory associateed with
   a record. If no storage unit slot has been assigned to the record yet,
   an empty string is returned. */
void drms_record_directory (DRMS_Record_t *rec, char *dirname, int retrieve) {
  int stat;
  if (drms_record_numsegments (rec) <= 0) {
#ifdef DEBUG
    fprintf (stderr, "ERROR: Calling drms_record_directory is valid only for "
	"records containing data segments.");
#endif
    dirname[0] = 0;
    return;
  }

  if (rec->sunum != -1LL && rec->su == NULL) {
#ifdef DEBUG
    printf ("Getting SU for record %s:#%lld, sunum=%lld\n",
	   rec->seriesinfo->seriesname,rec->recnum, rec->sunum);
#endif
    if ((rec->su = drms_getunit (rec->env, rec->seriesinfo->seriesname,
	rec->sunum, retrieve, &stat)) == NULL) {
      if (stat) 
	fprintf (stderr, "ERROR in drms_record_directory: Cannot retrieve "
	    "storage unit. stat = %d\n", stat);
      dirname[0] = '\0';
      return;
    }
    rec->su->refcount++;
#ifdef DEBUG    
    printf("Retrieved unit sunum=%lld, sudir=%s\n",
	   rec->su->sunum, rec->su->sudir);
#endif
  }
  CHECKSNPRINTF(snprintf(dirname, DRMS_MAXPATHLEN, "%s/" DRMS_SLOTDIR_FORMAT, 
			 rec->su->sudir, rec->slotnum), DRMS_MAXPATHLEN);
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
      if ((stat = drms_newslots(rec->env, 1, rec->seriesinfo->seriesname, 
				&rec->recnum, rec->lifetime, &rec->slotnum, 
				&rec->su)))
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
  int len = 0;
  while (*p != '\0') {
    char *start = p;
    int len = 0;
    while (*p != ',' && *p != '\0') {
      len++;
      p++;
    }
    char *key = strndup(start, len);
    DRMS_Keyword_t *keyword = drms_keyword_lookup(rec, key, 0);
    free(key);
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
    // skip the comma
    if (*p != '\0') {
      p++;
    }
  }

 bailout:
  free(list);
  return size;
}

int CopySeriesInfo(DRMS_Record_t *target, DRMS_Record_t *source)
{
   memcpy(target->seriesinfo, source->seriesinfo, sizeof(DRMS_SeriesInfo_t));
   memset(target->seriesinfo->pidx_keywords, 0, sizeof(DRMS_Keyword_t *) * DRMS_MAXPRIMIDX);

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

/* Caller owns sets. */
static int ParseRecSetDesc(const char *recsetsStr, 
			   char ***sets, 
			   DRMS_RecordSetType_t **settypes, 
			   int *nsets)
{
   int status = DRMS_SUCCESS;
   RSParseState_t state = kRSParseState_Begin;
   char *rsstr = strdup(recsetsStr);
   char *pc = rsstr;
   char **intSets = NULL;
   DRMS_RecordSetType_t *intSettypes = NULL;
   int count = 0;
   char buf[kMAXRSETSPEC];
   char *pcBuf = buf;
   char **multiRSQueries = NULL;
   DRMS_RecordSetType_t *multiRSTypes = NULL;
   DRMS_RecordSetType_t currSettype;
   int countMultiRS = 0;
   char *endInput = rsstr + strlen(rsstr); /* points to null terminator */

   *nsets = 0;

   if (rsstr)
   {
      while (pc && pc <= endInput && state != kRSParseState_Error)
      {
	 switch (state)
	 {
	    case kRSParseState_Begin:
	      intSets = (char **)malloc(sizeof(char *) * kMAXRSETS);
	      intSettypes = 
		(DRMS_RecordSetType_t *)malloc(sizeof(DRMS_RecordSetType_t) * kMAXRSETS);
	      if (!intSets || !intSettypes)
	      {
		 state = kRSParseState_Error;
		 status = DRMS_ERROR_OUTOFMEMORY;
	      }
	      else
	      {
		 memset(intSets, 0, sizeof(char *) * kMAXRSETS);
		 memset(intSettypes, 0, sizeof(DRMS_RecordSetType_t) * kMAXRSETS);
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
			  if (strstr(pc, "prog:") == pc)
			  {
			     state = kRSParseState_DSDS;
			  }
			  else if (strstr(pc, "vot:") == pc)
			  {
			     state = kRSParseState_VOT;
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
		    state = kRSParseState_DRMSFilt;
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
		 else if (DSElem_IsWS((const char **)&pc))
		 {
		    /* whitespace between the series name and a filter is allowed */
		    if (DSElem_SkipWS(&pc))
		    {
		       if (*pc == '[')
		       {
			  *pcBuf++ = *pc++;
			  state = kRSParseState_DRMSFilt;
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
		 currSettype = kRecordSetType_DRMS;
	      }
	      break;
	    case kRSParseState_DRMSFilt:
	      /* inside '[' and ']' */
	      if (pc < endInput)
	      {
		 DSElem_SkipWS(&pc); /* ingore ws, if any */

		 if (*pc == '[')
		 {
		    state = kRSParseState_Error;
		 }
		 else if (*pc == '?')
		 {
		    state = kRSParseState_DRMSFiltSQL;
		    *pcBuf++ = *pc++;
		 }
		 else if (*pc == ']')
		 {
		    *pcBuf++ = *pc++;
		    if (DSElem_SkipWS(&pc))
		    {
		       if (*pc == '[')
		       {
			  *pcBuf++ = *pc++;
			  state = kRSParseState_DRMSFilt;
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
		    *pcBuf++ = *pc++;
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
	      break;
	    case kRSParseState_DRMSFiltSQL:
	      if (pc < endInput)
	      {
		 if (*pc == '?')
		 {
		    *pcBuf++ = *pc++;
		    if (DSElem_SkipWS(&pc) && (*pc == ']'))
		    {
		       *pcBuf++ = *pc;
		       state = kRSParseState_DRMSFilt;
		    }
		    else
		    {
		       state = kRSParseState_Error;
		    }
		 }
		 else
		 {
		    /* whitespace okay in sql query 
		     *   see drms_names.c */
		    *pcBuf++ = *pc++;
		 }
	      }
	      else
	      {
		 /* didn't finish query */
		 state = kRSParseState_Error;
	      }
	      break;
	    case kRSParseState_DRMSSeglist:
	      /* first char after '{' */
	      if (pc < endInput)
	      {
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
		 currSettype = kRecordSetType_DSDS;
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
		 char **queriesAtFile = NULL;
		 DRMS_RecordSetType_t *typesAtFile = NULL;
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
		    multiRSQueries = (char **)malloc(sizeof(char *) * kMAXRSETS);
		    multiRSTypes = 
		      (DRMS_RecordSetType_t *)malloc(sizeof(DRMS_RecordSetType_t) * kMAXRSETS);

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

				/* skip empty lines*/
				if (len > 1)
				{
				   if (lineBuf[len - 1] == '\n')
				   {
				      lineBuf[len - 1] = '\0';
				   }
			
				   status = ParseRecSetDesc(lineBuf, 
							    &queriesAtFile, 
							    &typesAtFile, 
							    &nsetsAtFile);
				   if (status == DRMS_SUCCESS)
				   {
				      /* add all nsetsAtFile recordsets to multiRSQueries */
				      for (iSet = 0; iSet < nsetsAtFile; iSet++)
				      {
					 multiRSQueries[countMultiRS] = 
					   strdup(queriesAtFile[iSet]);
					 multiRSTypes[countMultiRS] = typesAtFile[iSet];
					 countMultiRS++;
				      }
				   }
				   else
				   {
				      state = kRSParseState_Error;
				      break;
				   }

				   FreeRecSetDescArr(&queriesAtFile, &typesAtFile, nsetsAtFile);
				}
			     }
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
	      pcBuf = buf;

	      if (!multiRSQueries)
	      {
		 intSets[count] = strdup(buf);
		 intSettypes[count] = currSettype;
		 count++;
	      }
	      else
	      {
		 int iSet;
		 for (iSet = 0; iSet < countMultiRS; iSet++)
		 {
		    intSets[count] = multiRSQueries[iSet];
		    intSettypes[count] = multiRSTypes[iSet];
		    count++;
		 }

		 free(multiRSQueries); /* don't deep-free; intSets now owns strings */
		 multiRSQueries = NULL;
		 free(multiRSTypes);
		 multiRSTypes = NULL;
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

      if (*sets && *settypes)
      {
	 *nsets = count;
	 memcpy(*sets, intSets, sizeof(char *) * count);
	 memcpy(*settypes, intSettypes, sizeof(DRMS_RecordSetType_t) * count);
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

   return status;
}

int FreeRecSetDescArr(char ***sets, DRMS_RecordSetType_t **types, int nsets)
{
   int error = 0;

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
      XASSERT(jsocversion = malloc(strlen(qres->field[0][0])+1));
      strcpy(jsocversion, qres->field[0][0]);
    }
  }
  db_free_text_result(qres);
  return jsocversion;
}

int drms_recordset_getssnrecs(DRMS_RecordSet_t *set, unsigned int setnum, int *status)
{
   const DRMS_Record_t *start = NULL;
   const DRMS_Record_t *end = NULL;
   int stat = DRMS_SUCCESS;
   int res = 0;

   if (setnum >= set->ss_n)
   {
      stat = DRMS_RANGE;
   }
   else 
   {
      start = set->ss_starts[setnum];      

      if (setnum == set->ss_n - 1)
      {
	 end = set->records[set->n - 1];
	 res = end - start + 1;
      }
      else
      {
	 end = set->ss_starts[setnum + 1];
	 res = end - start;
      }
   }

   if (status)
   {
      *status = stat;
   }

   return res;
}

/* Export */
static int CallExportToFile(DRMS_Segment_t *segout, 
			    DRMS_Segment_t *segin, 
			    const char *clname,
			    const char *mapfile,
			    unsigned long long *szout)
{
   int status = DRMS_SUCCESS;
   char fileout[DRMS_MAXPATHLEN];
   char filein[DRMS_MAXPATHLEN];
   char *basename = NULL;
   unsigned long long size = 0;
   struct stat filestat;

   if (segout)
   {
      drms_segment_filename(segin, filein); /* input seg file name */
      if (!stat(filein, &filestat))
      {
	 size = filestat.st_size;
	 basename = rindex(filein, '/');
	 if (basename) 
	 {
	    basename++;
	 }
	 else 
	 {
	    basename = filein;
	 }

	 CHECKSNPRINTF(snprintf(segout->filename, DRMS_MAXSEGFILENAME, "%s", basename), DRMS_MAXSEGFILENAME);
	 drms_segment_filename(segout, fileout);

	 status = drms_segment_mapexport_tofile(segin, clname, mapfile, fileout);
      }
      else
      {
	 fprintf(stderr, "Unable to open source file '%s'.\n", filein);
	 status = DRMS_ERROR_EXPORT;
      }
   }

   *szout = 0;
   if (status == DRMS_SUCCESS)
   {
      *szout = size;
   }

   return status;
}

/* recout is the export series' record to which the exported data will be saved. 
 * reqid is the primary key in the export series 
 *
 */
int drms_record_export(DRMS_Record_t *recout,
		       DRMS_Record_t *recin,
		       int *status)
{
   return drms_record_mapexport(recout,
				recin,
				NULL,
				NULL,
				status);
}

/* recout is the export series' record to which the exported data will be saved. */
int drms_record_mapexport(DRMS_Record_t *recout,
			  DRMS_Record_t *recin,
			  const char *classname, 
			  const char *mapfile,
			  int *status)
{
   int stat = DRMS_SUCCESS;
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
      /* segments not specified - do them all */
      hit = hiter_create(&(recin->segments));
      while ((segin = hiter_getnext(hit)) != NULL)
      {
	 size = 0;
	 stat = CallExportToFile(segout, segin, classname, mapfile, &size);
	 if (stat != DRMS_SUCCESS)
	 {
	    fprintf(stderr, "Failure exporting segment '%s'.\n", segin->info->name);
	    break;
	 }
	 else
	 {
	    tsize += size;
	 }
      }

      hiter_destroy(&hit);
   }
   else
   {
      fprintf(stderr, "Export series contains no segment!\n");
      stat = DRMS_ERROR_EXPORT;
   }

   if (status)
   {
      *status = stat;
   }

   return tsize;
}			  

int drms_recordset_export(DRMS_Env_t *env,
			  long long reqid,
			  int *status)
{
   return drms_recordset_mapexport(env,
				   reqid, 
				   NULL, 
				   NULL, 
				   status);
}

int drms_recordset_mapexport(DRMS_Env_t *env,
			     long long reqid,
			     const char *classname, 
			     const char *mapfile,
			     int *status)
{
   int stat;
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

   snprintf(rsoutquery, sizeof(rsoutquery), "%s[%lld]", kEXPORTSERIES, reqid);

   rs = drms_open_records(env, rsoutquery, &stat);

   if (rs)
   {
      /* XXX Change to DRMS_REPLACE_SEGMENTS (which isn't implemented yet). */
      rsout = drms_clone_records(rs, DRMS_PERMANENT, DRMS_COPY_SEGMENTS, &stat);
      drms_close_records(rs, DRMS_FREE_RECORD);
   }
   
   if (rsout && rsout->n == 1)
   {
      recout = rsout->records[0];
      rsinquery = drms_getkey_string(recout, gExpStr[kExport_Request].str, &stat);
      
      if (rsinquery && (rsin = drms_open_records(env, rsinquery, &stat)))
      {
	 nSets = rsin->ss_n;

	 for (iSet = 0; stat == DRMS_SUCCESS && iSet < nSets; iSet++)
	 {
	    /* Perhaps we will need this in the future?
	     * request = rsin->ss_queries[iSet];
	     */
	    nRecs = drms_recordset_getssnrecs(rsin, iSet, &stat);

	    for (iRec = 0; stat == DRMS_SUCCESS && iRec < nRecs; iRec++)
	    {
	       recin = (rsin->ss_starts)[iSet] + iRec;
	       tsize += drms_record_mapexport(recout, 
					      recin,
					      classname, 
					      mapfile, 
					      &stat);
	    }
	 }
	 
	 if (stat != DRMS_SUCCESS)
	 {
	    fprintf(stderr, "Export halted due to DRMS failure.\n");
	 }
      }
      else
      {
	 fprintf(stderr, 
		 "Export series keyword '%s' did not contain a valid recordset query.\n", 
		 gExpStr[kExport_Request].str);
	 stat = DRMS_ERROR_INVALIDDATA; 
      }
   }
   else
   {
      fprintf(stderr, "Could not open export destination record set with query '%lld'.\n", reqid);
      stat = DRMS_ERROR_INVALIDDATA;
   }

   /* Set output record keywords. */
   if (stat == DRMS_SUCCESS)
   {
      drms_setkey_time(recout, gExpStr[kExport_ExpTime].str, CURRENT_SYSTEM_TIME);
      drms_setkey_int(recout, gExpStr[kExport_DataSize].str, tsize);
      drms_setkey_int(recout, gExpStr[kExport_Status].str, 1);
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
      *status = stat;
   }
   
   return tsize;
}
